"""
Umbra Open Data S3 クローラー
79タスクのGECシーン一覧を収集し、各タスクの最新シーンのフットプリント(4隅GPS)を取得してJSONに保存。

JSON構造:
{
  "generated": "ISO8601",
  "total_scenes": 6113,
  "tasks": [
    {
      "name": "Kourou, French Guiana",
      "count": 198,
      "latest_corners": [[lat,lng],[lat,lng],[lat,lng],[lat,lng]],  // TL,TR,BR,BL
      "scenes": [
        {"key": "sar-data/tasks/.../xxx_GEC.tif", "date": "2025-10-24", "sat": "UMBRA-05"}
      ]
    }
  ]
}

外部依存: なし (stdlib + struct のみ)
"""
import urllib.parse
import xml.etree.ElementTree as ET
import json, re, struct, math
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import urllib3, os as _os
SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'UmbraFetcher/1.0'})
if _os.environ.get('UMBRA_NO_VERIFY', ''):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    SESSION.verify = False

BASE = "https://umbra-open-data-catalog.s3.us-west-2.amazonaws.com/"
NS   = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}

# ── UTM → WGS84 自前実装 ──────────────────────────────────────────────────────
def utm_to_latlon(easting, northing, zone, south=False):
    a  = 6378137.0
    f  = 1 / 298.257223563
    b  = a * (1 - f)
    e2 = 1 - (b/a)**2
    ep2 = e2 / (1 - e2)
    k0 = 0.9996
    x  = easting - 500000.0
    y  = northing - (10000000.0 if south else 0.0)
    M  = y / k0
    mu = M / (a * (1 - e2/4 - 3*e2**2/64 - 5*e2**3/256))
    e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))
    phi1 = mu \
        + (3*e1/2 - 27*e1**3/32) * math.sin(2*mu) \
        + (21*e1**2/16 - 55*e1**4/32) * math.sin(4*mu) \
        + (151*e1**3/96) * math.sin(6*mu) \
        + (1097*e1**4/512) * math.sin(8*mu)
    sp = math.sin(phi1); cp = math.cos(phi1); tp = math.tan(phi1)
    N1 = a / math.sqrt(1 - e2*sp**2)
    T1 = tp**2
    C1 = ep2 * cp**2
    R1 = a*(1-e2) / (1 - e2*sp**2)**1.5
    D  = x / (N1*k0)
    lat = phi1 \
        - (N1*tp/R1) * (D**2/2
            - (5 + 3*T1 + 10*C1 - 4*C1**2 - 9*ep2)*D**4/24
            + (61 + 90*T1 + 298*C1 + 45*T1**2 - 252*ep2 - 3*C1**2)*D**6/720)
    lon0 = math.radians((zone-1)*6 - 180 + 3)
    lon  = lon0 + (D
            - (1 + 2*T1 + C1)*D**3/6
            + (5 - 2*C1 + 28*T1 - 3*C1**2 + 8*ep2 + 24*T1**2)*D**5/120) / cp
    return round(math.degrees(lat), 6), round(math.degrees(lon), 6)

# ── TIFFヘッダー読み込み ──────────────────────────────────────────────────────
def range_get(url, start, length):
    r = SESSION.get(url, headers={'Range': f'bytes={start}-{start+length-1}'}, timeout=15)
    r.raise_for_status()
    return r.content

def read_tiff_corners(url):
    """GEC GeoTIFFの先頭を読んでフットプリント4隅[TL,TR,BR,BL]を返す。失敗時はNone。"""
    try:
        hdr = range_get(url, 0, 65536)
        bo  = hdr[:2]
        if bo not in (b'II', b'MM'):
            return None
        end = '<' if bo == b'II' else '>'

        magic = struct.unpack_from(end+'H', hdr, 2)[0]
        if magic == 42:
            ifd_off    = struct.unpack_from(end+'I', hdr, 4)[0]
            n_entry    = struct.unpack_from(end+'H', hdr, ifd_off)[0]
            entry_size = 12
            cnt_fmt    = end+'I'
            off_fmt    = end+'I'
            entry_base = ifd_off + 2
        elif magic == 43:
            ifd_off    = struct.unpack_from(end+'Q', hdr, 8)[0]
            n_entry    = struct.unpack_from(end+'Q', hdr, ifd_off)[0]
            entry_size = 20
            cnt_fmt    = end+'Q'
            off_fmt    = end+'Q'
            entry_base = ifd_off + 8
        else:
            return None

        tags = {}
        for i in range(n_entry):
            base = entry_base + i * entry_size
            tag  = struct.unpack_from(end+'H', hdr, base)[0]
            typ  = struct.unpack_from(end+'H', hdr, base+2)[0]
            cnt  = struct.unpack_from(cnt_fmt, hdr, base+4)[0]
            voff = base + 4 + struct.calcsize(cnt_fmt)
            tags[tag] = (typ, cnt, voff)

        def get_int(tag):
            if tag not in tags: return None
            typ, cnt, voff = tags[tag]
            fmt = end + ('H' if typ==3 else 'I' if typ==4 else 'Q')
            return struct.unpack_from(fmt, hdr, voff)[0]

        W = get_int(256)
        H = get_int(257)
        if W is None or H is None:
            return None

        # ModelTransformation(34264): 16 doubles
        if 34264 not in tags:
            return None
        _, cnt_mt, voff_mt = tags[34264]
        if cnt_mt != 16:
            return None
        data_size = 128  # 16 * 8
        inline_max = 8 if magic == 43 else 4
        if data_size > inline_max:
            off_mt = struct.unpack_from(off_fmt, hdr, voff_mt)[0]
            mt_data = hdr[off_mt:off_mt+data_size] if off_mt+data_size <= len(hdr) \
                      else range_get(url, off_mt, data_size)
        else:
            mt_data = hdr[voff_mt:voff_mt+data_size]
        M = struct.unpack_from(end+'16d', mt_data)

        # GeoKeyDirectory(34735) → 座標系判定
        if 34735 not in tags:
            return None
        _, cnt_gk, voff_gk = tags[34735]
        kd_size = cnt_gk * 2
        if kd_size > inline_max:
            off_gk = struct.unpack_from(off_fmt, hdr, voff_gk)[0]
            kd_raw = hdr[off_gk:off_gk+kd_size] if off_gk+kd_size <= len(hdr) \
                     else range_get(url, off_gk, kd_size)
        else:
            kd_raw = hdr[voff_gk:voff_gk+kd_size]
        ka = struct.unpack_from(end + f'{cnt_gk}H', kd_raw)

        model_type, epsg = None, None
        for j in range(1, ka[3]+1):
            kid = ka[j*4]
            if   kid == 1024: model_type = ka[j*4+3]
            elif kid == 3072: epsg       = ka[j*4+3]   # Projected (UTM)
            elif kid == 2048 and epsg is None:
                              epsg       = ka[j*4+3]   # Geographic

        def px2xy(col, row):
            return (M[0]*col + M[1]*row + M[3],
                    M[4]*col + M[5]*row + M[7])

        pts = [px2xy(0,0), px2xy(W,0), px2xy(W,H), px2xy(0,H)]  # TL,TR,BR,BL

        if model_type == 2 or epsg == 4326:
            # Geographic (WGS84): x=lon, y=lat
            return [[round(y,6), round(x,6)] for x,y in pts]
        elif epsg and 32601 <= epsg <= 32660:
            return [list(utm_to_latlon(x, y, epsg-32600, False)) for x,y in pts]
        elif epsg and 32701 <= epsg <= 32760:
            return [list(utm_to_latlon(x, y, epsg-32700, True)) for x,y in pts]
        return None

    except Exception:
        return None

# ── S3 ヘルパー ────────────────────────────────────────────────────────────────
def s3_list_prefixes(prefix):
    result, token = [], None
    while True:
        url = (BASE + '?list-type=2'
               + '&prefix=' + urllib.parse.quote(prefix, safe='/')
               + '&delimiter=/'
               + '&max-keys=1000'
               + (('&continuation-token=' + urllib.parse.quote(token, safe='')) if token else ''))
        root = ET.fromstring(SESSION.get(url, timeout=15).content)
        result += [p.text for p in root.findall('.//s3:CommonPrefixes/s3:Prefix', NS)]
        trunc = root.find('s3:IsTruncated', NS)
        if trunc is not None and trunc.text == 'true':
            t = root.find('s3:NextContinuationToken', NS)
            new_token = t.text if (t is not None and t.text) else None
            if not new_token or new_token == token:
                break  # トークンが取れない/進まない＝無限ループ防止
            token = new_token
        else:
            break
    return result

def s3_list_gec_keys(prefix):
    result, token = [], None
    while True:
        url = (BASE + '?list-type=2'
               + '&prefix=' + urllib.parse.quote(prefix, safe='/')
               + '&max-keys=1000'
               + (('&continuation-token=' + urllib.parse.quote(token, safe='')) if token else ''))
        root = ET.fromstring(SESSION.get(url, timeout=15).content)
        result += [k.text for k in root.findall('.//s3:Key', NS)
                   if k.text and k.text.endswith('_GEC.tif')]
        trunc = root.find('s3:IsTruncated', NS)
        if trunc is not None and trunc.text == 'true':
            t = root.find('s3:NextContinuationToken', NS)
            new_token = t.text if (t is not None and t.text) else None
            if not new_token or new_token == token:
                break  # トークンが取れない/進まない＝無限ループ防止
            token = new_token
        else:
            break
    return result

def parse_key(key):
    fn = key.split('/')[-1]
    m  = re.match(r'^(\d{4}-\d{2}-\d{2})-\d{2}-\d{2}-\d{2}_(UMBRA-\d+)_GEC\.tif$', fn)
    return {'key': key, 'date': m.group(1), 'sat': m.group(2)} if m else None

# ── タスク処理（最新シーンのみ Range GET）────────────────────────────────────
def fetch_task(task_prefix, prev_corners=None):
    name   = task_prefix.replace('sar-data/tasks/', '').rstrip('/')
    keys   = s3_list_gec_keys(task_prefix)
    scenes = sorted(filter(None, (parse_key(k) for k in keys)),
                    key=lambda s: s['date'])
    if not scenes:
        return {'name': name, 'count': 0, 'latest_corners': None, 'scenes': []}

    # 最新シーンの corners だけ取得（キャッシュ再利用）
    latest_key = scenes[-1]['key']
    if prev_corners is not None:
        corners = prev_corners
    else:
        tif_url = BASE + urllib.parse.quote(latest_key, safe='/')
        corners = read_tiff_corners(tif_url)

    return {
        'name':           name,
        'count':          len(scenes),
        'latest_corners': corners,
        'scenes':         scenes,
    }

# ── メイン ────────────────────────────────────────────────────────────────────
def main():
    import os, sys

    out_path  = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'umbra_scenes.json')
    prev_data = {}
    if os.path.exists(out_path):
        with open(out_path, encoding='utf-8') as f:
            old = json.load(f)
        # 前回の latest_corners をキャッシュ（同じ最新シーンなら再利用）
        for t in old.get('tasks', []):
            if t.get('scenes') and t.get('latest_corners'):
                prev_data[t['scenes'][-1]['key']] = t['latest_corners']
        print(f'前回JSON読み込み: {len(prev_data)}件のcornersキャッシュ')

    print('S3タスク一覧を取得中...')
    task_prefixes = s3_list_prefixes('sar-data/tasks/')
    print(f'{len(task_prefixes)}タスク発見\n')

    tasks, done = [], 0
    workers   = int(os.environ.get('UMBRA_WORKERS', '24'))
    # 全体タイムアウト(保険)。既定1800秒。真因は列挙の無限ループ(下のガードで解消)
    # だが、想定外のハングに備えて上限は必ず設ける。
    timeout_s = int(os.environ.get('UMBRA_TIMEOUT', '1800'))

    from concurrent.futures import TimeoutError as FutTimeoutError
    ex = ThreadPoolExecutor(max_workers=workers)
    futs = {ex.submit(fetch_task, tp): tp for tp in task_prefixes}
    timed_out = False
    try:
        for fut in as_completed(futs, timeout=timeout_s):
            try:
                result = fut.result(timeout=5)
            except Exception as e:
                name = futs[fut].replace('sar-data/tasks/', '').rstrip('/')
                print(f'  ERR {name}: {e}', file=sys.stderr)
                continue
            tasks.append(result)
            done += 1
            c = 'ok' if result['latest_corners'] else 'NG'
            print(f'  [{done:2d}/{len(task_prefixes)}] {result["name"]} '
                  f'({result["count"]}件, corners:{c})')
    except FutTimeoutError:
        timed_out = True
        print(f'\n警告: タイムアウト。{done}/{len(task_prefixes)}タスクで打ち切り', file=sys.stderr)
    ex.shutdown(wait=False, cancel_futures=True)

    tasks.sort(key=lambda t: t['name'])
    total = sum(t['count'] for t in tasks)
    out = {
        'generated':    datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'total_scenes': total,
        'tasks':        tasks,
    }
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, separators=(',', ':'))
    print(f'\n完了: {total}シーン / {len(tasks)}タスク → {out_path}')
    if timed_out:
        os._exit(0)

if __name__ == '__main__':
    main()
