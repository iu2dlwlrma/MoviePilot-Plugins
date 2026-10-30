"""刷流自身的持久分页状态和本地请求预算；不更改宿主站点限流。"""
from copy import deepcopy
import hashlib
import json
from urllib.parse import parse_qs, urlsplit


REQUEST_INTERVAL = 30
REQUEST_WINDOW = 24 * 60 * 60
REQUEST_LIMIT = 600


def load_source_scan(value, identity):
    if not isinstance(value, dict) or value.get('source_identity') != identity:
        return {'schema': 1, 'source_identity': identity, 'active': False,
                'head': {'page': 0, 'pending': False, 'processed_ids': [], 'retry_at': 0},
                'tail': {'page': 1, 'pending': False, 'processed_ids': [],
                         'retry_at': 0, 'previous_fingerprint': None}}
    return deepcopy(value)


def request_scan_head(scan):
    scan['active'] = True
    scan['request_seq'] = scan.get('request_seq', 0) + 1
    if not scan['head']['pending']:
        scan['head'] = {'page': 0, 'pending': True, 'processed_ids': [], 'retry_at': 0,
                        'start_tail': not scan['tail']['pending']}


def scan_pending(scan):
    return bool(scan and scan.get('active') and
                any(scan.get(lane, {}).get('pending') for lane in ('head', 'tail')))


def ready_scan_lane(scan, now):
    if scan_pending(scan):
        for name in ('head', 'tail'):
            lane = scan[name]
            if lane['pending'] and lane.get('retry_at', 0) <= now:
                return name
    return None


def pause_source_scan(scan, request_seq):
    if scan.get('request_seq', 0) == request_seq:
        scan['active'] = False


def defer_scan_lane(scan, lane, retry_at):
    scan[lane]['retry_at'] = retry_at


def mark_scan_processed(scan, lane, ids):
    scan[lane]['processed_ids'] = sorted(set(scan[lane]['processed_ids']).union(ids))


def finish_scan_page(scan, lane, fingerprint, has_more, rss=False):
    current = scan[lane]
    current.update(processed_ids=[], retry_at=0)
    if lane == 'head':
        current['pending'] = False
        if current.get('start_tail') and not scan['tail']['pending']:
            scan['tail'] = {'page': 1, 'pending': bool(fingerprint) and has_more is not False and not rss,
                            'processed_ids': [], 'retry_at': 0,
                            'previous_fingerprint': fingerprint}
    elif not fingerprint or has_more is False or fingerprint == current.get('previous_fingerprint'):
        current['pending'] = False
    else:
        current.update(page=current['page'] + 1, previous_fingerprint=fingerprint)
    if not any(scan[name]['pending'] for name in ('head', 'tail')):
        scan['active'] = False


def torrent_identity(torrent):
    """只持久化摘要；URL 的鉴权查询参数不参与稳定标识。"""
    for attr in ('page_url', 'enclosure'):
        value = getattr(torrent, attr, None)
        if not value:
            continue
        url = urlsplit(str(value))
        query = parse_qs(url.query)
        ids = [(key, query[key]) for key in ('id', 'tid', 'torrentid', 'torrent_id') if key in query]
        if ids or (url.path and url.path not in ('/', '/download', '/download.php')):
            identity = [url.hostname, url.path, ids]
            break
    else:
        identity = [getattr(torrent, name, None) for name in ('title', 'size', 'pubdate')]
    return hashlib.sha256(json.dumps(identity, sort_keys=True, default=str).encode()).hexdigest()


def page_fingerprint(torrents):
    ids = sorted({torrent_identity(torrent) for torrent in torrents})
    return hashlib.sha256(''.join(ids).encode()).hexdigest() if ids else None


def request_budget(value, now):
    """返回清理后的预算、下次时间和等待原因；预约由调用者先持久化。"""
    state = dict(value or {})
    state['requests'] = sorted(float(stamp) for stamp in state.get('requests', [])
                               if float(stamp) > now - REQUEST_WINDOW)
    requests = state['requests']
    if state.get('retry_at', 0) > now:
        return state, state['retry_at'], '站点限流退避等待'
    if len(requests) >= REQUEST_LIMIT:
        return state, max(requests[-1] + REQUEST_INTERVAL,
                          requests[-REQUEST_LIMIT] + REQUEST_WINDOW), '刷流滚动24小时请求预算已用完'
    if requests and requests[-1] + REQUEST_INTERVAL > now:
        return state, requests[-1] + REQUEST_INTERVAL, '刷流同站请求间隔等待'
    return state, 0, None
