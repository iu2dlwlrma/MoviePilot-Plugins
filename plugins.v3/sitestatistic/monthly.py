"""Calendar-month traffic from observed daily snapshots, with no extrapolation."""
import math
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo


PB_STEP = 1024 ** 5 / 1000


def identity(data):
    """Names are presentation only; an account belongs to a domain and userid."""
    return str(getattr(data, 'domain', '') or ''), str(getattr(data, 'userid', '') or '')


def valid_traffic(data):
    if not data or getattr(data, 'err_msg', None):
        return False
    for field in ('upload', 'download'):
        value = getattr(data, field, None)
        if value is None or isinstance(value, bool):
            return False
        try:
            number = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError, OverflowError):
            return False
        if not number.is_finite() or number < 0:
            return False
    return True


def _bytes(value):
    # Keep integer counters exact beyond IEEE-754's 2**53 limit. Float values
    # have already lost source precision; never pretend to recover those bytes.
    return int(value) if isinstance(value, (int, float)) else int(Decimal(str(value)))


def _snapshot_order(data):
    value = getattr(data, 'updated_time', None)
    try:
        if isinstance(value, datetime):
            updated = value
        else:
            text = str(value)
            # MoviePilot stores updated_day and HH:MM:SS separately. Accept
            # full ISO values too, for compatible hosts and imported history.
            if len(text) >= 8 and text[2:3] == ':' and text[5:6] == ':':
                text = f"{data.updated_day}T{text}"
            updated = datetime.fromisoformat(text)
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=ZoneInfo('UTC'))
        timestamp = updated.timestamp()
    except (ValueError, TypeError, OverflowError):
        timestamp = float('-inf')
    try:
        row_id = int(getattr(data, 'id', 0) or 0)
    except (TypeError, ValueError, OverflowError):
        row_id = 0
    return timestamp, row_id


def latest_by_domain(snapshots, active_domains, day=None):
    """Select a domain's latest valid reading before matching any account.

    An explicit day also rejects records from other dates. Without one, use
    the latest observed date, then update time and record ID, as the host's
    latest-date query may return several rows for the same domain and day.
    """
    latest = {}
    for data in snapshots or []:
        domain, _ = identity(data)
        observed_day = getattr(data, 'updated_day', None)
        if domain not in active_domains or not valid_traffic(data) or not observed_day:
            continue
        if day is not None and observed_day != day:
            continue
        key = observed_day, _snapshot_order(data)
        if domain not in latest or key > (latest[domain].updated_day, _snapshot_order(latest[domain])):
            latest[domain] = data
    return latest


def approximate_fields(previous, current):
    """Only annotate observed PB quantization; do not reconstruct missing bytes."""
    if not valid_traffic(previous) or not valid_traffic(current):
        return set()
    if identity(previous) != identity(current) or previous.updated_day == current.updated_day:
        return set()
    result = set()
    for field in ('upload', 'download'):
        values = [float(getattr(row, field)) for row in (previous, current)]
        if values[1] < values[0]:
            continue
        if all(value >= 1000 * 1024 ** 4
               and abs(value - round(value / PB_STEP) * PB_STEP) <= max(1, 2 * math.ulp(value))
               for value in values):
            result.add(field)
    return result


def collect_month(site_oper, timezone_name, now=None):
    """Read at most 32 dates: prior month end, then this month's observed days.

    A failed/invalid reading cannot create a zero baseline. Counter resets and
    account changes invalidate comparison for that domain for the whole month.
    """
    tz = ZoneInfo(timezone_name)
    local_now = (now or datetime.now(tz)).astimezone(tz)
    today = local_now.date()
    first_day = today.replace(day=1)
    baseline_day = first_day - timedelta(days=1)
    active = {str(site.domain): getattr(site, 'name', None) or str(site.domain)
              for site in site_oper.list_active()}
    observations = {domain: [] for domain in active}
    query_errors = []
    day = baseline_day
    while day <= today:
        date_string = day.isoformat()
        try:
            snapshots = site_oper.get_userdata_by_date(date_string) or []
        except Exception:
            snapshots = []
            query_errors.append(date_string)
        daily_latest = latest_by_domain(snapshots, active, date_string)
        for domain, data in daily_latest.items():
            observations[domain].append(data)
        day += timedelta(days=1)

    rows = []
    for domain, name in active.items():
        readings = observations[domain]
        month_readings = [data for data in readings if data.updated_day >= first_day.isoformat()]
        row = {'domain': domain, 'name': name, 'userid': '', 'start': None, 'end': None,
               'upload': None, 'download': None, 'approximate': set(), 'partial': True,
               'status': '暂无本月有效快照'}
        if month_readings:
            latest = month_readings[-1]
            row.update(name=getattr(latest, 'name', None) or name,
                       userid=identity(latest)[1], end=latest.updated_day)
            accounts = {identity(data)[1] for data in readings}
            if '' in accounts:
                row['status'] = '无法比较：缺少账号标识'
            elif len(accounts) > 1:
                row['status'] = '无法比较：账号变化'
            elif any(_bytes(getattr(current, field)) < _bytes(getattr(previous, field))
                     for previous, current in zip(readings, readings[1:])
                     for field in ('upload', 'download')):
                row['status'] = '无法比较：月内计数下降'
            else:
                baseline = next((data for data in readings if data.updated_day == baseline_day.isoformat()), None)
                previous = baseline or month_readings[0]
                row['start'] = previous.updated_day
                if previous.updated_day == latest.updated_day:
                    row['status'] = '缺月初基线；仅一份快照，不足以计算'
                else:
                    row.update(upload=_bytes(latest.upload) - _bytes(previous.upload),
                               download=_bytes(latest.download) - _bytes(previous.download),
                               approximate=approximate_fields(previous, latest),
                               partial=baseline is None,
                               status='缺月初基线；部分统计' if baseline is None else '已覆盖月初基线')
        if query_errors:
            row['partial'] = True
            row['status'] += '；部分日期读取失败'
        rows.append(row)

    comparable = [row for row in rows if row['upload'] is not None]
    return {
        'month': first_day.strftime('%Y-%m'), 'today': today.isoformat(),
        'start': min((row['start'] for row in comparable), default=None),
        'end': max((row['end'] for row in comparable), default=None),
        'upload': sum(row['upload'] for row in comparable) if comparable else None,
        'download': sum(row['download'] for row in comparable) if comparable else None,
        'covered': len(comparable), 'total': len(rows),
        'partial': bool(query_errors) or not rows or any(row['partial'] for row in rows),
        'approximate': set().union(*(row['approximate'] for row in comparable)),
        'rows': rows, 'query_errors': query_errors,
    }
