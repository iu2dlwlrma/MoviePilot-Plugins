import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


def _count(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None and number.is_integer() else None


def publication_age_minutes(
    pubdate: Any,
    timezone_offset: float,
    timezone_name: str,
    now: datetime | None = None,
) -> float | None:
    """显式时区按实际时刻计算；手动偏移仅修正无时区发布时间。"""
    if not pubdate:
        return None
    try:
        published = pubdate if isinstance(pubdate, datetime) else datetime.fromisoformat(str(pubdate).strip())
        if published.tzinfo is None or published.utcoffset() is None:
            local_zone = timezone.utc if timezone_name == "UTC" else ZoneInfo(timezone_name)
            offset = float(timezone_offset)
            if not math.isfinite(offset):
                return None
            published = (published + timedelta(hours=offset)).replace(tzinfo=local_zone)
        current = now if now is not None else datetime.now(timezone.utc)
        if current.tzinfo is None or current.utcoffset() is None:
            return None
        return (current.astimezone(timezone.utc) - published.astimezone(timezone.utc)).total_seconds() / 60
    except (TypeError, ValueError, OverflowError, ZoneInfoNotFoundError):
        return None


def demand_rank(peers: Any, seeders: Any, age_minutes: float | None) -> tuple[float, float]:
    """以需求人数和竞争程度排序，需求得分每六小时衰减一半。"""
    age = _number(age_minutes)
    if age is None:
        return 0.0, -math.inf
    peer_count, seed_count = _count(peers), _count(seeders)
    if peer_count is None or seed_count is None:
        return 0.0, -age
    demand = math.log1p(peer_count) * (peer_count / (peer_count + seed_count + 1))
    freshness = math.exp(-math.log(2) * age / 360)
    return demand * freshness, -age


def evaluate_peer_demand(
    peers: Any,
    seeders: Any,
    peer_range: str | None = "1",
    min_peer_seed_ratio: float | None = 0.1,
) -> tuple[bool, str | None]:
    """按下载人数和下载/做种人数比筛选，启用条件时拒绝未知值。"""
    limits = str(peer_range).strip() if peer_range is not None else ""
    peer_count = _count(peers)
    if limits:
        match = re.fullmatch(r"(\d+)(?:\s*-\s*(\d+))?", limits)
        if not match:
            return False, "下载人数筛选配置无效"
        minimum = int(match.group(1))
        maximum = int(match.group(2)) if match.group(2) is not None else None
        if maximum is not None and maximum < minimum:
            return False, "下载人数筛选配置无效"
        if peer_count is None:
            return False, "下载人数未知"
        if peer_count < minimum:
            return False, "下载人数低于下限"
        if maximum is not None and peer_count > maximum:
            return False, "下载人数不在范围内"
    if min_peer_seed_ratio is not None:
        minimum_ratio = _number(min_peer_seed_ratio)
        if minimum_ratio is None:
            return False, "下载/做种人数比配置无效"
        if minimum_ratio == 0:
            return True, None
        if peer_count is None:
            return False, "下载人数未知"
        seed_count = _count(seeders)
        if seed_count is None:
            return False, "做种人数未知"
        if peer_count / max(seed_count, 1) < minimum_ratio:
            return False, "下载/做种人数比不足"
    return True, None


def promotion_deadline(
    freedate: Any,
    timezone_offset: float,
    timezone_name: str,
    lead_minutes: float = 10,
) -> datetime | None:
    """换算促销截止并提前处理；手动小时偏移仅用于无时区时间。"""
    if not freedate:
        return None
    lead = _number(lead_minutes)
    if lead is None:
        return None
    try:
        local_zone = timezone.utc if timezone_name == "UTC" else ZoneInfo(timezone_name)
        expiry = freedate if isinstance(freedate, datetime) else datetime.fromisoformat(str(freedate).strip())
        if expiry.tzinfo is None or expiry.utcoffset() is None:
            offset = float(timezone_offset)
            if not math.isfinite(offset):
                return None
            expiry = (expiry + timedelta(hours=offset)).replace(tzinfo=local_zone)
        deadline = expiry.astimezone(timezone.utc) - timedelta(minutes=lead)
        return deadline.astimezone(local_zone)
    except (TypeError, ValueError, OverflowError, ZoneInfoNotFoundError):
        return None


def download_has_started(info: dict, record: dict) -> bool:
    """只凭持久记录、下载流量、已完成数据或明确下载状态判定开始。"""
    if record.get("download_started") is True or info.get("completed") is True:
        return True
    for field in ("downloaded", "transfer_downloaded"):
        amount = _number(info.get(field))
        if amount is not None and amount > 0:
            return True
    if info.get("is_downloading") is True:
        return True
    state = re.sub(r"[\s_-]", "", str(info.get("state") or "").lower())
    return state in {"downloading", "forceddl", "forceddownload"}
