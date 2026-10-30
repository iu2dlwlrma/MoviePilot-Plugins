"""失效媒体记录清理的隔离数据库适配器。"""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from collections.abc import Callable
from pathlib import PurePosixPath
from typing import TypeVar

from sqlalchemy import delete, exists, or_, select, update
from sqlalchemy.orm import Session

from .host_cleanup import (
    CleanupCandidate,
    CleanupProof,
    CleanupProtection,
    CleanupResult,
    proof_allows_cleanup,
    proof_allows_destination_repair,
    FileOnlyCandidate,
)
from .record_cleanup import MutationResult
from app.db.models.downloadhistory import DownloadFiles, DownloadHistory
from app.db.models.subscribe import Subscribe
from app.db.models.transferhistory import TransferHistory
from app.db.models.transferpending import TransferPending
from app.db.uow import run_sync_transaction


ResultT = TypeVar("ResultT")


def _read(operation: Callable[[Session], ResultT], session_factory: Callable[[], Session] | None) -> ResultT:
    """在调用方注入的隔离会话或组合根事务中读取。"""
    if session_factory is None:
        return run_sync_transaction(operation)
    with session_factory() as session:
        return operation(session)


def _single_number(text: str | None, prefix: str) -> int | None:
    """只接受单个完整 Sxx 或 Exx 标记，拒绝季包与范围。"""
    match = re.fullmatch(rf"{prefix}0*(\d+)", text or "", flags=re.IGNORECASE)
    return int(match.group(1)) if match else None


def _range_contains(text: str | None, prefix: str, target: int) -> bool | None:
    """解析可证明的季集范围；未知写法返回 None 而不猜测。"""
    if not text:
        return None
    found = False
    for item in re.split(r"\s*[,;|]\s*", text.strip()):
        match = re.fullmatch(
            rf"{prefix}0*(\d+)(?:\s*[-~]\s*{prefix}?0*(\d+))?",
            item,
            flags=re.IGNORECASE,
        )
        if match is None:
            return None
        start = int(match.group(1))
        end = int(match.group(2)) if match.group(2) else start
        if start > end:
            return None
        found = found or start <= target <= end
    return found


def _revision(*parts: object) -> str:
    """计算只供本次命令重读比较的不透明数据库快照版本。"""
    payload = json.dumps(parts, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _embedded_path_matches(fileitem: object, expected: str) -> bool:
    """旧文件项快照为空可兼容；存在路径时必须与行内路径一致。"""
    if fileitem in (None, {}):
        return True
    return isinstance(fileitem, dict) and fileitem.get("path") == expected


def _single_transfer_file_matches(files: object, source: str) -> bool:
    """多文件整理回执不可凭单个源与目标的缺失证据删除。"""
    return files in (None, []) or files == [source]


def _candidate(session: Session, file_id: int) -> CleanupCandidate | None:
    """在同一会话内生成单行候选及全部身份校验字段。"""
    file = session.get(DownloadFiles, file_id)
    if file is None or file.state != 1:
        return None
    source = file.fullpath or ""
    download_hash = file.download_hash or ""
    histories = list(session.execute(
        select(DownloadHistory)
        .where(DownloadHistory.download_hash == download_hash, DownloadHistory.downloader == file.downloader)
        .order_by(DownloadHistory.id.desc())
        .limit(20001)
    ).scalars()) if download_hash else []
    equivalent = len(histories) <= 20000 and len({_parent_identity(_snapshot(row, HISTORY_FIELDS)) for row in histories}) <= 1
    history = histories[0] if histories and equivalent else None
    transfers = list(session.execute(
        select(TransferHistory)
        .where(TransferHistory.src == source, TransferHistory.download_hash == download_hash,
               or_(TransferHistory.downloader == file.downloader, TransferHistory.downloader.is_(None)))
        .order_by(TransferHistory.id.desc())
        .limit(2)
    ).scalars()) if source else []
    transfer = transfers[0] if len(transfers) == 1 else None
    season = _single_number(history.seasons, "S") if history else None
    episode = _single_number(history.episodes, "E") if history else None
    if history and history.type == "电视剧" and (season is None or episode is None):
        matches = list(re.finditer(r"(?:^|[^a-z0-9])S(\d{1,3})E(\d{1,4})(?![0-9e~\-])", PurePosixPath(source).name, re.IGNORECASE))
        if len(matches) == 1:
            proposed_season, proposed_episode = map(int, matches[0].groups())
            if (_range_contains(history.seasons, "S", proposed_season) is True
                    and _range_contains(history.episodes, "E", proposed_episode) is True):
                season, episode = proposed_season, proposed_episode
    media_source = history.media_source if history else None
    media_id = history.media_id if history else None
    media_type = history.type if history else None
    subscribers = list(session.execute(
        select(Subscribe)
        .where(
            Subscribe.media_source == media_source,
            Subscribe.media_id == media_id,
            Subscribe.season == season,
            Subscribe.type == media_type,
        )
        .order_by(Subscribe.id.desc())
        .limit(2)
    ).scalars()) if media_source and media_id and season is not None else []
    subscribe = subscribers[0] if len(subscribers) == 1 else None
    reason = None
    if not source.startswith("/") or ".." in source.split("/"):
        reason = "unsafe_source_path"
    elif not download_hash or not history:
        reason = "ambiguous_download_history"
    elif not media_source or not media_id or media_type not in ("电视剧", "电影"):
        reason = "ambiguous_media_episode"
    elif media_type == "电视剧" and (season is None or episode is None):
        reason = "ambiguous_media_episode"
    elif media_type == "电影" and (history.seasons or history.episodes):
        reason = "ambiguous_media_episode"
    elif len(transfers) > 1:
        reason = "ambiguous_transfer_history"
    elif transfer and (
        transfer.download_hash != download_hash
        or transfer.media_source != media_source
        or transfer.media_id != media_id
        or transfer.type != media_type
        or (media_type == "电视剧" and _single_number(transfer.seasons, "S") != season)
        or (media_type == "电视剧" and _single_number(transfer.episodes, "E") != episode)
        or (media_type == "电影" and (transfer.seasons or transfer.episodes))
        or not transfer.dest
    ):
        reason = "transfer_identity_mismatch"
    elif transfer and (transfer.transfer_task_id or transfer.transfer_settlement_revision is not None):
        reason = "durable_transfer_task"
    elif transfer and (transfer.src_storage != "local" or transfer.dest_storage != "local"):
        reason = "nonlocal_transfer_storage"
    elif transfer and transfer.status is not True:
        reason = "failed_transfer_history"
    elif transfer and (
        not _embedded_path_matches(transfer.src_fileitem, source)
        or not _embedded_path_matches(transfer.dest_fileitem, transfer.dest)
        or not _single_transfer_file_matches(transfer.files, source)
    ):
        reason = "transfer_embedded_path_mismatch"
    elif len(subscribers) > 1:
        reason = "ambiguous_subscriptions"
    elif subscribe and subscribe.best_version:
        reason = "best_version_subscription"
    elif subscribe and subscribe.note is not None and (
        not isinstance(subscribe.note, list)
        or any(type(value) is not int or value <= 0 for value in subscribe.note)
    ):
        reason = "malformed_subscription_note"
    destination_paths = (transfer.dest,) if transfer and transfer.dest else ()
    if any(not path.startswith("/") or ".." in path.split("/") for path in destination_paths):
        reason = "unsafe_destination_path"
    identity_verified = reason is None
    retry_needed = bool(
        identity_verified and subscribe is not None and media_type == "电视剧"
        and isinstance(subscribe.note, list) and episode in subscribe.note
    )
    revision = _revision(
        (file.id, file.fullpath, file.download_hash, file.downloader, file.state),
        (history.id, history.path, history.type, history.media_source, history.media_id,
         history.seasons, history.episodes, history.download_hash) if history else None,
        (transfer.id, transfer.src, transfer.src_storage, transfer.dest, transfer.dest_storage,
         transfer.mode, transfer.status, transfer.transfer_settlement_revision,
         transfer.src_fileitem, transfer.dest_fileitem, transfer.files,
         transfer.type, transfer.media_source, transfer.media_id, transfer.seasons,
         transfer.episodes, transfer.download_hash, transfer.transfer_task_id) if transfer else None,
        (subscribe.id, subscribe.media_source, subscribe.media_id, subscribe.season,
         subscribe.type, subscribe.note, subscribe.lack_episode, subscribe.state,
         subscribe.best_version, subscribe.episode_priority, subscribe.start_episode,
         subscribe.total_episode, subscribe.activity_key) if subscribe else None,
        (len(histories), len(transfers), len(subscribers), reason),
        [_snapshot(row, HISTORY_FIELDS) for row in histories],
    )
    return CleanupCandidate(
        file_id=file.id,
        source_path=source,
        download_hash=download_hash,
        downloader=file.downloader,
        destination_paths=destination_paths,
        destination_lookup_complete=len(transfers) <= 1,
        media_source=media_source,
        media_id=media_id,
        media_type=media_type,
        season=season,
        episodes=(episode,) if episode is not None else (),
        storage=transfer.src_storage if transfer else "local",
        history_id=history.id if history else None,
        transfer_id=transfer.id if transfer else None,
        subscribe_id=subscribe.id if subscribe else None,
        subscription_retry_needed=retry_needed,
        identity_verified=identity_verified,
        revision=revision,
        review_reason=reason,
        history_ids=tuple(row.id for row in histories),
    )


def get_cleanup_candidate(
    file_id: int,
    *,
    session_factory: Callable[[], Session] | None = None,
) -> CleanupCandidate | None:
    """按下载文件主键读取精确候选，不检查任何外部路径。"""
    return _read(lambda session: _candidate(session, file_id), session_factory)


def list_cleanup_candidates(
    after_id: int = 0,
    limit: int = 100,
    *,
    session_factory: Callable[[], Session] | None = None,
) -> list[CleanupCandidate]:
    """按下载文件主键游标有界扫描，供后台任务分批观察。"""
    if after_id < 0 or not 1 <= limit <= 100:
        raise ValueError("下载文件扫描游标或批量上限无效")

    def execute(session: Session) -> list[CleanupCandidate]:
        """在一个数据库快照中读取本批主键和候选。"""
        ids = session.execute(
            select(DownloadFiles.id)
            .where(DownloadFiles.id > after_id, DownloadFiles.state == 1)
            .order_by(DownloadFiles.id)
            .limit(limit)
        ).scalars().all()
        return [candidate for file_id in ids if (candidate := _candidate(session, file_id)) is not None]

    return _read(execute, session_factory)


def _protection(
    session: Session,
    candidate: CleanupCandidate,
    limit: int,
    include_source_ownership: bool = False,
) -> CleanupProtection:
    """枚举同集路径；仅物理删除前额外核验全库源路径与 Hash 独占。"""
    current = _candidate(session, candidate.file_id)
    if current is None or current.revision != candidate.revision or not current.identity_verified:
        return CleanupProtection((), False, None)
    histories = list(session.execute(
        select(DownloadHistory)
        .where(
            DownloadHistory.media_source == current.media_source,
            DownloadHistory.media_id == current.media_id,
            DownloadHistory.type == current.media_type,
        )
        .order_by(DownloadHistory.id)
        .limit(limit + 1)
    ).scalars())
    transfers = list(session.execute(
        select(TransferHistory)
        .where(
            TransferHistory.media_source == current.media_source,
            TransferHistory.media_id == current.media_id,
            TransferHistory.type == current.media_type,
        )
        .order_by(TransferHistory.id)
        .limit(limit + 1)
    ).scalars())
    complete = len(histories) <= limit and len(transfers) <= limit
    paths: set[str] = set()
    hashes: set[str] = set()
    is_movie = current.media_type == "电影"
    target_episode = current.episodes[0] if not is_movie else None
    for history in histories[:limit]:
        if is_movie:
            if history.seasons or history.episodes:
                complete = False
                continue
        else:
            season_contains = _range_contains(history.seasons, "S", current.season)
            if season_contains is False:
                continue
            episode_contains = _range_contains(history.episodes, "E", target_episode)
            if episode_contains is False:
                continue
            if season_contains is None or episode_contains is None:
                complete = False
            elif _single_number(history.seasons, "S") is None or _single_number(history.episodes, "E") is None:
                complete = False
        if history.download_hash:
            hashes.add(history.download_hash)
        else:
            complete = False
    for transfer in transfers[:limit]:
        if is_movie:
            if transfer.seasons or transfer.episodes:
                complete = False
                continue
        else:
            season_contains = _range_contains(transfer.seasons, "S", current.season)
            if season_contains is False:
                continue
            episode_contains = _range_contains(transfer.episodes, "E", target_episode)
            if episode_contains is False:
                continue
            if season_contains is None or episode_contains is None:
                complete = False
            elif _single_number(transfer.seasons, "S") is None or _single_number(transfer.episodes, "E") is None:
                complete = False
        if transfer.id != current.transfer_id:
            if transfer.src_storage != "local" or transfer.dest_storage != "local":
                complete = False
            if transfer.src:
                paths.add(transfer.src)
            if transfer.dest:
                paths.add(transfer.dest)
            else:
                complete = False
    if hashes:
        files = list(session.execute(
            select(DownloadFiles)
            .where(DownloadFiles.download_hash.in_(hashes), DownloadFiles.state == 1)
            .order_by(DownloadFiles.id)
            .limit(limit + 1)
        ).scalars())
        if len(files) > limit:
            complete = False
        for file in files[:limit]:
            if file.id != current.file_id and file.fullpath:
                paths.add(file.fullpath)
    paths.discard(current.source_path)
    paths.difference_update(current.destination_paths)
    source = PurePosixPath(current.source_path)
    ancestors = tuple(str(path) for path in (source, *source.parents))
    escaped = current.source_path.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    pending = session.execute(
        select(TransferPending.id)
        .where(
            TransferPending.storage == current.storage,
            or_(
                TransferPending.src_path.in_(ancestors),
                TransferPending.src_path.like(f"{escaped}/%", escape="\\"),
            ),
        )
        .limit(1)
    ).scalar_one_or_none()
    if not include_source_ownership:
        return CleanupProtection(tuple(sorted(paths)), complete, pending is not None)
    other_source_file = session.execute(
        select(DownloadFiles.id)
        .where(
            DownloadFiles.id != current.file_id,
            DownloadFiles.fullpath == current.source_path,
        )
        .limit(1)
    ).scalar_one_or_none()
    other_source_transfer = session.execute(
        select(TransferHistory.id)
        .where(
            TransferHistory.id != current.transfer_id,
            or_(
                TransferHistory.src == current.source_path,
                TransferHistory.dest == current.source_path,
            ),
        )
        .limit(1)
    ).scalar_one_or_none()
    other_source_history = session.execute(
        select(DownloadHistory.id)
        .where(
            DownloadHistory.id != current.history_id,
            DownloadHistory.path == current.source_path,
        )
        .limit(1)
    ).scalar_one_or_none()
    other_hash_file = session.execute(
        select(DownloadFiles.id)
        .where(
            DownloadFiles.id != current.file_id,
            DownloadFiles.download_hash == current.download_hash,
        )
        .limit(1)
    ).scalar_one_or_none()
    other_hash_transfer = session.execute(
        select(TransferHistory.id)
        .where(
            TransferHistory.id != current.transfer_id,
            TransferHistory.download_hash == current.download_hash,
        )
        .limit(1)
    ).scalar_one_or_none()
    other_hash_history = session.execute(
        select(DownloadHistory.id)
        .where(
            DownloadHistory.id != current.history_id,
            DownloadHistory.download_hash == current.download_hash,
        )
        .limit(1)
    ).scalar_one_or_none()
    source_exclusive = (
        pending is None and other_source_file is None
        and other_source_transfer is None and other_source_history is None
    )
    hash_exclusive = (
        other_hash_file is None and other_hash_transfer is None and other_hash_history is None
    )
    return CleanupProtection(
        tuple(sorted(paths)), complete, pending is not None,
        source_exclusive=source_exclusive, hash_exclusive=hash_exclusive,
    )


def get_cleanup_protection(
    candidate: CleanupCandidate,
    *,
    limit: int = 500,
    include_source_ownership: bool = False,
    session_factory: Callable[[], Session] | None = None,
) -> CleanupProtection:
    """有界读取其他版本与任务，物理删除前按需加做源和 Hash 独占查询。"""
    if not 1 <= limit <= 500:
        raise ValueError("关联路径查询上限无效")
    return _read(
        lambda session: _protection(session, candidate, limit, include_source_ownership),
        session_factory,
    )


class CleanupConflict(RuntimeError):
    """事务内数据库事实改变，要求全部回滚并重新观察。"""


def _begin_serialized_write(session: Session) -> None:
    """SQLite 写命令先取得 RESERVED 锁，避免其忽略 FOR UPDATE 后读写穿插。"""
    if session.get_bind().dialect.name == "sqlite":
        session.connection().exec_driver_sql("BEGIN IMMEDIATE")


def _lock_related_rows(session: Session, candidate: CleanupCandidate, *, subscription: bool) -> None:
    """在重读候选前锁定父子与 JSON 快照行，随后按 revision/CAS 复核。"""
    session.execute(
        select(DownloadFiles.id)
        .where(DownloadFiles.id == candidate.file_id)
        .with_for_update()
    ).scalar_one_or_none()
    if candidate.history_id is not None:
        session.execute(
            select(DownloadHistory.id)
            .where(DownloadHistory.id == candidate.history_id)
            .with_for_update()
        ).scalar_one_or_none()
    if candidate.transfer_id is not None:
        session.execute(
            select(TransferHistory.id)
            .where(TransferHistory.id == candidate.transfer_id)
            .with_for_update()
        ).scalar_one_or_none()
    if subscription and candidate.subscribe_id is not None:
        session.execute(
            select(Subscribe.id)
            .where(Subscribe.id == candidate.subscribe_id)
            .with_for_update()
        ).scalar_one_or_none()


def _note_without_episode(note: list[object], episode: int) -> list[int] | None:
    """按数值精确移除一集，遇到非集号数据立即拒绝。"""
    result = []
    for value in note:
        if type(value) is not int or value <= 0:
            return None
        if value != episode:
            result.append(value)
    return result


def _resolve_progress(
    session: Session,
    candidate: CleanupCandidate,
    present_episodes: tuple[int, ...],
    season_lookup_complete: bool,
) -> int | None:
    """按事务内订阅快照与完整季媒体视图计算清理后的缺集数。"""
    current = _candidate(session, candidate.file_id)
    if (
        current != candidate
        or current is None
        or not current.identity_verified
        or not season_lookup_complete
        or current.subscribe_id is None
        or not current.subscription_retry_needed
        or len(current.episodes) != 1
    ):
        return None
    subscribe = session.get(Subscribe, current.subscribe_id)
    if subscribe is None or subscribe.best_version or not isinstance(subscribe.note, list):
        return None
    episode = current.episodes[0]
    note_after = _note_without_episode(subscribe.note, episode)
    start = subscribe.start_episode or 1
    total = subscribe.total_episode or 0
    if (
        note_after is None
        or note_after == subscribe.note
        or not 1 <= start <= episode <= total
        or any(type(item) is not int or item <= 0 for item in present_episodes)
        or episode in present_episodes
    ):
        return None
    target = set(range(start, total + 1))
    return len(target - (set(present_episodes) | set(note_after)))


def resolve_cleanup_lack_episode(
    candidate: CleanupCandidate,
    present_episodes: tuple[int, ...],
    *,
    season_lookup_complete: bool = True,
    session_factory: Callable[[], Session] | None = None,
) -> int | None:
    """返回严格完整季视图下的缺集数；任何身份或版本不确定返回 None。"""
    return _read(
        lambda session: _resolve_progress(session, candidate, present_episodes, season_lookup_complete),
        session_factory,
    )


def _apply(
    session: Session,
    candidate: CleanupCandidate,
    proof: CleanupProof,
    *,
    allow_subscription_retry: bool,
    resolved_lack_episode: int | None,
) -> CleanupResult:
    """在同一 Session 中重读、复核证据并暂存所有精确 CAS。"""
    allow_subscription_retry = False
    _begin_serialized_write(session)
    _lock_related_rows(session, candidate, subscription=allow_subscription_retry)
    current = _candidate(session, candidate.file_id)
    if current is None:
        return CleanupResult("not_found", "下载文件记录已失效或不存在")
    if current != candidate:
        return CleanupResult("stale", "数据库候选已变化，请重新观察")
    if current.review_reason or not current.identity_verified:
        return CleanupResult("needs_review", current.review_reason or "媒体身份无法确认")
    if (
        proof.source_path != current.source_path
        or proof.destination_paths != current.destination_paths
        or proof.destination_lookup_complete != current.destination_lookup_complete
        or len(proof.destinations) != len(current.destination_paths)
        or not proof_allows_cleanup(proof, revision=current.revision)
    ):
        return CleanupResult("needs_review", "源、目标或外部证据不完整")
    protection = _protection(session, current, 500)
    if not protection.other_versions_complete or protection.transfer_task_active is not False:
        return CleanupResult("needs_review", "同集版本或持久整理任务状态不完整")
    if proof.other_version_paths != protection.other_version_paths:
        return CleanupResult("stale", "同集其他版本路径已变化")
    subscribe = session.get(Subscribe, current.subscribe_id) if current.subscribe_id else None
    note_after = None
    sibling = session.execute(
        select(DownloadFiles.id)
        .where(DownloadFiles.download_hash == current.download_hash, DownloadFiles.id != current.file_id)
        .limit(1)
        .with_for_update()
    ).scalar_one_or_none()
    active_sibling = session.execute(
        select(DownloadFiles.id)
        .where(
            DownloadFiles.download_hash == current.download_hash,
            DownloadFiles.id != current.file_id,
            DownloadFiles.state == 1,
        )
        .limit(1)
        .with_for_update()
    ).scalar_one_or_none()
    if allow_subscription_retry and current.subscription_retry_needed and active_sibling is not None:
        return CleanupResult("needs_review", "同 Hash 仍有活动下载文件，保留订阅进度")
    retry_this_record = bool(
        allow_subscription_retry
        and current.subscription_retry_needed
        and subscribe is not None
        and not protection.other_version_paths
        and sibling is None
    )
    if retry_this_record:
        if not isinstance(subscribe.note, list):
            return CleanupResult("needs_review", "订阅 note 结构异常")
        note_after = _note_without_episode(subscribe.note, current.episodes[0])
        if note_after is None:
            return CleanupResult("needs_review", "订阅 note 含非集号数据")
        if note_after != subscribe.note:
            start = subscribe.start_episode or 1
            total = subscribe.total_episode or 0
            if (
                isinstance(resolved_lack_episode, bool)
                or not isinstance(resolved_lack_episode, int)
                or not 1 <= start <= current.episodes[0] <= total
                or not 0 <= resolved_lack_episode <= total - start + 1
                or resolved_lack_episode == 0
            ):
                return CleanupResult("needs_review", "缺少经完整季媒体库解析的缺集数")
    file_delete = session.execute(
        delete(DownloadFiles)
        .where(
            DownloadFiles.id == current.file_id,
            DownloadFiles.fullpath == current.source_path,
            DownloadFiles.download_hash == current.download_hash,
            DownloadFiles.downloader == current.downloader,
            DownloadFiles.state == 1,
        )
        .execution_options(synchronize_session=False)
    )
    if file_delete.rowcount != 1:
        raise CleanupConflict("下载文件记录已变化")
    remaining_file = session.execute(
        select(DownloadFiles.id)
        .where(DownloadFiles.download_hash == current.download_hash)
        .limit(1)
    ).scalar_one_or_none()
    if retry_this_record and remaining_file is not None:
        raise CleanupConflict("同 Hash 文件在提交期间变化")
    history_deleted = False
    if current.history_id is not None and remaining_file is None:
        history = session.get(DownloadHistory, current.history_id)
        if history is None:
            raise CleanupConflict("下载历史已变化")
        history_delete = session.execute(
            delete(DownloadHistory)
            .where(
                DownloadHistory.id == current.history_id,
                DownloadHistory.download_hash == current.download_hash,
                DownloadHistory.path == history.path,
                DownloadHistory.type == current.media_type,
                DownloadHistory.media_source == current.media_source,
                DownloadHistory.media_id == current.media_id,
                DownloadHistory.seasons == history.seasons,
                DownloadHistory.episodes == history.episodes,
            )
            .execution_options(synchronize_session=False)
        )
        if history_delete.rowcount != 1:
            raise CleanupConflict("下载历史已变化")
        history_deleted = True
    transfer_deleted = False
    if current.transfer_id is not None:
        transfer = session.get(TransferHistory, current.transfer_id)
        if transfer is None:
            raise CleanupConflict("整理历史已变化")
        transfer_delete = session.execute(
            delete(TransferHistory)
            .where(
                TransferHistory.id == current.transfer_id,
                TransferHistory.src == current.source_path,
                TransferHistory.dest == current.destination_paths[0],
                TransferHistory.download_hash == current.download_hash,
                TransferHistory.media_source == current.media_source,
                TransferHistory.media_id == current.media_id,
                TransferHistory.type == current.media_type,
                TransferHistory.seasons == transfer.seasons,
                TransferHistory.episodes == transfer.episodes,
                TransferHistory.mode == transfer.mode,
                TransferHistory.status.is_(True),
                TransferHistory.transfer_task_id.is_(None),
                TransferHistory.transfer_settlement_revision.is_(None),
            )
            .execution_options(synchronize_session=False)
        )
        if transfer_delete.rowcount != 1:
            raise CleanupConflict("整理历史已变化或存在持久任务")
        transfer_deleted = True
    note_updated = False
    if retry_this_record and subscribe is not None and note_after is not None and note_after != subscribe.note:
        note_update = session.execute(
            update(Subscribe)
            .where(
                Subscribe.id == current.subscribe_id,
                Subscribe.media_source == current.media_source,
                Subscribe.media_id == current.media_id,
                Subscribe.season == current.season,
                Subscribe.lack_episode == subscribe.lack_episode,
            )
            .values(note=note_after, lack_episode=resolved_lack_episode)
            .execution_options(synchronize_session=False)
        )
        if note_update.rowcount != 1:
            raise CleanupConflict("订阅进度已变化")
        note_updated = True
    return CleanupResult(
        "cleaned",
        file_marked_missing=True,
        history_deleted=history_deleted,
        transfer_deleted=transfer_deleted,
        note_updated=note_updated,
    )


def apply_cleanup_candidate(
    candidate: CleanupCandidate,
    proof: CleanupProof,
    *,
    allow_subscription_retry: bool = False,
    resolved_lack_episode: int | None = None,
    session_factory: Callable[[], Session] | None = None,
) -> CleanupResult:
    """原子清理单个失效下载文件及可证明独占的历史和订阅进度。"""
    def operation(session: Session) -> CleanupResult:
        """把本次数据库写入收敛在一个调用方拥有的事务中。"""
        return _apply(
            session, candidate, proof,
            allow_subscription_retry=allow_subscription_retry,
            resolved_lack_episode=resolved_lack_episode,
        )

    try:
        if session_factory is None:
            return run_sync_transaction(operation)
        with session_factory() as session:
            try:
                result = operation(session)
                session.commit()
                return result
            except Exception:
                session.rollback()
                raise
    except CleanupConflict as error:
        return CleanupResult("stale", str(error))


def _repair_destination(session: Session, candidate: CleanupCandidate, proof: CleanupProof) -> CleanupResult:
    """在同一事务复核精确整理回执、持久任务与完整外部证据。"""
    _begin_serialized_write(session)
    _lock_related_rows(session, candidate, subscription=False)
    current = _candidate(session, candidate.file_id)
    if current is None:
        return CleanupResult("not_found", "下载文件记录已失效或不存在")
    if current != candidate:
        return CleanupResult("stale", "数据库候选已变化，请重新观察")
    if current.review_reason or not current.identity_verified:
        return CleanupResult("needs_review", current.review_reason or "媒体身份无法确认")
    if current.transfer_id is None or len(current.destination_paths) != 1:
        return CleanupResult("needs_review", "没有可精确移除的单条整理回执")
    if (
        proof.source_path != current.source_path
        or proof.destination_paths != current.destination_paths
        or proof.destination_lookup_complete != current.destination_lookup_complete
        or len(proof.destinations) != len(current.destination_paths)
        or not proof_allows_destination_repair(proof, revision=current.revision)
    ):
        return CleanupResult("needs_review", "源、目标或媒体服务证据不完整")
    protection = _protection(session, current, 500)
    if not protection.other_versions_complete or protection.transfer_task_active is not False:
        return CleanupResult("needs_review", "同集版本或持久整理任务状态不完整")
    if proof.other_version_paths != protection.other_version_paths:
        return CleanupResult("stale", "同集其他版本路径已变化")
    transfer = session.get(TransferHistory, current.transfer_id)
    if transfer is None:
        raise CleanupConflict("整理回执已消失")
    if transfer.status is not True or transfer.transfer_settlement_revision is not None:
        return CleanupResult("needs_review", "整理回执未成功或属于持久任务")
    deleted = session.execute(
        delete(TransferHistory)
        .where(
            TransferHistory.id == current.transfer_id,
            TransferHistory.src == current.source_path,
            TransferHistory.dest == current.destination_paths[0],
            TransferHistory.download_hash == current.download_hash,
            TransferHistory.media_source == current.media_source,
            TransferHistory.media_id == current.media_id,
            TransferHistory.type == current.media_type,
            TransferHistory.seasons == transfer.seasons,
            TransferHistory.episodes == transfer.episodes,
            TransferHistory.mode == transfer.mode,
            TransferHistory.status.is_(True),
            TransferHistory.transfer_task_id.is_(None),
            TransferHistory.transfer_settlement_revision.is_(None),
        )
        .execution_options(synchronize_session=False)
    )
    if deleted.rowcount != 1:
        raise CleanupConflict("整理回执或关联任务已变化")
    return CleanupResult("cleaned", transfer_deleted=True)


def repair_missing_destination(
    candidate: CleanupCandidate,
    proof: CleanupProof,
    *,
    session_factory: Callable[[], Session] | None = None,
) -> CleanupResult:
    """原子删除源仍存在但目标和 Emby 精确集已缺失的旧整理回执。"""
    try:
        if session_factory is None:
            return run_sync_transaction(lambda session: _repair_destination(session, candidate, proof))
        with session_factory() as session:
            try:
                result = _repair_destination(session, candidate, proof)
                session.commit()
                return result
            except Exception:
                session.rollback()
                raise
    except CleanupConflict as error:
        return CleanupResult("stale", str(error))


class SqlAlchemyCleanupService:
    """使用启动事务执行器的清理端口实现。"""

    def __init__(self, *, session_factory: Callable[[], Session] | None = None) -> None:
        """保存只供隔离测试注入的会话工厂；生产使用组合根事务。"""
        self._session_factory = session_factory

    def list_candidates(self, after_id: int = 0, limit: int = 100) -> list[CleanupCandidate]:
        """有界扫描候选。"""
        return list_cleanup_candidates(after_id, limit, session_factory=self._session_factory)

    def get_candidate(self, file_id: int) -> CleanupCandidate | None:
        """读取精确候选。"""
        return get_cleanup_candidate(file_id, session_factory=self._session_factory)

    def get_protection(
        self,
        candidate: CleanupCandidate,
        limit: int = 500,
        include_source_ownership: bool = False,
    ) -> CleanupProtection:
        """读取同集保护；仅即将物理删除时查询全库独占。"""
        return get_cleanup_protection(
            candidate, limit=limit, include_source_ownership=include_source_ownership,
            session_factory=self._session_factory,
        )

    def resolve_lack_episode(
        self,
        candidate: CleanupCandidate,
        present_episodes: tuple[int, ...],
        season_lookup_complete: bool = True,
    ) -> int | None:
        """在数据库快照中计算清理后的缺集数。"""
        return resolve_cleanup_lack_episode(
            candidate, present_episodes,
            season_lookup_complete=season_lookup_complete,
            session_factory=self._session_factory,
        )

    def apply(
        self,
        candidate: CleanupCandidate,
        proof: CleanupProof,
        *,
        allow_subscription_retry: bool = False,
        resolved_lack_episode: int | None = None,
    ) -> CleanupResult:
        """在独占事务中复核并提交清理。"""
        return apply_cleanup_candidate(
            candidate, proof,
            allow_subscription_retry=allow_subscription_retry,
            resolved_lack_episode=resolved_lack_episode,
            session_factory=self._session_factory,
        )

    def repair_missing_destination(
        self,
        candidate: CleanupCandidate,
        proof: CleanupProof,
    ) -> CleanupResult:
        """只修正精确的旧整理回执，不触碰下载与订阅事实。"""
        return repair_missing_destination(candidate, proof, session_factory=self._session_factory)


# Narrow reconciliation ports. These do not import Subscribe or any media-server
# capability. Their transaction owns exact IDs, never a fuzzy media/title match.
GROUP_LIMIT = 20000
FILE_FIELDS = ("id", "downloader", "download_hash", "fullpath", "savepath", "filepath", "torrentname", "state")
HISTORY_FIELDS = ("id", "downloader", "download_hash", "path", "type", "media_source", "media_id", "seasons", "episodes")
TRANSFER_FIELDS = ("id", "downloader", "download_hash", "src", "src_storage", "dest", "dest_storage", "mode", "status",
                   "type", "media_source", "media_id", "seasons", "episodes", "src_fileitem", "dest_fileitem", "files",
                   "transfer_task_id", "transfer_settlement_revision")


def _snapshot(row, fields):
    return {field: getattr(row, field, None) for field in fields}


def _safe_path(path):
    return (isinstance(path, str) and path.startswith("/") and path != "/" and not path.endswith("/")
            and not path.startswith("//") and str(PurePosixPath(path)) == path
            and "\x00" not in path and not any(part in (".", "..") for part in path.split("/")))


def _file_candidate(session, file_id):
    row = session.get(DownloadFiles, file_id)
    if row is None or row.state != 1:
        return None
    valid = _safe_path(row.fullpath) and bool(row.download_hash) and bool(row.downloader)
    return FileOnlyCandidate(row.id, row.fullpath or "", row.download_hash or "", row.downloader,
        _revision(_snapshot(row, FILE_FIELDS)), identity_verified=valid,
        review_reason="" if valid else "invalid_file_identity")


def get_file_only_candidate(file_id, *, session_factory=None):
    return _read(lambda session: _file_candidate(session, file_id), session_factory)


def list_file_only_candidates(after_id=0, limit=100, *, session_factory=None):
    if type(after_id) is not int or after_id < 0 or type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("invalid_cursor_or_limit")
    def operation(session):
        ids = session.execute(select(DownloadFiles.id).where(DownloadFiles.id > after_id,
            DownloadFiles.state == 1).order_by(DownloadFiles.id).limit(limit)).scalars()
        return [candidate for file_id in ids if (candidate := _file_candidate(session, file_id)) is not None]
    return _read(operation, session_factory)


def _pending_paths(session, paths):
    for text in paths:
        if not _safe_path(text):
            return True
        source = PurePosixPath(text)
        ancestors = tuple(str(path) for path in (source, *source.parents))
        escaped = text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        if session.execute(select(TransferPending.id).where(TransferPending.storage == "local", or_(
            TransferPending.src_path.in_(ancestors), TransferPending.src_path.like(escaped + "/%", escape="\\"),
        )).limit(1)).scalar_one_or_none() is not None:
            return True
    return False


def get_file_only_protection(candidate, *, session_factory=None):
    def operation(session):
        current = _file_candidate(session, candidate.file_id)
        active = None if current != candidate else _pending_paths(session, (candidate.source_path,))
        return CleanupProtection((), False, active)
    return _read(operation, session_factory)


def _narrow_write(operation, session_factory):
    try:
        if session_factory is None:
            return run_sync_transaction(operation)
        with session_factory() as session:
            try:
                result = operation(session)
                session.commit()
                return result
            except Exception:
                session.rollback()
                raise
    except CleanupConflict:
        return MutationResult("needs_review", "database_changed")


def apply_file_only(candidate, proof, *, session_factory=None):
    def operation(session):
        _begin_serialized_write(session)
        session.execute(select(DownloadFiles.id).where(DownloadFiles.id == candidate.file_id).with_for_update()).scalar_one_or_none()
        current = _file_candidate(session, candidate.file_id)
        if current is None:
            return MutationResult("already_clean", "record_absent")
        if current != candidate or not current.identity_verified:
            return MutationResult("needs_review", "record_changed")
        if not (getattr(proof, "source", None) == "missing" and proof.storage_ready is True
                and proof.identity_verified is True and proof.record_present is True
                and proof.revision == current.revision and proof.downloaders_complete is True
                and proof.downloader_task_present is False and proof.transfer_task_active is False
                and not _pending_paths(session, (current.source_path,))):
            return MutationResult("needs_review", "file_evidence_incomplete")
        result = session.execute(delete(DownloadFiles).where(DownloadFiles.id == current.file_id,
            DownloadFiles.downloader == current.downloader, DownloadFiles.download_hash == current.download_hash,
            DownloadFiles.fullpath == current.source_path, DownloadFiles.state == 1).execution_options(synchronize_session=False))
        if result.rowcount != 1:
            raise CleanupConflict("file_changed")
        return MutationResult("applied", "file_record_removed", {"download_files": 1})
    return _narrow_write(operation, session_factory)


def _bounded_rows(session, model, fields, *conditions):
    rows = list(session.execute(select(model).where(*conditions).order_by(model.id).limit(GROUP_LIMIT + 1)).scalars())
    if len(rows) > GROUP_LIMIT:
        raise ValueError("group_inventory_limit")
    return [_snapshot(row, fields) for row in rows]


def _canonical_provider(value):
    return {"themoviedb": "tmdb", "thetvdb": "tvdb"}.get(value, value)


def _parent_identity(row):
    def ranges(text, prefix):
        if not text:
            return ()
        intervals = []
        for item in re.split(r"\s*[,;|]\s*", text.strip()):
            match = re.fullmatch(rf"{prefix}0*(\d+)(?:\s*[-~]\s*{prefix}?0*(\d+))?", item, re.IGNORECASE)
            if not match:
                return ("unknown", text.strip().upper())
            start, end = int(match[1]), int(match[2] or match[1])
            if end < start:
                return ("unknown", text.strip().upper())
            intervals.append((start, end))
        merged = []
        for start, end in sorted(intervals):
            if merged and start <= merged[-1][1] + 1:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))
        return tuple(merged)
    return (row["path"], row["type"], _canonical_provider(row["media_source"]), row["media_id"],
            ranges(row["seasons"], "S"), ranges(row["episodes"], "E"))


def _belongs(path, root):
    if not _safe_path(path) or not _safe_path(root):
        return False
    source, parent = PurePosixPath(path), PurePosixPath(root)
    return source == parent or parent in source.parents


def _group_inventory(session, downloader, download_hash, sources=(), roots=()):
    files = _bounded_rows(session, DownloadFiles, FILE_FIELDS,
        DownloadFiles.downloader == downloader, DownloadFiles.download_hash == download_hash)
    histories = _bounded_rows(session, DownloadHistory, HISTORY_FIELDS,
        DownloadHistory.downloader == downloader, DownloadHistory.download_hash == download_hash)
    roots = tuple(row["path"] for row in histories) + tuple(roots)
    sources = set(sources) | {row["fullpath"] for row in files}
    transfers = _bounded_rows(session, TransferHistory, TRANSFER_FIELDS,
        TransferHistory.download_hash == download_hash,
        or_(TransferHistory.downloader == downloader, TransferHistory.downloader.is_(None)))
    transfers = [row for row in transfers if row["downloader"] == downloader or row["src"] in sources or
                 row["downloader"] is None and _resolve_transfer_owner(session, row) == downloader
                 and any(_belongs(row["src"], root) for root in roots)]
    return files, histories, transfers


def _mapping_safe(row, histories):
    if not (_safe_path(row["src"]) and _safe_path(row["dest"]) and row["src"] != row["dest"]
            and row["src_storage"] == "local" and row["dest_storage"] == "local" and row["status"] is True
            and not row["transfer_task_id"] and row["transfer_settlement_revision"] is None
            and _embedded_path_matches(row["src_fileitem"], row["src"])
            and _embedded_path_matches(row["dest_fileitem"], row["dest"])
            and _single_transfer_file_matches(row["files"], row["src"])):
        return False
    if histories:
        parent = histories[0]
        if (row["type"], _canonical_provider(row["media_source"]), row["media_id"]) != (
                parent["type"], _canonical_provider(parent["media_source"]), parent["media_id"]):
            return False
        if row["type"] == "电视剧":
            season, episode = _single_number(row["seasons"], "S"), _single_number(row["episodes"], "E")
            return (season is not None and episode is not None
                    and _range_contains(parent["seasons"], "S", season) is True
                    and _range_contains(parent["episodes"], "E", episode) is True)
    return True


def _resolve_transfer_owner(session, transfer):
    parents = _bounded_rows(session, DownloadHistory, HISTORY_FIELDS, DownloadHistory.download_hash == transfer["download_hash"])
    owners = {row["downloader"] for row in parents if row["downloader"] and _belongs(transfer["src"], row["path"])
              and _mapping_safe(transfer, [row])}
    if len(owners) != 1:
        return None
    owner = next(iter(owners))
    foreign = session.execute(select(DownloadFiles.id).where(DownloadFiles.download_hash == transfer["download_hash"],
        DownloadFiles.fullpath == transfer["src"], or_(DownloadFiles.downloader != owner, DownloadFiles.downloader.is_(None)))
        .limit(1)).scalar_one_or_none()
    return owner if foreign is None else None


def prepare_receipt(file_id, *, session_factory=None):
    def operation(session):
        candidate = _file_candidate(session, file_id)
        if candidate is None or not candidate.identity_verified:
            raise ValueError("invalid_file_identity")
        files, histories, transfers = _group_inventory(session, candidate.downloader, candidate.download_hash)
        equivalent = len({_parent_identity(row) for row in histories}) <= 1
        complete = all(_safe_path(row["fullpath"]) for row in files)
        key = _revision(candidate.downloader, candidate.download_hash)
        return dict(version=1, key=key, revision=_revision(files, histories, transfers), complete=complete,
            equivalent_parents=equivalent, downloader=candidate.downloader, download_hash=candidate.download_hash,
            files=files, histories=histories, transfers=transfers)
    return _read(operation, session_factory)


def _receipt_view(session, receipt):
    if (not isinstance(receipt, dict) or receipt.get("version") != 1 or receipt.get("complete") is not True
            or not receipt.get("downloader") or not receipt.get("download_hash")
            or any(not isinstance(receipt.get(kind), list) or len(receipt[kind]) > GROUP_LIMIT for kind in ("files", "histories", "transfers"))):
        return dict(valid=False, reason="invalid_receipt")
    if (receipt.get("key") != _revision(receipt["downloader"], receipt["download_hash"])
            or receipt.get("revision") != _revision(receipt["files"], receipt["histories"], receipt["transfers"])):
        return dict(valid=False, reason="receipt_signature_mismatch")
    sources = tuple(row["fullpath"] for row in receipt["files"]) + tuple(row["src"] for row in receipt["transfers"])
    roots = tuple(row["path"] for row in receipt["histories"])
    current = _group_inventory(session, receipt["downloader"], receipt["download_hash"], sources, roots)
    for name, rows, model, fields in zip(("files", "histories", "transfers"), current,
        (DownloadFiles, DownloadHistory, TransferHistory), (FILE_FIELDS, HISTORY_FIELDS, TRANSFER_FIELDS)):
        original = {row["id"]: row for row in receipt[name]}
        if len(original) != len(receipt[name]) or any(original.get(row["id"]) != row for row in rows):
            return dict(valid=False, reason="new_or_changed_reference")
        included_ids = {row["id"] for row in rows}
        for row_id, snapshot in original.items():
            row = session.get(model, row_id)
            if row is not None and _snapshot(row, fields) != snapshot:
                return dict(valid=False, reason="new_or_changed_reference")
            if row is not None and row_id not in included_ids:
                return dict(valid=False, reason="association_owner_changed")
    files, histories, transfers = current
    paths = sources + tuple(row["dest"] for row in receipt["transfers"])
    return dict(valid=True, reason="", files=files, histories=histories, transfers=transfers,
        pending=_pending_paths(session, paths), revision=_revision(*current),
        mappings_safe=all(_mapping_safe(row, receipt["histories"]) for row in receipt["transfers"]),
        done=not any(current))


def receipt_view(receipt, *, session_factory=None):
    return _read(lambda session: _receipt_view(session, receipt), session_factory)


def propose_receipt_refresh(receipt, *, session_factory=None):
    """Accept only exact state=0 -> 1 completion deltas; retain every old path,
    deleted row snapshot and recovery intention for the new observation epoch."""
    def operation(session):
        candidate = deepcopy(receipt)
        changed = False
        for snapshot in candidate.get("files", ()):
            row = session.get(DownloadFiles, snapshot["id"])
            if row is None:
                continue
            actual = _snapshot(row, FILE_FIELDS)
            if actual == snapshot:
                continue
            if (snapshot.get("state") != 0 or actual.get("state") != 1
                    or any(actual[field] != snapshot.get(field) for field in FILE_FIELDS if field != "state")):
                return None
            snapshot["state"] = 1
            changed = True
        if not changed:
            return None
        candidate["revision"] = _revision(candidate["files"], candidate["histories"], candidate["transfers"])
        view = _receipt_view(session, candidate)
        if not view.get("valid") or view["pending"] or candidate.get("equivalent_parents") is not True:
            return None
        candidate["generation"] = candidate.get("generation", 0) + 1
        candidate.pop("source_cycle_id", None)
        candidate["source_cursor"] = 0
        return candidate
    return _read(operation, session_factory)


def _source_ownership(session, receipt, source, *, physical=True):
    view = _receipt_view(session, receipt)
    if not view.get("valid") or view["pending"]:
        return False
    if any(row["fullpath"] == source and row["state"] != 1 for row in view["files"]):
        return False
    if physical and session.execute(select(TransferHistory.id).where(TransferHistory.dest == source).limit(1)).scalar_one_or_none() is not None:
        return False
    allowed_files = {row["id"] for row in receipt["files"]}
    allowed_transfers = {row["id"] for row in receipt["transfers"]}
    allowed_histories = {row["id"] for row in receipt["histories"]}
    references = (
        (DownloadFiles, DownloadFiles.fullpath == source, allowed_files),
        (TransferHistory, or_(TransferHistory.src == source, TransferHistory.dest == source), allowed_transfers),
        (DownloadHistory, DownloadHistory.path == source, allowed_histories),
    )
    for model, condition, allowed in references:
        query = select(model.id).where(condition)
        if allowed:
            query = query.where(model.id.notin_(allowed))
        if session.execute(query.limit(1)).scalar_one_or_none() is not None:
            return False
    return True


def source_ownership(receipt, source, *, session_factory=None):
    return _read(lambda session: _source_ownership(session, receipt, source), session_factory)


def apply_receipt(receipt, proof, *, session_factory=None):
    def operation(session):
        _begin_serialized_write(session)
        for model, kind in ((DownloadHistory, "histories"), (TransferHistory, "transfers"), (DownloadFiles, "files")):
            ids = [row["id"] for row in receipt[kind]]
            if ids:
                list(session.execute(select(model.id).where(model.id.in_(ids)).with_for_update()).scalars())
        view = _receipt_view(session, receipt)
        if not view.get("valid"):
            return MutationResult("needs_review", view.get("reason", "invalid_receipt"))
        if view["done"]:
            return MutationResult("already_clean", "related_records_absent")
        if (view["files"] or view["pending"] or receipt.get("equivalent_parents") is not True
                or not view["mappings_safe"] or proof.get("revision") != view["revision"]
                or any(proof.get(field) is not True for field in ("sources_missing", "destinations_missing", "storage_ready", "tasks_absent"))):
            return MutationResult("needs_review", "related_evidence_incomplete")
        sources = {row["fullpath"] for row in receipt["files"]} | {row["src"] for row in receipt["transfers"]}
        if any(not _source_ownership(session, receipt, source, physical=False) for source in sources):
            return MutationResult("needs_review", "source_shared_reference")
        counts = {}
        for model, name, count_name, fields in ((TransferHistory, "transfers", "transfer_history", TRANSFER_FIELDS),
                (DownloadHistory, "histories", "download_history", HISTORY_FIELDS)):
            for row in view[name]:
                conditions = [getattr(model, field) == row[field] for field in fields
                              if field not in ("src_fileitem", "dest_fileitem", "files")]
                deleted = session.execute(delete(model).where(*conditions).execution_options(synchronize_session=False))
                if deleted.rowcount != 1:
                    raise CleanupConflict("related_record_changed")
                counts[count_name] = counts.get(count_name, 0) + 1
        return MutationResult("applied", "related_history_removed", counts)
    return _narrow_write(operation, session_factory)


def apply_transfer_scope(receipt, source, proof, *, session_factory=None):
    """Remove only stale successful mappings for one source; retain its source,
    siblings, parent group, tasks and subscriptions."""
    def operation(session):
        _begin_serialized_write(session)
        ids = [row["id"] for row in receipt["transfers"] if row["src"] == source]
        if ids:
            list(session.execute(select(TransferHistory.id).where(TransferHistory.id.in_(ids)).with_for_update()).scalars())
        view = _receipt_view(session, receipt)
        if not view.get("valid"):
            return MutationResult("needs_review", view.get("reason", "invalid_receipt"))
        rows = [row for row in view["transfers"] if row["src"] == source]
        if not rows:
            return MutationResult("already_clean", "transfer_records_absent")
        targets = tuple(sorted({row["dest"] for row in rows}))
        if (view["pending"] or receipt.get("equivalent_parents") is not True
                or any(not _mapping_safe(row, receipt["histories"]) for row in rows)
                or proof.get("revision") != view["revision"] or proof.get("destination_paths") != targets
                or proof.get("source") not in ("present", "missing")
                or any(proof.get(name) is not True for name in ("destinations_missing", "storage_ready", "tasks_inactive"))):
            return MutationResult("needs_review", "transfer_evidence_incomplete")
        for row in rows:
            fields = [field for field in TRANSFER_FIELDS if field not in ("src_fileitem", "dest_fileitem", "files")]
            deleted = session.execute(delete(TransferHistory).where(*[getattr(TransferHistory, field) == row[field] for field in fields])
                .execution_options(synchronize_session=False))
            if deleted.rowcount != 1:
                raise CleanupConflict("transfer_changed")
        return MutationResult("applied", "stale_transfer_removed", {"transfer_history": len(rows)})
    return _narrow_write(operation, session_factory)


def list_orphan_receipts(after_id=0, limit=100, *, kind="history", session_factory=None):
    """Bounded seeds for old orphan groups. A directory alone proves nothing."""
    if type(after_id) is not int or after_id < 0 or type(limit) is not int or not 1 <= limit <= 100 or kind not in ("history", "transfer"):
        raise ValueError("invalid_orphan_cursor")
    def operation(session):
        model = DownloadHistory if kind == "history" else TransferHistory
        file_conditions = [DownloadFiles.download_hash == model.download_hash, DownloadFiles.downloader == model.downloader]
        if kind == "transfer":
            file_conditions.append(DownloadFiles.fullpath == TransferHistory.src)
        has_files = exists(select(DownloadFiles.id).where(*file_conditions))
        query = select(model).where(model.id > after_id, ~has_files).order_by(model.id).limit(limit)
        seeds = list(session.execute(query).scalars())
        entries, prepared = [], {}
        for seed in seeds:
            downloader, download_hash = seed.downloader, seed.download_hash
            if not downloader and download_hash:
                downloader = _resolve_transfer_owner(session, _snapshot(seed, TRANSFER_FIELDS)) if kind == "transfer" else None
            if not downloader or not download_hash:
                entries.append(dict(record_id=seed.id, receipt=None, reason="orphan_downloader_unverified"))
                continue
            group_key = _revision(downloader, download_hash)
            if group_key not in prepared:
                files, histories, transfers = _group_inventory(session, downloader, download_hash)
                # Include every explicit same-owner mapping, not only a guessed
                # directory prefix; nullable legacy rows need the unique parent.
                all_transfers = _bounded_rows(session, TransferHistory, TRANSFER_FIELDS,
                    TransferHistory.download_hash == download_hash,
                    or_(TransferHistory.downloader == downloader, TransferHistory.downloader.is_(None)))
                transfers = [row for row in all_transfers if row["downloader"] == downloader or
                             row["downloader"] is None and _resolve_transfer_owner(session, row) == downloader]
                equivalent = len({_parent_identity(row) for row in histories}) <= 1
                reason = ("related_files_still_registered" if files and (kind == "history" or any(row["fullpath"] == getattr(seed, "src", None) for row in files)) else
                    "conflicting_parent_histories" if not equivalent else
                    "orphan_mapping_incomplete" if not transfers or any(not _mapping_safe(row, histories) for row in transfers) else "")
                if not reason and histories and any(not any(_belongs(row["src"], history["path"]) for history in histories) for row in transfers):
                    reason = "orphan_mapping_incomplete"
                if not reason and any(row["downloader"] is None and _resolve_transfer_owner(session, row) is None
                                      and any(_belongs(row["src"], history["path"]) for history in histories) for row in all_transfers):
                    reason = "orphan_downloader_unverified"
                receipt = (None if reason else dict(version=1, key=group_key, revision=_revision(files, histories, transfers),
                    complete=True, equivalent_parents=equivalent, downloader=downloader, download_hash=download_hash,
                    files=files, histories=histories, transfers=transfers))
                prepared[group_key] = (receipt, reason)
            receipt, reason = prepared[group_key]
            entries.append(dict(record_id=seed.id, receipt=receipt, reason=reason,
                                source_path=getattr(seed, "src", None) or getattr(seed, "path", "")))
        return dict(entries=entries, cursor=seeds[-1].id if len(seeds) == limit else 0, complete=True)
    return _read(operation, session_factory)
