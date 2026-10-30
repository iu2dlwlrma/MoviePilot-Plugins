"""Conservative cleanup policy, independent of MoviePilot imports.

The host adapter owns exact-file probes, complete identity/version lookups and
official transactional mutation commands. This module never opens, unlinks or
downloads media. A webhook is only a reason to run the same observation process.
Persist ``CleanupRun.state`` between runs; an absent/unreadable state restarts
the observation period. Run the host mutation with its revision/lease checks:
the in-process lock cannot replace a database transaction across processes.
"""

from dataclasses import dataclass
from enum import Enum
import hashlib
from itertools import islice
import json
import math
from pathlib import PurePosixPath
import re
from threading import Lock
import time
from typing import Callable, Iterable, Optional, Tuple


MIN_STABILITY_SECONDS = 24 * 60 * 60
MAX_BATCH_SIZE = 100
MAX_PENDING = 2000
MAX_STALE_SECONDS = 7 * 24 * 60 * 60
_RUN_LOCK = Lock()


class Presence(str, Enum):
    PRESENT = "present"
    MISSING = "missing"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class CleanupRecord:
    # Host key must bind the exact downloadfile ID + torrent/path fingerprint.
    key: str
    revision: str
    source_path: str
    destination_paths: Tuple[str, ...]
    media_source: str
    media_id: str
    media_type: str
    season: Optional[int] = None
    episodes: Tuple[int, ...] = ()
    storage: str = "local"
    kind: str = "download_file"
    record_id: Optional[int] = None


@dataclass(frozen=True)
class CleanupEvidence:
    source: Presence = Presence.UNKNOWN
    # One result per destination_paths entry, including unavailable targets.
    destinations: Tuple[Presence, ...] = ()
    # Both aggregate fields mean ALL exact-identity versions/items are missing.
    # A partial search, an unmatched identity or unavailable provider is UNKNOWN.
    other_versions: Presence = Presence.UNKNOWN
    media_library: Presence = Presence.UNKNOWN
    # True only after the host enumerated all target records for this identity;
    # an empty tuple alone says nothing about whether it was ever transferred.
    destination_lookup_complete: bool = False
    media_lookup_complete: bool = False
    downloaders_complete: bool = False
    # Includes paused, queued, completed and seeding tasks, not just downloads.
    downloader_task_present: Optional[bool] = None
    transfer_task_active: Optional[bool] = None
    storage_ready: bool = False
    identity_verified: bool = False
    revision: str = ""
    record_present: Optional[bool] = True
    # lstat snapshot: dev, inode, mode, size, mtime_ns, ctime_ns, nlink.
    # Required only for source-present cleanup; never inferred from a pathname.
    source_signature: Tuple[int, ...] = ()


@dataclass(frozen=True)
class CleanupOptions:
    stability_seconds: float = MIN_STABILITY_SECONDS
    batch_size: int = MAX_BATCH_SIZE
    pending_limit: int = MAX_PENDING
    # Full-missing cleanup always protects existing tasks. A dedicated source
    # purge adapter additionally requires explicit False before task removal.
    preserve_source_tasks: bool = True
    allow_subscription_retry: bool = False

    def __post_init__(self):
        if not _finite_number(self.stability_seconds) or self.stability_seconds < MIN_STABILITY_SECONDS:
            raise ValueError("stability_seconds must be at least 86400")
        if type(self.batch_size) is not int or not 1 <= self.batch_size <= MAX_BATCH_SIZE:
            raise ValueError("batch_size must be between 1 and 100")
        if type(self.pending_limit) is not int or not self.batch_size <= self.pending_limit <= MAX_PENDING:
            raise ValueError("pending_limit must cover batch_size and not exceed 2000")
        if type(self.preserve_source_tasks) is not bool or type(self.allow_subscription_retry) is not bool:
            raise ValueError("cleanup policy options must be boolean")


@dataclass(frozen=True)
class MutationResult:
    # Only an explicit host result confirms a committed, revision-checked change.
    status: str
    reason: str


@dataclass(frozen=True)
class CleanupResult:
    key: str
    status: str
    reason: str
    first_seen: Optional[float] = None
    eligible_at: Optional[float] = None


@dataclass(frozen=True)
class CleanupRun:
    results: Tuple[CleanupResult, ...]
    state: dict
    busy: bool = False
    truncated: bool = False


def _finite_number(value) -> bool:
    return type(value) in (int, float) and math.isfinite(value)


def _exact_local_path(value) -> bool:
    return (
        isinstance(value, str) and value.startswith("/") and value != "/"
        and "\x00" not in value and not value.endswith("/")
        and ".." not in value.split("/") and "." not in value.split("/")
        and PurePosixPath(value).is_absolute()
    )


def _valid_record(record: CleanupRecord) -> bool:
    if not isinstance(record, CleanupRecord):
        return False
    if not all(isinstance(value, str) and value.strip() for value in (
        record.key, record.revision, record.media_source, record.media_id,
    )):
        return False
    if record.storage != "local" or record.kind not in ("download_file", "transfer_history"):
        return False
    if not _exact_local_path(record.source_path):
        return False
    if not isinstance(record.destination_paths, (tuple, list)):
        return False
    if not all(_exact_local_path(path) for path in record.destination_paths):
        return False
    if type(record.record_id) is not int or record.record_id <= 0:
        return False
    if record.media_type in ("电视剧", "tv"):
        return (
            type(record.season) is int and record.season >= 0
            and isinstance(record.episodes, (tuple, list)) and bool(record.episodes)
            and all(type(episode) is int and episode > 0 for episode in record.episodes)
        )
    return record.media_type in ("电影", "movie") and not record.episodes


def _fingerprint(record: CleanupRecord, evidence: CleanupEvidence = None) -> str:
    value = (
        record.key, record.revision, record.kind, record.record_id, record.storage,
        record.source_path, tuple(record.destination_paths), record.media_source,
        record.media_id, record.media_type, record.season, tuple(record.episodes),
    )
    if evidence is not None and evidence.source is Presence.PRESENT:
        value += (tuple(evidence.source_signature),)
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def _decision(record: CleanupRecord, evidence: CleanupEvidence):
    if not isinstance(evidence, CleanupEvidence):
        return "needs_review", "invalid_evidence"
    if evidence.record_present is False:
        return "already_clean", "record_absent"
    if evidence.record_present is not True:
        return "needs_review", "record_lookup_incomplete"
    if evidence.revision != record.revision:
        return "needs_review", "record_changed"
    if evidence.downloader_task_present is True:
        return "skipped", "downloader_task_present"
    if evidence.transfer_task_active is True:
        return "skipped", "transfer_task_active"
    if not isinstance(evidence.destinations, (tuple, list)) or len(evidence.destinations) != len(record.destination_paths):
        return "needs_review", "destination_lookup_incomplete"
    probes = (
        ("source_present", evidence.source),
        *(("destination_present", presence) for presence in evidence.destinations),
        ("other_version_present", evidence.other_versions),
        ("media_library_present", evidence.media_library),
    )
    for reason, presence in probes:
        if isinstance(presence, Presence) and presence is Presence.PRESENT:
            return "skipped", reason
    if any(not isinstance(presence, Presence) or presence is not Presence.MISSING for _, presence in probes):
        return "needs_review", "presence_unknown"
    for complete, reason in (
        (evidence.storage_ready, "storage_unavailable"),
        (evidence.identity_verified, "identity_unverified"),
        (evidence.destination_lookup_complete, "destination_lookup_incomplete"),
        (evidence.media_lookup_complete, "media_lookup_incomplete"),
        (evidence.downloaders_complete, "downloaders_incomplete"),
    ):
        if complete is not True:
            return "needs_review", reason
    if evidence.downloader_task_present is not False or evidence.transfer_task_active is not False:
        return "needs_review", "task_lookup_incomplete"
    return None


def destination_decision(record: CleanupRecord, evidence: CleanupEvidence):
    """源仍在而入库目标缺失时的受控清理门禁。

    宿主还必须证明源与种子独占、单文件任务，并复核文件签名；本层不删除文件。
    """
    if not isinstance(evidence, CleanupEvidence):
        return "needs_review", "invalid_evidence"
    if evidence.record_present is False:
        return "already_clean", "record_absent"
    if evidence.record_present is not True or evidence.revision != record.revision:
        return "needs_review", "record_changed"
    if not record.destination_paths:
        return "needs_review", "no_transfer_destination"
    if not isinstance(evidence.destinations, (tuple, list)) or len(evidence.destinations) != len(record.destination_paths):
        return "needs_review", "destination_lookup_incomplete"
    if evidence.source is Presence.MISSING:
        return "skipped", "source_missing"
    for reason, value in (
        *(("destination_present", value) for value in evidence.destinations),
        ("other_version_present", evidence.other_versions),
        ("media_library_present", evidence.media_library),
    ):
        if isinstance(value, Presence) and value is Presence.PRESENT:
            return "skipped", reason
    if evidence.source is not Presence.PRESENT or any(
        value is not Presence.MISSING for value in (
            *evidence.destinations, evidence.other_versions, evidence.media_library
        )
    ):
        return "needs_review", "presence_unknown"
    for complete, reason in (
        (evidence.storage_ready, "storage_unavailable"),
        (evidence.identity_verified, "identity_unverified"),
        (evidence.destination_lookup_complete, "destination_lookup_incomplete"),
        (evidence.media_lookup_complete, "media_lookup_incomplete"),
        (evidence.downloaders_complete, "downloaders_incomplete"),
    ):
        if complete is not True:
            return "needs_review", reason
    if evidence.transfer_task_active is True:
        return "skipped", "transfer_task_active"
    if evidence.transfer_task_active is not False or type(evidence.downloader_task_present) is not bool:
        return "needs_review", "task_lookup_incomplete"
    signature = evidence.source_signature
    if (not isinstance(signature, (tuple, list)) or len(signature) != 7
            or any(type(value) is not int or value < 0 for value in signature)
            or signature[1] <= 0 or signature[6] <= 0):
        return "needs_review", "source_signature_unavailable"
    return None


def _copy_state(state, limit: int, now: float) -> dict:
    result = {"version": 1, "candidates": {}, "completed": {}}
    if not isinstance(state, dict) or state.get("version") != 1:
        return result
    for name in ("candidates", "completed"):
        entries = state.get(name)
        if isinstance(entries, dict):
            for key, value in islice(entries.items(), limit):
                if isinstance(key, str) and isinstance(value, dict):
                    if name == "candidates":
                        last = value.get("last_seen")
                        fingerprint = value.get("fingerprint")
                        # Rows that disappear from the DB no longer enter a page.
                        # Expire their observation state so they cannot exhaust
                        # capacity forever. Reappearance starts a new 24h window.
                        if (not isinstance(fingerprint, str) or len(fingerprint) != 64
                                or not _valid_observation(value, fingerprint, now)
                                or not now - MAX_STALE_SECONDS <= last <= now):
                            continue
                    # No input mutation: callers may be concurrently persisting the old result.
                    result[name][key] = dict(value)
    return result


def _valid_observation(value, fingerprint: str, now: float) -> bool:
    if not isinstance(value, dict) or value.get("fingerprint") != fingerprint:
        return False
    first, last = value.get("first_seen"), value.get("last_seen")
    count = value.get("observations")
    return (
        _finite_number(first) and _finite_number(last) and 0 <= first <= last <= now
        and type(count) is int and count >= 1
    )


def _safe_reason(value: str, fallback: str) -> str:
    # Host exceptions/messages can contain authenticated URLs; only reason codes leave here.
    return value if isinstance(value, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,79}", value) else fallback


class RecordCleanupEngine:
    def __init__(
        self,
        observer: Callable[[CleanupRecord], CleanupEvidence],
        mutation: Optional[Callable[[CleanupRecord, CleanupEvidence, CleanupOptions], MutationResult]] = None,
        decision: Callable[[CleanupRecord, CleanupEvidence], Optional[tuple]] = _decision,
    ):
        self._observer = observer
        self._mutation = mutation
        self._decision = decision

    def run(
        self, records: Iterable[CleanupRecord], state: Optional[dict] = None, *,
        now: Optional[float] = None, dry_run: bool = True, options: Optional[CleanupOptions] = None,
    ) -> CleanupRun:
        options = options or CleanupOptions()
        now = time.time() if now is None else now
        if not _finite_number(now) or now < 0:
            raise ValueError("now must be a nonnegative UTC timestamp")
        current = _copy_state(state, options.pending_limit, now)
        if not _RUN_LOCK.acquire(blocking=False):
            return CleanupRun((), current, busy=True)
        try:
            results = []
            iterator = iter(records)
            for record in islice(iterator, options.batch_size):
                results.append(self._process(record, current, now, dry_run, options))
            marker = object()
            truncated = next(iterator, marker) is not marker
            return CleanupRun(tuple(results), current, truncated=truncated)
        finally:
            _RUN_LOCK.release()

    def _observe(self, record):
        try:
            evidence = self._observer(record)
            return evidence, self._decision(record, evidence)
        except Exception:
            return None, ("needs_review", "observation_failed")

    def _process(self, record, state, now, dry_run, options):
        key = record.key if isinstance(record, CleanupRecord) and isinstance(record.key, str) else "invalid"
        candidates, completed = state["candidates"], state["completed"]
        if not _valid_record(record):
            candidates.pop(key, None)
            return CleanupResult(key, "needs_review", "invalid_record")
        fingerprint = _fingerprint(record)
        previous_completed = completed.get(key, {})
        if previous_completed.get("fingerprint") == fingerprint:
            return CleanupResult(key, "already_clean", "already_applied")
        evidence, decision = self._observe(record)
        if decision:
            candidates.pop(key, None)
            return CleanupResult(key, *decision)
        fingerprint = _fingerprint(record, evidence)
        if previous_completed.get("fingerprint") == fingerprint:
            return CleanupResult(key, "already_clean", "already_applied")

        observation = candidates.get(key)
        if not _valid_observation(observation, fingerprint, now):
            if key not in candidates and len(candidates) >= options.pending_limit:
                # A slow scan can take more than 24h before returning to a row.
                # Age alone must never evict that row's only observation. Only
                # rows already revisited AND reported eligible can rotate out.
                # Reappearance always starts a completely new window.
                mature = [candidate_key for candidate_key, value in candidates.items()
                          if value["observations"] >= 2
                          and _finite_number(value.get("previewed_at"))
                          and value["first_seen"] + options.stability_seconds <= value["previewed_at"] <= now]
                if not mature:
                    return CleanupResult(key, "waiting", "candidate_capacity_wait")
                oldest = min(mature, key=lambda candidate_key: (
                    candidates[candidate_key]["first_seen"], candidates[candidate_key]["last_seen"], candidate_key))
                candidates.pop(oldest)
            observation = {"fingerprint": fingerprint, "first_seen": now, "last_seen": now, "observations": 1}
            candidates[key] = observation
        else:
            observation["last_seen"] = now
            observation["observations"] += 1
        first_seen = observation["first_seen"]
        eligible_at = first_seen + options.stability_seconds
        if now < eligible_at or observation["observations"] < 2:
            return CleanupResult(key, "waiting", "stability_window", first_seen, eligible_at)
        if dry_run is not False:
            observation["previewed_at"] = now
            return CleanupResult(key, "eligible", "confirmed_missing_dry_run", first_seen, eligible_at)
        if self._mutation is None:
            return CleanupResult(key, "needs_review", "mutation_adapter_unavailable", first_seen, eligible_at)

        # Resolve every dependency again immediately before the host's atomic command.
        evidence, decision = self._observe(record)
        if decision:
            candidates.pop(key, None)
            return CleanupResult(key, *decision)
        fresh_fingerprint = _fingerprint(record, evidence)
        if fresh_fingerprint != fingerprint:
            candidates[key] = {"fingerprint": fresh_fingerprint, "first_seen": now,
                               "last_seen": now, "observations": 1}
            return CleanupResult(key, "waiting", "source_changed", now, now + options.stability_seconds)
        try:
            outcome = self._mutation(record, evidence, options)
        except Exception:
            candidates.pop(key, None)
            return CleanupResult(key, "needs_review", "mutation_failed")
        if not isinstance(outcome, MutationResult) or outcome.status not in ("applied", "already_clean"):
            candidates.pop(key, None)
            reason = _safe_reason(outcome.reason, "mutation_unconfirmed") if isinstance(outcome, MutationResult) else "mutation_unconfirmed"
            return CleanupResult(key, "needs_review", reason)
        candidates.pop(key, None)
        if key not in completed and len(completed) >= options.pending_limit:
            completed.pop(next(iter(completed)))
        completed[key] = {"fingerprint": fingerprint, "applied_at": now}
        return CleanupResult(key, outcome.status, _safe_reason(outcome.reason, "records_updated"))
