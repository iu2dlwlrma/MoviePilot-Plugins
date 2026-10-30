"""Bounded MoviePilot evidence and explicitly authorized exact-source cleanup.

The source-purge branch never asks a downloader to delete data. It verifies and
unlinks one signed local file; the host CAS remains the sole database writer.
"""
from dataclasses import dataclass, replace
from collections.abc import Sequence
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
from typing import Callable
from urllib.parse import urlsplit

from .record_cleanup import CleanupEvidence, CleanupRecord, MutationResult, Presence, file_only_decision


@dataclass(frozen=True)
class TrustedRoot:
    path: str
    device: int
    mountpoint: str
    mount_id: str


@dataclass(frozen=True)
class PathObservation:
    presence: Presence = Presence.UNKNOWN
    ready: bool = False
    signature: tuple = ()


@dataclass(frozen=True)
class PurgeTask:
    service: str
    download_hash: str
    path: str
    size: int


@dataclass(frozen=True)
class PurgePlan:
    complete: bool = False
    tasks: tuple = ()
    reason: str = "task_inventory_incomplete"


@dataclass(frozen=True)
class CandidatePage:
    records: tuple = ()
    cursor: int = 0
    complete: bool = False
    reason: str = ""


def _get(value, name, default=None):
    return value.get(name, default) if isinstance(value, dict) else getattr(value, name, default)


def _path(value):
    if not isinstance(value, str) or not value.startswith("/") or value == "/":
        return None
    if "\x00" in value or any(p in (".", "..") for p in value.split("/")):
        return None
    return PurePosixPath(value)


def _within(path, root):
    return path == root or root in path.parents


def _signature(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns, info.st_nlink)


def _exclusive_signature(value):
    return (isinstance(value, (tuple, list)) and len(value) == 7
            and all(type(item) is int and item >= 0 for item in value)
            and value[1] > 0 and stat.S_ISREG(value[2]) and value[3] > 0 and value[6] == 1)


def _torrent_hash(value):
    return (isinstance(value, str) and len(value) in (40, 64)
            and all(character in "0123456789abcdefABCDEF" for character in value))


def _task_lifecycle(service_type, row):
    lifecycle = _get(row, "state", _get(row, "status"))
    permitted = ({"uploading", "stalledUP", "queuedUP", "pausedUP", "stoppedUP", "forcedUP"}
                 if service_type == "qbittorrent" else {"seeding", "seed_pending", "stopped", 0, 5, 6})
    if lifecycle in permitted:
        return ""
    if lifecycle in ("moving", "allocating"):
        return "task_moving"
    if lifecycle in ("checkingUP", "checkingDL", "checkingResumeData", "checking", "check_pending", 1, 2):
        return "task_checking"
    if lifecycle in ("queuedDL", "download_pending", 3):
        return "task_queued_download"
    if lifecycle in ("downloading", "stalledDL", "forcedDL", "metaDL", 4):
        return "task_downloading"
    if lifecycle in ("pausedDL", "stoppedDL", "missingFiles", "error"):
        return "task_download_incomplete"
    return "task_state_unknown"


def _mounts():
    result = []
    with open("/proc/self/mountinfo", encoding="utf-8") as stream:
        for line in stream:
            parts = line.split()
            if len(parts) < 7:
                raise ValueError("invalid_mount_table")
            mountpoint = parts[4]
            for encoded, decoded in (("\\040", " "), ("\\011", "\t"), ("\\134", "\\")):
                mountpoint = mountpoint.replace(encoded, decoded)
            result.append((parts[0], mountpoint))
    return result


def _mounted(root):
    return (root.mount_id, root.mountpoint) in _mounts()


class LocalPathProbe:
    """ENOENT is absence only below a still-mounted, accessible trusted root."""

    def __init__(self, roots, *, mount_check: Callable = _mounted):
        self.roots = tuple(roots)[:32]
        self.mount_check = mount_check

    @classmethod
    def from_paths(cls, paths):
        roots = []
        mounts = _mounts()
        for text in tuple(paths)[:32]:
            path = _path(text)
            if path is None:
                continue
            matches = [(mid, name) for mid, name in mounts
                       if name != "/" and _within(path, PurePosixPath(name))]
            if not matches:
                continue  # A bare container root cannot prove its NAS is mounted.
            mid, mountpoint = max(matches, key=lambda item: len(item[1]))
            try:
                if Path(text).resolve(strict=True) != Path(text):
                    continue
                info = os.lstat(text)
                if stat.S_ISDIR(info.st_mode):
                    roots.append(TrustedRoot(str(path), info.st_dev, mountpoint, mid))
            except OSError:
                continue
        return cls(roots)

    @classmethod
    def from_storage_paths(cls, paths):
        """Current configured folders anchor their mounted storage, including
        exact historical paths that predate today's folder configuration."""
        configured = cls.from_paths(paths)
        roots, seen = [], set()
        for anchor in configured.roots:
            if anchor.mountpoint in seen:
                continue
            mount = Path(anchor.mountpoint)
            try:
                info = os.lstat(mount)
                if (str(mount) == "/" or mount.resolve(strict=True) != mount
                        or not stat.S_ISDIR(info.st_mode) or info.st_dev != anchor.device
                        or not _mounted(anchor)):
                    continue
                roots.append(TrustedRoot(str(mount), anchor.device, anchor.mountpoint, anchor.mount_id))
                seen.add(anchor.mountpoint)
            except (OSError, ValueError):
                continue
        return cls(roots)

    def check(self, text):
        path = _path(text)
        if path is None:
            return PathObservation()
        roots = [root for root in self.roots if _within(path, PurePosixPath(root.path))]
        if not roots:
            return PathObservation()
        root = max(roots, key=lambda item: len(item.path))
        if path == PurePosixPath(root.path):
            return PathObservation()
        try:
            if not self.mount_check(root):
                return PathObservation()
            info = os.lstat(root.path)
            if not stat.S_ISDIR(info.st_mode) or info.st_dev != root.device:
                return PathObservation()
            # Opening the trusted root catches permission / detached-mount errors.
            with os.scandir(root.path):
                pass
            current = Path(root.path)
            parts = path.relative_to(PurePosixPath(root.path)).parts
            if len(parts) > 64:
                return PathObservation()
            for index, part in enumerate(parts):
                current = current / part
                try:
                    info = os.lstat(current)
                except FileNotFoundError:
                    # Verify the root again after absence, not just before it.
                    healthy = self.mount_check(root) and os.lstat(root.path).st_dev == root.device
                    return PathObservation(Presence.MISSING, True) if healthy else PathObservation()
                if stat.S_ISLNK(info.st_mode) or info.st_dev != root.device:
                    return PathObservation()
                if index < len(parts) - 1 and not stat.S_ISDIR(info.st_mode):
                    return PathObservation()
            if not stat.S_ISREG(info.st_mode):
                return PathObservation()
            return PathObservation(Presence.PRESENT, True, _signature(info))
        except (OSError, ValueError, TypeError):
            return PathObservation()

    def unlink_exact(self, text, signature):
        """Unlink one unchanged regular file through a no-symlink directory FD."""
        path = _path(text)
        if path is None or not _exclusive_signature(signature):
            return False
        roots = [root for root in self.roots if _within(path, PurePosixPath(root.path))]
        if not roots:
            return False
        root = max(roots, key=lambda item: len(item.path))
        if path == PurePosixPath(root.path) or len(path.parts) > 64:
            return False
        descriptor = None
        try:
            observed = self.check(text)
            if (not observed.ready or observed.presence is not Presence.PRESENT
                    or tuple(observed.signature) != tuple(signature) or not self.mount_check(root)):
                return False
            flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
            descriptor = os.open("/", flags)
            for part in path.parts[1:-1]:
                child = os.open(part, flags, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
            if os.fstat(descriptor).st_dev != root.device or not self.mount_check(root):
                return False
            info = os.stat(path.name, dir_fd=descriptor, follow_symlinks=False)
            if _signature(info) != tuple(signature):
                return False
            os.unlink(path.name, dir_fd=descriptor)
            after = self.check(text)
            return after.ready and after.presence is Presence.MISSING
        except (OSError, ValueError, TypeError):
            return False
        finally:
            if descriptor is not None:
                os.close(descriptor)


class DownloaderProbe:
    """Ask each configured client for ALL tasks, retaining its error flag."""

    def __init__(self, helper, *, max_tasks=10000, max_services=16):
        self.helper = helper
        self.max_tasks = min(max(1, max_tasks), 10000)
        self.max_services = min(max(1, max_services), 16)
        self.stop_requested = lambda: False
        self.last_removed = ()
        self._batch_cache = None
        self._preview_mode = False

    def begin_batch(self):
        self._batch_cache = {}

    def end_batch(self):
        self._batch_cache = None
        self._preview_mode = False

    def _inventory(self, client, kind, task_hash=None):
        key = (id(client), kind, task_hash)
        def read():
            return (client.get_torrents(ids=None, status=None, tags=None) if kind == "tasks"
                    else client.get_files(tid=task_hash))
        if not self._preview_mode or self._batch_cache is None:
            return read()
        if key not in self._batch_cache:
            if len(self._batch_cache) >= 64:
                raise ValueError("preview_inventory_limit")
            self._batch_cache[key] = read()
        return self._batch_cache[key]

    def check_cached(self, download_hash, paths):
        previous = self._preview_mode
        self._preview_mode = True
        try:
            return self.check(download_hash, paths)
        finally:
            self._preview_mode = previous

    def check_scoped(self, download_hash, paths, downloader):
        return self.check(download_hash, paths, downloader=downloader)

    def check_scoped_cached(self, download_hash, paths, downloader):
        previous = self._preview_mode
        self._preview_mode = True
        try:
            return self.check(download_hash, paths, downloader=downloader)
        finally:
            self._preview_mode = previous

    def check_activity(self, download_hash, paths, downloader, *, cached=False):
        """Exact-file write activity, distinct from task presence/removal ownership."""
        previous = self._preview_mode
        self._preview_mode = cached
        self.activity_reason = ""
        try:
            targets = tuple(_path(path) for path in paths)
            if not targets or any(path is None for path in targets):
                return False, None
            configs = self.helper.get_configs(include_disabled=True)
            if not isinstance(configs, dict) or not configs or len(configs) > self.max_services:
                return False, None
            total = queries = 0
            complete = True
            for service_name, config in configs.items():
                if self.stop_requested():
                    return False, None
                service = self.helper.get_service(name=service_name)
                if (_get(config, "enabled") is not True or service is None or _get(service, "instance") is None
                        or _get(service, "type") not in ("qbittorrent", "transmission")):
                    complete = False
                    continue
                rows, error = self._inventory(service.instance, "tasks")
                if error is not False or not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
                    complete = False
                    continue
                total += len(rows)
                if total > self.max_tasks:
                    return False, None
                for row in rows:
                    if self.stop_requested():
                        return False, None
                    task_hash = _get(row, "hash") or _get(row, "hash_string") or _get(row, "hashString")
                    base = _path(_get(row, "save_path") or _get(row, "download_dir") or _get(row, "downloadDir"))
                    content = _get(row, "content_path")
                    if not content and base is not None and isinstance(_get(row, "name"), str):
                        content = str(base / _get(row, "name"))
                    content = _path(content)
                    if not isinstance(task_hash, str) or not task_hash or base is None or content is None:
                        complete = False
                        continue
                    own = service_name == downloader and task_hash.lower() == str(download_hash).lower()
                    if not own and not any(_within(path, content) or _within(content, path) for path in targets):
                        continue
                    queries += 1
                    if queries > 32:
                        return False, None
                    files = self._inventory(service.instance, "files", task_hash)
                    if not isinstance(files, Sequence) or isinstance(files, (str, bytes)) or not files or len(files) > 10000:
                        complete = False
                        continue
                    for file in files:
                        relative = _get(file, "name")
                        if (not isinstance(relative, str) or not relative or relative.startswith("/") or "\x00" in relative
                                or any(part in (".", "..") for part in relative.split("/"))):
                            complete = False
                            continue
                        if base / relative not in targets:
                            continue
                        reason = _task_lifecycle(_get(service, "type"), row)
                        length, completed, progress = _get(file, "size", _get(file, "length")), _get(file, "completed"), _get(file, "progress")
                        full = (type(length) is int and length > 0 and
                                (type(completed) is int and completed == length or type(progress) in (int, float) and progress == 1))
                        if reason or not full:
                            self.activity_reason = reason or "task_download_incomplete"
                            return complete, True
            return complete, False if complete else None
        except Exception:
            return False, None
        finally:
            self._preview_mode = previous

    def plan_purge_cached(self, download_hash, source_path, size, *, downloader=None):
        previous = self._preview_mode
        self._preview_mode = True
        try:
            return self.plan_purge(download_hash, source_path, size, downloader=downloader)
        finally:
            self._preview_mode = previous

    def check(self, download_hash, paths, *, downloader=None):
        try:
            configs = self.helper.get_configs(include_disabled=True)
            if not isinstance(configs, dict) or not configs or len(configs) > self.max_services:
                return False, None
            target_paths = tuple(_path(p) for p in paths)
            if not download_hash or any(p is None for p in target_paths):
                return False, None
            complete, present, total, file_queries = True, False, 0, 0
            for service_name, config in configs.items():
                if self.stop_requested():
                    return False, None
                service = self.helper.get_service(name=service_name)
                if (_get(config, "enabled") is not True or service is None
                        or _get(service, "type") not in ("qbittorrent", "transmission")
                        or _get(service, "instance") is None):
                    complete = False
                    continue
                rows, error = self._inventory(service.instance, "tasks")
                if error is not False or not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
                    complete = False
                    continue
                total += len(rows)
                if total > self.max_tasks:
                    return False, True if present else None
                for row in rows:
                    task_hash = _get(row, "hash") or _get(row, "hash_string") or _get(row, "hashString")
                    content = _get(row, "content_path")
                    if not content:
                        base = _get(row, "save_path") or _get(row, "download_dir") or _get(row, "downloadDir")
                        name = _get(row, "name")
                        if isinstance(base, str) and isinstance(name, str):
                            content = str(PurePosixPath(base) / name)
                    task_path = _path(content)
                    if not isinstance(task_hash, str) or not task_hash or task_path is None:
                        complete = False
                    if (isinstance(task_hash, str) and task_hash.lower() == download_hash.lower()
                            and (downloader is None or service_name == downloader)):
                        present = True
                        continue
                    if task_path in target_paths:
                        present = True
                        continue
                    if task_path and any(_within(p, task_path) or _within(task_path, p) for p in target_paths):
                        # Different episodes often share a torrent's content root.
                        # A directory prefix is only a reason to inspect its files.
                        file_queries += 1
                        if file_queries > 32 or not task_hash:
                            return False, True if present else None
                        base = _path(_get(row, "save_path") or _get(row, "download_dir") or _get(row, "downloadDir"))
                        files = self._inventory(service.instance, "files", task_hash)
                        if (base is None or not isinstance(files, Sequence) or isinstance(files, (str, bytes))
                                or not files or len(files) > 10000):
                            complete = False
                            continue
                        names = set()
                        for file in files:
                            name = _get(file, "name")
                            if (not isinstance(name, str) or not name or name.startswith("/")
                                    or "\x00" in name or any(p in (".", "..") for p in name.split("/"))
                                    or name in names):
                                complete = False
                                continue
                            names.add(name)
                            if base / name in target_paths:
                                present = True
            return complete, present if (complete or present) else None
        except Exception:
            return False, None

    def plan_purge(self, download_hash, source_path, size, *, downloader=None):
        """Enumerate all summaries and exact files of every potentially shared task."""
        try:
            source = _path(source_path)
            if source is None or not _torrent_hash(download_hash) or (size is not None and (type(size) is not int or size <= 0)):
                return PurgePlan(reason="invalid_source_identity")
            configs = self.helper.get_configs(include_disabled=True)
            if not isinstance(configs, dict) or not configs or len(configs) > self.max_services:
                return PurgePlan()
            tasks, total, file_count, file_queries = [], 0, 0, 0
            for service_name, config in configs.items():
                if self.stop_requested():
                    return PurgePlan(reason="cleanup_cancelled")
                service = self.helper.get_service(name=service_name)
                if (_get(config, "enabled") is not True or service is None
                        or _get(service, "type") not in ("qbittorrent", "transmission")
                        or _get(service, "instance") is None):
                    return PurgePlan()
                rows, error = self._inventory(service.instance, "tasks")
                if error is not False or not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
                    return PurgePlan()
                total += len(rows)
                if total > self.max_tasks:
                    return PurgePlan(reason="task_inventory_limit")
                seen = set()
                for row in rows:
                    if self.stop_requested():
                        return PurgePlan(reason="cleanup_cancelled")
                    task_hash = _get(row, "hash") or _get(row, "hash_string") or _get(row, "hashString")
                    base = _path(_get(row, "save_path") or _get(row, "download_dir") or _get(row, "downloadDir"))
                    if not _torrent_hash(task_hash) or base is None or task_hash.lower() in seen:
                        return PurgePlan()
                    seen.add(task_hash.lower())
                    content = _get(row, "content_path")
                    if not content:
                        name = _get(row, "name")
                        if (not isinstance(name, str) or not name or name.startswith("/")
                                or "\x00" in name or any(part in (".", "..") for part in name.split("/"))):
                            return PurgePlan(reason="task_path_ambiguous")
                        content = str(base / name)
                    content = _path(content)
                    if content is None or not _within(content, base):
                        return PurgePlan(reason="task_path_ambiguous")
                    same_hash = task_hash.lower() == download_hash.lower() and (downloader is None or service_name == downloader)
                    if not same_hash and not (_within(source, content) or _within(content, source)):
                        continue  # The native content summary proves no overlap.
                    file_queries += 1
                    if file_queries > 32:
                        return PurgePlan(reason="file_query_limit")
                    files = self._inventory(service.instance, "files", task_hash)
                    if not isinstance(files, Sequence) or isinstance(files, (str, bytes)) or not files:
                        return PurgePlan(reason="file_inventory_incomplete")
                    file_count += len(files)
                    if file_count > 10000:
                        return PurgePlan(reason="file_inventory_limit")
                    paths, matched = set(), None
                    for file in files:
                        name = _get(file, "name")
                        if (not isinstance(name, str) or not name or name.startswith("/") or "\x00" in name
                                or any(p in (".", "..") for p in name.split("/"))):
                            return PurgePlan(reason="file_path_ambiguous")
                        full = base / name
                        if full in paths:
                            return PurgePlan(reason="file_path_ambiguous")
                        paths.add(full)
                        if full == source:
                            matched = file
                    if not same_hash and source in paths:
                        return PurgePlan(reason="source_shared_by_task")
                    if same_hash:
                        lifecycle_reason = _task_lifecycle(_get(service, "type"), row)
                        if lifecycle_reason:
                            return PurgePlan(reason=lifecycle_reason)
                        if len(files) != 1 or matched is None:
                            return PurgePlan(reason="torrent_not_exclusive_single_file")
                        length = _get(matched, "size", _get(matched, "length"))
                        completed, progress = _get(matched, "completed"), _get(matched, "progress")
                        file_complete = (type(completed) is int and completed == length
                                         or type(progress) in (int, float) and progress == 1)
                        if type(length) is not int or length <= 0 or (size is not None and length != size) or not file_complete:
                            return PurgePlan(reason="torrent_file_incomplete")
                        tasks.append(PurgeTask(service_name, task_hash.lower(), str(source), length))
            return PurgePlan(True, tuple(sorted(tasks, key=lambda task: (task.service, task.download_hash))), "")
        except Exception:
            return PurgePlan()

    def remove_exclusive_tasks(self, download_hash, source_path, size, expected, *, downloader=None):
        """Remove only freshly revalidated tasks; success requires disappearance."""
        self.last_removed = ()
        if not isinstance(expected, PurgePlan) or not expected.complete:
            return False
        remaining = expected.tasks
        try:
            for task in expected.tasks:
                if self.stop_requested():
                    return False
                fresh = self.plan_purge(download_hash, source_path, size, downloader=downloader)
                if not fresh.complete or fresh.tasks != remaining:
                    return False
                service = self.helper.get_service(name=task.service)
                if service is None or service.instance.delete_torrents(delete_file=False, ids=[task.download_hash]) is not True:
                    return False
                remaining = tuple(item for item in remaining if item != task)
                after = self.plan_purge(download_hash, source_path, size, downloader=downloader)
                if not after.complete or after.tasks != remaining:
                    return False
                self.last_removed += (task,)
                if self._batch_cache is not None:
                    self._batch_cache.clear()
            final = self.plan_purge(download_hash, source_path, size, downloader=downloader)
            return final.complete and not final.tasks
        except Exception:
            return False


def _emby_json(service, endpoint, params):
    """Reuse the configured Emby instance and host HTTP utility, never follow redirects.

    Emby's get_data() cannot disable redirects. Its configured instance provides
    the same origin/auth material here, with the token confined to a header.
    """
    from app.adapters.network.http import RequestUtils
    client = _get(service, "instance")
    host, token = _get(client, "_host"), _get(client, "_apikey")
    if not isinstance(host, str) or not isinstance(token, str) or not token:
        raise ValueError("media_client_unavailable")
    parsed = urlsplit(host)
    if (parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.username
            or parsed.password or parsed.query or parsed.fragment or endpoint != "Items"):
        raise ValueError("media_origin_invalid")
    url = host.rstrip("/") + "/emby/Items"
    reader = RequestUtils(headers={"X-Emby-Token": token, "Accept": "application/json"},
                          timeout=8, verify=True)
    with reader.get_stream(url, params=params, allow_redirects=False, raise_exception=True) as response:
        if response is None or response.status_code != 200:
            raise ValueError("media_request_failed")
        body = bytearray()
        for block in response.iter_content(chunk_size=16384):
            body.extend(block)
            if len(body) > 2 * 1024 * 1024:
                raise ValueError("media_response_too_large")
        return json.loads(body)


class EmbyExactProbe:
    """Exact provider identity + complete server-side episode enumeration."""

    def __init__(self, helper, *, read_json=_emby_json, page_size=100, max_items=2000):
        self.helper, self.read_json = helper, read_json
        self.page_size = min(max(1, page_size), 100)
        self.max_items = min(max(1, max_items), 2000)

    def _items(self, service, params):
        rows, seen, expected, requests = [], set(), None, 0
        while True:
            requests += 1
            if requests > (self.max_items + self.page_size - 1) // self.page_size + 1:
                raise ValueError("media_page_limit")
            payload = self.read_json(service, "Items", dict(params, StartIndex=len(rows), Limit=self.page_size))
            if not isinstance(payload, dict):
                raise ValueError("invalid_media_page")
            total, page = payload.get("TotalRecordCount"), payload.get("Items")
            if (type(total) is not int or total < 0 or total > self.max_items
                    or not isinstance(page, list) or len(page) > self.page_size
                    or (expected is not None and total != expected)):
                raise ValueError("incomplete_media_page")
            expected = total
            for item in page:
                item_id = _get(item, "Id")
                if not isinstance(item, dict) or not isinstance(item_id, str) or not item_id or item_id in seen:
                    raise ValueError("ambiguous_media_item")
                rows.append(item)
                seen.add(item_id)
            if len(rows) == total:
                return rows
            if not page or len(rows) > total:
                raise ValueError("truncated_media_page")

    def _snapshot(self, candidate, *, for_source_purge=False):
        try:
            provider = {"themoviedb": "tmdb", "tmdb": "tmdb", "thetvdb": "tvdb", "tvdb": "tvdb",
                        "imdb": "imdb"}.get(candidate.media_source)
            if provider is None or not candidate.media_id:
                return Presence.UNKNOWN, False, ()
            is_tv = candidate.media_type in ("tv", "电视剧")
            if not is_tv and candidate.media_type not in ("movie", "电影"):
                return Presence.UNKNOWN, False, ()
            if is_tv and (type(candidate.season) is not int or not candidate.episodes):
                return Presence.UNKNOWN, False, ()
            configs = self.helper.get_configs(include_disabled=True)
            if not isinstance(configs, dict) or not configs or len(configs) > 16:
                return Presence.UNKNOWN, False, ()
            present_episodes = set()
            for name, config in configs.items():
                service = self.helper.get_service(name=name)
                if _get(config, "enabled") is not True or _get(config, "type") != "emby" or service is None:
                    return Presence.UNKNOWN, False, ()
                kind = "Series" if is_tv else "Movie"
                series = self._items(service, {
                    "IncludeItemTypes": kind, "Recursive": "true",
                    "AnyProviderIdEquals": provider + "." + str(candidate.media_id),
                    "Fields": "ProviderIds,Path", "EnableTotalRecordCount": "true",
                })
                if for_source_purge and not series:
                    # A missing provider mapping is not proof that the user
                    # deleted the physical source's former library item.
                    return Presence.UNKNOWN, False, ()
                server_season_episodes = set()
                for item in series:
                    providers = item.get("ProviderIds")
                    if not isinstance(providers, dict) or item.get("Type") != kind:
                        return Presence.UNKNOWN, False, ()
                    normalized = {str(k).lower(): str(v) for k, v in providers.items()}
                    if normalized.get(provider) != str(candidate.media_id):
                        return Presence.UNKNOWN, False, ()
                    if not is_tv:
                        return Presence.PRESENT, True, ()
                    episodes = self._items(service, {
                        "ParentId": item["Id"], "IncludeItemTypes": "Episode", "Recursive": "true",
                        "Fields": "Path,ProviderIds,SeriesId,ParentIndexNumber,IndexNumber,IndexNumberEnd",
                        "EnableTotalRecordCount": "true",
                    })
                    for episode in episodes:
                        season, start = episode.get("ParentIndexNumber"), episode.get("IndexNumber")
                        end = episode.get("IndexNumberEnd", start)
                        if end is None:
                            end = start
                        if (episode.get("Type") != "Episode" or episode.get("SeriesId") != item["Id"]
                                or type(season) is not int or type(start) is not int or type(end) is not int
                                or start < 0 or end < start or end - start > 2000
                                or not isinstance(episode.get("Path"), str) or not episode.get("Path")
                                or episode.get("IsVirtualItem") is True):
                            return Presence.UNKNOWN, False, ()
                        if season == candidate.season:
                            present_episodes.update(range(start, end + 1))
                            server_season_episodes.update(range(start, end + 1))
                            if len(present_episodes) > 2000:
                                return Presence.UNKNOWN, False, ()
                if for_source_purge and is_tv and not server_season_episodes:
                    return Presence.UNKNOWN, False, ()
            presence = Presence.PRESENT if set(candidate.episodes).intersection(present_episodes) else Presence.MISSING
            return presence, True, tuple(sorted(present_episodes))
        except Exception:
            return Presence.UNKNOWN, False, ()

    def check(self, candidate):
        presence, complete, _episodes = self._snapshot(candidate)
        return presence, complete

    def check_purge(self, candidate):
        # Movies whose exact item is gone need a trusted historical item ID or
        # authenticated deletion mapping; this adapter does not invent one.
        presence, complete, _episodes = self._snapshot(candidate, for_source_purge=True)
        return presence, complete

    def season_episodes(self, candidate):
        if candidate.media_type not in ("tv", "电视剧"):
            return (), False
        _presence, complete, episodes = self._snapshot(candidate)
        return episodes, complete


def _record(candidate):
    file_id = candidate.file_id
    if type(file_id) is not int or file_id <= 0 or not isinstance(candidate.revision, str) or not candidate.revision:
        raise ValueError("invalid_candidate")
    fingerprint = hashlib.sha256((str(candidate.download_hash) + "\0" + str(candidate.source_path)).encode()).hexdigest()
    return CleanupRecord(
        key="download_file:" + str(file_id) + ":" + fingerprint,
        revision=candidate.revision, record_id=file_id, source_path=candidate.source_path,
        destination_paths=tuple(candidate.destination_paths), media_source=candidate.media_source,
        media_id=candidate.media_id, media_type=candidate.media_type, season=candidate.season,
        episodes=tuple(candidate.episodes), storage=candidate.storage,
    )


def _file_record(candidate):
    fingerprint = hashlib.sha256((str(candidate.download_hash) + "\0" + str(candidate.source_path)).encode()).hexdigest()
    return CleanupRecord(key="download_file_only:" + str(candidate.file_id) + ":" + fingerprint,
        revision=candidate.revision, record_id=candidate.file_id, source_path=candidate.source_path,
        destination_paths=(), media_source="", media_id="", media_type="", storage=candidate.storage,
        kind="download_file_only", download_hash=candidate.download_hash, downloader=candidate.downloader)


def _merge_presence(values):
    if Presence.PRESENT in values:
        return Presence.PRESENT
    return Presence.UNKNOWN if Presence.UNKNOWN in values else Presence.MISSING


class MoviePilotCleanupAdapter:
    def __init__(self, gateway, paths, downloaders, media):
        self.gateway, self.paths, self.downloaders, self.media = gateway, paths, downloaders, media

    def reconcile_batch(self, state, *, persist, now=None, dry_run=True, options=None, stop_requested=None):
        from .reconcile import reconcile_batch
        return reconcile_batch(self, state, persist=persist, now=now, dry_run=dry_run,
                               options=options, stop_requested=stop_requested)

    def list_file_only_candidates(self, *, cursor=0, limit=100):
        if type(cursor) is not int or cursor < 0 or type(limit) is not int or not 1 <= limit <= 100:
            return CandidatePage(reason="invalid_cursor_or_limit")
        try:
            rows = self.gateway.list_file_only_candidates(after_id=cursor, limit=limit)
            if not isinstance(rows, (list, tuple)) or len(rows) > limit:
                raise ValueError("candidate_page_unbounded")
            records, previous = [], cursor
            for candidate in rows:
                if type(candidate.file_id) is not int or candidate.file_id <= previous:
                    raise ValueError("candidate_page_unordered")
                previous = candidate.file_id
                records.append(_file_record(candidate))
            return CandidatePage(tuple(records), previous if len(rows) == limit else 0, True)
        except Exception:
            return CandidatePage(cursor=cursor, reason="candidate_read_failed")

    def observe_file_only(self, record, *, fresh=False):
        if record.kind != "download_file_only":
            return CleanupEvidence(record_present=None)
        try:
            current = self.gateway.get_file_only_candidate(record.record_id)
            if current is None:
                return CleanupEvidence(record_present=False)
            if _file_record(current) != record:
                return CleanupEvidence(revision=current.revision, identity_verified=False)
            source = self.paths.check(record.source_path)
            evidence = CleanupEvidence(source=source.presence, storage_ready=source.ready,
                revision=current.revision, record_present=True,
                identity_verified=_get(current, "identity_verified") is True and not _get(current, "review_reason"))
            if source.presence is not Presence.MISSING or not source.ready:
                return evidence
            protection = self.gateway.get_file_only_protection(current)
            activity = getattr(self.downloaders, "check_activity", None)
            if activity:
                complete, present = activity(current.download_hash, (record.source_path,), current.downloader, cached=not fresh)
            else:
                check = self.downloaders.check if fresh else getattr(self.downloaders, "check_cached", self.downloaders.check)
                complete, present = check(current.download_hash, (record.source_path,))
            return replace(evidence, downloaders_complete=complete is True,
                           downloader_task_present=present, transfer_task_active=_get(protection, "transfer_task_active"),
                           blocking_reason=getattr(self.downloaders, "activity_reason", "") if present else "")
        except Exception:
            return CleanupEvidence(record_present=None)

    def mutate_file_only(self, record, evidence, options):
        fresh = self.observe_file_only(record, fresh=True)
        decision = file_only_decision(record, fresh)
        if decision:
            return MutationResult("already_clean" if decision[0] == "already_clean" else "needs_review", decision[1])
        # Inventory calls can be slow; absence must still be true at commit.
        path = self.paths.check(record.source_path)
        if not path.ready or path.presence is not Presence.MISSING:
            return MutationResult("needs_review", "source_reappeared")
        try:
            current = self.gateway.get_file_only_candidate(record.record_id)
            if current is None or _file_record(current) != record:
                return MutationResult("needs_review", "record_changed")
            result = self.gateway.apply_file_only(current, fresh)
            return result if isinstance(result, MutationResult) else MutationResult("needs_review", "host_retained_record")
        except Exception:
            return MutationResult("needs_review", "host_cleanup_failed")

    def list_candidates(self, *, cursor=0, limit=100):
        if type(cursor) is not int or cursor < 0 or type(limit) is not int or not 1 <= limit <= 100:
            return CandidatePage(cursor=0, reason="invalid_cursor_or_limit")
        try:
            candidates = self.gateway.list_cleanup_candidates(after_id=cursor, limit=limit)
            if not isinstance(candidates, (list, tuple)) or len(candidates) > limit:
                raise ValueError("candidate_page_unbounded")
            records, previous = [], cursor
            for candidate in candidates:
                if type(candidate.file_id) is not int or candidate.file_id <= previous:
                    raise ValueError("candidate_page_unordered")
                previous = candidate.file_id
                records.append(_record(candidate))
            return CandidatePage(tuple(records), previous if len(candidates) == limit else 0, True)
        except Exception:
            return CandidatePage(cursor=cursor, reason="candidate_read_failed")

    def _observe(self, record, *, fast_mode=None, for_source_purge=False):
        try:
            current = self.gateway.get_cleanup_candidate(record.record_id)
            if current is None:
                return None, CleanupEvidence(record_present=False), ()
            current_record = _record(current)
            if current_record != record:
                return current, CleanupEvidence(revision=current.revision, identity_verified=False), ()
            source = self.paths.check(record.source_path)
            destinations = tuple(self.paths.check(path) for path in record.destination_paths)
            local = CleanupEvidence(
                source=source.presence, destinations=tuple(item.presence for item in destinations),
                source_signature=source.signature,
                storage_ready=all(item.ready for item in (source,) + destinations),
                destination_lookup_complete=_get(current, "destination_lookup_complete") is True,
                identity_verified=_get(current, "identity_verified") is True and not _get(current, "review_reason"),
                revision=current.revision, record_present=True,
            )
            if fast_mode is not None:
                if fast_mode == "destination":
                    transfer_id = _get(current, "transfer_id")
                    if type(transfer_id) is not int or transfer_id <= 0 or len(record.destination_paths) != 1:
                        return current, replace(local, identity_verified=False), ()
                # Existence safely rules out record cleanup before remote calls.
                # Unknown storage cannot become missing by querying other services.
                source_blocks = (fast_mode == "missing" and source.presence is Presence.PRESENT
                                 or fast_mode == "destination" and source.presence is Presence.MISSING)
                if (source_blocks or any(item.presence is Presence.PRESENT for item in destinations)
                        or any(item.presence is Presence.UNKNOWN for item in (source,) + destinations)):
                    return current, local, ()
            protection = self.gateway.get_cleanup_protection(current, limit=500)
            other_paths = _get(protection, "other_version_paths", ())
            other_complete = _get(protection, "other_versions_complete") is True
            if not isinstance(other_paths, (list, tuple)) or len(other_paths) > 500:
                other_paths, other_complete = (), False
            other_paths = tuple(other_paths)
            versions = tuple(self.paths.check(path) for path in other_paths)
            storage_ready = all(item.ready for item in (source,) + destinations + versions)
            all_paths = (record.source_path,) + record.destination_paths + other_paths
            downloaders_complete, task_present = self.downloaders.check(current.download_hash, all_paths)
            media_presence, media_complete = (self.media.check_purge(current) if for_source_purge
                                              else self.media.check(current))
            evidence = CleanupEvidence(
                source=source.presence, destinations=tuple(item.presence for item in destinations),
                source_signature=source.signature,
                other_versions=_merge_presence(tuple(item.presence for item in versions)) if other_complete else Presence.UNKNOWN,
                media_library=media_presence, media_lookup_complete=media_complete is True,
                destination_lookup_complete=_get(current, "destination_lookup_complete") is True,
                downloaders_complete=downloaders_complete is True, downloader_task_present=task_present,
                transfer_task_active=_get(protection, "transfer_task_active"), storage_ready=storage_ready,
                identity_verified=_get(current, "identity_verified") is True and not _get(current, "review_reason"),
                revision=current.revision, record_present=True,
            )
            return current, evidence, other_paths
        except Exception:
            return None, CleanupEvidence(record_present=None), ()

    def observe(self, record):
        return self._observe(record, fast_mode="missing")[1]

    def _observe_destination(self, record, *, fast=False):
        current, evidence, other_paths = self._observe(
            record, fast_mode="destination" if fast else None, for_source_purge=True)
        # A destination-only repair must identify one exact legacy transfer.
        # The host command rechecks its durable-task status inside the CAS.
        transfer_id = _get(current, "transfer_id")
        if type(transfer_id) is not int or transfer_id <= 0 or len(record.destination_paths) != 1:
            evidence = replace(evidence, identity_verified=False)
        return current, evidence, other_paths

    def observe_destination(self, record):
        return self._observe_destination(record, fast=True)[1]

    def _source_purge_ready(self, record, signature):
        current, fresh, other_paths = self._observe_destination(record)
        safe = (
            current is not None and fresh.revision == record.revision and fresh.record_present is True
            and fresh.identity_verified and fresh.storage_ready and fresh.destination_lookup_complete
            and fresh.source is Presence.PRESENT and len(fresh.destinations) == 1
            and all(item is Presence.MISSING for item in fresh.destinations)
            and fresh.other_versions is Presence.MISSING and fresh.media_library is Presence.MISSING
            and fresh.media_lookup_complete and fresh.downloaders_complete
            and type(fresh.downloader_task_present) is bool and fresh.transfer_task_active is False
            and _exclusive_signature(fresh.source_signature)
            and tuple(fresh.source_signature) == tuple(signature)
        )
        if not safe:
            return None, fresh
        # Ownership checks intentionally run only at the physical mutation gate.
        # Normal 10k-row preview scans do not incur these global DB lookups.
        ownership = self.gateway.get_cleanup_protection(current, limit=500, include_source_ownership=True)
        if (not (_get(ownership, "source_exclusive") is True and _get(ownership, "hash_exclusive") is True)
                or _get(ownership, "transfer_task_active") is not False
                or _get(ownership, "other_versions_complete") is not True
                or tuple(_get(ownership, "other_version_paths", ())) != other_paths):
            return None, fresh
        return current, fresh

    def purge_source(self, record, evidence, options):
        """After 24h stable proof, remove exclusive tasks/file, then exact history.

        Task removal never requests data deletion. Partial failure preserves DB
        facts; after an unlink, the ordinary full-missing pass can finish them.
        """
        if options.preserve_source_tasks is not False:
            return MutationResult("needs_review", "source_purge_not_authorized")
        signature = _get(evidence, "source_signature", ())
        if (not isinstance(evidence, CleanupEvidence) or evidence.revision != record.revision
                or not _exclusive_signature(signature)):
            return MutationResult("needs_review", "source_signature_unverified")
        signature = tuple(signature)
        try:
            current, _fresh = self._source_purge_ready(record, signature)
            if current is None:
                return MutationResult("needs_review", "source_purge_evidence_changed")
            plan = self.downloaders.plan_purge(current.download_hash, record.source_path, signature[3])
            if not plan.complete:
                return MutationResult("needs_review", plan.reason)
            # Building a full file inventory can take time; recheck media and
            # ownership before issuing the first destructive task command.
            current, _fresh = self._source_purge_ready(record, signature)
            if current is None:
                return MutationResult("needs_review", "source_purge_evidence_changed")
            if not self.downloaders.remove_exclusive_tasks(
                    current.download_hash, record.source_path, signature[3], plan):
                return MutationResult("needs_review", "downloader_remove_unconfirmed")
            current, fresh = self._source_purge_ready(record, signature)
            if current is None or fresh.downloader_task_present is not False:
                return MutationResult("needs_review", "source_purge_evidence_changed")
            if not self.paths.unlink_exact(record.source_path, signature):
                return MutationResult("needs_review", "source_unlink_unconfirmed")
            # Emby deletion must never restart a subscription's downloads.
            result = self.mutate(record, fresh, replace(options, allow_subscription_retry=False))
            if result.status in ("applied", "already_clean"):
                return MutationResult(result.status, "source_purged")
            return MutationResult("needs_review", "source_purged_history_pending")
        except Exception:
            return MutationResult("needs_review", "source_purge_failed")

    def repair_destination(self, record, evidence=None, options=None):
        """Repair only a stale transfer record after a separate stability window.

        The caller must persist two observations at least 24 hours apart under a
        separate state namespace. Source files and downloader tasks are retained.
        """
        current, fresh, other_paths = self._observe_destination(record)
        safe = (
            current is not None and fresh.revision == record.revision and fresh.record_present is True
            and fresh.identity_verified and fresh.storage_ready and fresh.destination_lookup_complete
            and fresh.source == Presence.PRESENT and len(fresh.destinations) == 1
            and all(p == Presence.MISSING for p in fresh.destinations)
            and fresh.other_versions == Presence.MISSING and fresh.media_library == Presence.MISSING
            and fresh.media_lookup_complete and fresh.downloaders_complete
            and (fresh.downloader_task_present is True or fresh.downloader_task_present is False)
            and fresh.transfer_task_active is False
        )
        if not safe:
            return MutationResult("needs_review", "live_destination_evidence_changed")
        try:
            result = self.gateway.repair(current, fresh, other_paths)
            if not isinstance(result, MutationResult) or result.status not in ("applied", "already_clean"):
                return MutationResult("needs_review", "host_retained_transfer")
            return result
        except Exception:
            return MutationResult("needs_review", "host_destination_repair_failed")

    def mutate(self, record, evidence, options):
        # The engine enforces the persistent stability period. Recheck all live
        # evidence here too; never forward the engine's old observation as proof.
        options = replace(options, allow_subscription_retry=False)
        current, fresh, other_paths = self._observe(record)
        safe = (
            current is not None and fresh.revision == record.revision and fresh.record_present is True
            and fresh.identity_verified and fresh.storage_ready and fresh.destination_lookup_complete
            and fresh.source == Presence.MISSING and all(p == Presence.MISSING for p in fresh.destinations)
            and fresh.other_versions == Presence.MISSING and fresh.media_library == Presence.MISSING
            and fresh.media_lookup_complete and fresh.downloaders_complete
            and fresh.downloader_task_present is False and fresh.transfer_task_active is False
        )
        if not safe:
            return MutationResult("needs_review", "live_evidence_changed")
        try:
            resolved_lack = None
            retry_needed = _get(current, "subscription_retry_needed")
            if options.allow_subscription_retry is True and type(retry_needed) is not bool:
                return MutationResult("needs_review", "subscription_retry_state_unknown")
            # A different registered version is cleaned independently.  Its
            # absence was proven above, but subscription progress is reopened
            # only when the final registered version is gone.
            retry_subscription = (
                options.allow_subscription_retry is True and retry_needed is True
                and not other_paths
            )
            if retry_subscription:
                episodes, complete = self.media.season_episodes(current)
                if complete is not True or set(record.episodes).intersection(episodes):
                    return MutationResult("needs_review", "season_evidence_incomplete")
                resolved_lack = self.gateway.resolve_lack(current, episodes)
                if type(resolved_lack) is not int or resolved_lack < 0:
                    return MutationResult("needs_review", "subscription_progress_unresolved")
            result = self.gateway.apply(current, fresh, other_paths,
                                        allow_subscription_retry=retry_subscription,
                                        resolved_lack_episode=resolved_lack)
            status = _get(result, "status")
            if status not in ("applied", "already_clean"):
                return MutationResult("needs_review", "host_retained_record")
            return MutationResult(status, str(_get(result, "reason", "host_confirmed")))
        except Exception:
            return MutationResult("needs_review", "host_cleanup_failed")

    @staticmethod
    def webhook_hint(payload):
        # Never accept paths, IDs or claims of absence from webhook payloads.
        if not isinstance(payload, dict):
            return False
        event = payload.get("Event") or payload.get("event")
        return event in ("library.itemremoved", "library.deleted", "Library.ItemRemoved")


class MoviePilotGateway:
    """Late binding avoids importing a second runtime or touching a raw DB."""

    def __init__(self):
        from . import host_cleanup
        from .host_cleanup import CleanupProof
        self.module, self.proof_type = host_cleanup, CleanupProof

    def list_cleanup_candidates(self, **kwargs):
        return self.module.list_cleanup_candidates(**kwargs)

    def get_cleanup_candidate(self, file_id):
        return self.module.get_cleanup_candidate(file_id)

    def list_file_only_candidates(self, **kwargs):
        from .host_db import list_file_only_candidates
        return list_file_only_candidates(**kwargs)

    def get_file_only_candidate(self, file_id):
        from .host_db import get_file_only_candidate
        return get_file_only_candidate(file_id)

    def get_file_only_protection(self, candidate):
        from .host_db import get_file_only_protection
        return get_file_only_protection(candidate)

    def apply_file_only(self, candidate, evidence):
        from .host_db import apply_file_only
        return apply_file_only(candidate, evidence)

    def prepare_receipt(self, file_id):
        from .host_db import prepare_receipt
        return prepare_receipt(file_id)

    def receipt_view(self, receipt):
        from .host_db import receipt_view
        return receipt_view(receipt)

    def apply_receipt(self, receipt, proof):
        from .host_db import apply_receipt
        return apply_receipt(receipt, proof)

    def source_ownership(self, receipt, source):
        from .host_db import source_ownership
        return source_ownership(receipt, source)

    def apply_transfer_scope(self, receipt, source, proof):
        from .host_db import apply_transfer_scope
        return apply_transfer_scope(receipt, source, proof)

    def list_orphan_receipts(self, **kwargs):
        from .host_db import list_orphan_receipts
        return list_orphan_receipts(**kwargs)

    def propose_receipt_refresh(self, receipt):
        from .host_db import propose_receipt_refresh
        return propose_receipt_refresh(receipt)

    def get_cleanup_protection(self, candidate, **kwargs):
        return self.module.get_cleanup_protection(candidate, **kwargs)

    def resolve_lack(self, candidate, present_episodes):
        return self.module.resolve_cleanup_lack_episode(
            candidate, tuple(present_episodes), season_lookup_complete=True,
        )

    def _proof(self, candidate, evidence, other_paths):
        fields = dict(
            revision=evidence.revision, source_path=candidate.source_path,
            destination_paths=tuple(candidate.destination_paths), other_version_paths=tuple(other_paths),
            source=evidence.source.value, destinations=tuple(item.value for item in evidence.destinations),
            other_versions=evidence.other_versions.value, media_library=evidence.media_library.value,
            destination_lookup_complete=evidence.destination_lookup_complete,
            media_lookup_complete=evidence.media_lookup_complete, downloaders_complete=evidence.downloaders_complete,
            downloader_task_present=evidence.downloader_task_present, transfer_task_active=evidence.transfer_task_active,
            storage_ready=evidence.storage_ready, identity_verified=evidence.identity_verified,
            record_present=evidence.record_present,
        )
        return self.proof_type(**fields)

    def apply(self, candidate, evidence, other_paths, *, allow_subscription_retry=False, resolved_lack_episode=None):
        result = self.module.apply_cleanup_candidate(
            candidate, self._proof(candidate, evidence, other_paths), allow_subscription_retry=allow_subscription_retry is True,
            resolved_lack_episode=resolved_lack_episode,
        )
        if _get(result, "status") == "cleaned" and _get(result, "file_marked_missing") is True:
            return MutationResult("applied", "host_confirmed")
        return MutationResult("needs_review", "host_retained_record")

    def repair(self, candidate, evidence, other_paths):
        result = self.module.repair_missing_destination(candidate, self._proof(candidate, evidence, other_paths))
        if _get(result, "status") == "cleaned" and _get(result, "transfer_deleted") is True:
            return MutationResult("applied", "host_confirmed_transfer_repair")
        return MutationResult("needs_review", "host_retained_transfer")


def configured_local_roots(helper):
    """Read the host's actual download AND library storage roots."""
    directories = helper.get_dirs()
    if not isinstance(directories, (tuple, list)) or len(directories) > 32:
        raise ValueError("directory_inventory_incomplete")
    roots = []
    for directory in directories:
        for storage_key, path_key in (("storage", "download_path"), ("library_storage", "library_path")):
            text = _get(directory, path_key)
            if _get(directory, storage_key) != "local" or not text:
                continue
            if _path(text) is None:
                raise ValueError("configured_root_invalid")
            if text not in roots:
                roots.append(text)
    if len(roots) > 32:
        raise ValueError("directory_inventory_incomplete")
    return tuple(roots)


def build_host_adapter(roots=()):
    from app.application.directory import DirectoryHelper
    from app.application.downloader import DownloaderHelper
    try:
        configured = configured_local_roots(DirectoryHelper())
        all_roots = tuple(dict.fromkeys(configured + tuple(roots)))
        if not configured or len(all_roots) > 32:
            raise ValueError("trusted_roots_incomplete")
        paths = LocalPathProbe.from_storage_paths(all_roots)
    except Exception:
        paths = LocalPathProbe(())
    return MoviePilotCleanupAdapter(
        MoviePilotGateway(), paths,
        DownloaderProbe(DownloaderHelper()), None,
    )
