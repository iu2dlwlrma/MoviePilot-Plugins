import os
import errno
import time
import fcntl
import hashlib
import json
import uuid
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any, List, Dict, Tuple, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType, Response


class EmptyFolderCleaner(_PluginBase):
    # 插件名称
    plugin_name = "清理大师"
    # 插件描述
    plugin_desc = "清理空文件夹，核对失效记录并清理媒体库已删除资源的下载源。"
    # 插件图标
    plugin_icon = "clean.png"
    # 插件版本
    plugin_version = "2.1.1"
    # 插件作者
    plugin_author = "oriecho"
    # 作者主页
    author_url = "https://github.com/iu2dlwlrma"
    # 插件配置项ID前缀
    plugin_config_prefix = "emptyfoldercleaner_"
    # 加载顺序
    plugin_order = 19
    # 可使用的用户级别
    auth_level = 2

    # 私有属性
    _scheduler = None
    # 开关
    _enabled = False
    _cron = None
    _onlyonce = False
    _target_dirs = None
    _notify = False
    _recursive = True
    _exclude_dirs = None
    _dry_run = False
    # 失效记录核对；沿用原插件身份和已有配置，升级时默认先预览。
    _records_enabled = False
    _records_cron = "*/5 * * * *"
    _records_dry_run = True
    _records_batch_size = 100
    _records_grace_hours = 24
    _records_reopen_subscription = False
    _loaded_config = None
    # 退出事件
    _event = Event()

    def init_plugin(self, config: dict = None):
        self.stop_service()
        self._event = Event()
        if not hasattr(self, "_job_guard"):
            self._job_guard = Lock()
            self._run_thread = None
            self._run_event = None
            self._run_event_id = None
        self._load_config(config or {})
        # Only the legacy host one-shot setting consumes this legacy flag.
        # The dedicated settings API never calls this path.
        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(self._sync_run, 'date',
                args=("all", deepcopy(self._loaded_config), self._event),
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3))
            self._onlyonce = False
            saved = deepcopy(self._loaded_config)
            saved["onlyonce"] = False
            self.update_config(saved)
            self._loaded_config = saved
            self._scheduler.start()

    def _load_config(self, config: dict):
        self._loaded_config = deepcopy(config)
        self._enabled = config.get("enabled") is True
        self._onlyonce = config.get("onlyonce") is True
        self._cron = config.get("cron")
        self._notify = config.get("notify") is True
        self._target_dirs = config.get("target_dirs")
        self._recursive = config.get("recursive", True) is not False
        self._exclude_dirs = config.get("exclude_dirs")
        self._dry_run = config.get("dry_run", False) is True
        self._records_enabled = config.get("records_enabled") is True
        self._records_cron = config.get("records_cron", "*/5 * * * *")
        self._records_dry_run = config.get("records_dry_run", True) is not False
        self._records_batch_size = self._bounded_int(config.get("records_batch_size"), 100, 1, 100)
        self._records_grace_hours = self._bounded_int(config.get("records_grace_hours"), 24, 0, 720)
        # Preserve the historical value for compatibility; it grants no permission.
        self._records_reopen_subscription = config.get("records_reopen_subscription") is True

    def get_state(self):
        return bool(self._enabled and (
            (self._cron and self._target_dirs) or
            (self._records_enabled and self._records_cron)
        ))

    @staticmethod
    def _bounded_int(value: Any, default: int, minimum: int, maximum: int) -> int:
        """将表单数字限制到安全范围；非法输入保持已有默认值。"""
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return min(maximum, max(minimum, parsed))

    @staticmethod
    def _build_record_adapter():
        # 未启用记录核对时无需加载宿主清理扩展，原空目录功能继续独立可用。
        from .host_adapter import build_host_adapter
        return build_host_adapter()

    def run_once(self):
        return self._sync_run("all")

    def _save_record_report(self, report: dict) -> None:
        report = deepcopy(report)
        if report.get("status") != "running":
            report.setdefault("snapshot_id", uuid.uuid4().hex)
            snapshots = self.get_data("cleanup_snapshots") or []
            snapshots = [item for item in snapshots if isinstance(item, dict)]
            snapshots.append({"snapshot_id": report["snapshot_id"],
                              "items": report.get("items", []),
                              "clean_time": report.get("clean_time"),
                              "dry_run": report.get("dry_run", True),
                              "status": report.get("status"),
                              "run_error": self._controlled_error(report.get("error"))})
            self.save_data("cleanup_snapshots", snapshots[-4:])
            history = self.get_data("records_history") or []
            history = [item for item in history if isinstance(item, dict)]
            history.append({key: value for key, value in report.items() if key != "items"})
            self.save_data("records_history", history[-20:])
        self.save_data("records_report", report)

    def _take_process_lock(self):
        directory = Path(self.get_data_path())
        directory.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(str(directory / "cleanup-master.lock"),
                             os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return descriptor
        except BlockingIOError:
            os.close(descriptor)
            return None
        except Exception:
            os.close(descriptor)
            raise

    @staticmethod
    def _release_process_lock(descriptor):
        if descriptor is not None:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)

    def _state_transaction(self, callback):
        directory = Path(self.get_data_path())
        directory.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(str(directory / "cleanup-master-state.lock"),
                             os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            return callback()
        finally:
            self._release_process_lock(descriptor)

    def _request_receipts(self):
        receipts = self.get_data("cleanup_requests") or []
        if (not isinstance(receipts, list) or len(receipts) > 50
                or any(not isinstance(item, dict) or not isinstance(item.get("request_id"), str)
                       or not isinstance(item.get("run_id"), str)
                       or item.get("scope") not in ("folders", "records", "all") for item in receipts)):
            raise ValueError("invalid_request_receipts")
        return receipts

    def _remember_request(self, request_id, run, started=False):
        receipts = [item for item in self._request_receipts() if item["request_id"] != request_id]
        receipts.append({"request_id": request_id, "run_id": run["run_id"], "scope": run["scope"],
                         "status": run["status"], "started": bool(started),
                         "created_at": run.get("started_at"), "finished_at": run.get("finished_at")})
        self._persist_checked("cleanup_requests", receipts[-50:])

    def _write_run_if_current(self, run):
        current = self.get_data("cleanup_run") or {}
        if current.get("run_id") == run["run_id"]:
            if run["status"] == "running" and current.get("status") == "cancelling":
                run["status"] = "cancelling"
            self._persist_checked("cleanup_run", run)
            receipts = self._request_receipts()
            changed = False
            for receipt in receipts:
                if receipt["run_id"] == run["run_id"]:
                    receipt.update(status=run["status"], finished_at=run.get("finished_at"))
                    if run["status"] in ("running", "cancelling"):
                        receipt["started"] = True
                    changed = True
            if changed:
                self._persist_checked("cleanup_requests", receipts)

    @staticmethod
    def _request_response(receipt, reused=True):
        not_started = receipt.get("status") == "failed" and receipt.get("started") is not True
        data = {"run_id": receipt["run_id"], "scope": receipt["scope"],
                "status": receipt.get("status"), "reused": reused, "not_started": not_started}
        return Response(success=not not_started, data=data,
                        message="该运行请求未能创建任务，未开始清理；可重新发起一次运行" if not_started else None)

    def _capture_config(self):
        # _load_config replaces this dictionary, never mutates the old one.
        return deepcopy(self._loaded_config or {})

    def _persist_checked(self, key, value):
        snapshot = deepcopy(value)
        self.save_data(key, snapshot)
        if self.get_data(key) != snapshot:
            raise OSError("cleanup_state_readback_failed")

    def _run_records_batch(self, stop_requested, config):
        now = time.time()
        dry_run = config.get("records_dry_run", True) is not False
        report = {"status": "running", "clean_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                  "started_at": now, "dry_run": dry_run,
                  "processed": 0, "items": [], "summary": {}, "counts": {}}
        try:
            from .record_cleanup import CleanupOptions
            state = self.get_data("records_state")
            if state is not None and (not isinstance(state, dict) or state.get("version") != 1):
                raise ValueError("invalid_record_state")
            options = CleanupOptions(stability_seconds=self._bounded_int(config.get("records_grace_hours"), 24, 0, 720) * 3600,
                                     batch_size=self._bounded_int(config.get("records_batch_size"), 100, 1, 100),
                                     preserve_source_tasks=True, allow_subscription_retry=False)
            result = self._build_record_adapter().reconcile_batch(
                deepcopy(state) if state is not None else {"version": 1},
                persist=lambda snapshot: self._persist_checked("records_state", snapshot),
                now=now, dry_run=dry_run, options=options,
                stop_requested=stop_requested)
            if not isinstance(result.report, dict):
                raise ValueError("invalid_cleanup_report")
            report.update(result.report)
        except Exception as error:
            logger.error(f"记录核对运行失败：{type(error).__name__}")
            report.update(status="failed", error="record_run_failed", error_type=type(error).__name__)
        report["finished_at"] = time.time()
        self._save_record_report(report)
        return report

    def clean_records(self):
        if not self._records_enabled:
            return {"status": "disabled"}
        return self._sync_run("records")

    def _new_run(self, scope, request_id=None):
        return {"run_id": uuid.uuid4().hex, "scope": scope, "status": "queued",
                "request_id": request_id, "started_at": time.time()}

    def _worker(self, run, event, descriptor, config):
        result = {}
        def stopped():
            return event.is_set() or self.get_data("cleanup_cancel") == run["run_id"]
        try:
            run["status"] = "running"
            self._state_transaction(lambda: self._write_run_if_current(run))
            scope = run["scope"]
            if scope in ("folders", "all") and config.get("target_dirs"):
                result["folders"] = self._clean_folders_batch(stopped, config)
            if scope == "folders" and not config.get("target_dirs"):
                result["folders"] = {"status": "failed", "reason": "folder_config_invalid"}
            if not stopped() and (scope == "records" or scope == "all" and config.get("records_enabled") is True):
                result["records"] = self._run_records_batch(stopped, config)
            statuses = {item.get("status") for item in result.values() if isinstance(item, dict)}
            run["status"] = ("cancelled" if stopped() or "cancelled" in statuses else
                             "failed" if "failed" in statuses else
                             "yielded" if "yielded" in statuses else
                             "review" if "review" in statuses else "completed")
        except Exception as error:
            logger.error(f"清理任务失败：{type(error).__name__}")
            run["status"] = "failed"
            result = {run["scope"]: {"status": "failed", "reason": "run_failed"}}
        finally:
            try:
                run["finished_at"] = time.time()
                self._state_transaction(lambda: self._write_run_if_current(run))
            except Exception as error:
                # The OS lock still releases; the durable running receipt is
                # recovered as interrupted on the next successful status read.
                logger.error(f"清理任务回执保存失败：{type(error).__name__}")
                result = {run["scope"]: {"status": "failed", "reason": "run_state_save_failed"}}
            finally:
                self._release_process_lock(descriptor)
        return result.get(run["scope"], result)

    def _sync_run(self, scope, config=None, stop_event=None):
        def prepare():
            descriptor = self._take_process_lock()
            if descriptor is None:
                return None
            run = self._new_run(scope)
            try:
                self._persist_checked("cleanup_run", run)
            except Exception:
                self._release_process_lock(descriptor)
                raise
            return run, descriptor
        prepared = self._state_transaction(prepare)
        if prepared is None:
            return {"status": "busy"}
        run, descriptor = prepared
        return self._worker(run, stop_event or self._event, descriptor,
                            deepcopy(config) if config is not None else self._capture_config())

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    @staticmethod
    def get_render_mode():
        return "vue", "dist/assets"

    def get_api(self) -> List[Dict[str, Any]]:
        # Dynamic bear routes only verify login. Match the original plugin
        # configuration/page boundary using the host's supported dependency hook.
        from fastapi import Depends
        from app.api.dependencies.auth import get_current_active_superuser
        return [{"path": path, "endpoint": endpoint, "methods": [method], "auth": "bear",
                 "dependencies": [Depends(get_current_active_superuser)]}
                for path, endpoint, method in (
                    ("/status", self.get_status, "GET"),
                    ("/settings", self.get_settings, "GET"),
                    ("/settings", self.update_settings, "POST"),
                    ("/run", self.start_run, "POST"),
                    ("/cancel", self.cancel_run, "POST"),
                    ("/details", self.get_details, "GET"),
                    ("/history", self.get_history, "GET"))]

    def get_settings(self):
        stored = self.get_config()
        if stored is not None and not isinstance(stored, dict):
            raise ValueError("invalid_plugin_configuration")
        config = deepcopy(stored or {})
        revision = hashlib.sha256(json.dumps(config, sort_keys=True, ensure_ascii=False,
                                             separators=(",", ":")).encode()).hexdigest()
        return Response(success=True, data={"config": config, "revision": revision})

    def _refresh_services(self):
        from app.scheduler import Scheduler
        Scheduler().update_plugin_job(self.__class__.__name__)

    def update_settings(self, payload: dict):
        if not isinstance(payload, dict) or not isinstance(payload.get("changes"), dict):
            return Response(success=False, message="设置格式无效")
        descriptor = self._take_process_lock()
        if descriptor is None:
            return Response(success=False, message="清理仍在运行，请等待停止后保存；修改内容已保留")
        try:
            current = self.get_settings().data
            if payload.get("revision") != current["revision"]:
                return Response(success=False, message="配置已变化，请重新读取后再保存；修改内容已保留")
            changes = deepcopy(payload["changes"])
            booleans = {"enabled", "notify", "recursive", "dry_run", "records_enabled", "records_dry_run"}
            strings = {"cron", "target_dirs", "exclude_dirs", "records_cron"}
            numbers = {"records_batch_size": (1, 100), "records_grace_hours": (0, 720)}
            for key, value in changes.items():
                if key in booleans:
                    valid = type(value) is bool
                elif key in strings:
                    valid = isinstance(value, str)
                    if valid and key in ("cron", "records_cron") and value.strip():
                        try:
                            CronTrigger.from_crontab(value)
                        except (ValueError, TypeError):
                            valid = False
                elif key in numbers:
                    low, high = numbers[key]
                    valid = type(value) is int and low <= value <= high
                else:
                    valid = False
                if not valid:
                    return Response(success=False, message=f"请检查设置项：{key}")
            config = current["config"]
            config.update(changes)
            saved = self.update_config(config)
            if saved is False or self.get_config() != config:
                raise OSError("configuration_write_unconfirmed")
            # Preserve raw absent/unknown values; runtime defaults are never written.
            self._load_config(config)
            self._refresh_services()
            return self.get_settings()
        except Exception as error:
            logger.error(f"保存清理设置失败：{type(error).__name__}")
            return Response(success=False, message="设置保存或调度更新失败，请重新读取确认；修改内容已保留")
        finally:
            self._release_process_lock(descriptor)

    def start_run(self, payload: dict):
        if not isinstance(payload, dict) or payload.get("scope") not in ("folders", "records", "all"):
            return Response(success=False, message="请选择要运行的清理项目")
        request_id = payload.get("request_id")
        if not isinstance(request_id, str) or not 1 <= len(request_id) <= 100:
            return Response(success=False, message="运行请求标识无效")
        try:
            with self._job_guard:
                return self._state_transaction(lambda: self._start_run_locked(payload, request_id))
        except Exception as error:
            logger.error(f"清理请求回执不可用：{type(error).__name__}")
            return Response(success=False, message="未能确认运行请求回执，未创建新任务；请查看当前状态后重试")

    def _start_run_locked(self, payload, request_id):
        previous = self.get_data("cleanup_run") or {}
        receipts = self._request_receipts()
        known = next((item for item in receipts if item["request_id"] == request_id), None)
        if known is None and previous.get("request_id") == request_id:
            # Preserve the latest pre-upgrade receipt as the bounded ledger starts.
            self._remember_request(request_id, previous,
                started=previous.get("status") not in ("queued", "failed"))
            known = next(item for item in self._request_receipts() if item["request_id"] == request_id)
        if known is not None:
            if known["scope"] != payload["scope"]:
                return Response(success=False, message="同一运行请求不能更换清理项目，请发起新的请求")
            return self._request_response(known)
        descriptor = self._take_process_lock()
        if descriptor is None:
            if (previous.get("scope") == payload["scope"] and previous.get("run_id")
                    and previous.get("status") in ("queued", "running", "cancelling")):
                self._remember_request(request_id, previous, started=previous.get("status") != "queued")
                return self._request_response(self._request_receipts()[-1])
            return Response(success=False, message="已有其他清理项目正在停止或运行，请查看当前状态")
        run = self._new_run(payload["scope"], request_id)
        try:
            self._persist_checked("cleanup_run", run)
            self._remember_request(request_id, run, started=False)
            self._run_event = Event()
            self._run_event_id = run["run_id"]
            self._run_thread = Thread(target=self._worker,
                args=(run, self._run_event, descriptor, self._capture_config()),
                name="cleanup-master", daemon=True)
            self._run_thread.start()
        except Exception as error:
            run.update(status="failed", finished_at=time.time(), not_started=True)
            try:
                self._persist_checked("cleanup_run", run)
                self._remember_request(request_id, run, started=False)
            except Exception as failure:
                logger.error(f"清理启动失败回执未确认：{type(failure).__name__}")
            self._release_process_lock(descriptor)
            return self._request_response({"run_id": run["run_id"], "scope": run["scope"],
                                           "status": "failed", "started": False}, reused=False)
        return Response(success=True, data={"run_id": run["run_id"], "scope": run["scope"],
                                            "status": "queued", "reused": False})

    def cancel_run(self, payload: dict):
        first = self.get_data("cleanup_run") or {}
        if not isinstance(payload, dict) or payload.get("run_id") != first.get("run_id"):
            return Response(success=False, message="当前任务已变化，请刷新状态")
        with self._job_guard:
            return self._state_transaction(lambda: self._cancel_run_locked(payload["run_id"]))

    def _cancel_run_locked(self, run_id):
        run = self.get_data("cleanup_run") or {}
        if run.get("run_id") != run_id:
            return Response(success=False, message="当前任务已变化，请刷新状态")
        if run.get("status") not in ("queued", "running", "cancelling"):
            return Response(success=True, data={"run_id": run_id, "status": run.get("status")})
        self._persist_checked("cleanup_cancel", run_id)
        if self._run_event_id == run_id and self._run_event:
            self._run_event.set()
        run["status"] = "cancelling"
        self._persist_checked("cleanup_run", run)
        return Response(success=True, data={"run_id": run_id, "status": "cancelling"})

    def get_status(self):
        run = self.get_data("cleanup_run") or {}
        if run.get("status") in ("queued", "running", "cancelling"):
            descriptor = self._take_process_lock()
            if descriptor is not None:
                try:
                    def recover():
                        current = self.get_data("cleanup_run") or {}
                        if current.get("status") in ("queued", "running", "cancelling"):
                            current.update(status="interrupted", finished_at=time.time())
                            self._write_run_if_current(current)
                        return current
                    run = self._state_transaction(recover)
                finally:
                    self._release_process_lock(descriptor)
        report = self.get_data("records_report") or {}
        records = {key: report[key] for key in (
            "status", "dry_run", "clean_time", "processed", "summary", "counts",
            "observed_pending", "scan", "snapshot_id") if key in report}
        history = self.get_data("history") or []
        history = [item for item in history if isinstance(item, dict)]
        latest = max(history, key=lambda item: str(item.get("clean_time", ""))) if history else {}
        folders = {key: deepcopy(latest[key]) for key in (
            "clean_time", "removed_count", "target_dirs", "dry_run", "status") if key in latest} if history else None
        return Response(success=True, data={"enabled": self._enabled,
            "records_enabled": self._records_enabled, "run": {key: value for key, value in run.items()
                if key in ("run_id", "scope", "status", "started_at", "finished_at")},
            "folders": folders, "records": records,
            "configured_folders": len([line for line in (self._target_dirs or "").splitlines() if line.strip()])})

    @staticmethod
    def _paginate(items, page, page_size):
        if type(page) is not int or type(page_size) is not int or page < 1 or not 1 <= page_size <= 100:
            raise ValueError("invalid_page")
        start = (page - 1) * page_size
        return {"items": deepcopy(items[start:start + page_size]), "page": page,
                "page_size": page_size, "total": len(items)}

    @staticmethod
    def _controlled_error(error):
        if not error:
            return None
        allowed = {"record_run_failed", "receipt_store_failed", "candidate_read_failed",
                   "invalid_record_state", "receipt_ledger_size_limit", "invalid_cleanup_report",
                   "record_state_save_failed", "source_intent_store_failed", "batch_time_limit",
                   "batch_path_budget", "batch_host_budget"}
        return error if isinstance(error, str) and error in allowed else "record_run_failed"

    def get_details(self, kind: str = "records", page: int = 1, page_size: int = 20, snapshot_id: str = ""):
        if kind not in ("records", "sources"):
            return Response(success=False, message="详情类别无效")
        snapshots = self.get_data("cleanup_snapshots") or []
        report = self.get_data("records_report")
        if isinstance(report, dict) and not any(item.get("snapshot_id") == report.get("snapshot_id")
                                                for item in snapshots):
            # Existing reports predate the archive. Expose them without rewriting
            # history or racing a live worker's bounded snapshot writes.
            legacy_id = report.get("snapshot_id") or hashlib.sha256(
                json.dumps(report, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            snapshots = snapshots + [{"snapshot_id": legacy_id, "items": report.get("items", []),
                "clean_time": report.get("clean_time"), "dry_run": report.get("dry_run", True),
                "status": report.get("status"), "run_error": self._controlled_error(report.get("error"))}]
        snapshot = next((item for item in reversed(snapshots)
                         if not snapshot_id or item.get("snapshot_id") == snapshot_id), None)
        if snapshot_id and snapshot is None:
            return Response(success=False, message="明细快照已过期，请刷新当前批次")
        snapshot = snapshot or {"snapshot_id": "", "items": []}
        items = [item for item in snapshot.get("items", []) if isinstance(item, dict)]
        if kind == "sources":
            items = [item for item in items if item.get("branch") in ("source_purge", "residual_task")]
        else:
            items = [item for item in items if item.get("branch") not in ("source_purge", "residual_task")]
        try:
            data = self._paginate(items, page, page_size)
        except ValueError:
            return Response(success=False, message="分页参数无效")
        data.update(snapshot_id=snapshot["snapshot_id"], scope="latest_batch",
                    clean_time=snapshot.get("clean_time"), dry_run=snapshot.get("dry_run", True),
                    status=snapshot.get("status"), run_error=snapshot.get("run_error"))
        return Response(success=True, data=data)

    def get_history(self, kind: str = "folders", page: int = 1, page_size: int = 20):
        if kind not in ("folders", "records"):
            return Response(success=False, message="历史类别无效")
        history = self.get_data("history" if kind == "folders" else "records_history") or []
        items = sorted((item for item in history if isinstance(item, dict)),
                       key=lambda item: str(item.get("clean_time", "")), reverse=True)
        items = [{**item, "error": self._controlled_error(item.get("error"))} for item in items]
        try:
            return Response(success=True, data=self._paginate(items, page, page_size))
        except ValueError:
            return Response(success=False, message="分页参数无效")

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        """
        if not self._enabled:
            return []
        services = []
        if self._cron and self._target_dirs:
            try:
                folder_trigger = CronTrigger.from_crontab(self._cron)
            except (TypeError, ValueError) as error:
                logger.error(f"空文件夹执行周期无效：{error}")
            else:
                services.append({
                    "id": "EmptyFolderCleaner",
                    "name": "空文件夹清理服务",
                    "trigger": folder_trigger,
                    "func": self.clean_empty_folders,
                    "kwargs": {},
                })
        if self._records_enabled and self._records_cron:
            try:
                records_trigger = CronTrigger.from_crontab(self._records_cron)
            except (TypeError, ValueError) as error:
                logger.error(f"记录核对周期无效：{error}")
            else:
                services.append({
                    "id": "EmptyFolderCleanerRecords",
                    "name": "失效记录核对服务",
                    "trigger": records_trigger,
                    "func": self.clean_records,
                    "kwargs": {},
                })
        return services

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        # Retain a conventional form for older renderers; Vue reads live settings.
        defaults = {
            "enabled": False, "notify": False, "onlyonce": False,
            "cron": "0 2 * * *", "target_dirs": "", "recursive": True,
            "exclude_dirs": "", "dry_run": False, "records_enabled": False,
            "records_cron": "*/5 * * * *", "records_dry_run": True,
            "records_batch_size": 100, "records_grace_hours": 24,
            "records_reopen_subscription": False,
        }
        fields = [
            ("VSwitch", "enabled", "启用定时清理"),
            ("VSwitch", "notify", "空文件夹完成通知"),
            ("VTextarea", "target_dirs", "空文件夹扫描目录"),
            ("VTextarea", "exclude_dirs", "空文件夹排除目录"),
            ("VSwitch", "recursive", "递归清理新变空的父目录"),
            ("VSwitch", "dry_run", "空文件夹仅预览"),
            ("VCronField", "cron", "空文件夹执行周期"),
            ("VSwitch", "records_enabled", "自动核对记录与残留"),
            ("VSwitch", "records_dry_run", "记录与残留仅预览"),
            ("VCronField", "records_cron", "核对周期"),
            ("VTextField", "records_grace_hours", "缺失确认时间（小时）"),
            ("VTextField", "records_batch_size", "每批最多核对条目"),
        ]
        return [{"component": "VForm", "content": [
            {"component": component, "props": {"model": model, "label": label}}
            for component, model, label in fields]}], defaults

    def get_page(self) -> List[dict]:
        data = self.get_status().data
        report = data.get("records", {})
        folders = data.get("folders") or {}
        return [{"component": "VCard", "content": [
            {"component": "VCardTitle", "text": "清理统计"},
            {"component": "VCardText", "text": f"最近空文件夹处理 {folders.get('removed_count', 0)} 个；"
             f"最近一批核对 {report.get('processed', 0)} 条文件记录。"},
        ]}]

    def __validate_config(self, config=None) -> bool:
        """
        校验配置
        """
        target_dirs = config.get("target_dirs") if config is not None else self._target_dirs
        if not target_dirs:
            logger.error("未配置清理目录")
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title="空文件夹清理",
                text="未配置清理目录"
            )
            return False
        
        # 检查目录是否存在
        for target_dir in target_dirs.strip().split('\n'):
            if not target_dir.strip():
                continue
            if not Path(target_dir.strip()).exists():
                logger.error(f"清理目录不存在：{target_dir.strip()}")
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title="空文件夹清理",
                    text=f"清理目录不存在：{target_dir.strip()}"
                )
                return False
        return True

    def __is_excluded(self, folder_path: Path, config=None) -> bool:
        """
        检查文件夹是否在排除列表中
        """
        exclude_dirs = config.get("exclude_dirs") if config is not None else self._exclude_dirs
        if not exclude_dirs:
            return False
        
        folder_name = folder_path.name
        exclude_list = [name.strip() for name in exclude_dirs.split('\n') if name.strip()]
        
        # 检查文件夹名是否在排除列表中
        if folder_name in exclude_list:
            return True
        
        # 检查完整路径是否匹配排除模式
        folder_str = str(folder_path)
        for exclude_pattern in exclude_list:
            if exclude_pattern in folder_str:
                return True
        
        return False

    @staticmethod
    def __rmdir_nofollow(root_path: Path, dir_path: Path) -> None:
        """从文件系统根逐级打开目录句柄，避免祖先被换成链接后误删外部目录。"""
        nofollow = getattr(os, "O_NOFOLLOW", None)
        if nofollow is None or not root_path.is_absolute():
            raise OSError(errno.EPERM, "不支持安全目录删除")
        try:
            relative = dir_path.relative_to(root_path)
        except ValueError as error:
            raise OSError(errno.EPERM, "目录超出清理范围") from error
        if not relative.parts or any(part in (".", "..") for part in relative.parts):
            raise OSError(errno.EPERM, "无效的待清理目录")

        flags = os.O_RDONLY | os.O_DIRECTORY | nofollow | getattr(os, "O_CLOEXEC", 0)
        fd = os.open("/", flags)
        try:
            for part in (*root_path.parts[1:], *relative.parts[:-1]):
                next_fd = os.open(part, flags, dir_fd=fd)
                os.close(fd)
                fd = next_fd
            # rmdir 与扫描间新建的文件会触发 ENOTEMPTY；目录句柄保持在
            # 原始父目录，即便路径中的祖先已被替换为符号链接也不会跟随。
            os.rmdir(relative.parts[-1], dir_fd=fd)
        finally:
            os.close(fd)

    def __remove_empty_folders(self, root_path: Path, stop_event=None, config=None) -> Tuple[int, List[str]]:
        """
        递归删除空文件夹
        返回: (删除数量, 删除的文件夹列表)
        """
        config = deepcopy(config) if config is not None else {
            "dry_run": self._dry_run, "recursive": self._recursive, "exclude_dirs": self._exclude_dirs}
        removed_folders: List[str] = []
        stop_event = stop_event or self._event
        if root_path.is_symlink() or not root_path.is_dir():
            return 0, removed_folders

        # 先收集可访问的目录，再从叶子向上处理。排除目录、挂载点和符号链接
        # 都保留在父目录内，因此父目录不能被误判为空。
        all_dirs: List[Path] = []
        initially_empty = set()
        scan_errors: List[OSError] = []

        def onerror(error: OSError) -> None:
            scan_errors.append(error)

        for dirpath, dirnames, filenames in os.walk(root_path, topdown=True,
                                            followlinks=False, onerror=onerror):
            if stop_event.is_set():
                return 0, removed_folders
            parent = Path(dirpath)
            if not dirnames and not filenames:
                initially_empty.add(parent)
            dirnames[:] = [name for name in dirnames
                           if not self.__is_excluded(parent / name, config)
                           and not (parent / name).is_symlink()
                           and not os.path.ismount(parent / name)]
            if parent != root_path:
                all_dirs.append(parent)

        if scan_errors:
            for error in scan_errors:
                logger.warning(f"扫描文件夹失败，已取消本次目录清理：{error}")
            return 0, removed_folders

        all_dirs.sort(key=lambda path: len(path.parts), reverse=True)
        virtual_removed = set()
        for dir_path in all_dirs:
            if stop_event.is_set():
                logger.info("空文件夹清理服务停止")
                break
            if self.__is_excluded(dir_path, config) or dir_path.is_symlink() or os.path.ismount(dir_path):
                continue
            if not config.get("recursive", True) and dir_path not in initially_empty:
                continue
            try:
                if config.get("dry_run", False):
                    children = list(dir_path.iterdir())
                    if any(child not in virtual_removed for child in children):
                        continue
                    virtual_removed.add(dir_path)
                    logger.info(f"[模拟] 将删除空文件夹：{dir_path}")
                else:
                    # rmdir 由文件系统在删除瞬间再次判断“确实为空”；若扫描后
                    # 下载器新建了文件，ENOTEMPTY 会安全地保留文件和目录。
                    self.__rmdir_nofollow(root_path, dir_path)
                    logger.info(f"删除空文件夹：{dir_path}")
                removed_folders.append(str(dir_path))
            except OSError as error:
                if error.errno not in (errno.ENOTEMPTY, errno.EEXIST, errno.ENOENT):
                    logger.warning(f"保留文件夹 {dir_path}：{error}")

        return len(removed_folders), removed_folders

    def clean_empty_folders(self):
        return self._sync_run("folders")

    def _clean_folders_batch(self, stop_requested, config):
        """
        开始清理空文件夹
        """
        logger.info("开始清理空文件夹 ...")
        class StopSignal:
            def is_set(inner_self):
                return stop_requested()
        stop_event = StopSignal()

        if not self.__validate_config(config):
            return {"status": "failed", "reason": "folder_config_invalid"}
        
        total_removed = 0
        all_removed_folders = []
        
        # 处理每个目标目录
        target_directories = [dir_path.strip() for dir_path in config.get("target_dirs", "").split('\n') if dir_path.strip()]
        
        for target_dir in target_directories:
            if stop_event.is_set():
                logger.info("空文件夹清理服务停止")
                break
            
            target_path = Path(target_dir)
            logger.info(f"清理目录：{target_path}")
            
            if not target_path.exists():
                logger.warning(f"目录不存在，跳过：{target_path}")
                continue
            
            if not target_path.is_dir():
                logger.warning(f"路径不是目录，跳过：{target_path}")
                continue
            
            # 清理当前目录
            removed_count, removed_folders = self.__remove_empty_folders(target_path, stop_event=stop_event, config=config)
            total_removed += removed_count
            all_removed_folders.extend(removed_folders)
            
            logger.info(f"目录 {target_path} 清理完成，删除了 {removed_count} 个空文件夹")
        
        # 记录清理结果
        if total_removed > 0:
            logger.info(f"空文件夹清理完成，共删除 {total_removed} 个空文件夹")
            if logger.level <= 10:  # DEBUG级别
                for folder in all_removed_folders:
                    logger.debug(f"已删除：{folder}")
        else:
            logger.info("没有发现需要清理的空文件夹")
        
        # 保存清理历史
        history = self.get_data('history') or []
        history.append({
            "clean_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "removed_count": total_removed,
            "target_dirs": len(target_directories),
            "dry_run": config.get("dry_run", False),
            "status": "cancelled" if stop_event.is_set() else "completed"
        })
        # 只保留最近50条记录
        if len(history) > 50:
            history = history[-50:]
        self.save_data("history", history)
        
        # 发送通知
        if config.get("notify") is True:
            mode_text = "[模拟运行] " if config.get("dry_run", False) else ""
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title="【空文件夹清理已停止】" if stop_event.is_set() else "【空文件夹清理任务执行完成】",
                text=f"{mode_text}清理目录：{len(target_directories)} 个，删除空文件夹：{total_removed} 个"
            )
        
        logger.info("空文件夹清理任务执行完成")
        return {"status": "cancelled" if stop_event.is_set() else "completed", "removed_count": total_removed}

    def stop_service(self):
        """
        退出插件
        """
        self._event.set()
        if getattr(self, "_run_event", None):
            self._run_event.set()
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown(wait=False)
                self._scheduler = None
        except Exception as e:
            logger.error(f"停止服务时出错：{str(e)}")
