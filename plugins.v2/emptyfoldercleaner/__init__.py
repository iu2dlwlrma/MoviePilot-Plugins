import os
import errno
import time
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event, Lock
from typing import Any, List, Dict, Tuple, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType


class EmptyFolderCleaner(_PluginBase):
    # 插件名称
    plugin_name = "清理大师"
    # 插件描述
    plugin_desc = "清理空文件夹，核对失效记录并清理媒体库已删除资源的下载源。"
    # 插件图标
    plugin_icon = "clean.png"
    # 插件版本
    plugin_version = "2.0.0"
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
    _records_lock = Lock()
    # 退出事件
    _event = Event()

    def init_plugin(self, config: dict = None):
        self.stop_service()
        # 每次配置重载使用新的停止信号；旧任务持有旧信号，不能恢复执行。
        self._event = Event()
        self._loaded_config = dict(config or {})
        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._cron = config.get("cron")
            self._notify = config.get("notify")
            self._target_dirs = config.get("target_dirs")
            self._recursive = config.get("recursive", True)
            self._exclude_dirs = config.get("exclude_dirs")
            self._dry_run = config.get("dry_run", False)
            self._records_enabled = config.get("records_enabled") is True
            self._records_cron = config.get("records_cron") or "*/5 * * * *"
            # 只有表单明确给出布尔 False 才允许实际写记录；空值/旧配置
            # 一律保持预览，避免反序列化异常使清理自动升级为执行模式。
            self._records_dry_run = config.get("records_dry_run", True) is not False
            self._records_batch_size = self._bounded_int(
                config.get("records_batch_size"), default=100, minimum=1, maximum=100)
            self._records_grace_hours = self._bounded_int(
                config.get("records_grace_hours"), default=24, minimum=24, maximum=720)
            self._records_reopen_subscription = config.get("records_reopen_subscription", False) is True

        # 启动定时任务 & 立即运行一次
        if self.get_state() or self._onlyonce:
            # 两种功能独立校验；某个空目录路径离线不应关闭记录核对。
            # 定时服务
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            if self._onlyonce:
                logger.info("清理大师服务启动，立即运行一次")
                self._scheduler.add_job(self.run_once, 'date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(
                                            seconds=3))
                # 关闭一次性开关
                self._onlyonce = False
                self.update_config(self._config_payload())

            # 启动服务
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

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

    def _config_payload(self) -> dict:
        config = dict(self._loaded_config or {})
        config.update({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "notify": self._notify,
            "target_dirs": self._target_dirs,
            "recursive": self._recursive,
            "exclude_dirs": self._exclude_dirs,
            "dry_run": self._dry_run,
            "records_enabled": self._records_enabled,
            "records_cron": self._records_cron,
            "records_dry_run": self._records_dry_run,
            "records_batch_size": self._records_batch_size,
            "records_grace_hours": self._records_grace_hours,
            "records_reopen_subscription": self._records_reopen_subscription,
        })
        return config

    @staticmethod
    def _build_record_adapter():
        # 未启用记录核对时无需加载宿主清理扩展，原空目录功能继续独立可用。
        from .host_adapter import build_host_adapter
        return build_host_adapter()

    def run_once(self):
        """一次性入口沿用两项功能的真实配置，不更改预览或订阅选项。"""
        result = {"folders": "disabled", "records": {"status": "disabled"}}
        if self._target_dirs:
            try:
                folder_result = self.clean_empty_folders()
                result["folders"] = folder_result.get("status", "completed") if isinstance(folder_result, dict) else "completed"
            except Exception as error:
                logger.error(f"空目录清理失败：{type(error).__name__}")
                result["folders"] = "failed"
        if self._records_enabled:
            result["records"] = self.clean_records()
        return result

    def _save_record_report(self, report: dict) -> None:
        self.save_data("records_report", report)
        if report.get("status") != "running":
            history = self.get_data("records_history")
            history = [item for item in history if isinstance(item, dict)] if isinstance(history, list) else []
            history.append({key: value for key, value in report.items() if key != "items"})
            self.save_data("records_history", history[-20:])

    def clean_records(self):
        """有界核对精确文件记录；两种处理共用分页、分别持久化观察窗口。"""
        if not self._records_enabled:
            return {"status": "disabled"}
        if not self._records_lock.acquire(blocking=False):
            # 不覆盖正在运行者的报告或观察状态。
            return {"status": "busy"}
        report = None
        state = None
        next_state = None
        state_changed = False
        persistence_failed = False
        stop_event = self._event
        try:
            from copy import deepcopy
            from dataclasses import replace
            from .host_adapter import CandidatePage
            from .record_cleanup import CleanupOptions, MutationResult, RecordCleanupEngine, destination_decision, MAX_PENDING

            now = time.time()
            stored = self.get_data("records_state")
            if stored is not None and (not isinstance(stored, dict) or stored.get("version") != 1):
                raise ValueError("invalid_record_state")
            state = deepcopy(stored) if stored is not None else {"version": 1}
            intents = state.get("purged_source", {})
            if not isinstance(intents, dict) or len(intents) > MAX_PENDING:
                raise ValueError("invalid_source_cleanup_intents")
            for key, intent in intents.items():
                if (not isinstance(key, str) or not key or not isinstance(intent, dict)
                        or not isinstance(intent.get("revision"), str) or not intent["revision"]
                        or not isinstance(intent.get("source_signature"), (tuple, list))
                        or len(intent["source_signature"]) != 7
                        or any(type(value) is not int or value < 0 for value in intent["source_signature"])
                        or intent["source_signature"][1] <= 0 or intent["source_signature"][6] <= 0):
                    raise ValueError("invalid_source_cleanup_intent")
            state["purged_source"] = intents
            cursor = state.get("cursor", 0)
            if type(cursor) is not int or cursor < 0:
                cursor = 0
            state["cursor"] = cursor
            next_state = deepcopy(state)
            # 旧分支只修复整理记录，其观察与完成标记不能授权新的源文件清理。
            next_state.pop("destination_only", None)
            options = CleanupOptions(
                stability_seconds=self._records_grace_hours * 3600,
                batch_size=self._records_batch_size,
                preserve_source_tasks=True,
                allow_subscription_retry=self._records_reopen_subscription is True,
            )
            dry_run = self._records_dry_run is not False
            report = {
                "status": "running", "clean_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "started_at": now, "dry_run": dry_run, "cursor_before": cursor,
                "cursor_after": cursor, "processed": 0, "items": [], "summary": {},
                "observation_hours": self._records_grace_hours,
            }
            self._save_record_report(report)
            if stop_event.is_set():
                report["status"] = "cancelled"
                return report
            adapter = self._build_record_adapter()
            page = adapter.list_candidates(cursor=cursor, limit=options.batch_size)
            if (not isinstance(page, CandidatePage) or page.complete is not True or page.reason
                    or not isinstance(page.records, tuple) or len(page.records) > options.batch_size
                    or type(page.cursor) is not int or page.cursor < 0
                    or page.cursor != 0 and page.cursor <= cursor):
                report.update(status="failed", error="candidate_read_failed")
                logger.error("记录核对失败：候选页读取不完整，已保留原分页位置")
                return report

            def guarded(callback):
                def call(*args):
                    if stop_event.is_set():
                        raise RuntimeError("cleanup_cancelled")
                    return callback(*args)
                return call

            def persist_state():
                nonlocal persistence_failed
                try:
                    # A detached snapshot prevents failed writes from mutating
                    # an in-memory copy returned by a storage implementation.
                    snapshot = deepcopy(next_state)
                    self.save_data("records_state", snapshot)
                    if self.get_data("records_state") != snapshot:
                        raise OSError("record_state_readback_mismatch")
                except Exception:
                    persistence_failed = True
                    raise

            def clear_intent(record):
                intent = next_state["purged_source"].get(record.key)
                if intent is None:
                    return
                next_state["purged_source"].pop(record.key)
                try:
                    persist_state()
                except Exception:
                    # Cleanup may already be committed. Keeping the intention
                    # is conservative; losing it could reopen the subscription.
                    next_state["purged_source"][record.key] = intent
                    raise

            def mutate_missing(record, evidence, branch_options):
                if persistence_failed:
                    return MutationResult("needs_review", "source_intent_store_failed")
                intent = next_state["purged_source"].get(record.key)
                if intent is not None:
                    if intent["revision"] != record.revision:
                        return MutationResult("needs_review", "source_intent_revision_changed")
                    branch_options = replace(branch_options, allow_subscription_retry=False)
                outcome = adapter.mutate(record, evidence, branch_options)
                if intent is not None and isinstance(outcome, MutationResult) and outcome.status in ("applied", "already_clean"):
                    try:
                        clear_intent(record)
                    except Exception:
                        return MutationResult("needs_review", "source_intent_store_failed")
                return outcome

            def purge_source(record, evidence, branch_options):
                if persistence_failed:
                    return MutationResult("needs_review", "source_intent_store_failed")
                signature = list(evidence.source_signature)
                intent = next_state["purged_source"].get(record.key)
                if intent is not None and (intent["revision"] != record.revision
                                           or list(intent["source_signature"]) != signature):
                    return MutationResult("needs_review", "source_intent_revision_changed")
                if intent is None:
                    if len(next_state["purged_source"]) >= MAX_PENDING:
                        return MutationResult("needs_review", "source_intent_capacity_wait")
                    next_state["purged_source"][record.key] = {
                        "revision": record.revision, "source_signature": signature, "phase": "intent",
                    }
                # Persist BEFORE the first physical action: an interruption
                # between unlink and its result must not lose retry protection.
                try:
                    persist_state()
                except Exception:
                    return MutationResult("needs_review", "source_intent_store_failed")
                if stop_event.is_set():
                    return MutationResult("needs_review", "cleanup_cancelled")
                outcome = adapter.purge_source(record, evidence, branch_options)
                if isinstance(outcome, MutationResult):
                    try:
                        if outcome.status in ("applied", "already_clean"):
                            # The adapter confirms both physical and record cleanup.
                            clear_intent(record)
                        elif outcome.reason == "source_purged_history_pending":
                            next_state["purged_source"][record.key]["phase"] = "source_deleted"
                            persist_state()
                    except Exception:
                        return MutationResult("needs_review", "source_intent_store_failed")
                return outcome

            branches = (
                ("full_missing", page.records, adapter.observe, mutate_missing, None, options),
                ("source_purge", tuple(item for item in page.records if item.destination_paths),
                 adapter.observe_destination, purge_source, destination_decision,
                 replace(options, preserve_source_tasks=False, allow_subscription_retry=False)),
            )
            for namespace, records, observer, mutation, decision, branch_options in branches:
                if stop_event.is_set():
                    report["status"] = "cancelled"
                    break
                kwargs = {"decision": decision} if decision is not None else {}
                engine = RecordCleanupEngine(guarded(observer), guarded(mutation), **kwargs)
                outcome = engine.run(records, state.get(namespace), now=now, dry_run=dry_run, options=branch_options)
                if outcome.busy or outcome.truncated:
                    report.update(status="failed", error="record_engine_busy" if outcome.busy else "candidate_page_unbounded")
                    break
                next_state[namespace] = outcome.state
                state_changed = True
                for record, item in zip(records, outcome.results):
                    report["items"].append({
                        "key": item.key, "file_id": record.record_id,
                        "file": Path(record.source_path).name, "branch": namespace,
                        "status": item.status, "reason": item.reason,
                        "first_seen": item.first_seen, "eligible_at": item.eligible_at,
                    })
            else:
                report["status"] = "review" if any(item["status"] == "needs_review" for item in report["items"]) else "completed"

            if stop_event.is_set():
                report["status"] = "cancelled"
            if persistence_failed:
                report.update(status="failed", error="source_intent_store_failed")
            report["processed"] = len(page.records)
            # 单项保留/失败由下轮回卷复核；整页失败、取消或忙碌不移动游标。
            if report["status"] in ("completed", "review"):
                next_state["cursor"] = page.cursor
                report["cursor_after"] = page.cursor
                report["cycle_complete"] = page.cursor == 0
            if state_changed:
                persist_state()
                state_changed = False
            pending = set()
            for namespace in ("full_missing", "source_purge"):
                pending.update(next_state.get(namespace, {}).get("candidates", {}))
            report["observed_pending"] = len(pending)
            # 一条文件可能同时经过两个分支，摘要按文件计数，不把分支数当文件数。
            priority = ("applied", "eligible", "waiting", "needs_review", "already_clean", "skipped")
            summary = {status: 0 for status in priority}
            grouped = {}
            for item in report["items"]:
                grouped.setdefault(item["key"], set()).add(item["status"])
            for statuses in grouped.values():
                for status in priority:
                    if status in statuses:
                        summary[status] += 1
                        break
            report["summary"] = summary
            report["pending_in_batch"] = sum(summary[key] for key in ("eligible", "waiting", "needs_review"))
            return report
        except Exception as error:
            logger.error(f"记录核对运行失败：{type(error).__name__}")
            if report is None:
                report = {"clean_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "items": [], "summary": {}}
            report.update(status="failed", error="record_run_failed", error_type=type(error).__name__)
            if state_changed and next_state is not None:
                # 前一分支若已完成，只保存其观察/提交状态；游标仍在原页。
                next_state["cursor"] = state.get("cursor", 0)
                report["cursor_after"] = next_state["cursor"]
                try:
                    snapshot = deepcopy(next_state)
                    self.save_data("records_state", snapshot)
                    if self.get_data("records_state") != snapshot:
                        raise OSError("record_state_readback_mismatch")
                except Exception as save_error:
                    logger.error(f"记录观察状态保存失败：{type(save_error).__name__}")
                    report["error"] = "record_state_save_failed"
            return report
        finally:
            try:
                if report is not None:
                    report["finished_at"] = time.time()
                    self._save_record_report(report)
            finally:
                self._records_lock.release()

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

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
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'notify',
                                            'label': '发送通知',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'recursive',
                                            'label': '递归清理',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VCronField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '空文件夹执行周期',
                                            'placeholder': '0 2 * * *'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'dry_run',
                                            'label': '空文件夹模拟运行',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次（已启用的清理项）',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'target_dirs',
                                            'label': '空文件夹扫描目录',
                                            'rows': 3,
                                            'placeholder': '每一行一个目录路径\n例如：\n/downloads\n/media/movies'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'exclude_dirs',
                                            'label': '空文件夹排除目录',
                                            'rows': 3,
                                            'placeholder': '每一行一个目录路径，这些目录不会被删除\n例如：\n/downloads\n/media/movies'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VAlert',
                        'props': {
                            'type': 'info',
                            'variant': 'tonal',
                            'class': 'mb-4 text-break',
                        },
                        'text': '默认只预览。源文件、入库文件与媒体库均确认缺失时，删除对应下载记录及独占历史；源文件仍在时，至少观察 24 小时并复核媒体库缺失与文件身份，才删除源文件及独占单文件种子任务。共享文件、多文件种子和无法确认的项目保留复核；电影与整季缺失目前也保留复核。'
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {
                                    'model': 'records_enabled', 'label': '自动核对失效记录'}}],
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {
                                    'model': 'records_dry_run', 'label': '只预览，不删除记录或源文件'}}],
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {
                                    'model': 'records_reopen_subscription',
                                    'label': '更新订阅缺集进度',
                                    'hint': '默认关闭；手动开启后仅更新缺源记录对应的缺集进度，后续订阅可能补下。删除源文件时不更新订阅进度。',
                                    'persistent-hint': True}}],
                            },
                        ],
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VCronField', 'props': {
                                    'model': 'records_cron', 'label': '记录核对周期',
                                    'placeholder': '*/5 * * * *'}}],
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField', 'props': {
                                    'model': 'records_grace_hours', 'label': '缺失确认时间（小时）',
                                    'type': 'number', 'min': 24}}],
                            },
                            {
                                'component': 'VCol',
                                'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField', 'props': {
                                    'model': 'records_batch_size', 'label': '每次最多核对条目',
                                    'type': 'number', 'min': 1, 'max': 100}}],
                            },
                        ],
                    },
                ]
            }
        ], {
            "enabled": False,
            "notify": False,
            "onlyonce": False,
            "cron": "0 2 * * *",
            "target_dirs": "",
            "recursive": True,
            "exclude_dirs": "",
            "dry_run": False,
            "records_enabled": False,
            "records_cron": "*/5 * * * *",
            "records_dry_run": True,
            "records_batch_size": 100,
            "records_grace_hours": 24,
            "records_reopen_subscription": False,
        }

    def get_page(self) -> List[dict]:
        """
        拼装插件详情页面，需要返回页面配置，同时附带数据
        """
        report = self.get_data("records_report")
        contents = self._record_report_page(report) if isinstance(report, dict) else [{
            "component": "VAlert",
            "props": {"type": "info", "variant": "tonal", "class": "mb-3 text-break"},
            "text": f"尚未运行记录核对。首次只观察缺失状态，至少 {self._records_grace_hours} 小时后再次确认；预览不会删除记录、源文件或种子任务。",
        }]
        # 原空目录历史保持原数据键与格式，只增加独立的记录核对区。
        history = self.get_data('history')
        history = [item for item in history if isinstance(item, dict)] if isinstance(history, list) else []
        if not history:
            contents.append({
                'component': 'div', 'text': '暂无空文件夹清理历史',
                'props': {'class': 'text-center text-medium-emphasis py-3'},
            })
            return [
                {
                    'component': 'div',
                    'content': contents,
                }
            ]
        
        # 数据按时间降序排序
        history = sorted(history, key=lambda x: x.get('clean_time', ''), reverse=True)
        
        # 拼装页面
        for item in history[:20]:  # 只显示最近20条记录
            clean_time = item.get("clean_time", "未知时间")
            removed_count = item.get("removed_count", 0)
            target_dirs = item.get("target_dirs", 0)
            dry_run = item.get("dry_run", False)
            
            contents.append({
                'component': 'VCard',
                'props': {
                    'class': 'mb-2'
                },
                'content': [
                    {'component': 'VCardTitle', 'text': '空文件夹清理'},
                    {
                        'component': 'VCardText',
                        'props': {
                            'class': 'pa-2'
                        },
                        'text': f'清理时间：{clean_time}'
                    },
                    {
                        'component': 'VCardText',
                        'props': {
                            'class': 'pa-2'
                        },
                        'text': f'清理目录数：{target_dirs}'
                    },
                    {
                        'component': 'VCardText',
                        'props': {
                            'class': 'pa-2'
                        },
                        'text': f'删除文件夹数：{removed_count}'
                    },
                    {
                        'component': 'VCardText',
                        'props': {
                            'class': 'pa-2'
                        },
                        'text': f'运行模式：{"模拟运行" if dry_run else "正常运行"}'
                    }
                ]
            })

        return [
            {
                'component': 'div',
                'props': {
                    'class': 'd-flex flex-column gap-3',
                },
                'content': contents
            }
        ]

    def _record_report_page(self, report: dict) -> List[dict]:
        status_names = {
            "running": "正在核对", "completed": "本批核对完成", "review": "本批核对完成，有条目需要复核",
            "failed": "核对失败，分页位置已保留", "cancelled": "已停止，本页将在下次继续",
            "interrupted": "上次核对已中断，本页将在下次继续",
        }
        item_names = {
            "applied": "已处理", "eligible": "预览可处理", "waiting": "等待确认",
            "needs_review": "需复核", "skipped": "已保留", "already_clean": "已处理过",
        }
        reasons = {
            "stability_window": "等待缺失确认时间满后再次核对",
            "confirmed_missing_dry_run": "缺失证据符合条件，当前为预览，文件、任务及记录未修改",
            "source_present": "源文件仍存在", "source_missing": "源文件不存在，交由缺源分支核对",
            "destination_present": "入库文件仍存在", "other_version_present": "同集其他有效版本仍存在",
            "media_library_present": "媒体库中仍存在该电影或剧集", "downloader_task_present": "下载器中仍有对应任务，继续保留",
            "transfer_task_active": "仍有整理任务，继续保留", "presence_unknown": "文件或媒体库状态无法确定，未修改",
            "storage_unavailable": "存储不可用，未修改", "downloaders_incomplete": "下载器状态未完整取得，未修改",
            "media_lookup_incomplete": "媒体库范围、具体媒体身份或季集查询不完整，保留复核", "destination_lookup_incomplete": "入库记录查询不完整，未修改",
            "task_lookup_incomplete": "任务状态不完整，未修改", "identity_unverified": "电影或剧集身份、季集或目标记录尚不能准确对应，保留复核",
            "invalid_record": "记录身份、季集或文件路径无法安全确认", "record_changed": "记录已变化，等待重新核对",
            "record_lookup_incomplete": "记录读取失败，保留并在后续轮次重试",
            "observation_failed": "本项核对失败，保留并在后续轮次重试",
            "mutation_failed": "记录提交失败，未确认成功", "host_cleanup_failed": "宿主记录清理失败，未确认成功",
            "host_destination_repair_failed": "宿主入库记录修复失败，未确认成功",
            "host_retained_record": "宿主保护条件未满足，记录仍保留", "host_retained_transfer": "宿主保留了整理记录",
            "live_evidence_changed": "执行前状态已变化，未修改", "live_destination_evidence_changed": "执行前入库状态已变化，未修改",
            "candidate_capacity_reached": "观察队列已满，保留并等待后续核对",
            "candidate_capacity_wait": "观察队列已满，等待现有条目满确认时间并再次核对后轮换，未修改",
            "source_signature_unavailable": "源文件身份无法准确核对，保留复核",
            "source_changed": "源文件已变化，重新开始完整观察时间",
            "season_evidence_incomplete": "整季信息不完整，未更新订阅缺集进度",
            "subscription_progress_unresolved": "订阅进度尚不能准确计算，未修改",
            "record_absent": "记录已不存在", "already_applied": "该版本记录已经处理",
            "records_updated": "已更新失效记录", "host_confirmed": "宿主已确认记录更新",
            "transfer_record_updated": "已处理旧整理记录，源文件与做种任务保留",
            "source_purged": "已删除确认失效的源文件、独占任务及对应记录",
            "source_purge_unavailable": "宿主暂不支持安全源文件清理，保留复核",
            "source_purge_not_authorized": "本次运行不允许删除源文件与独占任务，已保留",
            "source_signature_unverified": "源文件身份或独占硬链接状态无法确认，保留复核",
            "source_purge_evidence_changed": "执行期间源文件、媒体身份或归属证据已变化，已停止后续清理；文件与任务状态需复核",
            "source_shared_by_task": "源文件路径由其他种子任务共用，保留文件与任务",
            "torrent_not_exclusive_single_file": "对应任务不是可确认的独占单文件种子，保留文件与任务",
            "torrent_file_incomplete": "种子文件尚未完整下载，或实际大小与任务不符，保留复核",
            "task_inventory_incomplete": "下载器的种子任务清单不完整，保留复核",
            "file_inventory_incomplete": "下载器的种子文件清单不完整，保留复核",
            "task_inventory_limit": "种子任务数超出本次安全核对上限，保留复核",
            "file_inventory_limit": "种子文件数超出本次安全核对上限，保留复核",
            "file_path_ambiguous": "下载器文件路径无法精确对应源文件，保留复核",
            "invalid_source_identity": "源文件身份或大小无法确认，保留复核",
            "downloader_remove_unconfirmed": "种子任务移除未全部确认，已停止后续源文件删除并保留记录",
            "source_unlink_unconfirmed": "源文件删除结果未确认；可能已移除种子任务，记录保留待复核",
            "source_purged_history_pending": "源文件已删除，剩余记录尚待清理；下轮缺源核对继续处理，不更新订阅进度",
            "source_purge_failed": "清理途中发生异常，可能部分完成；剩余文件、任务和记录等待复核",
            "source_intent_store_failed": "清理意图保存或回读失败，已停止后续清理；此前步骤可能部分完成，等待复核",
            "source_intent_revision_changed": "已保存的源清理意图与当前记录或文件身份不一致，保留复核，不更新订阅进度",
            "source_intent_capacity_wait": "待补清意图已达安全上限，先处理已标记项目，未开始本项源文件清理",
            "cleanup_cancelled": "本次操作已停止，清理意图保留供后续核对",
        }
        status = report.get("status")
        if status == "running" and not self._records_lock.locked():
            status = "interrupted"
        summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
        summary_text = " · ".join(f"{label} {summary.get(key, 0)}" for key, label in item_names.items())
        body = [
            {"component": "VCardTitle", "text": "失效记录核对"},
            {"component": "VCardSubtitle", "props": {"class": "text-wrap text-break"},
             "text": f"{report.get('clean_time', '未知时间')} · {'只预览' if report.get('dry_run', True) else '记录与失效源文件清理'}"},
            {"component": "VCardText", "content": [
                {"component": "VAlert", "props": {
                    "type": "error" if status == "failed" else "warning" if status in ("review", "cancelled", "interrupted") else "info",
                    "variant": "tonal", "class": "mb-3 text-break",
                }, "text": status_names.get(status, "状态待核对")},
                {"component": "div", "props": {"class": "text-break mb-2"},
                 "text": f"本批检查 {report.get('processed', 0)} 条文件记录；{summary_text}"},
                {"component": "div", "props": {"class": "text-break text-medium-emphasis"},
                 "text": f"本批待确认或复核 {report.get('pending_in_batch', 0)} 条；已进入观察 {report.get('observed_pending', 0)} 条。"
                         + ("本轮已到末页，下轮继续复核观察中的记录。" if report.get("cycle_complete") is True
                            else "后续批次继续核对；全量剩余数尚未统计。")},
            ]},
        ]
        if report.get("error"):
            body.append({"component": "VCardText", "props": {"class": "text-break"},
                         "text": f"错误：{report.get('error')}" + (f"（{report['error_type']}）" if report.get("error_type") else "")})
        rows = []
        items = report.get("items") if isinstance(report.get("items"), list) else []
        for item in items[:200]:
            if not isinstance(item, dict):
                continue
            branch = {"source_purge": "失效源文件", "destination_only": "旧入库记录"}.get(item.get("branch"), "缺源记录")
            reason = item.get("reason", "unknown")
            rows.append({"component": "div", "props": {"class": "py-3 text-break", "style": "overflow-wrap:anywhere"}, "content": [
                {"component": "div", "props": {"class": "font-weight-medium"}, "text": item.get("file") or f"文件记录 {item.get('file_id', '')}"},
                {"component": "div", "text": f"{branch} · {item_names.get(item.get('status'), '需复核')}"},
                {"component": "div", "props": {"class": "text-medium-emphasis"},
                 "text": reasons.get(reason, f"保护条件未满足，未确认修改（{reason}）")},
            ]})
        if rows:
            body.append({"component": "VExpansionPanels", "props": {"variant": "accordion"}, "content": [
                {"component": "VExpansionPanel", "props": {"title": f"逐项结果（{len(rows)} 项判断）"}, "content": [
                    {"component": "VExpansionPanelText", "content": rows},
                ]},
            ]})
        return [{"component": "VCard", "props": {"class": "mb-3"}, "content": body}]

    def __validate_config(self) -> bool:
        """
        校验配置
        """
        if not self._target_dirs:
            logger.error("未配置清理目录")
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title="空文件夹清理",
                text="未配置清理目录"
            )
            return False
        
        # 检查目录是否存在
        for target_dir in self._target_dirs.strip().split('\n'):
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

    def __is_excluded(self, folder_path: Path) -> bool:
        """
        检查文件夹是否在排除列表中
        """
        if not self._exclude_dirs:
            return False
        
        folder_name = folder_path.name
        exclude_list = [name.strip() for name in self._exclude_dirs.split('\n') if name.strip()]
        
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

    def __remove_empty_folders(self, root_path: Path, stop_event=None) -> Tuple[int, List[str]]:
        """
        递归删除空文件夹
        返回: (删除数量, 删除的文件夹列表)
        """
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
                           if not self.__is_excluded(parent / name)
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
            if self.__is_excluded(dir_path) or dir_path.is_symlink() or os.path.ismount(dir_path):
                continue
            if not self._recursive and dir_path not in initially_empty:
                continue
            try:
                if self._dry_run:
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
        """
        开始清理空文件夹
        """
        logger.info("开始清理空文件夹 ...")
        stop_event = self._event
        
        if not self.__validate_config():
            return {"status": "failed", "reason": "folder_config_invalid"}
        
        total_removed = 0
        all_removed_folders = []
        
        # 处理每个目标目录
        target_directories = [dir_path.strip() for dir_path in self._target_dirs.split('\n') if dir_path.strip()]
        
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
            removed_count, removed_folders = self.__remove_empty_folders(target_path, stop_event=stop_event)
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
            "dry_run": self._dry_run
        })
        # 只保留最近50条记录
        if len(history) > 50:
            history = history[-50:]
        self.save_data("history", history)
        
        # 发送通知
        if self._notify:
            mode_text = "[模拟运行] " if self._dry_run else ""
            self.post_message(
                mtype=NotificationType.SiteMessage,
                title="【空文件夹清理任务执行完成】",
                text=f"{mode_text}清理目录：{len(target_directories)} 个，删除空文件夹：{total_removed} 个"
            )
        
        logger.info("空文件夹清理任务执行完成")
        return {"status": "cancelled" if stop_event.is_set() else "completed", "removed_count": total_removed}

    def stop_service(self):
        """
        退出插件
        """
        self._event.set()
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown(wait=False)
                self._scheduler = None
        except Exception as e:
            logger.error(f"停止服务时出错：{str(e)}")
