import base64
import json
import math
import re
import threading
import time
import uuid
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union
from urllib.parse import parse_qs, parse_qsl, unquote, urlencode, urljoin, urlparse, urlunparse
from zoneinfo import ZoneInfo

from .rules import demand_rank, download_has_started, evaluate_peer_demand, promotion_deadline, publication_age_minutes, update_transfer_sample, transfer_window, allocate_downloads

from apscheduler.triggers.cron import CronTrigger
from fastapi import Query

from app import schemas
from app.api.endpoints.plugin import register_plugin_api
from app.chain.torrents import TorrentsChain
from app.chain.site import SiteChain
from app.sdk.config import settings
from app.sdk.media import MediaInfo
from app.sdk.events import Event, eventmanager
from app.sdk.media import MetaInfo
from app.db.oper.site import SiteOper
from app.db.oper.subscribe import SubscribeOper
from app.sdk.services import DownloaderHelper
from app.sdk.network import SitesHelper
from app.runtime.thread import ThreadHelper
from app.sdk.logging import logger
from app.modules.qbittorrent import Qbittorrent
from app.modules.transmission import Transmission
from app.plugins import _PluginBase
from app.scheduler import Scheduler
from app.schemas import MediaType, NotificationType, ServiceInfo, TorrentInfo
from app.schemas.types import EventType
from app.sdk.network import RequestUtils
from app.sdk.utilities import StringUtils

from .models import (
    DEFAULT_TASK_SETTINGS, SHARED_TASK_FIELDS,
    BrushFlowSettingsPayload, BrushTaskPayload, BrushTaskStatePayload,
)


TASK_CONFIG_FIELDS = (
    "enabled",
    "notify",
    "site_id",
    "downloader",
    "brush_interval",
    "check_interval",
    "cron",
    "active_time_range",
    "site_ratio_control",
    "site_ratio_target",
    "rotation_hours",
    "novice",
    "novice_until",
    "disksize",
    "maxupspeed",
    "maxdlspeed",
    "maxdlcount",
    "freeleech",
    "hr",
    "include",
    "exclude",
    "size",
    "seeder",
    "leecher",
    "min_peer_seed_ratio",
    "promotion_lead_minutes",
    "timezone_offset",
    "pubtime",
    "seed_time",
    "hr_seed_time",
    "seed_ratio",
    "seed_size",
    "download_time",
    "seed_avgspeed",
    "seed_inactivetime",
    "delete_size_range",
    "up_speed",
    "dl_speed",
    "auto_archive_days",
    "save_path",
    "delete_except_tags",
    "except_subscribe",
    "proxy_delete",
    "del_no_free",
    "qb_category",
    "site_hr_active",
    "site_skip_tips",
    "rss_support",
    "tag",
)

LEGACY_SITE_OVERRIDE_FIELDS = {
    "freeleech",
    "hr",
    "include",
    "exclude",
    "size",
    "seeder",
    "timezone_offset",
    "pubtime",
    "seed_time",
    "hr_seed_time",
    "seed_ratio",
    "seed_size",
    "download_time",
    "seed_avgspeed",
    "seed_inactivetime",
    "save_path",
    "proxy_delete",
    "qb_category",
    "site_hr_active",
    "site_skip_tips",
    "del_no_free",
    "rss_support",
}

GLOBAL_LIMIT_FIELDS = (
    "global_disksize",
)

GLOBAL_DYNAMIC_DELETE_FIELDS = (
    "global_proxy_delete",
    "global_delete_size_range",
)


class BrushTaskConfig:
    """
    单个站点刷流任务的运行配置
    """

    def __init__(self, config: dict, defaults: Optional[dict] = None):
        """读取并标准化一项刷流任务配置"""
        defaults = {**DEFAULT_TASK_SETTINGS, **(defaults or {})}
        self.use_global_defaults = bool(config.get("use_global_defaults", True))
        overrides = config.get("override_fields")
        if overrides is None:
            overrides = [key for key in SHARED_TASK_FIELDS if key in config and config[key] != defaults.get(key)]
        self.override_fields = sorted(set(overrides).intersection(SHARED_TASK_FIELDS))
        config = dict(config)
        if self.use_global_defaults:
            for key in SHARED_TASK_FIELDS:
                if key not in self.override_fields:
                    config[key] = defaults.get(key)
        self.id = str(config.get("id") or uuid.uuid4().hex)
        self.name = str(config.get("name") or "刷流任务").strip()
        self.enabled = bool(config.get("enabled", True))
        self.notify = bool(config.get("notify", True))
        self.site_id = int(config.get("site_id") or 0)
        self.downloader = str(config.get("downloader") or "").strip()
        self.brush_interval = max(int(self._parse_number(config.get("brush_interval")) or 10), 1)
        self.check_interval = max(int(self._parse_number(config.get("check_interval")) or 5), 1)
        self.cron = self._clean_text(config.get("cron"))
        self.active_time_range = self._clean_text(config.get("active_time_range"))
        self.site_ratio_control = True
        self.site_ratio_target = self._parse_number(config.get("site_ratio_target"))
        self.rotation_hours = min(max(float(self._parse_number(config.get("rotation_hours")) or 24), 0.01), 168)
        self.novice = bool(config.get("novice", False))
        self.novice_until = self._clean_text(config.get("novice_until"))
        self.disksize = self._parse_number(config.get("disksize"))
        self.maxupspeed = self.maxdlspeed = self.maxdlcount = None
        self.freeleech = config.get("freeleech", "free")
        self.hr = config.get("hr", "yes")
        self.include = self._clean_text(config.get("include"))
        self.exclude = self._clean_text(config.get("exclude"))
        self.size = self._clean_text(config.get("size"))
        self.seeder = self._clean_text(config.get("seeder"))
        self.leecher = self._clean_text(config.get("leecher", "1"))
        self.min_peer_seed_ratio = self._parse_number(config.get("min_peer_seed_ratio", 0.1))
        self.promotion_lead_minutes = max(float(config.get("promotion_lead_minutes", 10) or 0), 0)
        self.timezone_offset = float(self._parse_number(config.get("timezone_offset")) or 0)
        self.pubtime = self._clean_text(config.get("pubtime"))
        self.seed_time = self._parse_number(config.get("seed_time"))
        self.hr_seed_time = self._parse_number(config.get("hr_seed_time"))
        self.seed_ratio = self._parse_number(config.get("seed_ratio"))
        self.seed_size = self._parse_number(config.get("seed_size"))
        self.download_time = self._parse_number(config.get("download_time"))
        self.seed_avgspeed = self._parse_number(config.get("seed_avgspeed"))
        self.seed_inactivetime = self._parse_number(config.get("seed_inactivetime"))
        self.delete_size_range = self._clean_text(config.get("delete_size_range"))
        self.up_speed = self.dl_speed = None
        self.auto_archive_days = self._parse_number(config.get("auto_archive_days"))
        self.save_path = self._clean_text(config.get("save_path"))
        self.delete_except_tags = self._clean_text(config.get("delete_except_tags"))
        self.except_subscribe = bool(config.get("except_subscribe", True))
        self.proxy_delete = bool(config.get("proxy_delete", False))
        self.del_no_free = bool(config.get("del_no_free", False)) if self.freeleech in {"free", "2xfree"} else False
        self.qb_category = self._clean_text(config.get("qb_category"))
        self.site_hr_active = bool(config.get("site_hr_active", False))
        self.site_skip_tips = bool(config.get("site_skip_tips", False))
        self.rss_support = bool(config.get("rss_support", False))
        self.tag = self._clean_text(config.get("tag"))

    @property
    def brush_tag(self) -> str:
        """返回当前任务在下载器中使用的标签（默认按站点名称生成，保证可读性）"""
        if self.tag:
            return self.tag
        site_name = BrushFlow._get_site_name(self.site_id) if self.site_id else None
        return f"刷流-{site_name}" if site_name else f"刷流-{self.id[:8]}"

    @staticmethod
    def _clean_text(value: Any) -> Optional[str]:
        """把空白文本标准化为 None"""
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None

    @staticmethod
    def _parse_number(value: Any) -> Optional[Union[int, float]]:
        """兼容解析历史配置中的整数、浮点数和空值"""
        if value is None or value == "":
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(number):
            return None
        return int(number) if number.is_integer() else number

    def to_dict(self) -> Dict[str, Any]:
        """返回可持久化和供前端编辑的任务配置"""
        data = self.effective_dict()
        if self.use_global_defaults:
            for key in SHARED_TASK_FIELDS:
                if key not in self.override_fields:
                    data.pop(key, None)
        return data

    def effective_dict(self) -> Dict[str, Any]:
        """返回应用全局默认值后的运行配置。"""
        data = {"id": self.id, "name": self.name}
        data.update({field: getattr(self, field) for field in TASK_CONFIG_FIELDS})
        for field in ("maxupspeed", "maxdlspeed", "maxdlcount", "up_speed", "dl_speed", "site_ratio_control"):
            data.pop(field, None)
        data.update(use_global_defaults=self.use_global_defaults, override_fields=self.override_fields)
        return data


class BrushFlow(_PluginBase):
    """
    多站点独立任务刷流插件
    """

    plugin_name = "站点刷流"
    plugin_desc = "自动托管多个站点刷流任务，并独立调度、统计与诊断。"
    plugin_icon = "brush.jpg"
    plugin_version = "7.2.5"
    plugin_author = "jxxghp,InfinityPacer,Seed680"
    author_url = "https://github.com/InfinityPacer"
    plugin_config_prefix = "brushflow_"
    plugin_order = 21
    auth_level = 2

    DATA_SCHEMA_VERSION = 3
    MAX_RUN_HISTORY = 50
    GLOBAL_BRUSH_TAG = "刷流"
    TASK_DATA_NAMES = ("torrents", "archived", "unmanaged", "statistic", "runs", "rotation", "metrics")

    def init_plugin(self, config: dict = None) -> None:
        """初始化全局开关、任务配置、运行锁和历史数据迁移"""
        raw_config = config or {}
        self._task_context = threading.local()
        self._task_locks: Dict[str, threading.Lock] = {}
        self._brush_lock = threading.Lock()
        self._pending_brush: Dict[str, float] = {}
        self._stopping = False
        self._schedule_started = datetime.now().astimezone()
        self._global_delete_lock = threading.Lock()
        self._runtime_lock = threading.Lock()
        self._runtime: Dict[str, dict] = {}
        self._subscribe_infos: Dict[str, List[str]] = {}
        self._enabled = bool(raw_config.get("enabled", False))
        self._show_sidebar_nav = bool(raw_config.get("show_sidebar_nav", True))
        self._rotation_mode = raw_config.get("rotation_mode", "parallel")
        if self._rotation_mode not in {"parallel", "sequential"}:
            self._rotation_mode = "parallel"
        if self._rotation_mode == "parallel" and self.get_data("site_rotation"):
            self.save_data("site_rotation", {})
        self._global_defaults = {
            key: (raw_config.get("global_defaults") or {}).get(key, DEFAULT_TASK_SETTINGS.get(key))
            for key in SHARED_TASK_FIELDS
        }

        legacy_config = not isinstance(raw_config.get("tasks"), list) and bool(raw_config.get("brushsites"))
        for field in GLOBAL_LIMIT_FIELDS:
            value = raw_config.get(field)
            if value is None and legacy_config:
                value = raw_config.get(field.removeprefix("global_"))
            parsed_value = BrushTaskConfig._parse_number(value)
            setattr(self, f"_{field}", parsed_value if parsed_value and parsed_value > 0 else None)
        global_proxy_delete = raw_config.get(
            "global_proxy_delete",
            raw_config.get("proxy_delete", False) if legacy_config else False,
        )
        legacy_delete_range = raw_config.get("delete_size_range") if legacy_config else None
        self._global_proxy_delete, self._global_delete_size_range = self._validate_global_dynamic_delete_config(
            global_proxy_delete,
            raw_config.get("global_delete_size_range", legacy_delete_range),
        )

        task_rows = raw_config.get("tasks") if isinstance(raw_config.get("tasks"), list) else None
        migrated = task_rows is None and bool(raw_config.get("brushsites"))
        if migrated:
            task_rows = self._migrate_legacy_config(raw_config)
        task_rows = task_rows or []

        self._task_configs: Dict[str, BrushTaskConfig] = {}
        for row in task_rows:
            if not isinstance(row, dict):
                continue
            old_defaults = raw_config.get("global_defaults") or {}
            if "rss_support" in old_defaults and row.get("use_global_defaults", True) \
                    and "rss_support" not in (row.get("override_fields") or []):
                row = {**row, "rss_support": bool(old_defaults["rss_support"])}
            task = BrushTaskConfig(row, self._global_defaults)
            if not self._validate_task_reference(task, notify=False):
                task.enabled = False
            self._task_configs[task.id] = task
            self._task_locks[task.id] = threading.Lock()
            self._runtime[task.id] = {"state": "idle", "operation": None, "last_error": None}

        normalized = self._current_config()
        if migrated or raw_config != normalized:
            self.update_config(normalized)
        self._migrate_legacy_data()
        self._migrate_torrent_identity_data()

        # V5 为任务增加了唯一标签；启动时回收升级前已经失去任务配置的孤立标签。
        if self._enabled:
            try:
                ThreadHelper().submit(self._cleanup_unused_task_tags)
            except Exception as err:
                logger.warning(f"提交刷流标签清理任务失败：{str(err)}")

        if migrated and raw_config.get("onlyonce") and self._enabled:
            for task in self._task_configs.values():
                if task.enabled:
                    ThreadHelper().submit(self.brush, task.id)
                    ThreadHelper().submit(self.check, task.id)

    def get_state(self) -> bool:
        """返回插件全局启用状态"""
        return bool(getattr(self, "_enabled", False))

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """当前插件不注册远程命令"""
        return []

    @staticmethod
    def get_render_mode() -> Tuple[str, str]:
        """声明使用 Vue 联邦组件渲染插件界面"""
        return "vue", "dist/assets"

    def get_sidebar_nav(self) -> List[Dict[str, Any]]:
        """向主界面整理分组注册刷流任务入口"""
        if not self.get_state() or not getattr(self, "_show_sidebar_nav", True):
            return []
        return [
            {
                "nav_key": "main",
                "title": "站点刷流",
                "icon": "mdi-sync",
                "section": "organize",
                "permission": "manage",
                "order": 45,
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """注册 Vue 工作台使用的刷流任务 API"""
        return [
            {
                "path": "/status",
                "endpoint": self.get_status,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取刷流任务总览",
            },
            {
                "path": "/settings",
                "endpoint": self.update_settings,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "更新刷流插件设置",
            },
            {
                "path": "/tasks",
                "endpoint": self.create_task,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "创建刷流任务",
            },
            {
                "path": "/tasks/{task_id}",
                "endpoint": self.get_task_detail,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "获取刷流任务详情",
            },
            {
                "path": "/tasks/{task_id}",
                "endpoint": self.update_task,
                "methods": ["PUT"],
                "auth": "bear",
                "summary": "更新刷流任务",
            },
            {
                "path": "/tasks/{task_id}",
                "endpoint": self.delete_task,
                "methods": ["DELETE"],
                "auth": "bear",
                "summary": "删除刷流任务",
            },
            {
                "path": "/tasks/{task_id}/state",
                "endpoint": self.update_task_state,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "启用或暂停刷流任务",
            },
            {
                "path": "/tasks/{task_id}/run",
                "endpoint": self.run_task,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即执行刷流刷新",
            },
            {
                "path": "/tasks/{task_id}/check",
                "endpoint": self.check_task,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "立即检查刷流种子",
            },
            {
                "path": "/tasks/{task_id}/clear",
                "endpoint": self.clear_task_data,
                "methods": ["POST"],
                "auth": "bear",
                "summary": "清除单个刷流任务数据",
            },
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """Vue 配置组件只需要接收当前配置模型"""
        return [], self._current_config()

    def get_page(self) -> List[dict]:
        """Vue 详情组件自行通过插件 API 获取页面数据"""
        return []

    def get_dashboard(self, key: str, **kwargs) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], None]]:
        """保留原有仪表板入口并改由 Vue 组件渲染"""
        if not self.get_state():
            return None
        return (
            {"cols": 12, "sm": 6, "md": 6},
            {
                "title": "站点刷流",
                "subtitle": "多任务运行概览",
                "refresh": 30,
                "border": True,
            },
            None,
        )

    def get_service(self) -> List[Dict[str, Any]]:
        """为每个启用任务注册独立的刷流刷新和状态检查服务"""
        if not self.get_state():
            return []
        services: List[Dict[str, Any]] = []
        for task in self._task_configs.values():
            if not task.enabled:
                continue
            if task.cron:
                try:
                    brush_trigger: Union[str, CronTrigger] = CronTrigger.from_crontab(task.cron)
                    brush_kwargs: Dict[str, Any] = {}
                except ValueError as err:
                    logger.error(f"刷流任务 [{task.name}] CRON 表达式无效：{str(err)}")
                    brush_trigger = "interval"
                    brush_kwargs = {"minutes": task.brush_interval, "start_date": self._schedule_started}
            else:
                brush_trigger = "interval"
                brush_kwargs = {"minutes": task.brush_interval, "start_date": self._schedule_started}
            services.append(
                {
                    "id": f"Task_{task.id}_Brush",
                    "name": f"刷流刷新 - {task.name}",
                    "trigger": brush_trigger,
                    "func": self._queue_brush,
                    "kwargs": brush_kwargs,
                    "func_kwargs": {"task_id": task.id},
                }
            )
            services.append(
                {
                    "id": f"Task_{task.id}_Check",
                    "name": f"刷流检查 - {task.name}",
                    "trigger": "interval",
                    "func": self.check,
                    "kwargs": {"minutes": task.check_interval, "start_date": self._schedule_started},
                    "func_kwargs": {"task_id": task.id},
                }
            )
            promotion_expiry = self._next_promotion_expiry(task)
            if promotion_expiry:
                services.append(
                    {
                        "id": f"Task_{task.id}_PromotionExpiry",
                        "name": f"促销到期检查 - {task.name}",
                        "trigger": "date",
                        "func": self._check_promotion_expiry,
                        "kwargs": {"run_date": promotion_expiry},
                        "func_kwargs": {"task_id": task.id},
                    }
                )
        services.append(
            {
                "id": "FairBrushDispatch",
                "name": "刷流名额分配",
                "trigger": "interval",
                "func": self._dispatch_brush,
                "kwargs": {"seconds": 30, "start_date": self._schedule_started},
            }
        )
        return services

    def stop_service(self) -> None:
        """插件不再维护私有调度器，公共服务由宿主统一停止"""
        self._stopping = True
        self._restore_download_budgets()
        with getattr(self, "_runtime_lock", threading.Lock()):
            getattr(self, "_pending_brush", {}).clear()

    def _restore_download_budgets(self) -> None:
        """只撤销仍与本插件最后一次写入一致的并发设置。"""
        for name in {task.downloader for task in getattr(self, "_task_configs", {}).values() if task.downloader}:
            key = f"download_budget.{name}"
            state = self.get_data(key) or {}
            if not state.get("ceiling"):
                continue
            try:
                service = DownloaderHelper().get_service(name=name)
                tr = getattr(service.instance, "trc", None) if service and service.instance else None
                if tr and tr.get_session().download_queue_size == state.get("applied"):
                    tr.set_session(download_queue_size=state["ceiling"])
                self.save_data(key, {})
            except Exception:
                logger.warning(f"下载器 {name} 并发设置恢复失败，保留恢复记录")

    @property
    def service_info(self) -> Optional[ServiceInfo]:
        """获取当前任务绑定的下载器服务"""
        task = self._get_task_config()
        if not task or not task.downloader:
            return None
        service = DownloaderHelper().get_service(name=task.downloader)
        if not service:
            self._log_and_notify_error(f"刷流任务 [{task.name}] 获取下载器实例失败，请检查配置")
            return None
        if service.instance.is_inactive():
            self._log_and_notify_error(f"刷流任务 [{task.name}] 下载器未连接")
            return None
        return service

    @property
    def downloader(self) -> Optional[Union[Qbittorrent, Transmission]]:
        """返回当前任务绑定的下载器实例"""
        service = self.service_info
        return service.instance if service else None

    def get_status(self) -> schemas.Response:
        """返回全局设置、任务摘要和前端可选项"""
        return schemas.Response(success=True, data=self._build_status_data())

    def update_settings(self, payload: BrushFlowSettingsPayload) -> schemas.Response:
        """更新插件全局开关并刷新宿主任务调度"""
        if not self._brush_lock.acquire(blocking=False):
            return schemas.Response(success=False, message="刷流正在执行，请稍后保存设置")
        locks = []
        try:
            for task_id in self._task_configs:
                lock = self._task_locks.setdefault(task_id, threading.Lock())
                if not lock.acquire(blocking=False):
                    return schemas.Response(success=False, message="任务正在检查，请稍后保存设置")
                locks.append(lock)
            return self._apply_settings(payload)
        finally:
            for lock in reversed(locks):
                lock.release()
            self._brush_lock.release()

    def _apply_settings(self, payload: BrushFlowSettingsPayload) -> schemas.Response:
        """在任务空闲时应用全局继承并保存完整配置。"""
        defaults = (payload.global_defaults if "global_defaults" in payload.model_fields_set
                    else self._global_defaults)
        tasks = {}
        for task_id, task in self._task_configs.items():
            data = task.to_dict()
            if payload.apply_defaults_to_existing:
                data.update(use_global_defaults=True, override_fields=[])
            updated = BrushTaskConfig(data, defaults)
            if self._downloader_change_has_active_torrents(task, updated):
                return schemas.Response(success=False, message=f"{task.name} 仍有托管种子，请保留原下载器或处理完成后再切换")
            if updated.enabled and not self._validate_task_reference(updated, notify=False):
                return schemas.Response(success=False, message=f"{task.name} 的站点或下载器无效，请先选择全局下载器")
            tasks[task_id] = updated
        self._global_defaults = defaults
        self._task_configs = tasks
        global_dynamic_delete_was_enabled = self._global_dynamic_delete_enabled()
        self._enabled = payload.enabled
        self._show_sidebar_nav = payload.show_sidebar_nav
        rotation_mode = (payload.rotation_mode if "rotation_mode" in payload.model_fields_set
                         else getattr(self, "_rotation_mode", "parallel"))
        if rotation_mode != getattr(self, "_rotation_mode", "parallel"):
            self.save_data("site_rotation", {})
        self._rotation_mode = rotation_mode
        if not self._enabled or rotation_mode != "parallel":
            self._restore_download_budgets()
        for field in GLOBAL_LIMIT_FIELDS:
            setattr(self, f"_{field}", getattr(payload, field))
        for field in GLOBAL_DYNAMIC_DELETE_FIELDS:
            setattr(self, f"_{field}", getattr(payload, field))
        if global_dynamic_delete_was_enabled and not self._global_dynamic_delete_enabled():
            for task in self._task_configs.values():
                if task.proxy_delete and not task.delete_size_range:
                    task.proxy_delete = False
        self._save_config()
        self._refresh_scheduler()
        return schemas.Response(success=True, data=self._build_status_data())

    def create_task(self, payload: BrushTaskPayload) -> schemas.Response:
        """创建一个站点与下载器均独立的刷流任务"""
        task_data = payload.model_dump()
        task_data["id"] = uuid.uuid4().hex
        task = BrushTaskConfig(task_data, self._global_defaults)
        if not self._validate_task_reference(task):
            return schemas.Response(success=False, message="站点或下载器配置无效")
        self._task_configs[task.id] = task
        self._task_locks[task.id] = threading.Lock()
        self._runtime[task.id] = {"state": "idle", "operation": None, "last_error": None}
        self._save_config()
        self._refresh_scheduler()
        return schemas.Response(success=True, data=self._build_task_detail(task.id))

    def get_task_detail(
        self,
        task_id: str,
        state: str = Query("active", pattern="^(active|deleted|all)$"),
        page: int = Query(1, ge=1),
        page_size: int = Query(50, ge=10, le=200),
    ) -> schemas.Response:
        """分页返回任务配置、统计、诊断记录和种子明细"""
        if task_id not in self._task_configs:
            return schemas.Response(success=False, message="刷流任务不存在")
        return schemas.Response(
            success=True,
            data=self._build_task_detail(task_id, state=state, page=page, page_size=page_size),
        )

    def update_task(self, task_id: str, payload: BrushTaskPayload) -> schemas.Response:
        """更新任务配置并保持原任务 ID 与历史数据关联"""
        if task_id not in self._task_configs:
            return schemas.Response(success=False, message="刷流任务不存在")
        if self._is_task_busy(task_id):
            return schemas.Response(success=False, message="任务正在执行，请稍后再修改")
        task_data = payload.model_dump()
        task_data["id"] = task_id
        task = BrushTaskConfig(task_data, self._global_defaults)
        if self._downloader_change_has_active_torrents(self._task_configs[task_id], task):
            return schemas.Response(success=False, message="任务仍有托管种子，请保留原下载器或处理完成后再切换")
        if not self._validate_task_reference(task):
            return schemas.Response(success=False, message="站点或下载器配置无效")
        self._task_configs[task_id] = task
        self._save_config()
        self._refresh_scheduler()
        return schemas.Response(success=True, data=self._build_task_detail(task_id))

    def _downloader_change_has_active_torrents(self, previous: BrushTaskConfig, updated: BrushTaskConfig) -> bool:
        """有托管记录时保持下载器关联，避免切换后把原种子误标为删除。"""
        return previous.downloader != updated.downloader and any(
            not record.get("deleted")
            for record in (self._get_task_data(previous.id, "torrents") or {}).values()
        )

    def delete_task(self, task_id: str) -> schemas.Response:
        """删除没有活跃种子的任务及其独立历史数据"""
        task = self._task_configs.get(task_id)
        if not task:
            return schemas.Response(success=False, message="刷流任务不存在")
        if self._is_task_busy(task_id):
            return schemas.Response(success=False, message="任务正在执行，请稍后再删除")
        torrents = self._get_task_data(task_id, "torrents") or {}
        active_count = sum(1 for item in torrents.values() if not item.get("deleted"))
        if active_count:
            return schemas.Response(success=False, message="任务仍有活跃种子，请先处理后再删除")
        self._task_configs.pop(task_id, None)
        self._task_locks.pop(task_id, None)
        self._runtime.pop(task_id, None)
        for data_name in self.TASK_DATA_NAMES:
            self.del_data(self._task_data_key(task_id, data_name))
        self._save_config()
        self._refresh_scheduler()
        try:
            # 删除接口不应被下载器网络请求阻塞，标签清理由后台线程完成。
            ThreadHelper().submit(self._cleanup_unused_task_tag, task)
        except Exception as err:
            logger.warning(f"提交刷流任务标签清理失败：{str(err)}")
        return schemas.Response(success=True, data=self._build_status_data())

    def update_task_state(self, task_id: str, payload: BrushTaskStatePayload) -> schemas.Response:
        """启用或暂停单个任务并更新对应宿主调度"""
        task = self._task_configs.get(task_id)
        if not task:
            return schemas.Response(success=False, message="刷流任务不存在")
        task.enabled = payload.enabled
        self._save_config()
        self._refresh_scheduler()
        return schemas.Response(success=True, data=self._build_task_detail(task_id))

    def run_task(self, task_id: str) -> schemas.Response:
        """异步提交单个任务的立即刷流刷新"""
        return self._submit_task_operation(task_id, "brush")

    def check_task(self, task_id: str) -> schemas.Response:
        """异步提交单个任务的立即状态检查"""
        return self._submit_task_operation(task_id, "check")

    def clear_task_data(self, task_id: str) -> schemas.Response:
        """清除单个任务的统计、历史与托管记录"""
        if task_id not in self._task_configs:
            return schemas.Response(success=False, message="刷流任务不存在")
        if self._is_task_busy(task_id):
            return schemas.Response(success=False, message="任务正在执行，请稍后再清除")
        for data_name in self.TASK_DATA_NAMES:
            self._save_task_data(task_id, data_name, {} if data_name != "runs" else [])
        return schemas.Response(success=True, data=self._build_task_detail(task_id))

    @eventmanager.register(EventType.PluginReload)
    def reload(self, event: Event) -> None:
        """插件重载后重新注册动态 API 和任务调度"""
        if event and event.event_data.get("plugin_id") == self.__class__.__name__:
            register_plugin_api(plugin_id=self.__class__.__name__)
            Scheduler().update_plugin_job(self.__class__.__name__)

    def _current_config(self) -> Dict[str, Any]:
        """返回插件当前可持久化配置快照"""
        config = {
            "schema_version": self.DATA_SCHEMA_VERSION,
            "enabled": bool(getattr(self, "_enabled", False)),
            "show_sidebar_nav": bool(getattr(self, "_show_sidebar_nav", True)),
            "rotation_mode": getattr(self, "_rotation_mode", "parallel"),
            "global_defaults": getattr(self, "_global_defaults", {}),
            "tasks": [task.to_dict() for task in getattr(self, "_task_configs", {}).values()],
        }
        config.update({field: getattr(self, f"_{field}", None) for field in GLOBAL_LIMIT_FIELDS})
        config.update({field: getattr(self, f"_{field}", None) for field in GLOBAL_DYNAMIC_DELETE_FIELDS})
        return config

    def _save_config(self) -> None:
        """保存全局设置和全部任务配置"""
        self.update_config(self._current_config())

    def _refresh_scheduler(self) -> None:
        """通知宿主按最新任务列表重建插件服务"""
        try:
            Scheduler().update_plugin_job(self.__class__.__name__)
        except Exception as err:
            logger.error(f"更新站点刷流调度失败：{str(err)}")

    def _global_dynamic_delete_enabled(self) -> bool:
        """返回全局动态删种开关与阈值是否同时有效"""
        return bool(
            getattr(self, "_global_proxy_delete", False)
            and getattr(self, "_global_delete_size_range", None)
        )

    @staticmethod
    def _validate_global_dynamic_delete_config(enabled: Any, size_range: Any) -> Tuple[bool, Optional[str]]:
        """复用设置模型校验持久化或迁移得到的全局动态删种配置。"""
        try:
            payload = BrushFlowSettingsPayload.model_validate(
                {
                    "global_proxy_delete": enabled,
                    "global_delete_size_range": size_range,
                }
            )
        except ValueError as err:
            logger.warning(f"全局动态删种配置无效，已自动关闭：{str(err)}")
            return False, None
        return payload.global_proxy_delete, payload.global_delete_size_range

    @staticmethod
    def _promotion_expiry_at(
        freedate_origin: Any,
        timezone_offset: float,
        lead_minutes: float = 10,
    ) -> Optional[datetime]:
        """换算促销截止前的清理时刻，保留输入时间自身的时区。"""
        return promotion_deadline(freedate_origin, timezone_offset, settings.TZ, lead_minutes)

    def _next_promotion_expiry(self, task: BrushTaskConfig) -> Optional[datetime]:
        """返回未完成任务下一次促销清理时刻，不重复调度已到期项。"""
        if not task.del_no_free:
            return None
        now = datetime.now(ZoneInfo(settings.TZ))
        expiries: List[datetime] = []
        torrent_tasks = self._get_task_data(task.id, "torrents") or {}
        for torrent_task in torrent_tasks.values():
            if not isinstance(torrent_task, dict) or torrent_task.get("deleted"):
                continue
            try:
                total_size = float(torrent_task.get("size") or 0)
                downloaded = float(torrent_task.get("downloaded") or 0)
            except (TypeError, ValueError):
                total_size = downloaded = 0
            if torrent_task.get("completed") or total_size > 0 and downloaded >= total_size:
                continue
            expiry = self._promotion_expiry_at(
                torrent_task.get("freedate"), task.timezone_offset,
                getattr(task, "promotion_lead_minutes", 10),
            )
            if expiry and expiry > now:
                expiries.append(expiry)
        return min(expiries) if expiries else None

    def _check_promotion_expiry(self, task_id: str) -> None:
        """在最近促销截止时等待当前操作结束，检查后重排下一截止任务"""
        try:
            self.check(task_id, wait_for_lock=True)
        finally:
            self._refresh_scheduler()

    def _validate_task_reference(self, task: BrushTaskConfig, notify: bool = True) -> bool:
        """校验任务引用的私有站点和下载器是否仍然存在"""
        site = SiteOper().get(task.site_id)
        downloader_configs = DownloaderHelper().get_configs()
        valid = bool(
            site
            and not getattr(site, "public", False)
            and task.downloader
            and task.downloader in downloader_configs
        )
        if notify and not valid:
            self._log_and_notify_error(f"刷流任务 [{task.name}] 引用的站点或下载器不存在")
        return valid

    @staticmethod
    def _torrent_has_tag(torrent: Any, tag: str) -> bool:
        """判断 qBittorrent 种子是否仍绑定指定标签"""
        if not isinstance(torrent, dict):
            return False
        tags = {
            item.strip()
            for item in str(torrent.get("tags") or "").split(",")
            if item.strip()
        }
        return tag in tags

    @staticmethod
    def _delete_qbittorrent_tags(service: Any, tags: Union[str, List[str]]) -> bool:
        """删除 qBittorrent 全局标签定义，不调用需要种子 Hash 的 removeTags。"""
        client = getattr(getattr(service, "instance", None), "qbc", None)
        if not client or not tags:
            return False
        client.torrents_delete_tags(tags=tags)
        return True

    def _cleanup_unused_task_tag(
        self,
        task: BrushTaskConfig,
        torrents: Optional[List[Any]] = None,
    ) -> None:
        """仅删除不再被任何 qBittorrent 种子使用的任务唯一标签"""
        if not task or not task.downloader:
            return
        try:
            helper = DownloaderHelper()
            service = helper.get_service(name=task.downloader)
            if not service or not service.instance or not helper.is_downloader("qbittorrent", service=service):
                return
            if torrents is None:
                torrents, error = service.instance.get_torrents()
                if error:
                    logger.warning(f"清理刷流任务 [{task.name}] 标签时获取下载器种子失败")
                    return
            if any(self._torrent_has_tag(torrent, task.brush_tag) for torrent in torrents or []):
                return
            if self._delete_qbittorrent_tags(service, task.brush_tag):
                logger.info(f"清理刷流任务 [{task.name}] 未使用标签：{task.brush_tag}")
        except Exception as err:
            # 标签清理失败不应影响刷流检查或任务删除主流程。
            logger.warning(f"清理刷流任务 [{task.name}] 标签失败：{str(err)}")

    def _cleanup_unused_task_tags(self) -> None:
        """扫描全部 qBittorrent 下载器，清理历史遗留的刷流唯一标签"""
        try:
            helper = DownloaderHelper()
            downloader_names = set(helper.get_configs().keys())
            for downloader_name in downloader_names:
                service = helper.get_service(name=downloader_name)
                if not service or not service.instance or not helper.is_downloader("qbittorrent", service=service):
                    continue
                client = getattr(service.instance, "qbc", None)
                if not client:
                    continue
                all_tags = [str(tag).strip() for tag in client.torrents_tags() or [] if str(tag).strip()]
                task_tags = [tag for tag in all_tags if tag.startswith("刷流-")]
                if not task_tags:
                    continue
                torrents, error = service.instance.get_torrents()
                if error:
                    logger.warning(f"扫描下载器 [{downloader_name}] 刷流标签时获取种子失败")
                    continue
                used_tags = {
                    tag
                    for torrent in torrents or []
                    for tag in {
                        item.strip()
                        for item in str(torrent.get("tags") or "").split(",")
                        if item.strip()
                    }
                }
                unused_tags = [tag for tag in task_tags if tag not in used_tags]
                if unused_tags and self._delete_qbittorrent_tags(service, unused_tags):
                    logger.info(f"清理下载器 [{downloader_name}] 未使用刷流标签：{','.join(unused_tags)}")
        except Exception as err:
            logger.warning(f"扫描清理历史刷流标签失败：{str(err)}")

    def _migrate_legacy_config(self, config: dict) -> List[dict]:
        """把旧全局配置和站点覆盖 JSON 拆分为一站点一任务"""
        overrides = self._parse_legacy_site_overrides(config)
        tasks: List[dict] = []
        for site_id in config.get("brushsites") or []:
            site = SiteOper().get(site_id)
            if not site or getattr(site, "public", False):
                continue
            task_data = {field: config.get(field) for field in TASK_CONFIG_FIELDS if field in config}
            site_override = overrides.get(site.name, {})
            task_data.update(site_override)
            # 旧版会把全局时区小时数转换成分钟后再次持久化，站点覆盖 JSON 则始终保留小时数。
            if "timezone_offset" not in site_override:
                timezone_offset = BrushTaskConfig._parse_number(config.get("timezone_offset")) or 0
                task_data["timezone_offset"] = float(timezone_offset) / 60
            task_data.update(
                {
                    "id": uuid.uuid4().hex,
                    "name": site.name,
                    "site_id": site.id,
                    "enabled": True,
                    "brush_interval": 10,
                    "check_interval": 5,
                }
            )
            tasks.append(BrushTaskConfig(task_data).to_dict())
        return tasks

    @staticmethod
    def _parse_legacy_site_overrides(config: dict) -> Dict[str, dict]:
        """解析旧版允许注释的站点覆盖 JSON"""
        if not config.get("enable_site_config") or not config.get("site_config"):
            return {}
        try:
            content = re.sub(r"//.*?(?:\n|$)", "", str(config.get("site_config"))).strip()
            rows = json.loads(content)
        except (TypeError, ValueError) as err:
            logger.error(f"解析旧版站点独立配置失败：{str(err)}")
            return {}
        overrides: Dict[str, dict] = {}
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or not row.get("sitename"):
                continue
            overrides[str(row["sitename"])] = {
                key: row[key] for key in LEGACY_SITE_OVERRIDE_FIELDS if key in row
            }
        return overrides

    def _migrate_legacy_data(self) -> None:
        """按站点把旧全局种子、归档和未托管记录迁移到任务命名空间"""
        if (self.get_data("task_data_schema_version") or 0) >= self.DATA_SCHEMA_VERSION:
            return
        tasks = list(self._task_configs.values())
        by_site_id = {str(task.site_id): task for task in tasks}
        by_site_name = {
            self._get_site_name(task.site_id): task for task in tasks if self._get_site_name(task.site_id)
        }
        for data_name in ("torrents", "archived", "unmanaged"):
            legacy_rows = self.get_data(data_name) or {}
            buckets: Dict[str, dict] = {task.id: {} for task in tasks}
            for item_id, item in legacy_rows.items() if isinstance(legacy_rows, dict) else []:
                task = by_site_id.get(str(item.get("site"))) or by_site_name.get(item.get("site_name"))
                if not task and len(tasks) == 1:
                    task = tasks[0]
                if not task:
                    continue
                migrated_item = dict(item)
                migrated_item.update({"task_id": task.id, "task_name": task.name})
                buckets[task.id][item_id] = migrated_item
            for task_id, rows in buckets.items():
                current = self._get_task_data(task_id, data_name)
                if not current and rows:
                    self._save_task_data(task_id, data_name, rows)
        for task in tasks:
            self._recalculate_statistics(task.id)
        self.save_data("task_data_schema_version", self.DATA_SCHEMA_VERSION)

    def _migrate_torrent_identity_data(self) -> None:
        """移除任务记录中未参与业务判断的旧 IMDb ID 字段。"""
        for task in self._task_configs.values():
            for data_name in ("torrents", "archived", "unmanaged"):
                rows = self._get_task_data(task.id, data_name)
                if not isinstance(rows, dict):
                    continue
                changed = False
                for item in rows.values():
                    if isinstance(item, dict) and "imdbid" in item:
                        item.pop("imdbid", None)
                        changed = True
                if changed:
                    self._save_task_data(task.id, data_name, rows)

    @staticmethod
    def _get_site_name(site_id: int) -> Optional[str]:
        """按站点 ID 获取名称并兼容已删除站点"""
        site = SiteOper().get(site_id)
        return site.name if site else None

    @contextmanager
    def _task_scope(self, task_id: str) -> Iterator[BrushTaskConfig]:
        """在当前线程中绑定任务上下文，供深层核心逻辑读取"""
        previous = getattr(self._task_context, "task_id", None)
        self._task_context.task_id = task_id
        try:
            task = self._task_configs.get(task_id)
            if not task:
                raise KeyError(f"刷流任务不存在：{task_id}")
            yield task
        finally:
            if previous is None:
                if hasattr(self._task_context, "task_id"):
                    delattr(self._task_context, "task_id")
            else:
                self._task_context.task_id = previous

    @contextmanager
    def _all_task_locks_scope(self) -> Iterator[None]:
        """按任务 ID 顺序锁定全部任务，保护跨任务删种的数据一致性"""
        acquired_locks: List[threading.Lock] = []
        try:
            for task_id in sorted(self._task_configs):
                task_lock = self._task_locks.setdefault(task_id, threading.Lock())
                task_lock.acquire()
                acquired_locks.append(task_lock)
            yield
        finally:
            for task_lock in reversed(acquired_locks):
                task_lock.release()

    def _get_task_config(self, task_id: Optional[str] = None) -> Optional[BrushTaskConfig]:
        """获取显式任务或当前线程绑定的任务配置"""
        resolved_id = task_id or getattr(self._task_context, "task_id", None)
        return self._task_configs.get(resolved_id) if resolved_id else None

    def __get_brush_config(self, sitename: str = None) -> Optional[BrushTaskConfig]:
        """兼容核心逻辑获取当前任务配置"""
        task = self._get_task_config()
        if task or not sitename:
            return task
        return next(
            (item for item in self._task_configs.values() if self._get_site_name(item.site_id) == sitename),
            None,
        )

    @staticmethod
    def _task_data_key(task_id: str, data_name: str) -> str:
        """生成任务独立的插件数据键"""
        return f"task.{task_id}.{data_name}"

    def _get_task_data(self, task_id: str, data_name: str) -> Any:
        """读取指定任务的独立持久化数据"""
        return self.get_data(self._task_data_key(task_id, data_name))

    def _save_task_data(self, task_id: str, data_name: str, value: Any) -> None:
        """保存指定任务的独立持久化数据"""
        self.save_data(self._task_data_key(task_id, data_name), value)

    def _current_task_data(self, data_name: str, default: Any = None) -> Any:
        """读取当前线程任务的数据并提供缺省值"""
        task = self._get_task_config()
        if not task:
            return default
        value = self._get_task_data(task.id, data_name)
        return default if value is None else value

    def _save_current_task_data(self, data_name: str, value: Any) -> None:
        """保存当前线程任务的数据"""
        task = self._get_task_config()
        if task:
            self._save_task_data(task.id, data_name, value)

    def _submit_task_operation(self, task_id: str, operation: str) -> schemas.Response:
        """校验运行条件后把手动操作提交到宿主线程池"""
        task = self._task_configs.get(task_id)
        if not task:
            return schemas.Response(success=False, message="刷流任务不存在")
        if not self.get_state() or not task.enabled:
            return schemas.Response(success=False, message="插件或任务未启用")
        if not self._mark_task_queued(task_id, operation):
            return schemas.Response(success=False, message="任务已有操作正在执行")
        target = self.brush if operation == "brush" else self.check
        try:
            ThreadHelper().submit(target, task_id)
        except Exception as err:
            self._set_runtime(task_id, state="idle", operation=None, last_error=str(err))
            logger.error(f"提交刷流任务 [{task.name}] 失败：{str(err)}")
            return schemas.Response(success=False, message="任务提交失败")
        return schemas.Response(success=True, message="任务已提交", data=self._task_summary(task_id))

    def _is_task_busy(self, task_id: str) -> bool:
        """判断任务是否已经排队或正在执行，保护运行中的配置与数据"""
        task_lock = self._task_locks.get(task_id)
        with self._runtime_lock:
            runtime = self._runtime.get(task_id, {})
            return bool(
                runtime.get("state") in {"queued", "running"}
                or self._brush_lock.locked()
                or (task_lock and task_lock.locked())
            )

    def _mark_task_queued(self, task_id: str, operation: str) -> bool:
        """以原子方式把空闲任务标记为排队，避免重复提交手动操作"""
        with self._runtime_lock:
            runtime = self._runtime.setdefault(
                task_id,
                {"state": "idle", "operation": None, "last_error": None},
            )
            if runtime.get("state") in {"queued", "running"}:
                return False
            runtime.update({"state": "queued", "operation": operation, "last_error": None})
            return True

    def _set_runtime(self, task_id: str, **updates: Any) -> None:
        """线程安全地更新任务瞬时运行状态"""
        with self._runtime_lock:
            runtime = self._runtime.setdefault(task_id, {"state": "idle", "operation": None, "last_error": None})
            runtime.update(updates)

    def _append_run(self, task_id: str, report: dict) -> None:
        """保存最近的刷流或检查诊断记录"""
        history = self._get_task_data(task_id, "runs") or []
        stored_report = {
            **report,
            "reason_counts": dict(report.get("reason_counts") or {}),
        }
        history.insert(0, stored_report)
        self._save_task_data(task_id, "runs", history[: self.MAX_RUN_HISTORY])

    def _build_status_data(self) -> Dict[str, Any]:
        """组装工作台总览、任务摘要和可选站点下载器"""
        site_user_data = self._latest_site_user_data_by_domain()
        task_rows = [
            self._task_summary(task_id, site_user_data_by_domain=site_user_data)
            for task_id in self._task_configs
        ]
        ordered = sorted(task_rows, key=lambda row: (not row["enabled"], row["priority"],
                         -(row.get("site_ratio", {}).get("deficit_fraction") or 0), row["id"]))
        for position, row in enumerate(ordered, 1):
            row["priority_order"] = position
        aggregate = {
            "task_count": len(task_rows),
            "enabled_count": sum(1 for row in task_rows if row.get("enabled")),
            "running_count": sum(
                1 for row in task_rows if row.get("state") in {"running", "brush", "check"}
            ),
            "active_count": sum(row.get("statistic", {}).get("active", 0) for row in task_rows),
            "uploaded": sum(row.get("statistic", {}).get("uploaded", 0) for row in task_rows),
            "downloaded": sum(row.get("statistic", {}).get("downloaded", 0) for row in task_rows),
            "seeding_size": sum(row.get("seeding_size", 0) for row in task_rows),
        }
        site_options = [
            {"title": site.get("name"), "value": site.get("id")}
            for site in SitesHelper().get_indexers()
            if not site.get("public")
        ]
        downloader_options = [
            {"title": item.name, "value": item.name}
            for item in DownloaderHelper().get_configs().values()
        ]
        return {
            "enabled": self.get_state(),
            "show_sidebar_nav": self._show_sidebar_nav,
            "rotation_mode": getattr(self, "_rotation_mode", "parallel"),
            "site_rotation": self._build_rotation_status(),
            "global_defaults": self._global_defaults,
            **{field: getattr(self, f"_{field}", None) for field in GLOBAL_LIMIT_FIELDS},
            **{field: getattr(self, f"_{field}", None) for field in GLOBAL_DYNAMIC_DELETE_FIELDS},
            "summary": aggregate,
            "metrics": self.get_data("metrics") or {},
            "download_slots": (self.get_data("download_slots") or {}) if getattr(self, "_rotation_mode", "parallel") == "parallel" else {},
            "tasks": task_rows,
            "options": {"sites": site_options, "downloaders": downloader_options},
        }

    def _task_summary(
        self,
        task_id: str,
        site_user_data_by_domain: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """组装单个任务在左侧任务列表和仪表板中的摘要"""
        task = self._task_configs.get(task_id)
        if not task:
            return {}
        statistic = self._get_statistic_info(task_id)
        torrents = self._get_task_data(task_id, "torrents") or {}
        history = self._get_task_data(task_id, "runs") or []
        runtime = dict(self._runtime.get(task_id, {}))
        site_ratio = self._build_site_ratio_status(task, site_user_data_by_domain)
        if not self.get_state():
            display_state = "disabled"
        elif not task.enabled:
            display_state = "paused"
        elif runtime.get("state") in {"queued", "running"}:
            display_state = runtime.get("operation") or "running"
        elif not self._is_current_time_in_range(task):
            display_state = "waiting"
        elif not self._is_focus_site(task.site_id):
            display_state = "waiting"
        elif task_id in self._pending_brush:
            display_state = "waiting"
        elif runtime.get("last_error"):
            display_state = "error"
        else:
            display_state = "running"
        return {
            "id": task.id,
            "name": task.name,
            "enabled": task.enabled,
            "site_id": task.site_id,
            "site_name": self._get_site_name(task.site_id) or "站点已删除",
            "downloader": task.downloader,
            "brush_interval": task.brush_interval,
            "check_interval": task.check_interval,
            "cron": task.cron,
            "active_time_range": task.active_time_range,
            "state": display_state,
            "operation": runtime.get("operation"),
            "last_error": runtime.get("last_error"),
            "next_run_at": self._next_run_at(task, history),
            "last_run": history[0] if history else None,
            "statistic": statistic,
            "seeding_size": self.__calculate_seeding_torrents_size(torrents),
            "site_ratio": site_ratio,
            "priority": self._priority_tier(task, site_ratio),
            "priority_reason": "账号数据待刷新" if not site_ratio.get("available") or site_ratio.get("stale") else (
                ("新手考核" if self._novice_active(task) else "分享率未达标")
                if not site_ratio.get("reached") else "普通轮转"),
            "ratio_deficit_bytes": site_ratio.get("deficit_bytes"),
            "metrics": self._get_task_data(task_id, "metrics") or {},
            "novice": self._novice_active(task),
            "is_focus": ((self.get_data("site_rotation") or {}).get("site_id") == task.site_id
                         if getattr(self, "_rotation_mode", "parallel") == "sequential" else self._is_focus_site(task.site_id)),
        }

    @staticmethod
    def _latest_site_user_data_by_domain() -> Dict[str, Any]:
        """按标准化域名索引各站点最新一条有效用户统计。"""
        result: Dict[str, Any] = {}
        try:
            rows = SiteOper().get_userdata_latest() or []
        except Exception:
            logger.warning("站点统计读取失败，本轮按考核标记和轮转顺序分配")
            return result
        for row in rows:
            domain = StringUtils.get_url_domain(getattr(row, "domain", None))
            if domain and domain not in result:
                result[domain] = row
        return result

    @staticmethod
    def _site_ratio_values(user_data: Any) -> Optional[Tuple[Optional[float], Optional[float], Optional[float], bool, bool]]:
        if not user_data or getattr(user_data, "err_msg", None):
            return None
        ratio, upload, download = (
            None if isinstance(value, bool) else BrushTaskConfig._parse_number(value)
            for value in (getattr(user_data, "ratio", None), getattr(user_data, "upload", None),
                          getattr(user_data, "download", None))
        )
        upload = upload if upload is not None and upload >= 0 else None
        download = download if download is not None and download >= 0 else None
        known_bytes = upload is not None and download is not None
        unlimited = known_bytes and upload > 0 and download == 0
        estimated = (ratio is None or ratio <= 0) and known_bytes and download > 0
        if estimated:
            ratio = upload / download
        # 解析器的全零默认值不能当作真实低分享率。
        if (ratio is None or ratio <= 0) and not estimated and not unlimited:
            return None
        return ratio, upload, download, unlimited, estimated

    def _build_site_ratio_status(
        self,
        task: BrushTaskConfig,
        site_user_data_by_domain: Optional[Dict[str, Any]] = None,
        site: Any = None,
    ) -> Dict[str, Any]:
        """读取站点统计用于优先级，缺失数据仍允许轮转。"""
        status = {
            "enabled": True,
            "target": task.site_ratio_target,
            "current": None,
            "available": False,
            "unlimited": False,
            "reached": False,
            "updated_at": None,
            "estimated": False,
            "stale": False,
        }
        refresh = self.get_data(f"ratio_refresh.{task.site_id}") or {}
        if refresh.get("success") is False:
            return status
        site = site or SiteOper().get(task.site_id)
        if not site:
            return status
        if site_user_data_by_domain is None:
            site_user_data_by_domain = self._latest_site_user_data_by_domain()
        domain = StringUtils.get_url_domain(getattr(site, "domain", None))
        user_data = site_user_data_by_domain.get(domain)
        values = self._site_ratio_values(user_data)
        if values is None:
            return status
        ratio, upload, download, unlimited, estimated = values
        known_bytes = upload is not None and download is not None
        updated_day = getattr(user_data, "updated_day", None)
        updated_time = getattr(user_data, "updated_time", None)
        status.update(
            {
                "current": None if unlimited else float(ratio),
                "available": True,
                "unlimited": unlimited,
                "estimated": estimated,
                "reached": unlimited or bool(task.site_ratio_target and float(ratio) >= float(task.site_ratio_target)),
                "deficit_bytes": max(float(task.site_ratio_target or 0) * download - upload, 0) if known_bytes else None,
                "download_headroom": max(upload / float(task.site_ratio_target or 1) - download, 0) if known_bytes else None,
                "deficit_fraction": max(1 - float(ratio or 0) / float(task.site_ratio_target or 1), 0) if not unlimited else 0,
                "updated_at": " ".join(value for value in (updated_day, updated_time) if value) or None,
            }
        )
        if status["updated_at"]:
            try:
                updated = datetime.fromisoformat(status["updated_at"])
                if updated.tzinfo is None:
                    updated = updated.replace(tzinfo=ZoneInfo(settings.TZ))
                status["stale"] = (datetime.now().astimezone() - updated).total_seconds() > 86400
            except (TypeError, ValueError):
                status["stale"] = True
        return status

    @staticmethod
    def _novice_active(task: BrushTaskConfig) -> bool:
        """手动考核标记到期后自动恢复普通优先级。"""
        if not task.novice:
            return False
        if not task.novice_until:
            return True
        try:
            until = datetime.fromisoformat(task.novice_until.replace("Z", "+00:00"))
            if until.tzinfo is None:
                until = until.replace(tzinfo=ZoneInfo(settings.TZ))
            return until > datetime.now().astimezone()
        except (TypeError, ValueError):
            return False

    def _priority_tier(self, task: BrushTaskConfig, status: dict) -> int:
        """有效未达标站优先；缺失或陈旧数据不参与风险排序。"""
        if not status.get("available") or status.get("stale"):
            return 3
        if task.site_ratio_target and not status.get("reached"):
            return 0 if self._novice_active(task) else 1
        return 2

    def _sample_transfers(self) -> None:
        """集中读取下载器并保存各站真实窗口，界面请求不触发网络采样。"""
        snapshot = self._read_downloads(allow_partial=True)
        now = time.time()
        all_metrics = []
        seen = set()
        for task in self._task_configs.values():
            task_lock = self._task_locks.setdefault(task.id, threading.Lock())
            if not task_lock.acquire(blocking=False):
                all_metrics.append({"seconds": 0, "uploaded": None, "downloaded": None,
                                    "gap": True, "partial": True})
                continue
            try:
                records = self._get_task_data(task.id, "torrents") or {}
                metrics = {"sampled_at": now, "seconds": 0, "uploaded": 0, "downloaded": 0,
                           "upspeed": 0, "dlspeed": 0, "downloading": 0, "queued": 0,
                           "seeding": 0, "paused": 0, "gap": False, "partial": False}
                if task.downloader not in snapshot:
                    metrics.update(uploaded=None, downloaded=None, gap=True, reason="下载器不可用")
                    self._save_task_data(task.id, "metrics", metrics)
                    all_metrics.append(metrics)
                    continue
                with self._task_scope(task.id):
                    for torrent_hash, record in records.items():
                        row = snapshot[task.downloader].get(torrent_hash)
                        if record.get("deleted") and record.get("transfer_sample"):
                            previous = record["transfer_sample"]
                            update_transfer_sample(record, {"uploaded": previous["uploaded"],
                                "transfer_downloaded": previous["downloaded"], "completed": True,
                                "is_downloading": False}, now)
                        if row is not None and not record.get("deleted"):
                            info = self.__get_torrent_info(row)
                            update_transfer_sample(record, info, now)
                            metrics["upspeed"] += info.get("upspeed", 0)
                            metrics["dlspeed"] += info.get("dlspeed", 0)
                            if info["state"] in {"stopped", "stoppeddl", "pauseddl", "pausedup", "stoppedup"}:
                                metrics["paused"] += 1
                            elif info["completed"]:
                                metrics["seeding"] += 1
                            elif info["is_downloading"]:
                                metrics["downloading"] += 1
                            elif info["state"] in {"download pending", "queueddl"}:
                                metrics["queued"] += 1
                            else:
                                metrics["paused"] += 1
                        window = transfer_window(record, now)
                        if record.get("deleted") and now - (record.get("deleted_time") or 0) > 1800:
                            continue
                        if (task.downloader, torrent_hash) in seen:
                            continue
                        seen.add((task.downloader, torrent_hash))
                        for field in ("uploaded", "downloaded"):
                            if window[field] is None:
                                metrics["partial"] = True
                            else:
                                metrics[field] += window[field]
                        metrics["seconds"] = max(metrics["seconds"], window["seconds"])
                        metrics["gap"] = metrics["gap"] or window["gap"]
                self._save_task_data(task.id, "torrents", records)
                metrics["reason"] = ("正在下载" if metrics["downloading"] else "等待下载名额" if metrics["queued"]
                                     else "正在做种" if metrics["seeding"] else "等待合格种子")
                alert = self.get_data(f"allocation_alert.{task.downloader}") or []
                if any(h in snapshot[task.downloader] and str(getattr(snapshot[task.downloader][h], "status", "")) == "stopped" for h in alert):
                    metrics["reason"] = "上次调度恢复未确认，请检查暂停任务"
                    metrics["gap"] = True
                self._save_task_data(task.id, "metrics", metrics)
                all_metrics.append(metrics)
            finally:
                task_lock.release()
        if all_metrics:
            aggregate = {key: sum(item.get(key) or 0 for item in all_metrics)
                         for key in ("uploaded", "downloaded", "upspeed", "dlspeed", "downloading", "queued", "seeding", "paused")}
            aggregate.update(sampled_at=now, seconds=max(item["seconds"] for item in all_metrics),
                             gap=any(item["gap"] for item in all_metrics),
                             partial=any(item.get("partial") or item["uploaded"] is None for item in all_metrics))
            self.save_data("metrics", aggregate)

    def _evaluate_site_ratio_control(
        self,
        task: BrushTaskConfig,
        site: Any = None,
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """保留分享率诊断，达标与数据缺失均不阻止刷流。"""
        status = self._build_site_ratio_status(
            task, getattr(self._task_context, "site_user_data", None), site=site,
        )
        return True, None, status

    def _build_task_detail(
        self,
        task_id: str,
        state: str = "active",
        page: int = 1,
        page_size: int = 50,
    ) -> Dict[str, Any]:
        """组装任务编辑、概览、诊断与分页种子数据"""
        task = self._task_configs[task_id]
        torrents = self._get_task_data(task_id, "torrents") or {}
        archived = self._get_task_data(task_id, "archived") or {}
        rows = list(torrents.values())
        if state == "active":
            rows = [row for row in rows if not row.get("deleted")]
        elif state == "deleted":
            rows = [row for row in rows if row.get("deleted")] + list(archived.values())
        else:
            rows.extend(archived.values())
        rows.sort(key=lambda item: item.get("time") or 0, reverse=True)
        total = len(rows)
        start = (page - 1) * page_size
        selected_rows = rows[start : start + page_size]
        return {
            "task": task.to_dict(),
            "effective_config": task.effective_dict(),
            "global_defaults": self._global_defaults,
            "summary": self._task_summary(task_id),
            "runs": (self._get_task_data(task_id, "runs") or [])[:20],
            "torrents": {
                "items": selected_rows,
                "total": total,
                "page": page,
                "page_size": page_size,
                "state": state,
            },
        }

    @staticmethod
    def _next_run_at(task: BrushTaskConfig, history: List[dict]) -> Optional[str]:
        """按 CRON 或固定间隔估算任务下一次刷新时间"""
        now = datetime.now().astimezone()
        try:
            if task.cron:
                next_time = CronTrigger.from_crontab(task.cron, timezone=settings.TZ).get_next_fire_time(None, now)
            else:
                last_brush = next((item for item in history if item.get("kind") == "brush"), None)
                if last_brush and last_brush.get("started_at"):
                    base = datetime.fromisoformat(last_brush["started_at"])
                    next_time = max(base + timedelta(minutes=task.brush_interval), now)
                else:
                    next_time = now + timedelta(minutes=task.brush_interval)
            return next_time.isoformat(timespec="minutes") if next_time else None
        except (TypeError, ValueError):
            return None

    def _queue_brush(self, task_id: Optional[str] = None) -> None:
        """保留到期任务；重复触发不会改变排队顺序。"""
        task = self._get_task_config(task_id)
        if not task or not self.get_state() or not task.enabled or self._stopping:
            return
        with self._runtime_lock:
            self._pending_brush.setdefault(task.id, time.time())

    def _site_last_added(self, site_id: int) -> float:
        """跨同站点任务读取最近成功加种时间，重载后仍保留轮转顺序。"""
        latest = 0.0
        for task in list(self._task_configs.values()):
            if task.site_id != site_id:
                continue
            rotation = self._get_task_data(task.id, "rotation") or {}
            added = rotation.get("last_added")
            if added is None:
                records = self._get_task_data(task.id, "torrents") or {}
                added = max((row.get("time", 0) or 0 for row in records.values()), default=0)
            latest = max(latest, float(added))
        return latest

    def _is_focus_site(self, site_id: int) -> bool:
        """驻留控制新增种子和排队优先级，已有下载继续运行。"""
        if getattr(self, "_rotation_mode", "parallel") != "sequential":
            return True
        state = self.get_data("site_rotation") or {}
        deadline = BrushTaskConfig._parse_number(state.get("deadline_at")) or 0
        return state.get("site_id") == site_id and (state.get("phase") == "ratio" or deadline > time.time()) and not state.get("retry_at")

    def _select_rotation_site(self, site_data: dict, now: Optional[float] = None) -> Optional[int]:
        """未达标站按优先级逐站补分享率，全部达标后按配置顺序驻留。"""
        if getattr(self, "_rotation_mode", "parallel") != "sequential":
            return None
        now = time.time() if now is None else now
        tasks = [task for task in self._task_configs.values() if task.enabled]
        site_ids = {task.site_id for task in tasks}
        state = self.get_data("site_rotation") or {}
        cooldowns = {str(sid): until for sid, until in state.get("retry_cooldowns", {}).items()
                     if str(sid) in {str(site_id) for site_id in site_ids}}
        if not tasks:
            if state:
                self.save_data("site_rotation", {})
            return None
        visited = [sid for sid in state.get("visited_site_ids", []) if sid in site_ids]
        for task in tasks:
            refresh = self.get_data(f"ratio_refresh.{task.site_id}") or {}
            if refresh.get("failures", 0) >= 6 and refresh.get("next_attempt", 0) > now:
                cooldowns[str(task.site_id)] = refresh["next_attempt"]
        eligible = [task for task in tasks if cooldowns.get(str(task.site_id), 0) <= now]
        if not eligible:
            self.save_data("site_rotation", {"visited_site_ids": visited, "retry_cooldowns": cooldowns})
            return None
        ratios = {task.id: self._build_site_ratio_status(task, site_data) for task in eligible}
        current = next((task for task in eligible if task.site_id == state.get("site_id")), None)
        deadline = BrushTaskConfig._parse_number(state.get("deadline_at")) or 0
        current_unmet = current and any(not (ratios[task.id].get("available") and not ratios[task.id].get("stale")
                                            and ratios[task.id].get("reached"))
                                       for task in eligible if task.site_id == current.site_id)
        if current and (state.get("retry_at") or (state.get("phase") == "ratio" and current_unmet)
                        or (state.get("phase") != "ratio" and deadline > now)):
            # 已跳过的站点到期后可重试，但不能抢回仍在驻留的其他站。
            eligible = [task for task in eligible if task.site_id == current.site_id or str(task.site_id) not in cooldowns]
        unmet = [task for task in eligible if not (ratios[task.id].get("available")
                 and not ratios[task.id].get("stale") and ratios[task.id].get("reached"))]
        verified_unmet = [task for task in unmet if ratios[task.id].get("available") and not ratios[task.id].get("stale")]
        unmet = verified_unmet or unmet
        phase = "ratio" if unmet else "round_robin"
        if phase == "round_robin" and state.get("phase") == "ratio":
            visited = []
        if current and ((state.get("retry_at") and (not verified_unmet or current in verified_unmet)) or (phase == "ratio" and current.site_id in {task.site_id for task in unmet}
                and state.get("phase") == "ratio") or
                (phase == "round_robin" and state.get("phase") != "ratio" and deadline > now)):
            return current.site_id
        remaining = unmet or [task for task in eligible if task.site_id not in visited]
        if not remaining:
            visited = []
            remaining = eligible
        selected = min(remaining, key=lambda task: (
            self._priority_tier(task, ratios[task.id]), -(ratios[task.id].get("deficit_fraction") or 0),
            tasks.index(task),
        )) if unmet else remaining[0]
        cooldowns.pop(str(selected.site_id), None)
        self.save_data("site_rotation", {
            "site_id": selected.site_id,
            "started_at": now,
            "deadline_at": None if phase == "ratio" else now + selected.rotation_hours * 3600,
            "phase": phase,
            "visited_site_ids": [sid for sid in visited if sid != selected.site_id] + [selected.site_id],
            "retry_cooldowns": cooldowns,
        })
        for task in tasks:
            if task.site_id == selected.site_id:
                self._queue_brush(task.id)
        logger.info(f"刷流当前站点：{selected.name}，{'达标后切换' if phase == 'ratio' else '按驻留时间轮转'}，已有种子继续托管")
        return selected.site_id

    def _refresh_focus_ratio(self) -> None:
        """后台低频刷新当前站账号量，失败退避，不阻塞选种和流量采样。"""
        if getattr(self, "_rotation_mode", "parallel") != "sequential":
            return
        state = self.get_data("site_rotation") or {}
        site_id = state.get("site_id")
        if not site_id or not state.get("health_checked"):
            return
        key = f"ratio_refresh.{site_id}"
        refresh = self.get_data(key) or {}
        now = time.time()
        if refresh.get("next_attempt", 0) > now:
            return
        self.save_data(key, {**refresh, "next_attempt": now + 1800})

        def refresh_account():
            success = False
            try:
                site = SiteOper().get(site_id)
                indexer = SitesHelper().get_indexer(site.domain) if site else None
                data = SiteChain().refresh_userdata(site=indexer) if indexer else None
                success = bool(data and getattr(data, "userid", None) and self._site_ratio_values(data) is not None)
            except Exception:
                logger.warning("当前刷流站账号数据刷新失败，等待重试")
            failures = 0 if success else refresh.get("failures", 0) + 1
            self.save_data(key, {"success": success, "failures": failures, "checked_at": time.time(),
                                "next_attempt": time.time() + (1800 if success else 3600 if failures >= 6 else 600)})

        try:
            ThreadHelper().submit(refresh_account)
        except Exception:
            self.save_data(key, {**refresh, "next_attempt": now + 600})

    def _check_rotation_site(self, now: Optional[float] = None) -> bool:
        """首次失败后每十分钟重试，五次仍失败则让出驻留。"""
        if getattr(self, "_rotation_mode", "parallel") != "sequential":
            return True
        now = time.time() if now is None else now
        state = self.get_data("site_rotation") or {}
        task = next((task for task in self._task_configs.values()
                     if task.enabled and task.site_id == state.get("site_id")), None)
        if not task:
            return False
        if state.get("health_checked"):
            return True
        if state.get("retry_at", 0) > now:
            return False
        site = SiteOper().get(task.site_id)
        try:
            healthy = bool(site and SiteChain().test(site.domain)[0])
        except Exception:
            healthy = False
        if healthy:
            recovered = bool(state.get("retry_at"))
            if recovered:
                state.update(started_at=now, deadline_at=None if state.get("phase") == "ratio" else now + task.rotation_hours * 3600)
            state.update(health_checked=True, retry_count=0, retry_at=None)
            self.save_data("site_rotation", state)
            if recovered:
                for row in self._task_configs.values():
                    if row.enabled and row.site_id == task.site_id:
                        self._queue_brush(row.id)
            return True
        retries = state.get("retry_count", 0) + (1 if state.get("retry_at") else 0)
        if retries >= 5:
            cooldowns = state.get("retry_cooldowns", {})
            cooldowns[str(task.site_id)] = now + 600
            state.update(site_id=None, deadline_at=0, retry_at=None, retry_count=retries,
                         retry_cooldowns=cooldowns)
            logger.warning(f"刷流站点 [{task.name}] 重试五次仍不可用，跳过本轮")
        else:
            state.update(retry_count=retries, retry_at=now + 600)
            logger.warning(f"刷流站点 [{task.name}] 不可用，十分钟后重试（已重试 {retries}/5）")
        self.save_data("site_rotation", state)
        return False

    def _build_rotation_status(self) -> Dict[str, Any]:
        state = self.get_data("site_rotation") or {}
        mode = getattr(self, "_rotation_mode", "parallel")
        site_id = state.get("site_id") if mode == "sequential" else None
        task = next((row for row in self._task_configs.values() if row.site_id == site_id and row.enabled), None)
        started = BrushTaskConfig._parse_number(state.get("started_at")) if task else None
        deadline = BrushTaskConfig._parse_number(state.get("deadline_at")) if task else None
        return {
            "mode": mode,
            "phase": state.get("phase", "round_robin"),
            "site_id": site_id if task else None,
            "site_name": (self._get_site_name(site_id) or task.name) if task else None,
            "started_at": datetime.fromtimestamp(started, ZoneInfo(settings.TZ)).isoformat() if started else None,
            "deadline_at": datetime.fromtimestamp(deadline, ZoneInfo(settings.TZ)).isoformat() if deadline else None,
            "remaining_seconds": max(0, int(deadline - time.time())) if deadline else 0,
            "visited_site_ids": state.get("visited_site_ids", []) if mode == "sequential" else [],
            "retry_count": state.get("retry_count", 0),
            "retry_at": state.get("retry_at"),
        }

    def brush(self, task_id: Optional[str] = None) -> None:
        """手动刷新进入统一轮转，并检查空间额度。"""
        self._queue_brush(task_id)
        self._dispatch_brush()

    def _dispatch_brush(self) -> None:
        """按站点优先级逐轮添加候选，仅由空间约束停止。"""
        if not self.get_state() or self._stopping or not self._brush_lock.acquire(blocking=False):
            return
        reports = {}
        self._task_context.candidates = {}
        try:
            self._sample_transfers()
            site_data = self._latest_site_user_data_by_domain()
            self._select_rotation_site(site_data)
            if not self._check_rotation_site():
                return
            self._refresh_focus_ratio()
            self._prioritize_download_queue(site_data)
            with self._runtime_lock:
                for tid in list(self._pending_brush):
                    if tid not in self._task_configs or not self._task_configs[tid].enabled:
                        self._pending_brush.pop(tid, None)
                pending = dict(self._pending_brush)
            if not pending:
                return
            tasks = [self._task_configs[tid] for tid in pending
                     if self._is_focus_site(self._task_configs[tid].site_id)]
            if not tasks:
                return
            self._task_context.site_user_data = site_data
            priorities = {task.id: self._priority_tier(task, self._build_site_ratio_status(task, site_data))
                          for task in tasks}
            tasks.sort(key=lambda task: (priorities[task.id], -(self._build_site_ratio_status(task, site_data).get("deficit_fraction") or 0), self._site_last_added(task.site_id),
                                         pending[task.id], task.id))
            scheduled_deadlines = {task.id: self._next_promotion_expiry(task) for task in tasks}
            self._task_context.downloads = self._read_downloads(allow_partial=True)
            while tasks and not self._stopping and self.get_state():
                next_round = []
                for task in tasks:
                    if self._stopping or not self.get_state():
                        break
                    if not self._is_focus_site(task.site_id):
                        continue
                    if not task.enabled:
                        with self._runtime_lock:
                            self._pending_brush.pop(task.id, None)
                        continue
                    if task.downloader not in self._task_context.downloads:
                        self._set_runtime(task.id, state="idle", operation=None, last_error="下载器不可用，等待重试")
                        continue
                    report = reports.get(task.id) or self._new_run_report("brush")
                    before = report["added_count"]
                    result = self._execute_brush(task, report)
                    if result is None:
                        continue
                    reports[task.id] = result
                    if result.get("result") == "outside_focus":
                        continue
                    if result["added_count"] > before:
                        deadline = self._next_promotion_expiry(task)
                        previous = scheduled_deadlines.get(task.id)
                        if deadline and (previous is None or deadline < previous):
                            self._refresh_scheduler()
                            scheduled_deadlines[task.id] = deadline
                    if result.get("success") and result["added_count"] > before and self._task_context.candidates.get(task.id):
                        next_round.append(task)
                    else:
                        with self._runtime_lock:
                            self._pending_brush.pop(task.id, None)
                tasks = next_round
        except Exception as err:
            logger.error(f"刷流分配失败，保留待执行任务：{str(err)}")
        finally:
            try:
                for task_id, report in reports.items():
                    report["finished_at"] = self._now_iso()
                    self._append_run(task_id, report)
            finally:
                for key in ("downloads", "candidates", "site_user_data"):
                    if hasattr(self._task_context, key):
                        delattr(self._task_context, key)
                try:
                    if any(report.get("added_count") for report in reports.values()):
                        self._prioritize_download_queue(self._latest_site_user_data_by_domain())
                        self._refresh_scheduler()
                finally:
                    self._brush_lock.release()

    def _prioritize_download_queue(self, site_data: dict) -> None:
        """同步等待顺序，并让 Transmission 并行下载按站点优先级让位。"""
        tasks = [task for task in self._task_configs.values() if task.enabled]
        helper = DownloaderHelper()
        for name in {task.downloader for task in tasks if task.downloader}:
            try:
                service = helper.get_service(name=name)
                if not service or not service.instance:
                    continue
                downloader = service.instance
                tr = getattr(downloader, "trc", None)
                qb = getattr(downloader, "qbc", None)
                if tr:
                    rows = tr.get_torrents(arguments=["hashString", "status", "queuePosition", "labels", "isStalled",
                                                     "rateDownload", "rateUpload"])
                    candidates = [(row.hashString, row.queue_position, set(row.labels or []))
                                  for row in rows if str(row.status) in {"downloading", "download pending"}]
                    queued = [(row.hashString, row.queue_position, set(row.labels or []))
                              for row in rows if str(row.status) == "download pending"]
                elif qb:
                    rows = qb.torrents_info()
                    queued = [(row["hash"], row["priority"], self._download_labels(row))
                              for row in rows if row.get("state") == "queuedDL"]
                    candidates = queued
                else:
                    continue
                queued.sort(key=lambda row: row[1])
                ranks = {}
                for task in tasks:
                    if task.downloader != name or not self._is_focus_site(task.site_id):
                        continue
                    records = self._get_task_data(task.id, "torrents") or {}
                    tier = self._priority_tier(task, self._build_site_ratio_status(task, site_data))
                    ratio = self._build_site_ratio_status(task, site_data)
                    rank = (tier, -(ratio.get("deficit_fraction") or 0) if tier == 1 else 0)
                    for torrent_hash, position, tags in candidates:
                        if torrent_hash in records or task.brush_tag in tags:
                            ranks[torrent_hash] = min(ranks.get(torrent_hash, rank), rank)
                desired = [row[0] for row in sorted(queued, key=lambda row: (ranks.get(row[0], (3, 0)), row[1]))
                           if row[0] in ranks]
                if [row[0] for row in queued[:len(desired)]] != desired:
                    for torrent_hash in reversed(desired):
                        if tr:
                            tr.queue_top(ids=[torrent_hash])
                        else:
                            qb.torrents_top_priority(torrent_hashes=[torrent_hash])
                if tr:
                    self._adjust_download_budget(tr, rows, name)
                    self._rebalance_transmission_downloads(tr, rows, ranks, name)
            except Exception:
                logger.warning(f"下载器 {name} 刷流队列排序失败，等待重试")

    def _adjust_download_budget(self, tr, rows, name: str) -> None:
        """专用刷流下载器在用户上限内小步调整并发，共用下载器不改全局设置。"""
        if getattr(self, "_stopping", False) or not self.get_state() or getattr(self, "_rotation_mode", "parallel") != "parallel":
            return
        session = tr.get_session()
        limit = getattr(session, "download_queue_size", None)
        if session.download_queue_enabled is not True or not isinstance(limit, int) or limit < 1:
            return
        tasks = [task for task in self._task_configs.values() if task.downloader == name]
        records = {h for task in tasks for h in (self._get_task_data(task.id, "torrents") or {})}
        tags = {task.brush_tag for task in tasks}
        if any(row.hashString not in records and not tags.intersection(row.labels or []) for row in rows):
            state = self.get_data(f"download_budget.{name}") or {}
            if limit == state.get("applied") and state.get("ceiling"):
                tr.set_session(download_queue_size=state["ceiling"])
            self.save_data(f"download_budget.{name}", {})
            return
        key = f"download_budget.{name}"
        state = self.get_data(key) or {}
        now = time.time()
        if limit != state.get("applied"):
            state = {"ceiling": limit, "applied": limit, "changed_at": 0}
        ceiling = state["ceiling"]
        demand = sum(str(row.status) in {"downloading", "download pending"} for row in rows)
        active = sum(str(row.status) == "downloading" and not (
            getattr(session, "queue_stalled_enabled", False) is True and getattr(row, "is_stalled", False)) for row in rows)
        budget = limit
        reason = "保持并发"
        if not state["changed_at"] and demand:
            budget = min(ceiling, max(2, min(demand, 4)))
            reason = "从少量并发试跑"
        elif demand and now - state["changed_at"] >= 300:
            metrics = [self._get_task_data(task.id, "metrics") or {} for task in tasks]
            valid = [item for item in metrics if item.get("seconds", 0) >= 300
                     and not item.get("gap") and not item.get("partial")
                     and now - item.get("sampled_at", 0) <= 90]
            complete = valid and all(item in valid for item in metrics
                                     if item.get("downloading") or item.get("queued") or item.get("seeding")) \
                and not (self.get_data("metrics") or {}).get("partial")
            upload = sum((item.get("uploaded") or 0) / item["seconds"] for item in valid)
            download = sum((item.get("downloaded") or 0) / item["seconds"] for item in valid)
            up_limit = getattr(session, "speed_limit_up", 0) if getattr(session, "speed_limit_up_enabled", False) else 0
            down_limit = getattr(session, "speed_limit_down", 0) if getattr(session, "speed_limit_down_enabled", False) else 0
            if complete and up_limit and upload >= up_limit * 1000 * .9:
                budget = max(min(2, ceiling), limit - 1)
                reason = "上传已接近预算，减少新增下载竞争"
            elif complete and demand > limit and (not down_limit or download < down_limit * 1000 * .85):
                budget = min(ceiling, limit + 1)
                reason = "有排队需求且带宽有余量，增加一个名额"
            state["changed_at"] = now
        if budget != limit:
            tr.set_session(download_queue_size=budget)
            state.update(applied=budget, changed_at=now)
        elif not state["changed_at"] and demand:
            state["changed_at"] = now
        self.save_data(key, state)
        self.save_data("download_slots", {"active": active, "budget": budget, "limit": ceiling,
                                          "reason": reason, "downloader": name})

    def _rebalance_transmission_downloads(self, tr, rows, ranks: dict, name: str) -> None:
        """仅把低优先级刷流下载退回普通队列，不绕过下载器并发上限。"""
        key = f"priority_requeue.{name}"
        pending = self.get_data(key) or []
        if pending:
            existing = {row.hashString: row for row in rows}
            for torrent_hash in list(pending):
                row = existing.get(torrent_hash)
                if row and str(row.status) == "stopped":
                    logger.warning(f"下载器 {name} 上次让位任务仍暂停，保留人工处理")
                    alerts = self.get_data(f"allocation_alert.{name}") or []
                    self.save_data(f"allocation_alert.{name}", list(dict.fromkeys(alerts + [torrent_hash])))
                pending.remove(torrent_hash)
                self.save_data(key, pending)
            return
        if getattr(self, "_rotation_mode", "parallel") != "parallel":
            return
        session = tr.get_session()
        if session.download_queue_enabled is not True:
            return
        active = [row for row in rows if str(row.status) == "downloading"
                  and not (getattr(session, "queue_stalled_enabled", False) is True
                           and getattr(row, "is_stalled", False))]
        limit = getattr(session, "download_queue_size", None)
        slots = max(limit - sum(row.hashString not in ranks for row in active), 0) if isinstance(limit, int) else sum(row.hashString in ranks for row in active)
        owner = {}
        for task in self._task_configs.values():
            if not task.enabled or task.downloader != name:
                continue
            records = self._get_task_data(task.id, "torrents") or {}
            for row in rows:
                if row.hashString in records or task.brush_tag in (row.labels or []):
                    owner.setdefault(row.hashString, task.site_id)
        active_hashes = {row.hashString for row in active if row.hashString in ranks}
        candidates = [{"hash": row.hashString, "site_id": owner.get(row.hashString),
                       "priority": ranks[row.hashString], "active": row.hashString in active_hashes,
                       "position": row.queue_position}
                      for row in rows if row.hashString in ranks and
                      (row.hashString in active_hashes or str(row.status) == "download pending")]
        desired = allocate_downloads(candidates, slots)
        victims = sorted(active_hashes - set(desired))
        promoted = [h for h in desired if h not in active_hashes]
        if not victims:
            return
        allocation_key = f"allocation.{name}"
        allocation = self.get_data(allocation_key) or {}
        if time.time() - allocation.get("at", 0) < 120 and allocation.get("slots") == slots:
            return
        for torrent_hash in reversed(desired):
            tr.queue_top(ids=[torrent_hash])
        self.save_data(key, victims)
        try:
            tr.stop_torrent(ids=victims)
            self._wait_transmission_allocation(tr, victims, promoted)
        finally:
            # 只恢复本次短事务；中断后的暂停状态留待处理，避免覆盖人工暂停。
            tr.start_torrent(ids=victims, bypass_queue=False)
            self._wait_transmission_allocation(tr, [], [], resumed=victims)
            self.save_data(key, [])
            self.save_data(allocation_key, {"at": time.time(), "slots": slots})

    @staticmethod
    def _wait_transmission_allocation(tr, stopped: list, active: list, resumed: list = None) -> None:
        wanted = set(stopped + active + (resumed or []))
        for attempt in range(12):
            rows = tr.get_torrents(ids=list(wanted), arguments=["hashString", "status"])
            states = {row.hashString: str(row.status) for row in rows}
            if all(states.get(h) == "stopped" for h in stopped) \
                    and all(states.get(h) in {"downloading", "seeding", "seed pending"} for h in active) \
                    and all(h in states and states[h] != "stopped" for h in (resumed or [])):
                return
            time.sleep(.25)
        raise RuntimeError("下载器未确认调度状态，等待后续检查")

    def _execute_brush(self, task: BrushTaskConfig, report: Optional[dict] = None) -> Optional[dict]:
        """在分配锁内执行一个站点轮次，检查占用时保留请求。"""
        task_lock = self._task_locks.setdefault(task.id, threading.Lock())
        if not task_lock.acquire(blocking=False):
            return None
        report = report if report is not None else self._new_run_report("brush")
        self._set_runtime(task.id, state="running", operation="brush", last_error=None)
        try:
            with self._task_scope(task.id):
                self._run_brush(task, report)
            report["success"] = report.get("result") not in {"downloader_unavailable", "site_missing", "source_unavailable"}
            if report.get("error"):
                self._set_runtime(task.id, last_error=report["error"])
        except Exception as err:
            report.update({"success": False, "error": str(err)})
            self._set_runtime(task.id, last_error=str(err))
            logger.error(f"刷流任务 [{task.name}] 执行失败：{str(err)}")
        finally:
            try:
                report["finished_at"] = self._now_iso()
                self._set_runtime(task.id, state="idle", operation=None)
            finally:
                task_lock.release()
        return report

    def _run_brush(self, task: BrushTaskConfig, report: dict) -> None:
        """在已绑定任务上下文中执行刷流核心流程"""
        if not self._validate_task_reference(task) or not self.downloader:
            report["result"] = "downloader_unavailable"
            return
        if not self._is_current_time_in_range(task):
            report["result"] = "outside_active_time"
            report["reason_counts"]["不在开启时间段"] = 1
            return
        site = SiteOper().get(task.site_id)
        if not site:
            report["result"] = "site_missing"
            return
        ratio_passed, ratio_reason, ratio_status = self._evaluate_site_ratio_control(task, site=site)
        if ratio_status["enabled"]:
            report["site_ratio"] = ratio_status
        if not ratio_passed:
            report["result"] = "site_ratio_blocked"
            report["reason_counts"][ratio_reason] = 1
            return
        torrent_tasks: Dict[str, dict] = self._current_task_data("torrents", {})
        snapshot = getattr(self._task_context, "downloads", {})
        if self._resolve_pending_downloads(torrent_tasks, snapshot.get(task.downloader, {})):
            self._save_current_task_data("torrents", torrent_tasks)
        seeding_size = self.__calculate_seeding_torrents_size(torrent_tasks)
        global_seeding_size = self._calculate_global_seeding_size(task.id, torrent_tasks)
        passed, reason = self.__evaluate_size_condition_for_brush(
            seeding_size,
            global_torrents_size=global_seeding_size,
        )
        if not passed:
            report["result"] = "precondition_blocked"
            report["reason_counts"][reason] = 1
            return
        passed, reason = self.__evaluate_pre_conditions_for_brush()
        if not passed:
            report["result"] = "precondition_blocked"
            report["reason_counts"][reason] = 1
            return
        all_torrent_tasks = self._load_all_torrent_tasks()
        subscribe_titles = self.__get_subscribe_titles()
        self.__brush_site_torrents(
            site=site,
            torrent_tasks=torrent_tasks,
            all_torrent_tasks=all_torrent_tasks,
            subscribe_titles=subscribe_titles,
            report=report,
            global_seeding_size=global_seeding_size,
        )
        self._save_current_task_data("torrents", torrent_tasks)
        self._recalculate_statistics(task.id)

    def _load_all_torrent_tasks(self) -> Dict[str, dict]:
        """聚合所有任务的当前记录以保持跨站点重复种子保护"""
        rows: Dict[str, dict] = {}
        for task_id in self._task_configs:
            task_rows = self._get_task_data(task_id, "torrents") or {}
            rows.update(task_rows)
        return rows

    def _calculate_global_seeding_size(
        self,
        current_task_id: Optional[str] = None,
        current_torrent_tasks: Optional[Dict[str, dict]] = None,
    ) -> float:
        """汇总所有任务未删除种子的体积，并允许使用当前任务的内存快照。"""
        sizes = {}
        for task_id in self._task_configs:
            if task_id == current_task_id and current_torrent_tasks is not None:
                task_rows = current_torrent_tasks
            else:
                task_rows = self._get_task_data(task_id, "torrents") or {}
            for torrent_hash, record in task_rows.items():
                if not record.get("deleted"):
                    key = (self._task_configs[task_id].downloader, torrent_hash)
                    sizes[key] = max(sizes.get(key, 0), record.get("size") or 0)
        return sum(sizes.values())

    def __brush_site_torrents(
        self,
        site: Any,
        torrent_tasks: Dict[str, dict],
        all_torrent_tasks: Dict[str, dict],
        subscribe_titles: Set[str],
        report: dict,
        global_seeding_size: float,
    ) -> None:
        """获取当前任务站点候选并逐项执行保留的选种规则"""
        task = self._get_task_config()
        queues = getattr(self._task_context, "candidates", {})
        if task.id not in queues:
            logger.info(f"刷流任务 [{task.name}] 开始获取站点 {site.name} 的新种子")
            torrents = TorrentsChain().rss(domain=site.domain) if task.rss_support else TorrentsChain().browse(domain=site.domain)
            if not torrents:
                report["result"] = "no_candidates"
                queues[task.id] = []
                try:
                    healthy = bool(SiteChain().test(site.domain)[0])
                except Exception:
                    healthy = False
                if healthy:
                    report["reason_counts"]["站点连接正常，未取得资源列表"] += 1
                else:
                    report["result"] = "source_unavailable"
                    report["error"] = "站点连接或登录认证失败，请检查站点状态"
                    report["reason_counts"][report["error"]] += 1
                    state = self.get_data("site_rotation") or {}
                    if getattr(self, "_rotation_mode", "parallel") == "sequential" \
                            and state.get("site_id") == task.site_id and not state.get("retry_at"):
                        state.update(health_checked=False, retry_count=0, retry_at=time.time() + 600)
                        self.save_data("site_rotation", state)
                return
            report["source_count"] = len(torrents)
            if task.except_subscribe:
                before_count = len(torrents)
                torrents = self.__filter_torrents_contains_subscribe(torrents, subscribe_titles)
                report["subscription_excluded"] = before_count - len(torrents)
                if report["subscription_excluded"]:
                    report["reason_counts"]["命中订阅内容"] = report["subscription_excluded"]
            report["candidate_count"] = len(torrents)
            ranked_at = datetime.now().astimezone()
            torrents.sort(key=lambda item: demand_rank(
                getattr(item, "peers", None), getattr(item, "seeders", None),
                publication_age_minutes(item.pubdate, task.timezone_offset, settings.TZ, ranked_at),
            ), reverse=True)
            queues[task.id] = list(torrents)
        torrents = queues[task.id]
        seeding_size = self.__calculate_seeding_torrents_size(torrent_tasks)
        while torrents:
            if not self._is_focus_site(task.site_id):
                report["result"] = "outside_focus"
                report["reason_counts"]["等待站点轮转"] += 1
                return
            torrent = torrents.pop(0)
            passed, reason = self.__evaluate_pre_conditions_for_brush(include_network_conditions=False)
            if not passed:
                report["reason_counts"][reason] += 1
                report["result"] = "precondition_blocked"
                break
            passed, reason = self.__evaluate_size_condition_for_brush(
                seeding_size,
                torrent.size,
                global_torrents_size=global_seeding_size,
            )
            if not passed:
                report["reason_counts"][reason] += 1
                continue
            passed, reason = self.__evaluate_conditions_for_brush(torrent, all_torrent_tasks)
            if not passed:
                report["reason_counts"][reason] += 1
                continue
            hash_string = self.__download(torrent)
            if not hash_string:
                if not self._is_focus_site(task.site_id):
                    report["result"] = "outside_focus"
                    report["reason_counts"]["等待站点轮转"] += 1
                    return
                report["reason_counts"]["下载器添加失败"] += 1
                break
            self._reserve_download(task, hash_string)
            torrent_task = self._torrent_to_task_record(torrent, site, task)
            torrent_tasks[hash_string] = torrent_task
            all_torrent_tasks[hash_string] = torrent_task
            self._save_current_task_data("torrents", torrent_tasks)
            seeding_size += torrent.size
            global_seeding_size += torrent.size
            report["added_count"] += 1
            report["added_titles"].append(torrent.title)
            self.eventmanager.send_event(
                etype=EventType.PluginTriggered,
                data={
                    "plugin_id": self.__class__.__name__,
                    "event_name": "brushflow_download_added",
                    "hash": hash_string,
                    "data": torrent_task,
                    "downloader": self.service_info.name,
                },
            )
            logger.info(f"刷流任务 [{task.name}] 新增种子：{torrent.title}|{torrent.description}")
            self.__send_add_message(torrent)
            break
        report["filtered_count"] = max(report["candidate_count"] - report["added_count"], 0)
        report["result"] = "completed"

    @staticmethod
    def _torrent_to_task_record(torrent: TorrentInfo, site: Any, task: BrushTaskConfig) -> dict:
        """把站点候选种子转换为可持久化的任务记录"""
        return {
            "task_id": task.id,
            "task_name": task.name,
            "site": site.id,
            "site_name": site.name,
            "title": torrent.title,
            "size": torrent.size,
            "pubdate": torrent.pubdate,
            "description": torrent.description,
            "page_url": torrent.page_url,
            "date_elapsed": torrent.date_elapsed,
            "freedate": torrent.freedate,
            "uploadvolumefactor": torrent.uploadvolumefactor,
            "downloadvolumefactor": torrent.downloadvolumefactor,
            "hit_and_run": torrent.hit_and_run or task.site_hr_active,
            "volume_factor": torrent.volume_factor,
            "freedate_diff": torrent.freedate_diff,
            "ratio": 0,
            "downloaded": 0,
            "uploaded": 0,
            "seeding_time": 0,
            "deleted": False,
            "time": time.time(),
        }

    @staticmethod
    def _new_run_report(kind: str) -> dict:
        """创建一条结构稳定的运行诊断记录"""
        return {
            "id": uuid.uuid4().hex,
            "kind": kind,
            "started_at": BrushFlow._now_iso(),
            "finished_at": None,
            "success": None,
            "result": None,
            "error": None,
            "source_count": 0,
            "subscription_excluded": 0,
            "candidate_count": 0,
            "filtered_count": 0,
            "added_count": 0,
            "deleted_count": 0,
            "active_count": 0,
            "reason_counts": Counter(),
            "added_titles": [],
        }

    @staticmethod
    def _now_iso() -> str:
        """返回带本地时区且精确到秒的时间文本"""
        return datetime.now().astimezone().isoformat(timespec="seconds")

    def __evaluate_size_condition_for_brush(
        self,
        torrents_size: float,
        add_torrent_size: float = 0.0,
        global_torrents_size: Optional[float] = None,
    ) -> Tuple[bool, Optional[str]]:
        """校验当前任务及所有任务新增种子后是否超过保种体积。"""
        task = self._get_task_config()
        if not task:
            return False, "任务配置不存在"
        estimated_size = torrents_size + (add_torrent_size or 0)
        if task.disksize:
            limit_size = float(task.disksize) * 1024 ** 3
            if estimated_size > limit_size:
                reason = (
                    f"预计做种体积 {self.__bytes_to_gb(estimated_size):.1f} GB，"
                    f"超过任务保种上限 {task.disksize} GB"
                )
                return False, reason
        global_disksize = getattr(self, "_global_disksize", None)
        if global_disksize:
            if global_torrents_size is None:
                global_torrents_size = self._calculate_global_seeding_size()
            estimated_global_size = global_torrents_size + (add_torrent_size or 0)
            if estimated_global_size > float(global_disksize) * 1024 ** 3:
                reason = (
                    f"预计全局做种体积 {self.__bytes_to_gb(estimated_global_size):.1f} GB，"
                    f"超过全局保种上限 {global_disksize} GB"
                )
                return False, reason
        return True, None

    def __evaluate_pre_conditions_for_brush(
        self,
        include_network_conditions: bool = True,
    ) -> Tuple[bool, Optional[str]]:
        """已完成保种达到每站五十个时停止新增，下载并发仍由下载器管理。"""
        task = self._get_task_config()
        if not task:
            return False, "任务配置不存在"
        if self._site_completed_count(task.site_id) >= 50:
            return False, "本站已完成保种达到 50 个，等待按删除规则释放名额"
        budget = (self.get_data(f"download_budget.{task.downloader}") or {}).get("applied")
        if isinstance(budget, int) and self._site_downloading_count(task.site_id) >= max(4, budget * 2):
            return False, "本站已有足够待下载任务，等待释放名额后重新选种"
        return True, None

    def __evaluate_conditions_for_brush(
        self,
        torrent: TorrentInfo,
        torrent_tasks: Dict[str, dict],
    ) -> Tuple[bool, Optional[str]]:
        """按原有促销、H&R、规则、体积、人数和发布时间筛选候选"""
        task = self._get_task_config()
        if not task:
            return False, "任务配置不存在"
        if not torrent.size or torrent.size <= 0:
            return False, "种子体积未知"
        task_key = f"{torrent.site_name}{torrent.title}"
        if any(task_key == f"{item.get('site_name')}{item.get('title')}" for item in torrent_tasks.values()):
            return False, "重复种子"
        if torrent.page_url:
            page_key = f"{torrent.site_name}{torrent.page_url}"
            if any(page_key == f"{item.get('site_name')}{item.get('page_url')}" for item in torrent_tasks.values()):
                return False, "重复种子"
        if torrent.title and any(
            torrent.site_name != item.get("site_name")
            and torrent.title == item.get("title")
            and not item.get("deleted")
            and (item.get("downloaded") or 0) < (item.get("size") or 0)
            for item in torrent_tasks.values()
        ):
            return False, "其他站点存在尚未下载完成的相同种子"
        if task.freeleech and torrent.downloadvolumefactor != 0:
            return False, "非免费种子"
        if task.freeleech == "2xfree" and torrent.uploadvolumefactor != 2:
            return False, "非双倍上传种子"
        if torrent.downloadvolumefactor != 0 and task.site_ratio_target:
            ratio_status = self._build_site_ratio_status(task, getattr(self._task_context, "site_user_data", None))
            if not ratio_status.get("available") or ratio_status.get("stale") or not ratio_status.get("reached"):
                return False, "账号分享率不足或数据待刷新，仅选择免费种子"
            headroom = ratio_status.get("download_headroom")
            if headroom is None:
                return False, "账号上传下载量待刷新，仅选择免费种子"
            reserved = sum(max((item.get("size") or 0) - (item.get("downloaded") or 0), 0)
                           * float(item.get("downloadvolumefactor") or 0) for item in torrent_tasks.values()
                           if item.get("site") == task.site_id and not item.get("deleted") and not item.get("completed"))
            if reserved + torrent.size * float(torrent.downloadvolumefactor or 1) > headroom:
                return False, "新增记账下载会低于目标分享率"
        if task.del_no_free:
            deadline = self._promotion_expiry_at(torrent.freedate, task.timezone_offset, task.promotion_lead_minutes)
            if deadline and deadline <= datetime.now().astimezone():
                return False, "促销剩余时间不足"
            metrics = self._get_task_data(task.id, "metrics") or {}
            per_torrent_speed = (metrics.get("downloaded") or 0) / max(metrics.get("seconds", 0), 1) / max(metrics.get("downloading", 0), 1)
            if deadline and metrics.get("seconds", 0) >= 300 and not metrics.get("gap") \
                    and per_torrent_speed > 0 and torrent.size / per_torrent_speed * 1.5 > (deadline - datetime.now().astimezone()).total_seconds():
                return False, "按近期下载速度预计无法在促销保护时间前完成"
        if task.hr == "yes" and torrent.hit_and_run:
            return False, "存在 H&R"
        if task.include:
            include_match = bool(
                (torrent.title and re.search(task.include, torrent.title, re.I))
                or (torrent.description and re.search(task.include, torrent.description, re.I))
            )
            if not include_match:
                return False, "不符合包含规则"
        if task.exclude:
            exclude_match = bool(
                (torrent.title and re.search(task.exclude, torrent.title, re.I))
                or (torrent.description and re.search(task.exclude, torrent.description, re.I))
            )
            if exclude_match:
                return False, "符合排除规则"
        if task.size:
            size_range = [float(value) * 1024 ** 3 for value in task.size.split("-")]
            if len(size_range) == 1 and torrent.size < size_range[0]:
                return False, "种子大小低于下限"
            if len(size_range) > 1 and not size_range[0] <= torrent.size <= size_range[1]:
                return False, "种子大小不在范围内"
        if task.seeder:
            seeder_range = [float(value) for value in task.seeder.split("-")]
            seeders = torrent.seeders
            if seeders is None:
                return False, "做种人数未知"
            if len(seeder_range) == 1 and seeders > seeder_range[0]:
                return False, "做种人数超过上限"
            if len(seeder_range) > 1 and not seeder_range[0] <= seeders <= seeder_range[1]:
                return False, "做种人数不在范围内"
        passed, reason = evaluate_peer_demand(
            torrent.peers, torrent.seeders, task.leecher, task.min_peer_seed_ratio,
        )
        if not passed:
            return False, reason
        if task.pubtime:
            pubdate_minutes = self.__get_pubminutes(torrent.pubdate, task.timezone_offset)
            if pubdate_minutes is None:
                return False, "发布时间未知"
            pubtime_range = [float(value) for value in task.pubtime.split("-")]
            if len(pubtime_range) == 1 and pubdate_minutes > pubtime_range[0]:
                return False, "发布时间超过上限"
            if len(pubtime_range) > 1 and not pubtime_range[0] <= pubdate_minutes <= pubtime_range[1]:
                return False, "发布时间不在范围内"
        return True, None

    def check(self, task_id: Optional[str] = None, wait_for_lock: bool = False) -> None:
        """执行状态同步、删种和归档，到期检查可等待同任务的当前操作"""
        task = self._get_task_config(task_id)
        if not task or not self.get_state() or not task.enabled:
            return
        task_lock = self._task_locks.setdefault(task.id, threading.Lock())
        if not task_lock.acquire(blocking=wait_for_lock):
            logger.info(f"刷流任务 [{task.name}] 已有操作执行中，本轮检查跳过")
            return
        report = self._new_run_report("check")
        self._set_runtime(task.id, state="running", operation="check", last_error=None)
        try:
            with self._task_scope(task.id):
                self._run_check(task, report)
            report["success"] = report.get("result") not in {"downloader_unavailable", "downloader_error"}
        except Exception as err:
            report.update({"success": False, "error": str(err)})
            self._set_runtime(task.id, last_error=str(err))
            logger.error(f"刷流任务 [{task.name}] 检查失败：{str(err)}")
        finally:
            task_lock.release()
        if self._global_dynamic_delete_enabled():
            try:
                global_deleted_count = self._run_global_dynamic_delete()
                report["global_deleted_count"] = global_deleted_count
                report["deleted_count"] = report.get("deleted_count", 0) + global_deleted_count
            except Exception as err:
                report.update({"success": False, "error": str(err)})
                self._set_runtime(task.id, last_error=str(err))
                logger.error(f"全局动态删种失败：{str(err)}")
        report["finished_at"] = self._now_iso()
        self._append_run(task.id, report)
        self._set_runtime(task.id, state="idle", operation=None)

    def _run_check(self, task: BrushTaskConfig, report: dict) -> None:
        """在已绑定任务上下文中执行刷流种子检查"""
        if not self._validate_task_reference(task) or not self.downloader:
            report["result"] = "downloader_unavailable"
            return
        torrent_tasks: Dict[str, dict] = self._current_task_data("torrents", {})
        unmanaged_tasks: Dict[str, dict] = self._current_task_data("unmanaged", {})
        downloader = self.downloader
        seeding_torrents, error = downloader.get_torrents()
        if error:
            report["result"] = "downloader_error"
            raise RuntimeError("连接下载器出错")
        seeding_torrents_dict = {self.__get_hash(torrent): torrent for torrent in seeding_torrents}
        self._resolve_pending_downloads(torrent_tasks, seeding_torrents_dict)
        self.__update_seeding_tasks_based_on_tags(torrent_tasks, unmanaged_tasks, seeding_torrents_dict)
        check_hashes = list(torrent_tasks.keys())
        if not check_hashes:
            self._cleanup_unused_task_tag(task, torrents=seeding_torrents)
            report.update({"result": "no_managed_torrents", "active_count": 0})
            self._recalculate_statistics(task.id)
            return
        check_torrents = [seeding_torrents_dict[item] for item in check_hashes if item in seeding_torrents_dict]
        self.__update_torrent_tasks_state(check_torrents, torrent_tasks)
        self.__update_undeleted_torrents_missing_in_downloader(torrent_tasks, check_hashes, seeding_torrents)
        filtered_torrents = self.__filter_torrents_by_tag(check_torrents, task.delete_except_tags)
        if self._global_dynamic_delete_enabled():
            need_delete_hashes = []
        elif task.proxy_delete and task.delete_size_range:
            need_delete_hashes = self.__delete_torrent_for_proxy(filtered_torrents, torrent_tasks)
        else:
            need_delete_hashes = self.__delete_torrent_for_evaluate_conditions(filtered_torrents, torrent_tasks)
        need_delete_hashes = list(dict.fromkeys(need_delete_hashes or []))
        deleted_from_downloader = False
        if need_delete_hashes:
            if DownloaderHelper().is_downloader("qbittorrent", service=self.service_info):
                self.__qb_torrents_reannounce(need_delete_hashes)
            if downloader.delete_torrents(ids=need_delete_hashes, delete_file=True):
                deleted_from_downloader = True
                for torrent_hash in need_delete_hashes:
                    if torrent_hash in torrent_tasks:
                        torrent_tasks[torrent_hash]["deleted"] = True
                        torrent_tasks[torrent_hash]["deleted_time"] = time.time()
        self.__auto_archive_tasks(torrent_tasks)
        self._cleanup_unused_task_tag(
            task,
            torrents=None if deleted_from_downloader else seeding_torrents,
        )
        self._save_current_task_data("torrents", torrent_tasks)
        self._recalculate_statistics(task.id)
        report.update(
            {
                "result": "completed",
                "deleted_count": len(need_delete_hashes),
                "active_count": sum(1 for item in torrent_tasks.values() if not item.get("deleted")),
            }
        )

    def __update_torrent_tasks_state(self, torrents: List[Any], torrent_tasks: Dict[str, dict]) -> None:
        """保存流量和完成状态，并持续记录首次确认开始下载的时间。"""
        for torrent in torrents:
            torrent_hash = self.__get_hash(torrent)
            torrent_task = torrent_tasks.get(torrent_hash)
            if not torrent_task:
                continue
            torrent_info = self.__get_torrent_info(torrent)
            update_transfer_sample(torrent_task, torrent_info, time.time())
            if download_has_started(torrent_info, torrent_task):
                now_timestamp = time.time()
                started_at = BrushTaskConfig._parse_number(torrent_task.get("download_started_at"))
                if not started_at or not 0 < started_at <= now_timestamp:
                    observed_start = torrent_info.get("download_started_at") or 0
                    started_at = observed_start if 0 < observed_start <= now_timestamp else now_timestamp
                torrent_task.update({"download_started": True, "download_started_at": started_at})
            torrent_task.update(
                {
                    "downloaded": torrent_info.get("downloaded"),
                    "transfer_downloaded": torrent_info.get("transfer_downloaded"),
                    "completed": torrent_info.get("completed", False),
                    "uploaded": torrent_info.get("uploaded"),
                    "ratio": torrent_info.get("ratio"),
                    "seeding_time": torrent_info.get("seeding_time"),
                }
            )

    def __update_seeding_tasks_based_on_tags(
        self,
        torrent_tasks: Dict[str, dict],
        unmanaged_tasks: Dict[str, dict],
        seeding_torrents_dict: Dict[str, Any],
    ) -> None:
        """按任务唯一标签同步 qBittorrent 中的纳管和移除状态"""
        task = self._get_task_config()
        if not task or not DownloaderHelper().is_downloader("qbittorrent", service=self.service_info):
            return
        added_tasks: List[dict] = []
        removed_tasks: List[dict] = []
        reset_tasks: List[dict] = []
        for torrent_hash, torrent in seeding_torrents_dict.items():
            tags = self.__get_label(torrent)
            has_unique_tag = task.brush_tag in tags
            has_global_tag = self.GLOBAL_BRUSH_TAG in tags
            existing = torrent_hash in torrent_tasks
            adopt_legacy = (
                has_global_tag
                and not existing
                and self._is_primary_task_for_torrent(task, torrent)
            )
            managed = has_unique_tag or (has_global_tag and existing) or adopt_legacy
            if managed:
                if not existing:
                    torrent_task = unmanaged_tasks.pop(torrent_hash, None) or self.__convert_torrent_info_to_task(torrent)
                    torrent_task.update({"task_id": task.id, "task_name": task.name})
                    torrent_tasks[torrent_hash] = torrent_task
                    added_tasks.append(torrent_task)
                elif torrent_tasks[torrent_hash].get("deleted"):
                    torrent_tasks[torrent_hash]["deleted"] = False
                    torrent_tasks[torrent_hash].pop("deleted_time", None)
                    reset_tasks.append(torrent_tasks[torrent_hash])
            elif existing:
                unmanaged_tasks[torrent_hash] = torrent_tasks.pop(torrent_hash)
                removed_tasks.append(unmanaged_tasks[torrent_hash])
        self._save_current_task_data("torrents", torrent_tasks)
        self._save_current_task_data("unmanaged", unmanaged_tasks)
        if added_tasks:
            self.__log_and_send_torrent_task_update_message(
                "【刷流任务种子加入】", "纳入刷流管理", "刷流任务标签匹配", added_tasks
            )
        if removed_tasks:
            self.__log_and_send_torrent_task_update_message(
                "【刷流任务种子移除】", "移除刷流管理", "刷流任务标签移除", removed_tasks
            )
        if reset_tasks:
            self.__log_and_send_torrent_task_update_message(
                "【刷流任务状态更新】", "恢复为正常", "下载器中仍存在对应种子", reset_tasks
            )

    def _is_primary_task_for_torrent(self, task: BrushTaskConfig, torrent: Any) -> bool:
        """仅让同站点第一项任务接管没有唯一标签的旧版刷流种子"""
        site_id, _ = self.__get_site_by_torrent(torrent)
        if site_id != task.site_id:
            return False
        site_tasks = [item for item in self._task_configs.values() if item.site_id == site_id]
        return bool(site_tasks and site_tasks[0].id == task.id)

    def __evaluate_conditions_for_delete(
        self,
        torrent_info: dict,
        torrent_task: dict,
    ) -> Tuple[bool, str]:
        """评估删除条件，促销清理以外的规则只处理已经开始的种子。"""
        task = self._get_task_config()
        if not task:
            return False, "任务配置不存在"
        if not download_has_started(torrent_info, torrent_task):
            promotion_expired, promotion_reason = self.__promotion_expired(torrent_info, torrent_task)
            return (True, promotion_reason) if promotion_expired else (False, "种子尚未开始下载")
        hit_and_run = bool(torrent_task.get("hit_and_run"))
        if hit_and_run:
            if task.hr_seed_time and torrent_info.get("seeding_time", 0) >= float(task.hr_seed_time) * 3600:
                return True, f"H&R 做种时间达到 {task.hr_seed_time} 小时"
            if task.seed_ratio and torrent_info.get("ratio", 0) >= float(task.seed_ratio):
                return True, f"H&R 分享率达到 {task.seed_ratio}"
            return False, "H&R 种子尚未满足删除条件"
        promotion_expired, promotion_reason = self.__promotion_expired(torrent_info, torrent_task)
        if promotion_expired:
            return True, promotion_reason
        if task.seed_time and torrent_info.get("seeding_time", 0) >= float(task.seed_time) * 3600:
            return True, f"做种时间达到 {task.seed_time} 小时"
        if task.seed_ratio and torrent_info.get("ratio", 0) >= float(task.seed_ratio):
            return True, f"分享率达到 {task.seed_ratio}"
        if task.seed_size and torrent_info.get("uploaded", 0) >= float(task.seed_size) * 1024 ** 3:
            return True, f"上传量达到 {task.seed_size} GB"
        started_at = BrushTaskConfig._parse_number(torrent_task.get("download_started_at"))
        download_elapsed = torrent_task.get("actual_download_seconds", max(time.time() - started_at, 0) if started_at and started_at > 0 else 0)
        if (
            task.download_time
            and not torrent_info.get("completed", False)
            and download_elapsed >= float(task.download_time) * 3600
        ):
            return True, f"下载耗时达到 {task.download_time} 小时"
        window = transfer_window(torrent_task, time.time())
        if not torrent_info.get("completed") and window["seconds"] >= 1200 and not window["gap"] \
                and window["active_download_seconds"] >= 1200 and window["uploaded"] == 0 and window["downloaded"] == 0:
            return True, "实际运行满 20 分钟仍无上传和下载进展"
        if (
            task.seed_avgspeed and window["seconds"] >= 1200 and not window["gap"]
            and window["avg_upspeed"] is not None and window["avg_upspeed"] <= float(task.seed_avgspeed) * 1024
            and torrent_info.get("seeding_time", 0) >= 30 * 60
        ):
            return True, f"平均上传速度低于 {task.seed_avgspeed} KB/s"
        if task.seed_inactivetime and torrent_info.get("iatime", 0) >= float(task.seed_inactivetime) * 60:
            return True, f"未活动时间达到 {task.seed_inactivetime} 分钟"
        return False, "尚未满足删除条件"

    def __promotion_expired(self, torrent_info: dict, torrent_task: dict) -> Tuple[bool, str]:
        """在促销结束前清理未完成任务，保留 H&R 种子。"""
        task = self._get_task_config()
        if (
            not task
            or not task.del_no_free
            or torrent_task.get("hit_and_run")
            or torrent_info.get("completed", False)
            or (
                torrent_info.get("total_size", 0) > 0
                and torrent_info.get("downloaded", 0) >= torrent_info.get("total_size", 0)
            )
        ):
            return False, ""
        lead_minutes = getattr(task, "promotion_lead_minutes", 10)
        expiry = self._promotion_expiry_at(torrent_task.get("freedate"), task.timezone_offset, lead_minutes)
        if not expiry:
            return False, ""
        expired = datetime.now(expiry.tzinfo) >= expiry
        return expired, f"进入促销结束前 {lead_minutes:g} 分钟清理窗口" if expired else ""

    def __delete_torrent_for_evaluate_conditions(
        self,
        torrents: List[Any],
        torrent_tasks: Dict[str, dict],
        dynamic: bool = False,
    ) -> List[str]:
        """找出满足用户删除条件的种子并发送对应通知"""
        delete_hashes: List[str] = []
        for torrent in torrents:
            torrent_hash = self.__get_hash(torrent)
            torrent_task = torrent_tasks.get(torrent_hash)
            if not torrent_task:
                continue
            torrent_info = self.__get_torrent_info(torrent)
            should_delete, reason = self.__evaluate_conditions_for_delete(torrent_info, torrent_task)
            if not should_delete:
                continue
            delete_hashes.append(torrent_hash)
            if dynamic:
                reason = f"触发动态删除阈值，{reason}"
            self.__send_delete_message(torrent_task, reason)
            logger.info(f"刷流任务删除种子：{torrent_task.get('title')}，原因：{reason}")
        return delete_hashes

    def __delete_torrent_for_evaluate_proxy_pre_conditions(
        self,
        torrents: List[Any],
        torrent_tasks: Dict[str, dict],
    ) -> List[str]:
        """动态删种前处理促销窗口和从开始下载计时的超时任务。"""
        task = self._get_task_config()
        delete_hashes: List[str] = []
        for torrent in torrents:
            torrent_hash = self.__get_hash(torrent)
            torrent_task = torrent_tasks.get(torrent_hash)
            if not task or not torrent_task or torrent_task.get("hit_and_run"):
                continue
            torrent_info = self.__get_torrent_info(torrent)
            expired, reason = self.__promotion_expired(torrent_info, torrent_task)
            started_at = BrushTaskConfig._parse_number(torrent_task.get("download_started_at"))
            download_elapsed = torrent_task.get("actual_download_seconds", max(time.time() - started_at, 0) if started_at and started_at > 0 else 0)
            timed_out = bool(
                task.download_time
                and download_has_started(torrent_info, torrent_task)
                and not torrent_info.get("completed", False)
                and download_elapsed >= float(task.download_time) * 3600
            )
            if not expired and not timed_out:
                continue
            if timed_out and not reason:
                reason = f"下载耗时达到 {task.download_time} 小时"
            delete_hashes.append(torrent_hash)
            self.__send_delete_message(torrent_task, reason)
        return delete_hashes

    @staticmethod
    def _select_global_dynamic_deletions(
        candidates: List[dict],
        total_size: float,
        min_size: float,
        max_size: float,
    ) -> Tuple[List[dict], float, bool]:
        """按 V4 优先级从跨任务候选中生成全局动态删种计划"""
        selected: List[dict] = []
        selected_keys: Set[Tuple[str, str]] = set()
        remaining_size = total_size

        def select(
            candidate: dict,
            reason: str,
            reason_field: Optional[str] = None,
            dynamic_reason: bool = False,
        ) -> None:
            """把未选候选加入计划并扣减预计做种体积"""
            nonlocal remaining_size
            candidate_key = (
                candidate["downloader_name"],
                candidate["torrent_hash"],
            )
            if candidate_key in selected_keys:
                return
            selected_keys.add(candidate_key)
            task_delete_reasons: Dict[str, str] = {}
            for task, _ in candidate.get(
                "associated_records",
                [(candidate["task"], candidate.get("torrent_task"))],
            ):
                task_reason = (
                    candidate.get("task_condition_reasons", {}).get(task.id, {}).get(reason_field)
                    if reason_field
                    else None
                ) or reason
                if dynamic_reason:
                    task_reason = f"触发全局动态删除阈值，{task_reason}"
                task_delete_reasons[task.id] = task_reason
            selected.append(
                {
                    **candidate,
                    "delete_reason": reason,
                    "task_delete_reasons": task_delete_reasons,
                }
            )
            remaining_size = max(remaining_size - float(candidate.get("size") or 0), 0)

        for candidate in candidates:
            if candidate.get("pre_delete_reason"):
                select(
                    candidate,
                    candidate["pre_delete_reason"],
                    reason_field="pre_delete_reason",
                )

        threshold_triggered = remaining_size >= max_size
        if not threshold_triggered:
            return selected, remaining_size, False

        for candidate in candidates:
            if remaining_size <= min_size:
                break
            if not candidate.get("proxy_delete") and candidate.get("conditional_reason"):
                select(
                    candidate,
                    candidate["conditional_reason"],
                    reason_field="conditional_reason",
                )

        if remaining_size > min_size:
            for candidate in candidates:
                if remaining_size <= min_size:
                    break
                if candidate.get("proxy_delete") and candidate.get("conditional_reason"):
                    select(
                        candidate,
                        f"触发全局动态删除阈值，{candidate['conditional_reason']}",
                        reason_field="conditional_reason",
                        dynamic_reason=True,
                    )

        fallback_candidates = sorted(
            (
                candidate
                for candidate in candidates
                if candidate.get("proxy_delete")
                and candidate.get("completed")
                and not candidate.get("hit_and_run")
            ),
            key=lambda item: BrushFlow._retention_rank(item.get("torrent_task") or {}, item.get("size") or 0,
                                                     item.get("seeding_time", 0)),
        )
        for candidate in fallback_candidates:
            if remaining_size <= min_size:
                break
            select(candidate, "触发全局动态删除阈值，系统按做种时间清理")

        return selected, remaining_size, True

    def _collect_global_dynamic_delete_candidates(
        self,
    ) -> Tuple[List[dict], float, Dict[str, Dict[str, dict]], Dict[str, ServiceInfo]]:
        """汇总启用任务的最新下载器状态、做种体积和全局删种候选"""
        candidate_rows: Dict[Tuple[str, str], List[dict]] = {}
        total_size = 0.0
        task_records: Dict[str, Dict[str, dict]] = {}
        services: Dict[str, ServiceInfo] = {}
        downloader_cache: Dict[str, Tuple[ServiceInfo, List[Any]]] = {}
        counted_torrents: Set[Tuple[str, str]] = set()
        associated_records: Dict[Tuple[str, str], List[Tuple[BrushTaskConfig, dict]]] = {}
        downloader_helper = DownloaderHelper()

        for task in self._task_configs.values():
            torrent_tasks: Dict[str, dict] = self._get_task_data(task.id, "torrents") or {}
            task_records[task.id] = torrent_tasks

        for task in self._task_configs.values():
            if not task.enabled:
                continue
            torrent_tasks = task_records[task.id]
            if task.downloader not in downloader_cache:
                service = downloader_helper.get_service(name=task.downloader)
                if not service or not service.instance or service.instance.is_inactive():
                    raise RuntimeError(
                        f"全局动态删种无法获取下载器 [{task.downloader}] 实时状态，本轮已中止"
                    )
                torrents, error = service.instance.get_torrents()
                if error:
                    raise RuntimeError(
                        f"全局动态删种获取下载器 [{task.downloader}] 种子失败，本轮已中止"
                    )
                downloader_cache[task.downloader] = (service, torrents or [])
                services[task.downloader] = service

            service, downloader_torrents = downloader_cache[task.downloader]
            with self._task_scope(task.id):
                downloader_torrent_map: Dict[str, Any] = {}
                for torrent in downloader_torrents:
                    torrent_hash = self.__get_hash(torrent)
                    if torrent_hash:
                        downloader_torrent_map[torrent_hash] = torrent
                self._resolve_pending_downloads(torrent_tasks, downloader_torrent_map)
                check_hashes = list(torrent_tasks)
                check_torrents = [
                    downloader_torrent_map[torrent_hash]
                    for torrent_hash in check_hashes
                    if torrent_hash in downloader_torrent_map
                ]
                self.__update_torrent_tasks_state(check_torrents, torrent_tasks)
                self.__update_undeleted_torrents_missing_in_downloader(
                    torrent_tasks,
                    check_hashes,
                    downloader_torrents,
                )
                self._save_task_data(task.id, "torrents", torrent_tasks)
                for torrent in check_torrents:
                    torrent_hash = self.__get_hash(torrent)
                    torrent_task = torrent_tasks.get(torrent_hash)
                    if not torrent_task or torrent_task.get("deleted"):
                        continue
                    torrent_key = (task.downloader, torrent_hash)
                    if torrent_key not in counted_torrents:
                        torrent_info = self.__get_torrent_info(torrent)
                        total_size += float(
                            torrent_info.get("total_size") or torrent_task.get("size") or 0
                        )
                        counted_torrents.add(torrent_key)

                filtered_torrents = self.__filter_torrents_by_tag(check_torrents, task.delete_except_tags)
                for torrent in filtered_torrents:
                    torrent_hash = self.__get_hash(torrent)
                    torrent_task = torrent_tasks.get(torrent_hash)
                    if not torrent_task or torrent_task.get("deleted"):
                        continue
                    torrent_key = (task.downloader, torrent_hash)
                    torrent_info = self.__get_torrent_info(torrent)
                    pre_delete_reason = ""
                    if not torrent_task.get("hit_and_run"):
                        expired, expired_reason = self.__promotion_expired(torrent_info, torrent_task)
                        started_at = BrushTaskConfig._parse_number(torrent_task.get("download_started_at"))
                        download_elapsed = torrent_task.get("actual_download_seconds", max(time.time() - started_at, 0) if started_at and started_at > 0 else 0)
                        timed_out = bool(
                            task.download_time
                            and download_has_started(torrent_info, torrent_task)
                            and not torrent_info.get("completed", False)
                            and download_elapsed >= float(task.download_time) * 3600
                        )
                        if expired:
                            pre_delete_reason = expired_reason
                        elif timed_out:
                            pre_delete_reason = f"下载耗时达到 {task.download_time} 小时"
                    should_delete, conditional_reason = self.__evaluate_conditions_for_delete(
                        torrent_info,
                        torrent_task,
                    )
                    torrent_size = float(torrent_info.get("total_size") or torrent_task.get("size") or 0)
                    candidate_rows.setdefault(torrent_key, []).append(
                        {
                            "task": task,
                            "torrent_hash": torrent_hash,
                            "torrent_task": torrent_task,
                            "downloader_name": task.downloader,
                            "size": torrent_size,
                            "pre_delete_reason": pre_delete_reason,
                            "conditional_reason": conditional_reason if should_delete else "",
                            "proxy_delete": task.proxy_delete,
                            "completed": bool(torrent_info.get("completed", False)),
                            "hit_and_run": bool(torrent_task.get("hit_and_run")),
                            "seeding_time": torrent_info.get("seeding_time", 0),
                        }
                    )

        for task in self._task_configs.values():
            for torrent_hash, torrent_task in task_records[task.id].items():
                if not torrent_task.get("deleted"):
                    torrent_key = (task.downloader, torrent_hash)
                    associated_records.setdefault(torrent_key, []).append((task, torrent_task))

        candidates: List[dict] = []
        for torrent_key, rows in candidate_rows.items():
            associations = associated_records.get(torrent_key, [])
            if len(rows) != len(associations):
                continue
            candidate = dict(rows[0])
            candidate.update(
                {
                    "associated_records": associations,
                    "proxy_delete": all(row["proxy_delete"] for row in rows),
                    "completed": all(row["completed"] for row in rows),
                    "hit_and_run": any(row["hit_and_run"] for row in rows),
                    "pre_delete_reason": (
                        rows[0]["pre_delete_reason"]
                        if all(row["pre_delete_reason"] for row in rows)
                        else ""
                    ),
                    "conditional_reason": (
                        rows[0]["conditional_reason"]
                        if all(row["conditional_reason"] for row in rows)
                        else ""
                    ),
                    "seeding_time": max(row["seeding_time"] for row in rows),
                    "task_condition_reasons": {
                        row["task"].id: {
                            "pre_delete_reason": row["pre_delete_reason"],
                            "conditional_reason": row["conditional_reason"],
                        }
                        for row in rows
                    },
                }
            )
            candidates.append(candidate)
        return candidates, total_size, task_records, services

    def _send_global_dynamic_delete_summary(
        self,
        deleted_entries: List[dict],
        remaining_size: float,
    ) -> None:
        """按受影响任务通知开关发送全局区间删种汇总"""
        notified_tasks = {
            task.id: task
            for entry in deleted_entries
            for task, _ in entry.get(
                "associated_records",
                [(entry["task"], entry.get("torrent_task"))],
            )
            if task.notify
        }
        if not notified_tasks:
            return
        task_names = "、".join(task.name for task in notified_tasks.values())
        self.post_message(
            mtype=NotificationType.SiteMessage,
            title="【刷流任务全局动态删除】",
            text=(
                f"任务：{task_names}\n"
                f"删除：{len(deleted_entries)} 个种子\n"
                f"当前做种：{self.__bytes_to_gb(remaining_size):.1f} GB"
            ),
        )

    def _run_global_dynamic_delete(self) -> int:
        """串行执行跨任务、跨下载器的全局动态删种并返回成功删除数"""
        if not self._global_dynamic_delete_enabled():
            return 0
        global_lock = getattr(self, "_global_delete_lock", None)
        if global_lock is None:
            self._global_delete_lock = threading.Lock()
            global_lock = self._global_delete_lock
        if not global_lock.acquire(blocking=False):
            logger.info("已有全局动态删种正在执行，本轮跳过")
            return 0

        try:
            with self._all_task_locks_scope():
                candidates, total_size, task_records, services = self._collect_global_dynamic_delete_candidates()
                limits = [
                    float(value) * 1024 ** 3
                    for value in str(self._global_delete_size_range).split("-")
                ]
                min_size = limits[0]
                max_size = limits[1] if len(limits) > 1 else limits[0]
                delete_plan, _, threshold_triggered = self._select_global_dynamic_deletions(
                    candidates,
                    total_size,
                    min_size,
                    max_size,
                )
                if not delete_plan:
                    if threshold_triggered:
                        logger.info(
                            f"全局做种体积 {self.__bytes_to_gb(total_size):.1f} GB 已达到动态删种上限，"
                            "但没有符合任务策略的可删除种子"
                        )
                    else:
                        logger.info(
                            f"全局做种体积 {self.__bytes_to_gb(total_size):.1f} GB，"
                            f"未达到动态删种上限 {self.__bytes_to_gb(max_size):.1f} GB"
                        )
                    return 0

                plan_by_downloader: Dict[str, List[dict]] = {}
                for entry in delete_plan:
                    plan_by_downloader.setdefault(entry["downloader_name"], []).append(entry)

                deleted_entries: List[dict] = []
                downloader_helper = DownloaderHelper()
                for downloader_name, entries in plan_by_downloader.items():
                    service = services.get(downloader_name)
                    if not service or not service.instance:
                        continue
                    torrent_hashes = list(dict.fromkeys(entry["torrent_hash"] for entry in entries))
                    if downloader_helper.is_downloader("qbittorrent", service=service):
                        try:
                            if getattr(service.instance, "qbc", None):
                                service.instance.qbc.torrents_reannounce(torrent_hashes=torrent_hashes)
                        except Exception as err:
                            logger.warning(f"全局动态删种重新汇报下载器 [{downloader_name}] 失败：{str(err)}")
                    try:
                        if service.instance.delete_torrents(ids=torrent_hashes, delete_file=True):
                            deleted_entries.extend(entries)
                    except Exception as err:
                        logger.error(
                            f"全局动态删种调用下载器 [{downloader_name}] 删除失败：{str(err)}"
                        )

                deleted_at = time.time()
                affected_task_ids: Set[str] = set()
                recorded_entries: List[dict] = []
                notification_entries: List[Tuple[BrushTaskConfig, dict, str]] = []
                for entry in deleted_entries:
                    torrent_hash = entry["torrent_hash"]
                    entry_recorded = False
                    for task, _ in entry.get(
                        "associated_records",
                        [(entry["task"], entry.get("torrent_task"))],
                    ):
                        torrent_task = task_records.get(task.id, {}).get(torrent_hash)
                        if not torrent_task:
                            continue
                        delete_reason = entry.get("task_delete_reasons", {}).get(
                            task.id,
                            entry["delete_reason"],
                        )
                        torrent_task.update({"deleted": True, "deleted_time": deleted_at})
                        affected_task_ids.add(task.id)
                        notification_entries.append((task, torrent_task, delete_reason))
                        entry_recorded = True
                    if entry_recorded:
                        recorded_entries.append(entry)

                for affected_task_id in affected_task_ids:
                    self._save_task_data(
                        affected_task_id,
                        "torrents",
                        task_records[affected_task_id],
                    )
                    self._recalculate_statistics(affected_task_id)

                for task, torrent_task, delete_reason in notification_entries:
                    try:
                        with self._task_scope(task.id):
                            self.__send_delete_message(torrent_task, delete_reason)
                    except Exception as err:
                        logger.warning(f"全局动态删种发送任务 [{task.name}] 通知失败：{str(err)}")
                    logger.info(
                        f"全局动态删种删除任务 [{task.name}] 种子："
                        f"{torrent_task.get('title')}，原因：{delete_reason}"
                    )

                remaining_size = max(
                    total_size - sum(float(entry.get("size") or 0) for entry in deleted_entries),
                    0,
                )
                if threshold_triggered and len(limits) > 1 and recorded_entries:
                    try:
                        self._send_global_dynamic_delete_summary(recorded_entries, remaining_size)
                    except Exception as err:
                        logger.warning(f"全局动态删种发送汇总通知失败：{str(err)}")
                return len(recorded_entries)
        finally:
            global_lock.release()

    def __delete_torrent_for_proxy(
        self,
        torrents: List[Any],
        torrent_tasks: Dict[str, dict],
    ) -> List[str]:
        """按动态体积阈值执行前置、条件和兜底删种"""
        task = self._get_task_config()
        if not task or not task.proxy_delete or not task.delete_size_range:
            return []
        torrent_info_map = {
            self.__get_hash(torrent): self.__get_torrent_info(torrent) for torrent in torrents
        }
        total_size = self.__calculate_seeding_torrents_size(torrent_tasks)
        pre_delete_hashes = self.__delete_torrent_for_evaluate_proxy_pre_conditions(torrents, torrent_tasks)
        total_size -= sum(torrent_info_map[item].get("total_size", 0) for item in pre_delete_hashes if item in torrent_info_map)
        remaining_torrents = [torrent for torrent in torrents if self.__get_hash(torrent) not in pre_delete_hashes]
        limits = [float(value) * 1024 ** 3 for value in task.delete_size_range.split("-")]
        min_size = limits[0]
        max_size = limits[1] if len(limits) > 1 else limits[0]
        if total_size < max_size:
            return pre_delete_hashes
        delete_hashes = list(pre_delete_hashes)
        conditional_hashes = self.__delete_torrent_for_evaluate_conditions(
            remaining_torrents, torrent_tasks, dynamic=True
        )
        delete_hashes.extend(conditional_hashes)
        total_size -= sum(
            torrent_info_map[item].get("total_size", 0)
            for item in conditional_hashes
            if item in torrent_info_map
        )
        if total_size > min_size:
            remaining_hashes = [
                self.__get_hash(torrent)
                for torrent in remaining_torrents
                if self.__get_hash(torrent) not in delete_hashes
            ]
            candidates = []
            for torrent_hash in remaining_hashes:
                record = torrent_tasks.get(torrent_hash)
                info = torrent_info_map.get(torrent_hash) or {}
                if not record or record.get("deleted") or record.get("hit_and_run"):
                    continue
                if not info.get("completed") or not download_has_started(info, record):
                    continue
                candidates.append((torrent_hash, info))
            candidates.sort(key=lambda item: self._retention_rank(torrent_tasks[item[0]],
                             item[1].get("total_size") or 0, item[1].get("seeding_time", 0)))
            for torrent_hash, torrent_info in candidates:
                if total_size <= min_size:
                    break
                delete_hashes.append(torrent_hash)
                total_size -= torrent_info.get("total_size", 0)
                torrent_task = torrent_tasks.get(torrent_hash, {})
                self.__send_delete_message(torrent_task, "触发动态删除阈值，系统按做种时间清理")
        if len(limits) > 1 and delete_hashes:
            self.__send_message(
                "【刷流任务动态删除】",
                f"任务：{task.name}\n删除：{len(delete_hashes)} 个种子\n当前做种：{self.__bytes_to_gb(total_size):.1f} GB",
            )
        return delete_hashes

    @staticmethod
    def _retention_rank(record: dict, size: float, seeding_time: float) -> tuple:
        window = transfer_window(record, time.time())
        if window["seconds"] >= 1200 and not window["gap"] and window["avg_upspeed"] is not None:
            return window["avg_upspeed"] / max(size, 1), -seeding_time
        return math.inf, -seeding_time

    def __update_undeleted_torrents_missing_in_downloader(
        self,
        torrent_tasks: Dict[str, dict],
        torrent_check_hashes: List[str],
        torrents: List[Any],
    ) -> None:
        """把下载器中已不存在但仍标记正常的记录更新为已删除"""
        existing_hashes = set(self.__get_all_hashes(torrents))
        missing_hashes = [
            item for item in torrent_check_hashes
            if item not in existing_hashes and not item.startswith("pending:")
            and not torrent_tasks[item].get("deleted")
        ]
        deleted_tasks: List[dict] = []
        for torrent_hash in missing_hashes:
            torrent_task = torrent_tasks[torrent_hash]
            torrent_task.update({"deleted": True, "deleted_time": time.time()})
            deleted_tasks.append(torrent_task)
        if deleted_tasks:
            self.__log_and_send_torrent_task_update_message(
                "【刷流任务状态更新】", "更新为已删除", "下载器中找不到对应种子", deleted_tasks
            )

    def __convert_torrent_info_to_task(self, torrent: Any) -> dict:
        """把下载器种子转换为当前任务的托管记录"""
        torrent_info = self.__get_torrent_info(torrent)
        site_id, site_name = self.__get_site_by_torrent(torrent)
        task = self._get_task_config()
        return {
            "task_id": task.id if task else None,
            "task_name": task.name if task else None,
            "site": site_id,
            "site_name": site_name,
            "title": torrent_info.get("title", ""),
            "size": torrent_info.get("total_size", 0),
            "pubdate": None,
            "description": None,
            "page_url": None,
            "date_elapsed": None,
            "freedate": None,
            "uploadvolumefactor": None,
            "downloadvolumefactor": None,
            "hit_and_run": False,
            "volume_factor": None,
            "freedate_diff": None,
            "ratio": torrent_info.get("ratio", 0),
            "downloaded": torrent_info.get("downloaded", 0),
            "uploaded": torrent_info.get("uploaded", 0),
            "seeding_time": torrent_info.get("seeding_time", 0),
            "deleted": False,
            "time": torrent_info.get("add_on", time.time()),
        }

    @staticmethod
    def __get_redict_url(
        url: str,
        proxies: str = None,
        ua: str = None,
        cookie: str = None,
    ) -> Optional[str]:
        """解析带请求参数的跳转下载链接并返回真实地址"""
        match = re.search(r"\[(.*)](.*)", url)
        if not match:
            return None
        base64_str, request_url = match.group(1), match.group(2)
        if not base64_str:
            return request_url
        try:
            request_text = base64.b64decode(base64_str.encode("utf-8")).decode("utf-8")
            request_params: Dict[str, dict] = json.loads(request_text)
        except (ValueError, UnicodeDecodeError) as err:
            logger.error(f"解析种子跳转下载参数失败：{str(err)}")
            return None
        if not request_params.get("cookie"):
            cookie = None
        headers = request_params.get("header") or None
        request = RequestUtils(ua=ua, proxies=proxies, cookies=cookie, headers=headers)
        if request_params.get("method") == "get":
            response = request.get_res(request_url, params=request_params.get("params"))
        else:
            response = request.post_res(request_url, params=request_params.get("params"))
        if not response:
            return None
        result_path = request_params.get("result")
        if not result_path:
            return response.text
        data = response.json()
        success_key = request_params.get("success")
        if success_key and not data.get(success_key):
            return None
        for key in str(result_path).split("."):
            if not isinstance(data, dict):
                return None
            data = data.get(key)
            if data is None:
                return None
        result_url_path = request_params.get("result_path")
        result_query_param = request_params.get("result_query_param")
        if result_url_path and result_query_param:
            result_url = urljoin(
                f"{str(request_params.get('result_base_url')).rstrip('/')}/",
                str(result_url_path).lstrip("/"),
            )
            return f"{result_url}?{urlencode({result_query_param: data})}"
        return str(data)

    @staticmethod
    def __reset_download_url(torrent_url: str, site_id: int) -> str:
        """为支持的 NexusPHP 站点追加跳过下载提示参数"""
        try:
            if not torrent_url or torrent_url.startswith("magnet"):
                return torrent_url
            site = next(
                (item for item in SitesHelper().get_indexers() if item.get("id") == site_id),
                None,
            )
            if not site or site.get("name") in {"天空"} or not site.get("schema", "").startswith("Nexus"):
                return torrent_url
            parsed_url = urlparse(torrent_url)
            query_params = dict(parse_qsl(parsed_url.query))
            query_params["letdown"] = "1"
            return str(urlunparse(parsed_url._replace(query=urlencode(query_params))))
        except Exception as err:
            logger.error(f"处理种子下载提示地址失败：{str(err)}")
            return torrent_url

    def _reserve_download(self, task: BrushTaskConfig, torrent_hash: str) -> None:
        """提交已被接受即预占名额，避免下载器列表延迟或取 Hash 失败时超额。"""
        snapshot = getattr(self._task_context, "downloads", None)
        if snapshot is not None:
            snapshot.setdefault(task.downloader, {})[torrent_hash] = {
                "hash": torrent_hash, "progress": 0, "tags": task.brush_tag,
            }
        self._save_task_data(task.id, "rotation", {"last_added": time.time()})

    def __download(self, torrent: TorrentInfo) -> Optional[str]:
        """按当前任务配置向 qBittorrent 或 Transmission 添加种子"""
        task = self._get_task_config()
        if not task or not torrent.enclosure:
            logger.error(f"获取种子下载链接失败：{torrent.title}")
            return None
        torrent_content: Union[str, bytes] = torrent.enclosure
        proxies = settings.PROXY if torrent.site_proxy else None
        cookies = torrent.site_cookie
        if isinstance(torrent_content, str) and torrent_content.startswith("["):
            torrent_content = self.__get_redict_url(
                torrent_content,
                proxies=proxies,
                ua=torrent.site_ua,
                cookie=cookies,
            )
            cookies = None
        if not torrent_content:
            return None
        if task.site_skip_tips and isinstance(torrent_content, str):
            torrent_content = self.__reset_download_url(torrent_content, torrent.site)
        downloader = self.downloader
        service = self.service_info
        if not downloader or not service:
            return None
        downloader_helper = DownloaderHelper()
        if downloader_helper.is_downloader("qbittorrent", service=service):
            random_tag = StringUtils.generate_random_str(10)
            if isinstance(torrent_content, str) and not torrent_content.startswith("magnet"):
                response = RequestUtils(cookies=cookies, proxies=proxies, ua=torrent.site_ua).get_res(
                    url=torrent_content
                )
                if response and response.ok:
                    torrent_content = response.content
            if not self._is_focus_site(task.site_id):
                return None
            if not downloader.add_torrent(
                content=torrent_content,
                download_dir=task.save_path,
                cookie=cookies,
                category=task.qb_category,
                tag=[task.brush_tag, random_tag],
            ):
                return None
            reservation = f"pending:{random_tag}"
            self._reserve_download(task, reservation)
            try:
                torrent_hash = downloader.get_torrent_id_by_tag(tags=random_tag)
            except Exception:
                torrent_hash = None
            if not torrent_hash:
                logger.warning(f"刷流任务 [{task.name}] 等待种子 Hash，暂按完整体积预留空间")
                return reservation
            else:
                snapshot = getattr(self._task_context, "downloads", None)
                if snapshot is not None:
                    snapshot[task.downloader].pop(reservation, None)
                self._reserve_download(task, torrent_hash)
            return torrent_hash
        if downloader_helper.is_downloader("transmission", service=service):
            if isinstance(torrent_content, str) and not torrent_content.startswith("magnet"):
                response = RequestUtils(cookies=cookies, proxies=proxies, ua=torrent.site_ua).get_res(
                    url=torrent_content
                )
                if response and response.ok:
                    torrent_content = response.content
            if not self._is_focus_site(task.site_id):
                return None
            added_torrent = downloader.add_torrent(
                content=torrent_content,
                download_dir=task.save_path,
                cookie=cookies,
                labels=[task.brush_tag],
            )
            if not added_torrent:
                return None
            self._reserve_download(task, added_torrent.hashString)
            return added_torrent.hashString
        return None

    def __qb_torrents_reannounce(self, torrent_hashes: List[str]) -> None:
        """删除 qBittorrent 种子前强制重新汇报 Tracker"""
        downloader = self.downloader
        if not downloader or not getattr(downloader, "qbc", None) or not torrent_hashes:
            return
        try:
            downloader.qbc.torrents_reannounce(torrent_hashes=torrent_hashes)
        except Exception as err:
            logger.error(f"强制重新汇报 Tracker 失败：{str(err)}")

    def __get_hash(self, torrent: Any) -> str:
        """兼容获取 qBittorrent 与 Transmission 种子 Hash"""
        try:
            service = self.service_info
            if service and DownloaderHelper().is_downloader("qbittorrent", service=service):
                return torrent.get("hash") or ""
            return getattr(torrent, "hashString", "") or ""
        except Exception as err:
            logger.error(f"获取种子 Hash 失败：{str(err)}")
            return ""

    def __get_all_hashes(self, torrents: List[Any]) -> List[str]:
        """提取下载器种子列表中的全部有效 Hash"""
        return [torrent_hash for torrent in torrents if (torrent_hash := self.__get_hash(torrent))]

    def __get_label(self, torrent: Any) -> List[str]:
        """兼容获取 qBittorrent 标签和 Transmission Labels"""
        try:
            service = self.service_info
            if service and DownloaderHelper().is_downloader("qbittorrent", service=service):
                return [item.strip() for item in str(torrent.get("tags") or "").split(",") if item.strip()]
            return [str(item).strip() for item in getattr(torrent, "labels", None) or [] if str(item).strip()]
        except Exception as err:
            logger.error(f"获取种子标签失败：{str(err)}")
            return []

    def __get_torrent_info(self, torrent: Any) -> dict:
        """统一提取 qBittorrent 与 transmission-rpc v7 种子状态"""
        now_timestamp = int(time.time())
        service = self.service_info
        download_started_at = 0
        if service and DownloaderHelper().is_downloader("qbittorrent", service=service):
            torrent_id = torrent.get("hash")
            title = torrent.get("name")
            added_on = torrent.get("added_on") or 0
            completion_on = torrent.get("completion_on") or 0
            last_activity = torrent.get("last_activity") or 0
            dltime = now_timestamp - added_on if added_on > 0 else 0
            seeding_time = now_timestamp - completion_on if completion_on > 0 else 0
            iatime = now_timestamp - last_activity if last_activity > 0 else 0
            ratio = torrent.get("ratio") or 0
            uploaded = torrent.get("uploaded") or 0
            transfer_downloaded = torrent.get("downloaded") or 0
            sample_uploaded = torrent.get("uploaded")
            sample_downloaded = torrent.get("downloaded")
            total_size = torrent.get("size") or torrent.get("total_size") or 0
            progress = torrent.get("progress")
            downloaded = torrent.get("completed")
            if downloaded is None:
                downloaded = int(total_size * progress) if progress is not None else 0
            completed = bool(
                progress is not None and progress >= 1
                or total_size > 0 and downloaded >= total_size
            )
            state = str(torrent.get("state") or "").lower().replace("_", " ")
            is_downloading = state in {"downloading", "forceddl", "stalleddl"} or (torrent.get("dlspeed") or 0) > 0
            upspeed = torrent.get("upspeed") or 0
            dlspeed = torrent.get("dlspeed") or 0
            tags = torrent.get("tags") or ""
            tracker = torrent.get("tracker") or ""
        else:
            def read_field(name: str, default: Any = None) -> Any:
                try:
                    return getattr(torrent, name, default)
                except (AttributeError, KeyError):
                    return default

            torrent_id = read_field("hashString", "") or read_field("hash_string", "")
            title = read_field("name", "")
            done_date = read_field("done_date") or read_field("date_done")
            added_date = read_field("added_date") or read_field("date_added")
            activity_date = read_field("activity_date") or read_field("date_active")
            start_date = read_field("start_date")
            download_started_at = int(start_date.timestamp()) if start_date and start_date.timestamp() > 0 else 0
            done_timestamp = int(done_date.timestamp()) if done_date and done_date.timestamp() > 0 else 0
            added_on = int(added_date.timestamp()) if added_date and added_date.timestamp() > 0 else 0
            activity_timestamp = int(activity_date.timestamp()) if activity_date and activity_date.timestamp() > 0 else 0
            seeding_time = now_timestamp - done_timestamp if done_timestamp else 0
            dltime = now_timestamp - added_on if added_on else 0
            iatime = now_timestamp - activity_timestamp if activity_timestamp else 0
            total_size = read_field("size_when_done") or read_field("total_size", 0) or 0
            progress = read_field("percent_done")
            if progress is None:
                progress = (read_field("progress", 0) or 0) / 100
            downloaded = int(total_size * progress)
            transfer_downloaded = read_field("downloaded_ever", 0) or 0
            completed = progress >= 1
            ratio = read_field("ratio", 0) or 0
            uploaded = read_field("uploaded_ever", 0) or 0
            sample_uploaded = read_field("uploaded_ever")
            sample_downloaded = read_field("downloaded_ever")
            state = str(read_field("status", "") or "").lower().replace("_", " ")
            is_downloading = state == "downloading" or (read_field("rate_download", 0) or 0) > 0
            upspeed = read_field("rate_upload", 0) or 0
            dlspeed = read_field("rate_download", 0) or 0
            tags = read_field("labels") or ""
            tracker_list = read_field("tracker_list")
            tracker = tracker_list[0] if tracker_list else ""
        avg_upspeed = int(uploaded / dltime) if dltime else uploaded
        return {
            "hash": torrent_id,
            "title": title,
            "seeding_time": seeding_time,
            "ratio": ratio,
            "uploaded": uploaded,
            "sample_uploaded": sample_uploaded,
            "sample_downloaded": sample_downloaded,
            "downloaded": downloaded,
            "transfer_downloaded": transfer_downloaded,
            "completed": completed,
            "state": state,
            "is_downloading": is_downloading,
            "download_started_at": download_started_at,
            "avg_upspeed": avg_upspeed,
            "upspeed": upspeed,
            "dlspeed": dlspeed,
            "iatime": iatime,
            "dltime": dltime,
            "total_size": total_size,
            "add_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(added_on)),
            "add_on": added_on,
            "tags": tags,
            "tracker": tracker,
        }

    def __get_average_bandwidth(
        self,
        sample_count: int = 5,
        interval: float = 3.0,
    ) -> Tuple[Optional[float], Optional[float]]:
        """多次采样所有下载器带宽并返回平均值"""
        upload_speeds: List[float] = []
        download_speeds: List[float] = []
        for index in range(sample_count):
            downloader_info = self.__get_downloader_info()
            if downloader_info:
                upload_speeds.append(downloader_info.upload_speed or 0)
                download_speeds.append(downloader_info.download_speed or 0)
            if index < sample_count - 1:
                time.sleep(interval)
        if not upload_speeds or not download_speeds:
            return None, None
        return sum(upload_speeds) / len(upload_speeds), sum(download_speeds) / len(download_speeds)

    def __get_downloader_info(self) -> schemas.DownloaderInfo:
        """通过插件链汇总当前所有下载器的实时传输信息"""
        result = schemas.DownloaderInfo()
        transfer_infos = self.chain.run_module("downloader_info")
        for transfer_info in transfer_infos or []:
            result.download_speed += transfer_info.download_speed
            result.upload_speed += transfer_info.upload_speed
            result.download_size += transfer_info.download_size
            result.upload_size += transfer_info.upload_size
        return result

    @staticmethod
    def _download_labels(torrent: Any) -> Set[str]:
        """按种子对象本身取标签，避免跨下载器汇总依赖当前任务上下文。"""
        if isinstance(torrent, dict):
            return {tag.strip() for tag in str(torrent.get("tags") or "").split(",") if tag.strip()}
        return {str(tag).strip() for tag in getattr(torrent, "labels", None) or [] if str(tag).strip()}

    @staticmethod
    def _unfinished_download(torrent: Any) -> bool:
        """按完成进度计数，包含排队、暂停、校验和异常的未完成任务。"""
        if isinstance(torrent, dict):
            progress = torrent.get("progress")
        else:
            progress = getattr(torrent, "percent_done", None)
            if progress is None:
                progress = getattr(torrent, "progress", None)
                if progress is not None:
                    progress /= 100
        if progress is not None:
            return float(progress) < 1
        state = str(torrent.get("state", "") if isinstance(torrent, dict)
                    else getattr(torrent, "status", "")).lower().replace("_", " ")
        return state not in {"seeding", "seed pending", "uploading", "stalledup", "queuedup", "forcedup"}

    def _resolve_pending_downloads(self, records: Dict[str, dict], torrents: Dict[str, Any]) -> bool:
        """通过添加时的唯一标签恢复尚未返回 Hash 的 qBittorrent 任务。"""
        changed = False
        for key in list(records):
            if not key.startswith("pending:") or records[key].get("deleted"):
                continue
            tag = key.removeprefix("pending:")
            for torrent_hash, torrent in torrents.items():
                if tag in self._download_labels(torrent):
                    record = records.pop(key)
                    records[torrent_hash] = {**records.get(torrent_hash, {}), **record}
                    changed = True
                    break
        return changed

    def _read_downloads(self, allow_partial: bool = False) -> Dict[str, Dict[str, Any]]:
        """下载器读取失败不视为零占用，可隔离失败下载器继续其他站点。"""
        snapshot = {}
        helper = DownloaderHelper()
        for name in {task.downloader for task in list(self._task_configs.values()) if task.downloader}:
            try:
                service = helper.get_service(name=name)
                if not service or not service.instance:
                    raise RuntimeError(f"下载器 {name} 不可用")
                torrents, error = service.instance.get_torrents()
                if error or torrents is None:
                    raise RuntimeError(f"下载器 {name} 种子列表读取失败")
                rows = {}
                for torrent in torrents:
                    torrent_hash = (torrent.get("hash") if isinstance(torrent, dict)
                                    else getattr(torrent, "hashString", None))
                    if not torrent_hash:
                        raise RuntimeError(f"下载器 {name} 返回缺少 Hash 的种子")
                    rows[str(torrent_hash)] = torrent
                snapshot[name] = rows
            except Exception:
                if not allow_partial:
                    raise
                logger.warning(f"下载器 {name} 读取失败，其任务保留等待重试")
        return snapshot

    def _downloading_keys(self, tasks: List[BrushTaskConfig], include_global: bool = False) -> Set[Tuple[str, str]]:
        """按下载器和 Hash 去重，兼容任务标签、旧标签及已托管记录。"""
        snapshot = getattr(self._task_context, "downloads", None)
        if snapshot is None:
            snapshot = self._read_downloads()
        keys: Set[Tuple[str, str]] = set()
        for task in tasks:
            records = self._get_task_data(task.id, "torrents") or {}
            tags = {task.brush_tag}
            if include_global:
                tags.add(self.GLOBAL_BRUSH_TAG)
            for torrent_hash, torrent in snapshot.get(task.downloader, {}).items():
                if (tags.intersection(self._download_labels(torrent)) or torrent_hash in records) \
                        and self._unfinished_download(torrent):
                    keys.add((task.downloader, torrent_hash))
        return keys

    def _site_downloading_count(self, site_id: int) -> int:
        """统计同站点各下载器中的未完成任务。"""
        return len(self._downloading_keys([
            task for task in list(self._task_configs.values()) if task.site_id == site_id
        ]))

    def _site_completed_count(self, site_id: int) -> int:
        """按实际仍保留的已完成种子计数，删除落地后才释放名额。"""
        snapshot = getattr(self._task_context, "downloads", None)
        if snapshot is None:
            snapshot = self._read_downloads()
        keys = set()
        for task in self._task_configs.values():
            if task.site_id != site_id:
                continue
            records = self._get_task_data(task.id, "torrents") or {}
            for torrent_hash, torrent in snapshot.get(task.downloader, {}).items():
                if (torrent_hash in records or task.brush_tag in self._download_labels(torrent)) \
                        and not self._unfinished_download(torrent):
                    keys.add((task.downloader, torrent_hash))
        return len(keys)

    def __get_downloading_count(self) -> int:
        task = self._get_task_config()
        return len(self._downloading_keys([task])) if task else 0

    def __get_global_downloading_count(self) -> int:
        return len(self._downloading_keys(list(self._task_configs.values()), include_global=True))

    @staticmethod
    def __get_pubminutes(pubdate: str, timezone_offset: float = 0) -> Optional[float]:
        """计算站点发布时间距当前时间的分钟数"""
        return publication_age_minutes(pubdate, timezone_offset, settings.TZ)

    def __filter_torrents_by_tag(self, torrents: List[Any], exclude_tag: Optional[str]) -> List[Any]:
        """过滤包含任一删除排除标签的种子"""
        if not exclude_tag:
            return torrents
        excluded_tags = {item.strip() for item in exclude_tag.split(",") if item.strip()}
        return [
            torrent for torrent in torrents
            if not excluded_tags.intersection(self.__get_label(torrent))
        ]

    def __get_subscribe_titles(self) -> Set[str]:
        """识别并缓存当前订阅可用于排除匹配的标题集合"""
        task = self._get_task_config()
        if not task or not task.except_subscribe:
            return set()
        subscribes = SubscribeOper().list() or []
        for subscribe in subscribes:
            cache_key = f"{subscribe.id}_{subscribe.name}"
            if cache_key in self._subscribe_infos:
                continue
            titles = [subscribe.name]
            try:
                meta = MetaInfo(subscribe.name)
                meta.year = subscribe.year
                meta.begin_season = subscribe.season or None
                meta.type = MediaType(subscribe.type)
                mediainfo: MediaInfo = self.chain.recognize_media(
                    meta=meta,
                    mtype=meta.type,
                    media_source=subscribe.media_source,
                    media_id=subscribe.media_id,
                    cache=True,
                )
                if mediainfo:
                    titles.extend(mediainfo.names)
            except Exception as err:
                logger.error(f"识别订阅 {subscribe.name} 失败：{str(err)}")
            self._subscribe_infos[cache_key] = [item.strip() for item in titles if item and item.strip()]
        current_keys = {f"{subscribe.id}_{subscribe.name}" for subscribe in subscribes}
        for cache_key in set(self._subscribe_infos) - current_keys:
            self._subscribe_infos.pop(cache_key, None)
        return {title for titles in self._subscribe_infos.values() for title in titles}

    @staticmethod
    def __filter_torrents_contains_subscribe(
        torrents: List[TorrentInfo],
        subscribe_titles: Set[str],
    ) -> List[TorrentInfo]:
        """排除标题或描述命中任一订阅名称的候选种子"""
        if not subscribe_titles:
            return torrents
        included: List[TorrentInfo] = []
        for torrent in torrents:
            title = torrent.title or ""
            description = torrent.description or ""
            if any(item in title or item in description for item in subscribe_titles):
                logger.info(f"命中订阅内容，排除种子：{title}|{description}")
                continue
            included.append(torrent)
        return included

    @staticmethod
    def __bytes_to_gb(size_in_bytes: float) -> float:
        """把字节数转换为 GB"""
        return float(size_in_bytes or 0) / 1024 ** 3

    @staticmethod
    def __calculate_seeding_torrents_size(torrent_tasks: Dict[str, dict]) -> float:
        """计算未删除托管种子的总做种体积"""
        return sum(
            item.get("size", 0) or 0
            for item in torrent_tasks.values()
            if not item.get("deleted")
        )

    def __auto_archive_tasks(self, torrent_tasks: Dict[str, dict]) -> None:
        """按当前任务保留天数归档已删除种子记录"""
        task = self._get_task_config()
        if not task or not task.auto_archive_days or task.auto_archive_days <= 0:
            return
        archived_tasks: Dict[str, dict] = self._current_task_data("archived", {})
        threshold = float(task.auto_archive_days) * 86400
        now_timestamp = time.time()
        archive_hashes = []
        for torrent_hash, item in torrent_tasks.items():
            if not item.get("deleted"):
                continue
            deleted_time = item.get("deleted_time")
            if deleted_time is None or (
                isinstance(deleted_time, (int, float))
                and now_timestamp - deleted_time > threshold
            ):
                archive_hashes.append(torrent_hash)
        for torrent_hash in archive_hashes:
            archived_tasks[torrent_hash] = torrent_tasks.pop(torrent_hash)
        self._save_current_task_data("archived", archived_tasks)

    def _recalculate_statistics(self, task_id: str) -> Dict[str, int]:
        """从当前和归档记录重新计算单任务统计"""
        torrents = self._get_task_data(task_id, "torrents") or {}
        archived = self._get_task_data(task_id, "archived") or {}
        combined = {**archived, **torrents}
        statistic = {
            "count": len(combined),
            "deleted": sum(1 for item in combined.values() if item.get("deleted")),
            "uploaded": sum(item.get("uploaded", 0) or 0 for item in combined.values()),
            "downloaded": sum(item.get("downloaded", 0) or 0 for item in combined.values()),
            "unarchived": sum(1 for item in torrents.values() if item.get("deleted")),
            "active": sum(1 for item in torrents.values() if not item.get("deleted")),
            "active_uploaded": sum(
                item.get("uploaded", 0) or 0 for item in torrents.values() if not item.get("deleted")
            ),
            "active_downloaded": sum(
                item.get("downloaded", 0) or 0 for item in torrents.values() if not item.get("deleted")
            ),
        }
        self._save_task_data(task_id, "statistic", statistic)
        return statistic

    def _get_statistic_info(self, task_id: str) -> Dict[str, int]:
        """读取单任务统计并为历史空数据补齐字段"""
        defaults = {
            "count": 0,
            "deleted": 0,
            "uploaded": 0,
            "downloaded": 0,
            "unarchived": 0,
            "active": 0,
            "active_uploaded": 0,
            "active_downloaded": 0,
        }
        statistic = self._get_task_data(task_id, "statistic") or {}
        return {**defaults, **statistic}

    @staticmethod
    def _is_valid_time_range(time_range: Optional[str]) -> bool:
        """校验 HH:MM-HH:MM 格式的每日时间段"""
        if not time_range or not re.fullmatch(r"\d{2}:\d{2}-\d{2}:\d{2}", time_range):
            return False
        try:
            start, end = time_range.split("-", 1)
            datetime.strptime(start, "%H:%M")
            datetime.strptime(end, "%H:%M")
            return True
        except ValueError:
            return False

    def _is_current_time_in_range(self, task: Optional[BrushTaskConfig] = None) -> bool:
        """判断当前时间是否处于任务允许的每日开启区间"""
        task = task or self._get_task_config()
        if not task or not self._is_valid_time_range(task.active_time_range):
            return True
        start_text, end_text = task.active_time_range.split("-", 1)
        start_time = datetime.strptime(start_text, "%H:%M").time()
        end_time = datetime.strptime(end_text, "%H:%M").time()
        current_time = datetime.now().time()
        if start_time <= end_time:
            return start_time <= current_time <= end_time
        return current_time >= start_time or current_time <= end_time

    @staticmethod
    def __get_site_by_torrent(torrent: Any) -> Tuple[int, str]:
        """根据 Tracker 或磁力链接识别种子所属站点"""
        trackers: List[str] = []
        last_domain = "未知"
        tracker_url = torrent.get("tracker") if isinstance(torrent, dict) else None
        if not tracker_url:
            tracker_list = getattr(torrent, "tracker_list", None)
            tracker_url = tracker_list[0] if tracker_list else None
        if tracker_url:
            trackers.append(tracker_url)
        magnet_link = torrent.get("magnet_uri") if isinstance(torrent, dict) else getattr(torrent, "magnet_link", None)
        if magnet_link:
            trackers.extend(unquote(item) for item in parse_qs(urlparse(magnet_link).query).get("tr", []))
        tracker_mappings = {
            "chdbits.xyz": "ptchdbits.co",
            "agsvpt.trackers.work": "agsvpt.com",
            "tracker.cinefiles.info": "audiences.me",
        }
        for tracker in trackers:
            if not tracker:
                continue
            domain = next(
                (mapped for keyword, mapped in tracker_mappings.items() if keyword in tracker),
                StringUtils.get_url_domain(tracker),
            )
            last_domain = domain or last_domain
            site = SitesHelper().get_indexer(domain)
            if site:
                return site.get("id"), site.get("name")
        return 0, last_domain

    def _log_and_notify_error(self, message: str) -> None:
        """记录错误并写入系统消息中心"""
        logger.error(message)
        self.systemmessage.put(message, title="站点刷流")

    def __send_delete_message(self, torrent_task: dict, reason: str) -> None:
        """发送包含任务、站点、种子和原因的删种通知"""
        task = self._get_task_config()
        if not task or not task.notify:
            return
        text = (
            f"任务：{task.name}\n"
            f"站点：{torrent_task.get('site_name') or '未知'}\n"
            f"标题：{torrent_task.get('title') or '未知'}\n"
            f"原因：{reason}"
        )
        self.post_message(mtype=NotificationType.SiteMessage, title="【刷流任务种子删除】", text=text)

    @staticmethod
    def __build_add_message_text(torrent: Union[TorrentInfo, dict], task_name: str) -> str:
        """兼容候选对象和任务字典构建新增通知文本"""
        def read_value(key: str, default: Any = None) -> Any:
            """统一读取候选对象或字典字段"""
            return torrent.get(key, default) if isinstance(torrent, dict) else getattr(torrent, key, default)

        lines = [f"任务：{task_name}"]
        labels = {
            "site_name": "站点",
            "title": "标题",
            "description": "内容",
            "size": "大小",
            "pubdate": "发布时间",
            "seeders": "做种数",
            "volume_factor": "促销",
            "hit_and_run": "Hit&Run",
        }
        for key, label in labels.items():
            value = read_value(key)
            if key == "size" and value:
                value = StringUtils.str_filesize(value)
            if value not in (None, "", False):
                lines.append(f"{label}：{'是' if key == 'hit_and_run' else value}")
        return "\n".join(lines)

    def __send_add_message(self, torrent: Union[TorrentInfo, dict]) -> None:
        """发送当前任务新增刷流种子的通知"""
        task = self._get_task_config()
        if not task or not task.notify:
            return
        self.post_message(
            mtype=NotificationType.SiteMessage,
            title="【刷流任务种子下载】",
            text=self.__build_add_message_text(torrent, task.name),
        )

    def __send_message(self, title: str, text: str) -> None:
        """按当前任务通知开关发送通用站点消息"""
        task = self._get_task_config()
        if task and task.notify:
            self.post_message(mtype=NotificationType.SiteMessage, title=title, text=text)

    def __log_and_send_torrent_task_update_message(
        self,
        title: str,
        status: str,
        reason: str,
        torrent_tasks: List[dict],
    ) -> None:
        """记录并汇总发送标签同步导致的任务状态变更"""
        if not torrent_tasks:
            return
        task = self._get_task_config()
        first_title = torrent_tasks[0].get("title") or "未知种子"
        text = (
            f"任务：{task.name if task else '未知'}\n"
            f"内容：{first_title} 等 {len(torrent_tasks)} 个种子已{status}\n"
            f"原因：{reason}"
        )
        logger.info(f"{title}，{text}")
        self.__send_message(title, text)
