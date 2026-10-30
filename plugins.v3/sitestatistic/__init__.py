import gc
import hashlib
import math
import warnings
from datetime import datetime, timedelta
from threading import Lock
from typing import Optional, Any, List, Dict, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler

from app import schemas
from app.chain.site import SiteChain
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.db.models.siteuserdata import SiteUserData
from app.db.site_oper import SiteOper
from app.helper.sites import SitesHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, NotificationType
from app.utils.string import StringUtils

from .monthly import collect_month, identity, valid_traffic, approximate_fields, latest_by_domain, _bytes as traffic_bytes

warnings.filterwarnings("ignore", category=FutureWarning)

lock = Lock()


class OriechoSiteStatistic(_PluginBase):
    """站点数据统计插件，聚合各站点最新用户数据并展示统计信息。"""

    # 插件名称
    plugin_name = "站点数据统计"
    # 插件描述
    plugin_desc = "站点数据图表与自然月上传、下载增量统计。"
    # 插件图标
    plugin_icon = "statistic.png"
    # 插件版本
    plugin_version = "1.1.0"
    # 插件作者
    plugin_author = "oriecho"
    # 作者主页
    author_url = "https://github.com/iu2dlwlrma"
    # 插件配置项ID前缀
    plugin_config_prefix = "oriechositestatistic_"
    # 加载顺序
    plugin_order = 1
    # 可使用的用户级别
    auth_level = 2

    # 配置属性
    _enabled: bool = False
    _onlyonce: bool = False
    _dashboard_type: str = "today"
    _notify_type = ""
    _scheduler = None
    _PB_STEP = 1024 ** 5 / 1000
    _PRECISION_NOTE = (
        "≈ 表示两期读数疑似经过 PB 舍入，显示步长约 1.02T；"
        "两期差值的误差可达一个步长，读数未变不代表没有流量，无法据此还原真实期间增量。"
    )

    def init_plugin(self, config: dict = None):
        """加载配置，并在请求立即运行时调度一次站点数据刷新。"""
        # 停止现有任务
        self.stop_service()

        # 独立配置，不读取或迁移官方插件；清空配置也必须恢复禁用默认值。
        config = dict(config or {})
        self._enabled = bool(config.get("enabled", False))
        self._onlyonce = bool(config.get("onlyonce", False))
        self._dashboard_type = config.get("dashboard_type") or "today"
        self._notify_type = config.get("notify_type") or ""

        if self._onlyonce:
            config["onlyonce"] = False
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(SiteChain().refresh_userdatas, "date",
                                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                    name="站点数据统计服务")
            self._scheduler.print_jobs()
            self._scheduler.start()
            self.update_config(config=config)

    def get_state(self) -> bool:
        """返回插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """本插件不额外注册远程命令。"""
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        """
        获取插件API
        [{
            "path": "/xx",
            "endpoint": self.xxx,
            "methods": ["GET", "POST"],
            "summary": "API说明"
        }]
        """
        return [{
            "path": "/refresh_by_domain",
            "endpoint": self.refresh_by_domain,
            "methods": ["GET"],
            "summary": "刷新站点数据",
            "description": "刷新对应域名的站点数据",
        }]

    def get_service(self) -> List[Dict[str, Any]]:
        """复用宿主刷新事件，不注册周期服务。"""
        pass

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
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
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
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'dashboard_type',
                                            'label': '仪表板组件',
                                            'aria-label': '仪表板组件',
                                            'items': [
                                                {'title': '今日数据', 'value': 'today'},
                                                {'title': '汇总数据', 'value': 'total'},
                                                {'title': '本月数据', 'value': 'month'},
                                                {'title': '所有数据', 'value': 'all'}
                                            ]
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'notify_type',
                                            'label': '数据刷新时发送通知',
                                            'aria-label': '数据刷新时发送通知',
                                            'items': [
                                                {'title': '不发送', 'value': ''},
                                                {'title': '今日增量数据', 'value': 'inc'},
                                                {'title': '累计全量数据', 'value': 'all'},
                                                {'title': '月增量数据', 'value': 'month'}
                                            ]
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "dashboard_type": 'today',
            "notify_type": ""
        }

    @eventmanager.register(EventType.SiteRefreshed)
    def send_msg(self, event: Event = None):
        """在全量站点刷新后发送通知，并仅跳过内容完全相同的重复事件。

        宿主事件不会区分凌晨首次刷新和后续定时刷新，因此不能按日期锁定通知；
        后续刷新只要产生新的统计内容，就应继续发送今日增量或累计数据。
        """
        # 插件重载窗口期事件可能携带空 event 或未启用通知，此时直接返回避免误报
        if not self._enabled or not self._notify_type or not event:
            return
        if not event.event_data or event.event_data.get('site_id') != "*":
            return
        notify_type = self._notify_type
        if notify_type == "month":
            self._send_month_notification()
            return
        logger.debug(f"收到站点数据刷新事件，开始计算{notify_type}数据通知 ...")
        # 获取站点数据
        today, today_data, yesterday_data = self.__get_data()
        # 转换为字典
        today_data_dict = {identity(data): data for data in today_data}
        yesterday_data_dict = {identity(data): data for data in yesterday_data}
        approximate_fields = self.__get_approximate_fields(today_data, yesterday_data)
        total_approximate = set()
        # 消息内容
        messages = {}
        # 总上传
        incUploads = 0
        # 总下载
        incDownloads = 0
        # 今天的日期
        today_date = datetime.now(tz=pytz.timezone(settings.TZ)).strftime("%Y-%m-%d")

        for rand, site in enumerate(today_data_dict.keys()):
            approximate = approximate_fields.get(site, set())
            upload = int(today_data_dict[site].upload or 0)
            download = int(today_data_dict[site].download or 0)
            updated_date = today_data_dict[site].updated_day

            if notify_type == "inc":
                # 增量数据模式：只有当有昨天数据时才计算增量
                if yesterday_data_dict.get(site):
                    yesterday_upload = int(yesterday_data_dict[site].upload or 0)
                    yesterday_download = int(yesterday_data_dict[site].download or 0)
                    
                    # 如果昨天上传和下载数据为0，则跳过该站点
                    # 因为昨天数据为0时，计算出来的是累计数据而非增量
                    if not yesterday_upload and not yesterday_download:
                        continue
                    
                    upload -= yesterday_upload
                    download -= yesterday_download
                    # 确保增量不为负数
                    upload = max(0, upload)
                    download = max(0, download)
                else:
                    # 没有昨天数据时，跳过该站点
                    continue

            if updated_date and updated_date != today_date:
                updated_date = f"（{updated_date}）"
            else:
                updated_date = ""

            if upload > 0 or download > 0 or approximate:
                incUploads += upload
                incDownloads += download
                total_approximate.update(approximate)
                # PB 级整数与小数相加会丢失后缀，使用整数元组避免等量站点互相覆盖。
                messages[(upload, rand)] = (
                        f"【{today_data_dict[site].name} · {today_data_dict[site].domain}】{updated_date}\n"
                        + f"上传量：{self.__format_traffic(upload, 'upload' in approximate, notify_type == 'inc')}\n"
                        + f"下载量：{self.__format_traffic(download, 'download' in approximate, notify_type == 'inc')}\n"
                        + "————————————"
                )

        if messages:
            sorted_messages = [messages[key] for key in sorted(messages.keys(), reverse=True)]
            sorted_messages.insert(0, f"【汇总】\n"
                                      f"总上传：{self.__format_traffic(incUploads, 'upload' in total_approximate, notify_type == 'inc')}\n"
                                      f"总下载：{self.__format_traffic(incDownloads, 'download' in total_approximate, notify_type == 'inc')}\n"
                                      f"————————————")
            if total_approximate:
                sorted_messages.append(self._PRECISION_NOTE)
            notification_text = "\n".join(sorted_messages)
            notification_fingerprint = self.__get_notification_fingerprint(notification_text)

            # 同一份统计内容只推送一次；后续定时刷新若数据发生变化，仍需继续推送。
            with lock:
                if not self._enabled or self._notify_type != notify_type:
                    return
                last_notify = self.get_data("last_notify") or {}
                if (last_notify.get("date") == today_date
                        and last_notify.get("type") == notify_type
                        and last_notify.get("fingerprint") == notification_fingerprint):
                    logger.info(f"站点数据统计通知内容未变化，跳过本次重复通知（{today_date}），"
                                f"本次增量：上传 {self.__format_filesize(incUploads)}，"
                                f"下载 {self.__format_filesize(incDownloads)}")
                    return
                self.post_message(mtype=NotificationType.SiteMessage,
                                  title="站点数据统计", text=notification_text)
                # 持久化最近一次通知内容，避免插件重载或重启后重复推送同一份快照。
                self.save_data("last_notify", {
                    "date": today_date,
                    "type": notify_type,
                    "fingerprint": notification_fingerprint,
                    "time": datetime.now().strftime("%H:%M:%S")
                })
                logger.info(f"站点数据统计通知发送完成（{today_date}），"
                            f"总上传 {self.__format_filesize(incUploads)}，"
                            f"总下载 {self.__format_filesize(incDownloads)}")

    @staticmethod
    def __get_approximate_fields(current_data: List[SiteUserData], previous_data: List[SiteUserData]) -> Dict[str, set]:
        """用两期有效读数识别疑似 PB 三位小数量化，仅影响精度提示。

        原始单位未入库，不能证明数据源精度，故不平滑、不推算日均、不改写字节数。
        阈值对应 NexusPHP 从 1000 TiB 起显示 PB；容差仅覆盖字节取整和浮点误差，
        不能使用相对步长容差，否则会把偏离格点数 MB 的精确 API 数据也标为近似。
        """
        previous_by_account = {identity(data): data for data in previous_data}
        result = {}
        for current in current_data:
            previous = previous_by_account.get(identity(current))
            fields = approximate_fields(previous, current)
            if fields:
                result[identity(current)] = fields
        return result

    @staticmethod
    def __format_traffic(size: Any, approximate: bool = False, increment: bool = False) -> str:
        """标注低精度流量；零差值不能被解释成精确的零增量。"""
        if approximate and increment and not size:
            return "≈ 读数未变（实际增量未知）"
        return ('≈ ' if approximate else '') + OriechoSiteStatistic.__format_filesize(size)

    @staticmethod
    def __get_notification_fingerprint(notification_text: str) -> str:
        """计算通知正文指纹，用于识别同一轮刷新产生的重复通知。"""
        return hashlib.sha256(notification_text.encode("utf-8")).hexdigest()

    @staticmethod
    def __get_data() -> Tuple[str, List[SiteUserData], List[SiteUserData]]:
        """通知与原有调用兼容三项返回值；读取诊断由页面单独使用。"""
        return OriechoSiteStatistic._get_snapshot_data()[:3]

    @staticmethod
    def _get_snapshot_data():
        """
        获取站点最新数据、前一份可用数据及对应的日期标签。

        宿主按站点分别返回最新日期，因此多个站点可能对应不同日期；只有所有站点
        日期一致时才返回具体日期，否则使用“各站点最近更新日”避免误导页面用户。
        前一份数据按日期缓存，避免为每个站点重复查询相同日期的整批快照。
        """
        site_oper = SiteOper()
        # 优化：只获取最近的站点数据，而不是所有历史数据
        latest_data: List[SiteUserData] = site_oper.get_userdata_latest()
        if not latest_data:
            return "", [], [], []

        # 过滤未启用或不存在的站点
        site_domains = {site.domain for site in site_oper.list_active()}
        latest_data = list(latest_by_domain(latest_data, site_domains).values())
        if not latest_data:
            return "", [], [], []

        # 只有各站点日期一致时才显示具体日期，混合日期用统一说明避免误标整张表。
        latest_days = {data.updated_day for data in latest_data}
        latest_day = next(iter(latest_days)) if len(latest_days) == 1 else "各站点最近更新日"
        
        # 按上传量降序排序
        latest_data.sort(key=lambda data: traffic_bytes(data.upload), reverse=True)

        # 为每个站点查找对应的前一天数据
        previous_data = []
        previous_by_date: Dict[str, Dict[str, SiteUserData]] = {}
        query_errors = []
        for current_site in latest_data:
            site_account = identity(current_site)
            current_day = current_site.updated_day
            if not site_account[1]:
                continue
            # 保留旧图的最多七日回退；先选该域名当日最新记录，再核对账号。
            # 遇到已更换的账号或读取失败就停止，不能挑旧账号或跨过未知日期。
            for offset in range(1, 8):
                previous_day = (datetime.strptime(current_day, '%Y-%m-%d')
                                - timedelta(days=offset)).strftime('%Y-%m-%d')
                if previous_day not in previous_by_date:
                    try:
                        snapshots = site_oper.get_userdata_by_date(previous_day)
                        previous_by_date[previous_day] = latest_by_domain(snapshots, site_domains, previous_day)
                    except Exception:
                        previous_by_date[previous_day] = {}
                        query_errors.append(previous_day)
                if previous_day in query_errors:
                    break
                baseline = previous_by_date[previous_day].get(site_account[0])
                if baseline:
                    if identity(baseline) == site_account:
                        previous_data.append(baseline)
                    break

        return latest_day, latest_data, previous_data, query_errors

    @staticmethod
    def __format_filesize(size: Any) -> str:
        """格式化站点字节数，兼容宿主旧接口无法处理的 PB 和科学计数法。"""
        if size is None:
            return ""
        try:
            numeric_size = float(size)
        except (TypeError, ValueError):
            return StringUtils.str_filesize(size)

        # SiteUserData 的流量字段在 V2 宿主中是 Float，数值较大时会先变成科学计数法。
        # 旧 str_filesize 只支持到 T，故在插件边界补齐 PB，并统一转回整数输入。
        if numeric_size >= 1024 ** 5:
            return f"{numeric_size / (1024 ** 5):.2f}PB"
        try:
            return StringUtils.str_filesize(int(numeric_size))
        except (OverflowError, ValueError):
            return StringUtils.str_filesize(size)

    @staticmethod
    def __get_total_elements(today: str, stattistic_data: List[SiteUserData], yesterday_sites_data: List[SiteUserData],
                             dashboard: str = "today") -> List[dict]:
        """
        获取统计元素，统一使用插件侧的大容量字节格式化逻辑。
        """
        approximate_fields = OriechoSiteStatistic.__get_approximate_fields(stattistic_data, yesterday_sites_data)
        upload_approximate = any('upload' in fields for fields in approximate_fields.values())
        download_approximate = any('download' in fields for fields in approximate_fields.values())

        def __gb(value: int) -> float:
            """
            转换为GB，保留1位小数
            """
            if not value:
                return 0
            return round(float(value) / 1024 / 1024 / 1024, 1)

        def __is_digit(value: any) -> bool:
            """
            判断是否为数字
            """
            if value is None:
                return False
            if isinstance(value, float) or isinstance(value, int):
                return True
            if isinstance(value, str):
                return value.isdigit()
            return False

        def __to_numeric(value: any) -> int:
            """
            将值转换为整数
            """
            if isinstance(value, str):
                return int(float(value))
            elif isinstance(value, float) or isinstance(value, int):
                return int(value)
            else:
                logger.error(f'数据类型转换错误 ({value})')
                return 0

        def __sub_data(d1: dict, d2: dict) -> dict:
            """
            计算同名数字字段差值；上传和下载各自要求有效且大于零的历史基线。
            """
            if not d1:
                return {}
            if not d2:
                # 如果没有昨天数据，返回空字典，表示无增量数据
                return {}
            
            # 上传和下载分别校验历史基线，避免零下载量阻断有效上传增量。
            # 历史流量必须大于 0，防止将首次有效累计读数误计为增量。
            d = {k: __to_numeric(d1.get(k)) - __to_numeric(d2.get(k)) for k in d1
                 if k in d2 and __is_digit(d1.get(k)) and __is_digit(d2.get(k))
                 and (k not in ('upload', 'download') or __to_numeric(d2.get(k)) > 0)}
            # 把小于0的数据变成0
            for k, v in d.items():
                if isinstance(v, (int, float)) and v < 0:
                    d[k] = 0
            return d

        if dashboard in ['total', 'all']:
            # 总上传量
            total_upload = sum([data.upload for data in stattistic_data if data.upload])
            # 总下载量
            total_download = sum([data.download for data in stattistic_data if data.download])
            # 总做种数
            total_seed = sum([data.seeding for data in stattistic_data if data.seeding])
            # 总做种体积
            total_seed_size = sum([data.seeding_size for data in stattistic_data if data.seeding_size])

            total_elements = [
                # 总上传量
                {
                    'component': 'VCol',
                    'props': {
                        'cols': 6,
                        'md': 3
                    },
                    'content': [
                        {
                            'component': 'VCard',
                            'props': {
                                'variant': 'tonal',
                            },
                            'content': [
                                {
                                    'component': 'VCardText',
                                    'props': {
                                        'class': 'd-flex align-center',
                                    },
                                    'content': [
                                        {
                                            'component': 'VAvatar',
                                            'props': {
                                                'rounded': True,
                                                'variant': 'text',
                                                'class': 'me-3'
                                            },
                                            'content': [
                                                {
                                                    'component': 'VImg',
                                                    'props': {
                                                        'src': '/plugin_icon/upload.png'
                                                    }
                                                }
                                            ]
                                        },
                                        {
                                            'component': 'div',
                                            'content': [
                                                {
                                                    'component': 'span',
                                                    'props': {
                                                        'class': 'text-caption'
                                                    },
                                                    'text': '总上传量'
                                                },
                                                {
                                                    'component': 'div',
                                                    'props': {
                                                        'class': 'd-flex align-center flex-wrap'
                                                    },
                                                    'content': [
                                                        {
                                                            'component': 'span',
                                                            'props': {
                                                                'class': 'text-h6'
                                                            },
                                                            'text': OriechoSiteStatistic.__format_traffic(total_upload, upload_approximate)
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        },
                    ]
                },
                # 总下载量
                {
                    'component': 'VCol',
                    'props': {
                        'cols': 6,
                        'md': 3,
                    },
                    'content': [
                        {
                            'component': 'VCard',
                            'props': {
                                'variant': 'tonal',
                            },
                            'content': [
                                {
                                    'component': 'VCardText',
                                    'props': {
                                        'class': 'd-flex align-center',
                                    },
                                    'content': [
                                        {
                                            'component': 'VAvatar',
                                            'props': {
                                                'rounded': True,
                                                'variant': 'text',
                                                'class': 'me-3'
                                            },
                                            'content': [
                                                {
                                                    'component': 'VImg',
                                                    'props': {
                                                        'src': '/plugin_icon/download.png'
                                                    }
                                                }
                                            ]
                                        },
                                        {
                                            'component': 'div',
                                            'content': [
                                                {
                                                    'component': 'span',
                                                    'props': {
                                                        'class': 'text-caption'
                                                    },
                                                    'text': '总下载量'
                                                },
                                                {
                                                    'component': 'div',
                                                    'props': {
                                                        'class': 'd-flex align-center flex-wrap'
                                                    },
                                                    'content': [
                                                        {
                                                            'component': 'span',
                                                            'props': {
                                                                'class': 'text-h6'
                                                            },
                                                            'text': OriechoSiteStatistic.__format_traffic(total_download, download_approximate)
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        },
                    ]
                },
                # 总做种数
                {
                    'component': 'VCol',
                    'props': {
                        'cols': 6,
                        'md': 3
                    },
                    'content': [
                        {
                            'component': 'VCard',
                            'props': {
                                'variant': 'tonal',
                            },
                            'content': [
                                {
                                    'component': 'VCardText',
                                    'props': {
                                        'class': 'd-flex align-center',
                                    },
                                    'content': [
                                        {
                                            'component': 'VAvatar',
                                            'props': {
                                                'rounded': True,
                                                'variant': 'text',
                                                'class': 'me-3'
                                            },
                                            'content': [
                                                {
                                                    'component': 'VImg',
                                                    'props': {
                                                        'src': '/plugin_icon/seed.png'
                                                    }
                                                }
                                            ]
                                        },
                                        {
                                            'component': 'div',
                                            'content': [
                                                {
                                                    'component': 'span',
                                                    'props': {
                                                        'class': 'text-caption'
                                                    },
                                                    'text': '总做种数'
                                                },
                                                {
                                                    'component': 'div',
                                                    'props': {
                                                        'class': 'd-flex align-center flex-wrap'
                                                    },
                                                    'content': [
                                                        {
                                                            'component': 'span',
                                                            'props': {
                                                                'class': 'text-h6'
                                                            },
                                                            'text': f'{"{:,}".format(total_seed)}'
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        },
                    ]
                },
                # 总做种体积
                {
                    'component': 'VCol',
                    'props': {
                        'cols': 6,
                        'md': 3
                    },
                    'content': [
                        {
                            'component': 'VCard',
                            'props': {
                                'variant': 'tonal',
                            },
                            'content': [
                                {
                                    'component': 'VCardText',
                                    'props': {
                                        'class': 'd-flex align-center',
                                    },
                                    'content': [
                                        {
                                            'component': 'VAvatar',
                                            'props': {
                                                'rounded': True,
                                                'variant': 'text',
                                                'class': 'me-3'
                                            },
                                            'content': [
                                                {
                                                    'component': 'VImg',
                                                    'props': {
                                                        'src': '/plugin_icon/database.png'
                                                    }
                                                }
                                            ]
                                        },
                                        {
                                            'component': 'div',
                                            'content': [
                                                {
                                                    'component': 'span',
                                                    'props': {
                                                        'class': 'text-caption'
                                                    },
                                                    'text': '总做种体积'
                                                },
                                                {
                                                    'component': 'div',
                                                    'props': {
                                                        'class': 'd-flex align-center flex-wrap'
                                                    },
                                                    'content': [
                                                        {
                                                            'component': 'span',
                                                            'props': {
                                                                'class': 'text-h6'
                                                            },
                                                            'text': OriechoSiteStatistic.__format_filesize(total_seed_size)
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        else:
            total_elements = []

        if dashboard in ["today", "all"]:
            # 计算增量数据集
            inc_data = {}
            site_labels = {identity(data): f"{data.name} · {data.domain}" for data in stattistic_data}
            for data in stattistic_data:
                # 修复：使用name进行匹配，保持一致性
                yesterday_datas = [yd for yd in yesterday_sites_data if identity(yd) == identity(data)]
                if yesterday_datas:
                    yesterday_data = yesterday_datas[0]
                    # 只有当有昨天数据时才计算增量
                    inc = __sub_data(data.to_dict(), yesterday_data.to_dict())
                    if inc:
                        inc_data[identity(data)] = inc
                # 如果没有昨天数据，不添加到inc_data中
            # 今日上传
            uploads = {k: v for k, v in inc_data.items() if v.get("upload") if v.get("upload") > 0}
            # 今日上传站点
            upload_sites = [('≈ ' if 'upload' in approximate_fields.get(site, set()) else '') + site_labels[site]
                            for site in uploads.keys()]
            # 今日上传数据
            upload_datas = [__gb(data.get("upload")) for data in uploads.values()]
            # 今日上传总量
            today_upload = round(sum(upload_datas), 2)
            # 今日下载
            downloads = {k: v for k, v in inc_data.items() if v.get("download") if v.get("download") > 0}
            # 今日下载站点
            download_sites = [('≈ ' if 'download' in approximate_fields.get(site, set()) else '') + site_labels[site]
                              for site in downloads.keys()]
            # 今日下载数据
            download_datas = [__gb(data.get("download")) for data in downloads.values()]
            # 今日下载总量
            today_download = round(sum(download_datas), 2)
            # 今日上传下载元素
            today_elements = [
                # 上传量图表
                {
                    'component': 'VCol',
                    'props': {
                        'cols': 12,
                        'md': 6
                    },
                    'content': [
                        {
                            'component': 'VApexChart',
                            'props': {
                                'height': 300,
                                'options': {
                                    'chart': {
                                        'type': 'pie',
                                    },
                                    'labels': upload_sites,
                                    'title': {
                                        'text': f'上传增量（{today}）共 {"≈ " if upload_approximate else ""}{today_upload} GB'
                                    },
                                    'legend': {
                                        'show': True
                                    },
                                    'plotOptions': {
                                        'pie': {
                                            'expandOnClick': False
                                        }
                                    },
                                    'noData': {
                                        'text': '暂无数据'
                                    }
                                },
                                'series': upload_datas
                            }
                        }
                    ]
                },
                # 下载量图表
                {
                    'component': 'VCol',
                    'props': {
                        'cols': 12,
                        'md': 6
                    },
                    'content': [
                        {
                            'component': 'VApexChart',
                            'props': {
                                'height': 300,
                                'options': {
                                    'chart': {
                                        'type': 'pie',
                                    },
                                    'labels': download_sites,
                                    'title': {
                                        'text': f'下载增量（{today}）共 {"≈ " if download_approximate else ""}{today_download} GB'
                                    },
                                    'legend': {
                                        'show': True
                                    },
                                    'plotOptions': {
                                        'pie': {
                                            'expandOnClick': False
                                        }
                                    },
                                    'noData': {
                                        'text': '暂无数据'
                                    }
                                },
                                'series': download_datas
                            }
                        }
                    ]
                }
            ]
        else:
            today_elements = []
        # 合并返回
        precision_elements = []
        if approximate_fields:
            previous_by_name = {identity(data): data for data in yesterday_sites_data}
            details = []
            for data in stattistic_data:
                for field, label in (('upload', '上传'), ('download', '下载')):
                    if field not in approximate_fields.get(identity(data), set()):
                        continue
                    previous = previous_by_name[identity(data)]
                    difference = int(getattr(data, field)) - int(getattr(previous, field))
                    details.append({
                        'component': 'div',
                        'text': f'{data.name} {label}（{previous.updated_day} → {data.updated_day}）：'
                                + OriechoSiteStatistic.__format_traffic(difference, True, True),
                    })
            precision_elements = [{
                'component': 'VCol',
                'props': {'cols': 12},
                'content': [{
                    'component': 'VAlert',
                    'props': {'type': 'info', 'variant': 'tonal', 'class': 'text-break'},
                    'content': [{'component': 'div', 'text': OriechoSiteStatistic._PRECISION_NOTE}] + details,
                }],
            }]
        return precision_elements + total_elements + today_elements

    def _get_monthly_data(self):
        """月统计只读宿主快照，不主动刷新站点或操作下载器。"""
        return collect_month(SiteOper(), settings.TZ)

    @staticmethod
    def _month_coverage(data):
        actual_range = f"{data['start']} → {data['end']}" if data['start'] else "暂无可比较范围"
        return (f"{data['month']} · 实际范围：{actual_range} · "
                f"覆盖 {data['covered']}/{data['total']} 站 · "
                + ("部分统计" if data['partial'] else "已覆盖月初基线")
                + "；各站实际范围见明细，不推算缺失流量。")

    def _month_value(self, data, field):
        if data[field] is None:
            return "—（暂无可比较数据）"
        return self.__format_traffic(data[field], field in data['approximate'], True)

    @staticmethod
    def _traffic_bytes(value):
        # 与自然月共用 Decimal 解析，科学计数法字符串也保留整数字节精度。
        return traffic_bytes(value)

    def _daily_values(self, current_data, previous_data, today):
        """今日只比较同账号相邻两日的真实读数，缺失或计数下降保持未知。"""
        previous = {identity(data): data for data in previous_data if valid_traffic(data)}
        values = {}
        for current in current_data:
            account = identity(current)
            baseline = previous.get(account)
            if not account[1] or not valid_traffic(current) or not baseline:
                continue
            try:
                expected = (datetime.strptime(today, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
            except (TypeError, ValueError):
                continue
            if current.updated_day != today or baseline.updated_day != expected:
                continue
            differences = {field: self._traffic_bytes(getattr(current, field))
                            - self._traffic_bytes(getattr(baseline, field))
                           for field in ('upload', 'download')}
            if any(value < 0 for value in differences.values()):
                continue
            values[account] = dict(differences, approximate=approximate_fields(baseline, current))
        return values

    @staticmethod
    def _summary_group(title, metrics, note):
        return {
            'component': 'VCol', 'props': {'cols': 12, 'md': 4, 'style': 'min-width: 0;'},
            'content': [{'component': 'VCard', 'props': {'variant': 'tonal', 'class': 'h-100'}, 'content': [
                {'component': 'VCardTitle', 'text': title},
                {'component': 'VCardText', 'content': [
                    {'component': 'VRow', 'content': [
                        {'component': 'VCol', 'props': {'cols': 6, 'style': 'min-width: 0;'}, 'content': [
                            {'component': 'div', 'props': {'class': 'text-caption'}, 'text': label},
                            {'component': 'div', 'props': {'class': 'text-h6 text-break'}, 'text': value},
                        ]} for label, value in metrics
                    ]},
                    {'component': 'div', 'props': {'class': 'text-caption text-break mt-3'}, 'text': note},
                ]},
            ]}],
        }

    def _summary_elements(self, latest_day, current, previous, monthly, include_precision=True, daily_errors=()):
        """详情和全量仪表板共用固定顺序，缺数据不把未知伪装成零。"""
        approximate = self.__get_approximate_fields(current, previous)
        total_metrics = []
        for field, label in (('upload', '总上传量'), ('download', '总下载量')):
            value = sum(self._traffic_bytes(getattr(row, field)) for row in current) if current else None
            uncertain = any(field in fields for fields in approximate.values())
            total_metrics.append((label, self.__format_traffic(value, uncertain) if value is not None else '—'))
        for field, label in (('seeding', '总做种数'), ('seeding_size', '总做种体积')):
            observed = [getattr(row, field, None) for row in current
                        if getattr(row, field, None) is not None]
            value = sum(observed) if observed else None
            total_metrics.append((label, ('—' if value is None else
                                         self.__format_filesize(value) if field == 'seeding_size' else str(value))))
        daily = self._daily_values(current, previous, monthly['today'])
        daily_metrics = []
        for field, label in (('upload', '今日上传'), ('download', '今日下载')):
            value = sum(row[field] for row in daily.values()) if daily else None
            uncertain = any(field in row['approximate'] for row in daily.values())
            daily_metrics.append((label, self.__format_traffic(value, uncertain, True)
                                  if value is not None else '—（暂无可比较数据）'))
        daily_note = (f"{monthly['today']} · 可比较 {len(daily)}/{monthly['total']} 站；"
                      '仅对比同账号相邻两日，缺失或计数下降显示 —。')
        if daily_errors:
            daily_note += f"日基线读取失败：{'、'.join(daily_errors)}；相关站点今日增量未知。"
        groups = [
            self._summary_group('总计', total_metrics,
                                f"最新数据：{latest_day or '暂无最新站点数据'}；仅合计有效读数。"),
            self._summary_group('本月', [(label, self._month_value(monthly, field))
                                        for field, label in (('upload', '本月上传'), ('download', '本月下载'))],
                                self._month_coverage(monthly)),
            self._summary_group('今日', daily_metrics, daily_note),
        ]
        if include_precision and (monthly['approximate'] or approximate):
            groups.append({'component': 'VCol', 'props': {'cols': 12}, 'content': [
                {'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'class': 'text-break'},
                 'text': self._PRECISION_NOTE},
            ]})
        return groups

    def _read_latest_data(self):
        try:
            return self._get_snapshot_data()
        except Exception:
            logger.error('站点数据统计：读取最新站点快照失败，保留月统计与空状态。')
            return '读取失败', [], [], []

    def _send_month_notification(self):
        data = self._get_monthly_data()
        lines = [f"【月增量数据 · {data['month']}】", self._month_coverage(data),
                 f"总上传：{self._month_value(data, 'upload')}",
                 f"总下载：{self._month_value(data, 'download')}", '————————————']
        for row in data['rows']:
            lines.extend([f"【{row['name']} · {row['domain']}】",
                          f"实际范围：{row['start'] or '—'} → {row['end'] or '—'}；{row['status']}",
                          f"上传量：{self._month_value(row, 'upload')}",
                          f"下载量：{self._month_value(row, 'download')}", '————————————'])
        if data['approximate']:
            lines.append(self._PRECISION_NOTE)
        text = '\n'.join(lines)
        fingerprint = self.__get_notification_fingerprint(text)
        with lock:
            if not self._enabled or self._notify_type != 'month':
                return
            previous = self.get_data('last_notify') or {}
            if previous.get('type') == 'month' and previous.get('fingerprint') == fingerprint:
                return
            self.post_message(mtype=NotificationType.SiteMessage, title='站点数据统计', text=text)
            self.save_data('last_notify', {'type': 'month', 'fingerprint': fingerprint, 'month': data['month']})

    def get_dashboard(self, key: str, **kwargs) -> Optional[Tuple[Dict[str, Any], Dict[str, Any], List[dict]]]:
        """
        获取插件仪表盘页面，需要返回：1、仪表板col配置字典；2、仪表板页面元素配置json（含数据）；3、全局配置（自动刷新等）
        1、col配置参考：
        {
            "cols": 12, "md": 6
        }
        2、页面配置使用Vuetify组件拼装，参考：https://vuetifyjs.com/
        3、全局配置参考：
        {
            "refresh": 10 // 自动刷新时间，单位秒
        }
        """
        # 列配置
        cols = {
            "cols": 12
        }
        # 全局配置
        attrs = {}
        today, stattistic_data, yesterday_sites_data, daily_errors = self._read_latest_data()
        if self._dashboard_type in ('month', 'all'):
            content = self._summary_elements(today, stattistic_data, yesterday_sites_data, self._get_monthly_data(),
                                             daily_errors=daily_errors)
        else:
            content = self.__get_total_elements(today, stattistic_data, yesterday_sites_data, self._dashboard_type)
            if daily_errors:
                content.append({'component': 'VCol', 'props': {'cols': 12}, 'content': [
                    {'component': 'VAlert', 'props': {'type': 'warning', 'variant': 'tonal'},
                     'text': f"日基线读取失败：{'、'.join(daily_errors)}；已保留可用累计读数。"},
                ]})
        return cols, attrs, [{'component': 'VRow', 'content': content}]

    def get_page(self) -> List[dict]:
        """
        拼装插件详情页面，需要返回页面配置，同时附带数据
        """

        def format_bonus(bonus):
            """将魔力值格式化为一位小数，空值或非法值显示为零。"""
            try:
                return f'{float(bonus):,.1f}'
            except (TypeError, ValueError):
                return '0.0'

        monthly = self._get_monthly_data()
        today, stattistic_data, yesterday_sites_data, daily_errors = self._read_latest_data()
        precision = self.__get_approximate_fields(stattistic_data, yesterday_sites_data)
        site_totals = self._summary_elements(today, stattistic_data, yesterday_sites_data, monthly,
                                             include_precision=not precision, daily_errors=daily_errors)
        # 保留原有日增量图；所有站点只在最后的合并表格出现一次。
        site_totals += self.__get_total_elements(today, stattistic_data, yesterday_sites_data, 'today')
        if not stattistic_data:
            site_totals.append({
                'component': 'VCol', 'props': {'cols': 12},
                'content': [{'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal'},
                             'text': '暂无最新站点数据；月统计仅使用已保存的每日快照。'
                                     + ('最新快照读取失败。' if today == '读取失败' else '')}],
            })
        table_headers = ['站点', '数据日期', '用户名', '用户等级', '上传量', '下载量',
                         '分享率', '魔力值', '做种数', '做种体积', '今日上传', '今日下载',
                         '本月上传', '本月下载', '本月统计起止', '月覆盖状态']
        header_row = {'component': 'thead', 'content': [{'component': 'tr', 'content': [
            {'component': 'th', 'props': {'class': 'text-start text-no-wrap ps-4', 'scope': 'col'}, 'text': label}
            for label in table_headers
        ]}]}
        daily = self._daily_values(stattistic_data, yesterday_sites_data, monthly['today'])
        month_by_domain = {row['domain']: row for row in monthly['rows']}
        current_by_domain = {identity(data)[0]: data for data in stattistic_data}
        domains = list(dict.fromkeys([identity(data)[0] for data in stattistic_data] + list(month_by_domain)))
        table_rows = []
        for domain in domains:
            data, month = current_by_domain.get(domain), month_by_domain.get(domain)
            account = identity(data) if data else None
            approximate = precision.get(account, set())
            if data:
                cells = [f'{data.name} · {domain}', data.updated_day, data.username, data.user_level,
                         self.__format_traffic(data.upload, 'upload' in approximate),
                         self.__format_traffic(data.download, 'download' in approximate), data.ratio,
                         format_bonus(data.bonus) if data.bonus is not None else '—',
                         data.seeding if data.seeding is not None else '—',
                         self.__format_filesize(data.seeding_size) if data.seeding_size is not None else '—']
            else:
                cells = [f"{month['name']} · {domain}"] + ['—'] * 9
            day = daily.get(account)
            cells += [self.__format_traffic(day[field], field in day['approximate'], True) if day else '—'
                      for field in ('upload', 'download')]
            # 月快照属于别的账号时，不能按域名把数值接到当前账号。
            mismatch = bool(data and month and month['userid'] and account[1] != month['userid'])
            if month and not mismatch:
                cells += [self._month_value(month, field) if month[field] is not None else '—'
                          for field in ('upload', 'download')]
                cells += [f"{month['start'] or '—'} → {month['end'] or '—'}", month['status']]
            else:
                cells += ['—', '—', '—', '无法比较：账号变化' if mismatch else '暂无本月有效快照']
            table_rows.append({'component': 'tr', 'content': [
                {'component': 'td', 'props': {'class': 'text-no-wrap'}, 'text': value} for value in cells
            ]})
        if not table_rows:
            table_rows = [{'component': 'tr', 'content': [
                {'component': 'td', 'props': {'colspan': len(table_headers)}, 'text': '暂无启用站点或本月快照'},
            ]}]
        table = {
            'component': 'VCol', 'props': {'cols': 12, 'style': 'min-width: 0; max-width: 100%;'},
            'content': [{'component': 'div',
                         'props': {'style': 'max-width: 100%; overflow-x: auto;', 'tabindex': 0,
                                   'role': 'region', 'aria-label': '所有站点统计明细，可横向滚动'},
                         'content': [{'component': 'VTable',
                                      'props': {'hover': True, 'style': 'min-width: max-content;'},
                                      'content': [header_row, {'component': 'tbody', 'content': table_rows}]}]}],
        }
        return [{'component': 'VRow', 'props': {'style': 'min-width: 0;'}, 'content': site_totals + [table]}]

    def stop_service(self):
        """回收立即运行调度器，避免反复保存配置残留线程。"""
        scheduler, self._scheduler = self._scheduler, None
        if scheduler:
            scheduler.remove_all_jobs()
            if scheduler.running:
                scheduler.shutdown(wait=False)

    @staticmethod
    def refresh_by_domain(domain: str, apikey: str) -> schemas.Response:
        """
        刷新一个站点数据，可由API调用
        """
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        site_info = SitesHelper().get_indexer(domain)
        if site_info:
            site_data = SiteChain().refresh_userdata(site=site_info)
            if site_data:
                return schemas.Response(
                    success=True,
                    message=f"站点 {domain} 刷新成功",
                    data=site_data.dict()
                )
            return schemas.Response(
                success=False,
                message=f"站点 {domain} 刷新数据失败，未获取到数据"
            )
        return schemas.Response(
            success=False,
            message=f"站点 {domain} 不存在"
        )
