import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
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
    plugin_name = "空文件夹清理"
    # 插件描述
    plugin_desc = "定期清理指定目录下的空文件夹，支持递归清理。"
    # 插件图标
    plugin_icon = "clean.png"
    # 插件版本
    plugin_version = "1.0"
    # 插件作者
    plugin_author = "Assistant"
    # 作者主页
    author_url = "https://github.com/assistant"
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
    # 退出事件
    _event = Event()

    def init_plugin(self, config: dict = None):
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

        # 停止现有任务
        self.stop_service()

        # 启动定时任务 & 立即运行一次
        if self.get_state() or self._onlyonce:
            if not self.__validate_config():
                self._enabled = False
                self._onlyonce = False
                self.update_config({
                    "enabled": self._enabled,
                    "onlyonce": self._onlyonce,
                    "cron": self._cron,
                    "notify": self._notify,
                    "target_dirs": self._target_dirs,
                    "recursive": self._recursive,
                    "exclude_dirs": self._exclude_dirs,
                    "dry_run": self._dry_run
                })
                return

            # 定时服务
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            if self._onlyonce:
                logger.info(f"空文件夹清理服务启动，立即运行一次")
                self._scheduler.add_job(self.clean_empty_folders, 'date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(
                                            seconds=3))
                # 关闭一次性开关
                self._onlyonce = False
                self.update_config({
                    "enabled": self._enabled,
                    "onlyonce": self._onlyonce,
                    "cron": self._cron,
                    "notify": self._notify,
                    "target_dirs": self._target_dirs,
                    "recursive": self._recursive,
                    "exclude_dirs": self._exclude_dirs,
                    "dry_run": self._dry_run
                })

            # 启动服务
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def get_state(self):
        return True if self._enabled and self._cron and self._target_dirs else False

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        """
        if self.get_state():
            return [
                {
                    "id": "EmptyFolderCleaner",
                    "name": "空文件夹清理服务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.clean_empty_folders,
                    "kwargs": {}
                }
            ]
        return []

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
                                            'label': '执行周期',
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
                                            'label': '模拟运行',
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
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'target_dirs',
                                            'label': '清理目录',
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
                                            'label': '排除目录',
                                            'rows': 3,
                                            'placeholder': '每一行一个目录路径，这些目录不会被删除\n例如：\n.git\n.svn\nnode_modules'
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
            "notify": False,
            "onlyonce": False,
            "cron": "0 2 * * *",
            "target_dirs": "",
            "recursive": True,
            "exclude_dirs": ".git\n.svn\nnode_modules\n.DS_Store\nThumbs.db",
            "dry_run": False
        }

    def get_page(self) -> List[dict]:
        """
        拼装插件详情页面，需要返回页面配置，同时附带数据
        """
        # 查询清理历史
        history = self.get_data('history') or []
        if not history:
            return [
                {
                    'component': 'div',
                    'text': '暂无清理历史',
                    'props': {
                        'class': 'text-center',
                    }
                }
            ]
        
        # 数据按时间降序排序
        history = sorted(history, key=lambda x: x.get('clean_time', ''), reverse=True)
        
        # 拼装页面
        contents = []
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
                    'class': 'grid gap-3 grid-info-card',
                },
                'content': contents
            }
        ]

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

    def __is_empty_folder(self, folder_path: Path) -> bool:
        """
        检查文件夹是否为空
        """
        try:
            # 检查文件夹是否存在
            if not folder_path.exists() or not folder_path.is_dir():
                return False
            
            # 获取文件夹内容
            items = list(folder_path.iterdir())
            
            # 如果没有任何内容，则为空文件夹
            if not items:
                return True
            
            # 如果开启递归模式，检查是否只包含空文件夹
            if self._recursive:
                for item in items:
                    if item.is_file():
                        return False
                    elif item.is_dir() and not self.__is_empty_folder(item):
                        return False
                return True
            
            return False
        except (OSError, PermissionError) as e:
            logger.warning(f"检查文件夹 {folder_path} 时出错：{str(e)}")
            return False

    def __remove_empty_folders(self, root_path: Path) -> Tuple[int, List[str]]:
        """
        递归删除空文件夹
        返回: (删除数量, 删除的文件夹列表)
        """
        removed_count = 0
        removed_folders = []
        
        try:
            # 如果根路径不存在，直接返回
            if not root_path.exists():
                return removed_count, removed_folders
            
            # 遍历所有子目录，从最深层开始
            all_dirs = []
            for dirpath, dirnames, filenames in os.walk(root_path, topdown=False):
                dir_path = Path(dirpath)
                if dir_path != root_path:  # 不删除根目录本身
                    all_dirs.append(dir_path)
            
            # 按深度排序，先处理最深的目录
            all_dirs.sort(key=lambda x: len(x.parts), reverse=True)
            
            for dir_path in all_dirs:
                if self._event.is_set():
                    logger.info("空文件夹清理服务停止")
                    break
                
                # 检查是否在排除列表中
                if self.__is_excluded(dir_path):
                    logger.debug(f"跳过排除目录：{dir_path}")
                    continue
                
                # 检查是否为空文件夹
                if self.__is_empty_folder(dir_path):
                    try:
                        if self._dry_run:
                            logger.info(f"[模拟] 将删除空文件夹：{dir_path}")
                            removed_folders.append(str(dir_path))
                            removed_count += 1
                        else:
                            logger.info(f"删除空文件夹：{dir_path}")
                            shutil.rmtree(dir_path)
                            removed_folders.append(str(dir_path))
                            removed_count += 1
                    except (OSError, PermissionError) as e:
                        logger.error(f"删除文件夹 {dir_path} 失败：{str(e)}")
        
        except Exception as e:
            logger.error(f"清理过程中出错：{str(e)}")
        
        return removed_count, removed_folders

    def clean_empty_folders(self):
        """
        开始清理空文件夹
        """
        logger.info("开始清理空文件夹 ...")
        
        if not self.__validate_config():
            return
        
        total_removed = 0
        all_removed_folders = []
        
        # 处理每个目标目录
        target_directories = [dir_path.strip() for dir_path in self._target_dirs.split('\n') if dir_path.strip()]
        
        for target_dir in target_directories:
            if self._event.is_set():
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
            removed_count, removed_folders = self.__remove_empty_folders(target_path)
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

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            logger.error(f"停止服务时出错：{str(e)}")
