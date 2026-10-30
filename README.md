# MoviePilot 个人插件 · oriecho

这是 [oriecho 独立维护的 MoviePilot 插件仓库](https://github.com/iu2dlwlrma/MoviePilot-Plugins)，保留上游许可和贡献署名。

仓库已清空历史发行包与清单中的版本历史。两个 v3 插件依赖正式 Release 安装，重新手动发布前无法从本仓库安装或更新；已安装的插件、配置和业务数据不受仓库重置影响。清理大师沿用 v2 清单和源码安装方式，没有独立的 Release 工作流。

| 插件 | ID | 版本 | 清单 |
| --- | --- | --- | --- |
| 站点数据统计 | `OriechoSiteStatistic` | `1.1.0` | `package.v3.json` |
| 站点刷流 | `OriechoBrushFlow` | `7.3.2` | `package.v3.json` |
| 清理大师 | `EmptyFolderCleaner` | `2.1.0` | `package.v2.json` |

## 清理大师：空目录与失效记录

原“空文件夹清理”沿用 `EmptyFolderCleaner` 身份、`emptyfoldercleaner_` 配置空间和已有历史。源码及前端位于 `plugins.v2/emptyfoldercleaner/`，安装清单为 `package.v2.json`；通过 MoviePilot 插件市场添加本个人仓库并更新，无需修改宿主、镜像或部署配置。

空目录清理保留目标目录、排除规则、递归、周期、通知、预览及单次执行。根目标不删除；排除项按目录名相等或完整路径包含匹配。递归开启时自底向上继续删除新变空的父目录，关闭时只删除扫描时原本为空的目录。删除使用 `rmdir` 并拒绝符号链接与路径替换。

记录与残留清理共用一组核对开关、周期、预览和观察时长，按具体路径和关联事实分开处理：

- 源路径缺失但库文件正常，或没有确定入库映射时，只精确清理失效下载文件行，保留正常库文件及仍使用的历史。整季范围和等价重复父历史不会因此阻断这项窄操作。
- 已知源与准确入库目标均缺失时，核对完整关联后清理相应历史和已完成且独占的残留下载任务。任务删除始终禁用下载器的数据删除。
- 源仍在且有成功的准确整理映射、全部关联库目标已缺失时，经过分离的稳定观察、签名和归属复核，才处理准确残留源与任务。活动任务、挂载异常、路径冲突、共享文件或不完整关联保留，并在详情解释原因。

不依赖 Emby 配置或删除事件，不按相似名称扩散清理，不修改订阅或触发补下载；历史 `records_reopen_subscription` 值仅为配置兼容保留。硬链接的另一处路径仍在时独立有效，不能凭源路径消失判断库文件损坏。删除文件行之前持久保存精确关联回执，使父历史在子行消失后仍有核对入口；状态保存、回读或条件更新失败时保留待恢复。

首页只显示空文件夹、失效记录、残留文件与任务的统计及运行状态。详情按批次快照分页；设置与执行历史分别进入，旧空目录历史继续可查。扫描状态区分本轮进度、最近完成轮次和本批结果，预览不计为实际删除。普通保存只合并修改项，保留真实旧值和未知字段，不执行单次清理；单次运行与取消使用明确按钮，重复请求不会创建并行清理作业。

已有配置按原值继续使用。观察时长是可配置策略，原有 24 小时不会在升级时重置；设置为零仍需至少两次分离核对。初次安装默认关闭记录核对、开启预览。可先查看详情中的真实保留原因，再决定是否调整设置。

维护与验收：在仓库根目录运行 `python3 -B -m unittest discover -s tests -p 'test_cleanup_master_*.py' -v` 与 `node --test tests/test_cleanup_master_frontend.mjs`；前端在 `plugins.v2/emptyfoldercleaner` 执行 `npm ci --ignore-scripts`、`npm run build`。`tests/runtime_host_db.py` 仅用于拥有宿主依赖的隔离测试环境，必须设置临时 `CONFIG_DIR` 和候选 `CLEANUP_PLUGIN_PATH`，不得连接生产数据库。发布包含该插件 `dist` 联邦产物，宿主通过原有受保护插件接口加载 `Page/Config`。

## 站点数据统计：自然月增量

“站点数据统计”详情顶部同时展示总计、本月、今日三组汇总，底部使用一个站点列表展示原有资料及今日、本月上传下载和月统计覆盖情况。“本月数据”和“所有数据”仪表板同时显示三组汇总，显式选择“今日数据”或“汇总数据”仍保留对应模式。适用于 MoviePilot 3.0.0 及以上版本。

按宿主时区计算自然月，读取现有每日快照，以前月最后一天到本月最新成功读数的差值计算流量，不新增站点抓取请求。缺月初快照时明确标为部分统计；仅一份快照、账号变化或计数下降时不伪造增量。PB 舍入读数保留近似提示。详细口径和维护说明见 [统计插件 README](plugins.v3/sitestatistic/README.md)。

正式 Release 重新发布后，在 MoviePilot 插件市场添加本仓库，选择作者为 `oriecho`、ID 为 `OriechoSiteStatistic` 的“站点数据统计”，在其设置中选择“本月数据”。它与官方 `SiteStatistic` 使用不同类名、配置和数据空间；安装不覆盖或迁移官方插件，不自动启用新插件。两者若同时启用通知可能各自推送，请按需要设置。

统计插件源码位于 `plugins.v3/sitestatistic/`。源码目录使用功能名称；内部插件 ID、API 和配置空间保持兼容，更新不会丢失既有配置。

本仓库的 v3 源码只保留 `brushflow/`、`sitestatistic/` 两个目录，`brushflow/` 为当前公共规则实现，旧版单站配置源码已移除。插件通过正式 Release 安装；源码目录与宿主按 ID 命名的运行目录不同，不使用原始文件列表回退安装。维护时应先确认相应版本 Release 已成功发布，自动化安装指定 `release_version`，发布包不可用时保留原运行版本。

## 站点刷流：统一规则与分页续扫

插件在一个“刷流设置”窗口中选择站点并配置公共规则，首次默认选择全部站点。所有站点共用下载器、选种和清理参数，分别保留任务与上传统计；支持多站并行与单站驻留。取消选择只停用任务，不删除既有种子、历史与分页预算。完整配置说明见 [插件 README](plugins.v3/brushflow/README.md)。

候选采用多页游标续扫：记录本轮进度，后续调度从待处理位置继续。每站 `browse` 种子列表调用至少间隔 30 秒，滚动 24 小时最多 600 次；等待间隔或预算恢复时保留进度，额度不足时结束本轮并留待后续续扫。页面内候选仍按原筛选规则处理。列表调用预算保存在插件数据中，重启后继续生效。

上述控制仅约束本插件的 `browse` 种子列表调用，RSS 路径沿用原有调度，不计入此预算。MoviePilot 的手动搜索、订阅和其他客户端拥有各自调用来源；总调用量仍需一起考虑。插件不修改宿主站点的全局搜索间隔。站点返回限流或失败时，应按实际提示等待恢复。

## 安装与迁移

正式 Release 重新发布后，在 MoviePilot 插件市场来源中添加本仓库，选择作者为 `oriecho`、ID 为 `OriechoBrushFlow` 的“站点刷流”。市场清单为根目录 [package.v3.json](package.v3.json)，源码为 `plugins.v3/brushflow/`，使用正式 Release 安装。

旧 `BrushFlow` 与新 `OriechoBrushFlow` 是不同的插件身份。直接安装新 ID 不会自动继承旧配置；迁移需要同时保全原任务、托管种子记录、统计和请求预算。迁移前停止旧插件的任务调度，保留旧数据，完成配置和数据核对后再决定启用哪些任务，避免两套插件同时操作同一批种子。

迁移时保留任务 ID、`task.<id>.<name>` 数据键、`source_budget.<site_id>` 请求预算、原下载器关联以及现有 `刷流-*` 标签。`brushflow_download_added` 事件名称继续兼容旧联动；新插件的 Python 类、API 路径、前端联邦模块和配置前缀使用独立身份。安装或迁移本身不应启动任务、移动下载文件或重新添加已有种子。

## 目录与文件

| 路径 | 用途 |
| --- | --- |
| `package.v2.json` | 清理大师市场清单 |
| `package.v3.json` | 两个 v3 插件的市场清单 |
| `package.json` | 保留为空的旧版清单 |
| `plugins.v2/emptyfoldercleaner/` | 清理大师源码与宿主适配 |
| `plugins.v3/brushflow/` | 站点刷流后端、Vue 前端及 `dist` 产物 |
| `plugins.v3/sitestatistic/` | 站点数据统计后端、月统计逻辑及原生页面 |
| `tests/` | 插件业务、身份、前端和发布校验 |
| `.github/workflows/` | 两个 v3 插件的手动发布工作流 |
| `icons/` | 插件图标资源 |
| `docs/V2_Plugin_Development.md` | v2 插件开发说明 |
| `LICENSE` | GNU GPLv3 许可证 |

## 开发与验证

仓库重置后的 Git 操作以新 `main` 为基线；移植旧工作区的未发布改动时只保留所需文件改动或挑选必要提交，不合并旧分支历史，也不使用镜像推送。

- 后端入口：`plugins.v3/brushflow/__init__.py`，主类 `OriechoBrushFlow`；源码目录按功能命名，安装后的运行目录由宿主按插件 ID 命名。
- 清单 ID、源码版本与发布包版本必须一致；插件使用站点功能，保留 `level=2`。
- Python 验证：在仓库根目录安装 Pydantic 2 后执行 `python3 -m unittest discover -s tests -v`。测试隔离真实站点、下载器与生产数据库。
- 前端身份验证：在仓库根目录执行 `node --test tests/*.mjs`，核对默认插件 API 身份及传入实例身份。
- 前端构建：在 `plugins.v3/brushflow` 执行 `npm ci`、`npm run build`；发布时保留 `dist` 产物。
- 更改功能时同步插件源码 `plugin_version`、对应清单的 `version`，并更新相应说明和测试；清单的 `history` 从当前独立维护阶段开始记录后续变更。

## 手动发布

推送提交和创建 Pull Request 均不会自动执行发布。维护者在 GitHub **Actions** 中选择工作流，点击 **Run workflow**，选择 `main` 后手动运行：

| 工作流 | 插件 | 标签 | 安装包 |
| --- | --- | --- | --- |
| [Plugin Release](.github/workflows/release.yml) | 站点刷流 | `OriechoBrushFlow_v<版本>` | `oriechobrushflow_v<版本>.zip` |
| [Site Statistic Plugin Release](.github/workflows/site-statistic-release.yml) | 站点数据统计 | `OriechoSiteStatistic_v<版本>` | `oriechositestatistic_v<版本>.zip` |

工作流先执行测试、身份和发布输入校验，成功后按对应市场清单版本打包。ZIP 分别以 `brushflow/`、`sitestatistic/` 为顶层目录，包含说明和许可证；刷流包还包含 `dist` 前端产物。宿主安装时剥离源码目录并装入原运行身份。已存在的标签和发行包不可覆盖，内容改变时须使用新版本号；已有标签缺少预期附件时工作流会停止，需先排查中断原因。

发布前核对清单、源码版本与待发布内容，确认远程 `main` 未被其他提交推进。发布后检查 Actions 结果、标签、Release 和对应 ZIP 均可用，再通过 MoviePilot 正常安装入口更新；自动化安装指定 `release_version`，发行包不可用时保留已有运行包。清理大师按 v2 清单与源码维护，不由上述两个工作流打包。

更多宿主插件开发规范见 [MoviePilot 官方插件开发说明](https://github.com/jxxghp/MoviePilot-Plugins/blob/main/README.md)。

## 来源与许可

本仓库由 `oriecho` 独立维护，包含基于已有插件扩展的实现。感谢 `jxxghp`、`InfinityPacer`、`Seed680`、`lightolly` 及既有贡献者；上游实现与来源可在 [MoviePilot 官方插件仓库](https://github.com/jxxghp/MoviePilot-Plugins) 查询。作者展示表示当前维护者，不改变上游贡献归属。

沿用仓库的 [GNU GPLv3 许可证](LICENSE)，保留许可证全文、插件内的许可文件和上游贡献署名。
