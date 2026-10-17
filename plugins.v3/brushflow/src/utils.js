export const taskDefaults = {
  id: '',
  name: '',
  enabled: true,
  use_global_defaults: true,
  override_fields: [],
  novice: false,
  novice_until: null,
  notify: true,
  site_id: null,
  downloader: '',
  brush_interval: 10,
  check_interval: 5,
  rotation_hours: 24,
  cron: null,
  active_time_range: null,
  site_ratio_target: 2,
  disksize: null,
  freeleech: 'free',
  hr: 'yes',
  include: null,
  exclude: null,
  size: null,
  seeder: '1-50',
  leecher: '1',
  min_peer_seed_ratio: 0.1,
  promotion_lead_minutes: 10,
  timezone_offset: 0,
  pubtime: null,
  seed_time: null,
  hr_seed_time: null,
  seed_ratio: null,
  seed_size: null,
  download_time: null,
  download_min_speed: null,
  download_slow_minutes: null,
  seed_avgspeed: null,
  seed_inactivetime: null,
  delete_size_range: null,
  auto_archive_days: null,
  save_path: null,
  delete_except_tags: null,
  except_subscribe: true,
  proxy_delete: false,
  del_no_free: true,
  qb_category: null,
  site_hr_active: false,
  site_skip_tips: false,
  rss_support: false,
  tag: null,
}

export const siteFields = ['id', 'name', 'enabled', 'site_id', 'novice', 'novice_until', 'timezone_offset', 'site_hr_active', 'site_skip_tips', 'tag', 'rss_support']
export const sharedFields = Object.keys(taskDefaults).filter(key => !siteFields.includes(key) && !['use_global_defaults', 'override_fields'].includes(key))
const retiredFields = ['site_ratio_control', 'maxdlcount', 'maxupspeed', 'maxdlspeed', 'up_speed', 'dl_speed', 'global_maxdlcount', 'global_maxupspeed', 'global_maxdlspeed']
const nullableNumbers = ['disksize', 'site_ratio_target', 'seed_time', 'hr_seed_time', 'seed_ratio', 'seed_size', 'download_time', 'download_min_speed', 'download_slow_minutes', 'seed_avgspeed', 'seed_inactivetime', 'auto_archive_days']

export function taskOverrideFields(task = {}) {
  return sharedFields.filter(key => task[key] != null && (key === 'freeleech' || typeof task[key] !== 'string' || task[key].trim() !== ''))
}

export function editableTask(task = {}) {
  const result = cloneTask(task)
  sharedFields.forEach(key => {
    if (result.use_global_defaults && !result.override_fields.includes(key)) result[key] = null
    // 数字不限覆盖在后端存为 null，表单用 0 与继承区分。
    else if (key !== 'site_ratio_target' && nullableNumbers.includes(key) && result[key] == null) result[key] = 0
  })
  result.use_global_defaults = true
  result.override_fields = taskOverrideFields(result)
  return result
}

export function effectiveTask(task = {}, defaults = {}) {
  const result = cloneTask(task)
  if (result.use_global_defaults) {
    const globalValues = { ...taskDefaults, ...defaults }
    sharedFields.forEach(key => {
      if (!result.override_fields.includes(key)) result[key] = globalValues[key]
    })
  }
  return result
}

/** 统一提取宿主 API 客户端与标准响应模型中的业务数据。 */
export function unwrapResponse(response) {
  if (response && Object.prototype.hasOwnProperty.call(response, 'success')) {
    if (response.success === false) throw new Error(response.message || '操作失败')
    return response.data
  }
  return response?.data ?? response
}

/** 基于完整默认值创建可安全编辑的任务深拷贝。 */
export function cloneTask(task = {}) {
  const result = { ...taskDefaults, ...(task || {}) }
  result.use_global_defaults = task.use_global_defaults ?? !task.id
  result.override_fields = (Array.isArray(task.override_fields) ? task.override_fields : []).filter(key => sharedFields.includes(key))
  retiredFields.forEach(key => delete result[key])
  return JSON.parse(JSON.stringify(result))
}

/** 把表单空值和数字字段标准化为后端请求模型需要的类型。 */
export function normalizeTask(task) {
  const result = cloneTask(task)
  const optionalText = [
    'cron',
    'active_time_range',
    'include',
    'exclude',
    'size',
    'seeder',
    'leecher',
    'pubtime',
    'delete_size_range',
    'save_path',
    'delete_except_tags',
    'qb_category',
    'tag',
    'novice_until',
  ]
  nullableNumbers.forEach(key => {
    const value = result[key] === '' || result[key] === null ? null : Number(result[key])
    result[key] = value === 0 ? null : value
  })
  optionalText.forEach(key => {
    result[key] = String(result[key] || '').trim() || null
  })
  result.site_id = Number(result.site_id)
  result.brush_interval = Number(result.brush_interval || 10)
  result.check_interval = Number(result.check_interval || 5)
  result.rotation_hours = Number(result.rotation_hours ?? 24)
  result.timezone_offset = Number(result.timezone_offset || 0)
  result.min_peer_seed_ratio = result.min_peer_seed_ratio === '' || result.min_peer_seed_ratio == null ? 0 : Number(result.min_peer_seed_ratio)
  result.promotion_lead_minutes = Number(result.promotion_lead_minutes ?? 10)
  if (result.novice_until) result.novice_until = new Date(result.novice_until).toISOString()
  return result
}

export function normalizeDefaults(defaults = {}) {
  const normalized = normalizeTask({ ...taskDefaults, ...defaults })
  return Object.fromEntries(sharedFields.map(key => [key, normalized[key]]))
}

/** 把全局设置中的空值、零值和正数标准化为后端请求类型。 */
export function normalizeSettings(settings = {}) {
  const result = { ...(settings || {}) }
  result.rotation_mode = result.rotation_mode === 'sequential' ? 'sequential' : 'parallel'
  const manualFocusSiteId = ['number', 'string'].includes(typeof result.manual_focus_site_id) ? Number(result.manual_focus_site_id) : NaN
  result.manual_focus_site_id = Number.isSafeInteger(manualFocusSiteId) && manualFocusSiteId > 0 ? manualFocusSiteId : null
  const limitFields = ['global_disksize']
  limitFields.forEach(key => {
    const value = Number(result[key] || 0)
    result[key] = value > 0 ? value : null
  })
  result.global_proxy_delete = Boolean(result.global_proxy_delete)
  result.global_delete_size_range = result.global_proxy_delete
    ? String(result.global_delete_size_range || '').trim() || null
    : null
  retiredFields.forEach(key => delete result[key])
  result.global_defaults = normalizeDefaults(result.global_defaults)
  return result
}

/** 将字节数格式化为适合紧凑界面展示的容量文本。 */
export function formatBytes(value) {
  const bytes = Number(value || 0)
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  const number = bytes / 1024 ** index
  return `${number >= 100 ? number.toFixed(0) : number.toFixed(1)} ${units[index]}`
}

/** 将时间值格式化为当前界面使用的月日与时分。 */
export function formatDateTime(value) {
  if (!value) return '暂无'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return String(value)
  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date)
}

/** 计算一次运行记录的秒级耗时。 */
export function formatDuration(startedAt, finishedAt) {
  if (!startedAt || !finishedAt) return '-'
  const seconds = Math.max(Math.round((new Date(finishedAt) - new Date(startedAt)) / 1000), 0)
  return `${seconds} 秒`
}

/** 返回任务状态对应的中文文本、主题色和图标。 */
export function taskStateMeta(state, waitingRotation = false) {
  const states = {
    running: { text: '运行中', color: 'success', icon: 'mdi-check-circle-outline' },
    brush: { text: '正在刷新', color: 'primary', icon: 'mdi-sync' },
    check: { text: '正在检查', color: 'info', icon: 'mdi-progress-check' },
    paused: { text: '管理已停用', color: 'secondary', icon: 'mdi-stop-circle-outline' },
    waiting: { text: '等待时段', color: 'warning', icon: 'mdi-clock-outline' },
    waiting_rotation: { text: '等待轮转', color: 'info', icon: 'mdi-clock-outline' },
    disabled: { text: '插件停用', color: 'secondary', icon: 'mdi-stop-circle-outline' },
    error: { text: '运行异常', color: 'error', icon: 'mdi-alert-circle-outline' },
  }
  return states[state === 'waiting' && waitingRotation ? 'waiting_rotation' : state] || states.running
}

export function formatRemainingTime(value) {
  const minutes = Math.max(0, Math.ceil(Number(value || 0) / 60))
  if (!minutes) return '即将切换'
  const hours = Math.floor(minutes / 60)
  const remaining = minutes % 60
  return hours ? `${hours} 小时${remaining ? ` ${remaining} 分钟` : ''}` : `${remaining} 分钟`
}

/** 根据已下载量和总大小计算种子完成百分比。 */
export function torrentProgress(item) {
  const size = Number(item?.size || 0)
  if (!size) return 0
  return Math.min(Math.round((Number(item.downloaded || 0) * 100) / size), 100)
}
