// Request ownership is independent from Vue so races can be verified directly.
export const defaults = Object.freeze({
  enabled: false, notify: false, recursive: true, cron: '0 2 * * *', target_dirs: '',
  exclude_dirs: '', dry_run: false, records_enabled: false, records_cron: '*/5 * * * *',
  records_dry_run: true, records_batch_size: 100, records_grace_hours: 24,
})
const clone = value => value == null ? value : JSON.parse(JSON.stringify(value))
const display = config => ({ ...clone(defaults), ...clone(config) })
export function unwrap(response) {
  const body = response?.data && typeof response.success !== 'boolean' && typeof response.data.success === 'boolean'
    ? response.data : response
  if (body?.success === false) {
    const error = new Error(body.message || '请求失败，请重试')
    error.data = body.data
    throw error
  }
  return typeof body?.success === 'boolean' ? body.data : body?.data ?? body
}
export function createCleanupState() {
  return {
    view: 'overview', status: null, statusLoading: false, statusError: '', actionBusy: false, actionNotice: '',
    settings: { open: false, loading: false, ready: false, saving: false, baseline: null, draft: null, revision: '', error: '', notice: '' },
    detail: { kind: 'records', loading: false, error: '', items: [], total: 0, page: 1, page_size: 20, snapshot_id: '', scope: 'latest_batch', expanded: '' },
    history: { kind: 'folders', loading: false, error: '', items: [], total: 0, page: 1, page_size: 20 },
  }
}
export function createCleanupController(api, state) {
  const base = 'plugin/EmptyFolderCleaner'
  const epochs = { status: 0, settings: 0, detail: 0, history: 0 }
  const requests = new Set()
  const requestSlots = new Map()
  let alive = true, action = null, retryRun = null
  async function request(method, path, payload) {
    const abort = new AbortController()
    const slot = `${method}:${path.split('?')[0]}`
    requestSlots.get(slot)?.abort()
    requestSlots.set(slot, abort)
    requests.add(abort)
    let timer, abortListener
    try {
      const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => { abort.abort(); reject(new Error('请求超时，请重试')) }, 15000)
      })
      const operation = method === 'get' ? api.get(path, { signal: abort.signal }) : api.post(path, payload, { signal: abort.signal })
      const cancelled = new Promise((_, reject) => {
        abortListener = () => reject(new Error('请求已取消'))
        abort.signal.addEventListener('abort', abortListener, { once: true })
      })
      return unwrap(await Promise.race([operation, timeout, cancelled]))
    } finally {
      clearTimeout(timer); requests.delete(abort)
      if (requestSlots.get(slot) === abort) requestSlots.delete(slot)
      if (abortListener) abort.signal.removeEventListener('abort', abortListener)
    }
  }
  const current = (kind, id) => alive && epochs[kind] === id
  const controller = {
    async loadStatus() {
      const id = ++epochs.status
      state.statusLoading = !state.status
      try {
        const result = await request('get', `${base}/status`)
        if (current('status', id)) { state.status = result; state.statusError = '' }
      } catch (_) {
        if (current('status', id)) state.statusError = '统计读取失败，请重试'
      } finally { if (current('status', id)) state.statusLoading = false }
    },
    async openSettings() {
      state.view = 'settings'; state.settings.open = true
      if (!state.settings.ready) await controller.loadSettings()
    },
    async loadSettings({ preserveDraft = true } = {}) {
      if (!state.settings.open || state.settings.saving) return
      const id = ++epochs.settings, form = state.settings
      const previous = preserveDraft && form.ready ? display(form.baseline) : null
      form.loading = true; form.error = ''; form.notice = ''
      try {
        const data = await request('get', `${base}/settings`)
        if (!current('settings', id) || !form.open) return
        if (!data?.config || typeof data.revision !== 'string') throw new Error('设置读取不完整，请重试')
        const dirty = previous ? Object.fromEntries(Object.keys(defaults)
          .filter(key => JSON.stringify(form.draft[key]) !== JSON.stringify(previous[key]))
          .map(key => [key, clone(form.draft[key])])) : {}
        form.baseline = clone(data.config); form.draft = {...display(data.config),...dirty}; form.revision = data.revision; form.ready = true
        if (Object.keys(dirty).length) form.notice = '已读取最新配置并保留本地修改，请核对后再次保存'
      } catch (error) { if (current('settings', id) && form.open) form.error = error.message || '设置读取失败' }
      finally { if (current('settings', id)) form.loading = false }
    },
    closeSettings() {
      epochs.settings++
      requestSlots.get(`get:${base}/settings`)?.abort()
      requestSlots.get(`post:${base}/settings`)?.abort()
      Object.assign(state.settings, {open:false,loading:false,ready:false,saving:false,baseline:null,draft:null,revision:'',error:'',notice:''})
    },
    async saveSettings() {
      const form = state.settings
      if (!form.open || !form.ready || form.loading || form.saving) return
      const id = epochs.settings, sent = clone(form.draft), baseline = display(form.baseline)
      const changes = Object.fromEntries(Object.keys(defaults).filter(key => JSON.stringify(sent[key]) !== JSON.stringify(baseline[key])).map(key => [key, sent[key]]))
      if (!Object.keys(changes).length) { form.notice = '没有需要保存的修改'; return }
      form.saving = true; form.error = ''; form.notice = ''
      try {
        const data = await request('post', `${base}/settings`, {revision:form.revision,changes})
        if (!current('settings', id) || !form.open) return
        if (!data?.config || typeof data.revision !== 'string') throw new Error('保存结果不完整，请重新读取确认')
        const remaining = Object.fromEntries(Object.keys(defaults).filter(key => JSON.stringify(form.draft[key]) !== JSON.stringify(sent[key])).map(key => [key, clone(form.draft[key])]))
        form.baseline = clone(data.config); form.draft = {...display(data.config),...remaining}; form.revision = data.revision
        form.notice = Object.keys(remaining).length ? '已保存，后续修改仍保留在表单中' : '设置已保存'
        await controller.loadStatus()
      } catch (error) { if (current('settings', id) && form.open) form.error = error.message || '设置保存失败，修改内容已保留' }
      finally { if (current('settings', id)) form.saving = false }
    },
    async loadDetails(kind = state.detail.kind, page = 1, latest = false) {
      state.view = 'details'
      const target = state.detail, id = ++epochs.detail
      const snapshot = latest || kind !== target.kind ? '' : target.snapshot_id
      target.kind = kind; target.loading = true; target.error = ''
      try {
        const query = new URLSearchParams({kind,page:String(page),page_size:'20',snapshot_id:snapshot})
        const result = await request('get', `${base}/details?${query}`)
        if (current('detail', id)) Object.assign(target, result)
      } catch (error) { if (current('detail', id)) target.error = error.message || '详情读取失败' }
      finally { if (current('detail', id)) target.loading = false }
    },
    async loadHistory(kind = state.history.kind, page = 1) {
      state.view = 'history'
      const target = state.history, id = ++epochs.history
      target.kind = kind; target.loading = true; target.error = ''
      try {
        const result = await request('get', `${base}/history?${new URLSearchParams({kind,page:String(page),page_size:'20'})}`)
        if (current('history', id)) Object.assign(target, result)
      } catch (error) { if (current('history', id)) target.error = error.message || '历史读取失败' }
      finally { if (current('history', id)) target.loading = false }
    },
    run(scope) {
      if (action) return action
      state.actionBusy = true; state.actionNotice = ''
      if (!retryRun || retryRun.scope !== scope) retryRun = {scope,request_id:globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random()}`}
      action = (async () => {
        try {
          const result = await request('post', `${base}/run`, retryRun)
          retryRun = null
          if (alive) {
            state.actionNotice = !result?.reused ? '运行请求已接收'
              : result.status === 'failed' ? '该请求已运行但失败，可查看执行历史后重新核对'
              : result.status === 'interrupted' ? '上次请求已中断，可查看状态后重新核对'
              : ['queued','running','cancelling'].includes(result.status) ? '已复用正在运行的任务，未创建重复任务'
              : '该运行请求已有结果，未创建重复任务'
            await controller.loadStatus()
          }
        } catch (error) {
          if (error.data?.not_started === true) retryRun = null
          if (alive) state.actionNotice = error.message || '运行请求未确认，请重试'
        }
        finally { if (alive) state.actionBusy = false; action = null }
      })()
      return action
    },
    async cancel() {
      if (state.actionBusy || !state.status?.run?.run_id) return
      state.actionBusy = true
      try {
        await request('post', `${base}/cancel`, {run_id:state.status.run.run_id})
        if (alive) { state.actionNotice = '已请求停止，正在等待当前步骤结束'; await controller.loadStatus() }
      } catch (error) { if (alive) state.actionNotice = error.message || '停止请求未确认，请重试' }
      finally { if (alive) state.actionBusy = false }
    },
    destroy() {
      alive = false
      Object.keys(epochs).forEach(key => epochs[key]++)
      requests.forEach(item => item.abort()); requests.clear()
      controller.closeSettings()
    },
  }
  return controller
}
