import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import vm from 'node:vm'
import * as utils from '../plugins.v3/brushflow/src/utils.js'

const source = await readFile(new URL('../plugins.v3/brushflow/src/components/BrushFlowWorkbench.vue', import.meta.url), 'utf8')
const script = source.match(/<script setup>([\s\S]*?)<\/script>/)[1]
  .replace(/^import[\s\S]*?from ['"][^'"]+['"]\s*$/gm, '')
const snapshot = () => ({
  enabled: true, show_sidebar_nav: false, rotation_mode: 'sequential', manual_focus_site_id: 10,
  global_disksize: 800, global_proxy_delete: true, global_delete_size_range: '600-750',
  global_defaults: { downloader: 'transmission', save_path: '/downloads', brush_interval: 10,
    check_interval: 5, site_ratio_target: 2, rotation_hours: 24, include: 'KEEP',
    exclude: 'SKIP', del_no_free: true, promotion_lead_minutes: 15 },
  selected_site_ids: [10], summary: {}, tasks: [{ id: 'stable', site_id: 10, enabled: true }],
  options: { sites: [{ title: 'A', value: 1 }, { title: 'B', value: 10 }],
    downloaders: [{ title: 'transmission', value: 'transmission' }] },
})

function workbench(api) {
  const context = { ...utils, console, URLSearchParams,
    ref: value => ({ value }), computed: getter => ({ get value() { return getter() } }),
    inject: () => null, nextTick: async () => {}, watch() {}, onMounted() {}, onUnmounted() {}, defineExpose() {},
    defineEmits: () => () => {},
    defineProps: fields => ({ ...Object.fromEntries(Object.entries(fields).map(([key, field]) =>
      [key, typeof field.default === 'function' ? field.default() : field.default])), api }),
  }
  vm.createContext(context)
  vm.runInContext(script, context)
  return expression => vm.runInContext(expression, context)
}

let failures = 0
async function test(label, run) {
  try { await run(); console.log(`PASS ${label}`) }
  catch (error) { failures++; console.error(`FAIL ${label}: ${error.message}`) }
}

await test('settings read returns exact selection and existing common values', async () => {
  assert.equal(typeof utils.settingsFromStatus, 'function', 'settings snapshot conversion is missing')
  const original = snapshot()
  const draft = utils.settingsFromStatus(original)
  assert.deepEqual(draft.selected_site_ids, [10])
  assert.equal(draft.global_defaults.include, 'KEEP')
  assert.equal(draft.global_defaults.save_path, '/downloads')
  assert.equal(draft.global_proxy_delete, true)
  assert.equal(draft.show_sidebar_nav, false)
  draft.global_defaults.include = 'changed'
  assert.equal(original.global_defaults.include, 'KEEP')
})

await test('initial empty task list selects all options while explicit empty stays empty', () => {
  assert.equal(typeof utils.settingsFromStatus, 'function', 'settings snapshot conversion is missing')
  const data = snapshot(); data.tasks = []; delete data.selected_site_ids
  assert.deepEqual(utils.settingsFromStatus(data).selected_site_ids, [1, 10])
  data.selected_site_ids = []
  assert.deepEqual(utils.settingsFromStatus(data).selected_site_ids, [])
})

await test('one save sends selection and all common settings atomically', async () => {
  const posts = []
  const api = { get: async () => ({ success: true, data: snapshot() }),
    post: async (path, body) => { posts.push({ path, body }); return { success: true, data: snapshot() } } }
  const run = workbench(api)
  assert.equal(run('typeof openSettings'), 'function', 'shared settings entry is missing')
  await run('openSettings()')
  await run('saveSettings({ ...settingsSnapshot.value, selected_site_ids: [1, 10] })')
  assert.equal(posts.length, 1)
  assert.equal(posts[0].path, 'plugin/OriechoBrushFlow/settings')
  assert.deepEqual(Array.from(posts[0].body.selected_site_ids), [1, 10])
  assert.equal(posts[0].body.global_defaults.include, 'KEEP')
  assert.equal(posts[0].body.manual_focus_site_id, 10)
  assert.equal(posts[0].body.global_delete_size_range, '600-750')
  assert.equal(run('settingsOpen.value'), false)
})

await test('read failure blocks save and successful retry permits it', async () => {
  let fail = true; let posts = 0
  const run = workbench({ get: async () => { if (fail) throw Error('read unavailable'); return { success: true, data: snapshot() } },
    post: async () => { posts++; return { success: true, data: snapshot() } } })
  assert.equal(run('typeof openSettings'), 'function', 'shared settings entry is missing')
  await run('openSettings()')
  assert.equal(run('settingsReady.value'), false)
  await run('saveSettings({})')
  assert.equal(posts, 0)
  fail = false
  await run('loadSettings()')
  assert.equal(run('settingsReady.value'), true)
})

await test('failed save leaves window and complete draft available', async () => {
  const run = workbench({ get: async () => ({ success: true, data: snapshot() }),
    post: async () => ({ success: false, message: 'busy, retry later' }) })
  assert.equal(run('typeof openSettings'), 'function', 'shared settings entry is missing')
  await run('openSettings()')
  await run('saveSettings(settingsSnapshot.value)')
  assert.equal(run('settingsOpen.value'), true)
  assert.equal(run('settingsSnapshot.value.global_defaults.include'), 'KEEP')
  assert.equal(run('settingsError.value'), 'busy, retry later')
})

await test('late settings response cannot refill a closed window', async () => {
  let release
  const run = workbench({ get: () => new Promise(resolve => { release = resolve }) })
  assert.equal(run('typeof openSettings'), 'function', 'shared settings entry is missing')
  const pending = run('openSettings()')
  run('closeSettings()')
  release({ success: true, data: snapshot() })
  await pending
  assert.equal(run('settingsOpen.value'), false)
  assert.equal(run('settingsReady.value'), false)
  assert.equal(run('settingsSnapshot.value'), null)
})

await test('close restores trigger focus after the dialog leave transition', async () => {
  const run = workbench({ get: async () => ({ success: true, data: snapshot() }) })
  assert.equal(run('typeof restoreSettingsFocus'), 'function', 'explicit focus restoration is missing')
  run('globalThis.focusCount = 0; settingsTrigger.value = { $el: { isConnected: true, focus() { focusCount++ } } }')
  await run('openSettings()')
  run('closeSettings()')
  await run('restoreSettingsFocus()')
  assert.equal(run('focusCount'), 0)
  await run('onSettingsClosed()')
  assert.equal(run('focusCount'), 1)
})

await test('save restores trigger focus once saving and leave transition finish', async () => {
  let finishReload; let readCount = 0; let markReloadStarted
  const reloadStarted = new Promise(resolve => { markReloadStarted = resolve })
  const run = workbench({
    get: async () => { readCount++; if (readCount === 2) return new Promise(resolve => { finishReload = resolve; markReloadStarted() }); return { success: true, data: snapshot() } },
    post: async () => ({ success: true, data: snapshot() }),
  })
  assert.equal(run('typeof restoreSettingsFocus'), 'function', 'explicit focus restoration is missing')
  run('globalThis.focusCount = 0; settingsTrigger.value = { $el: { isConnected: true, focus() { focusCount++ } } }')
  await run('openSettings()')
  const pending = run('saveSettings(settingsSnapshot.value)')
  await reloadStarted
  await run('onSettingsClosed()')
  assert.equal(run('focusCount'), 0)
  finishReload({ success: true, data: snapshot() })
  await pending
  assert.equal(run('focusCount'), 1)
})

await test('legacy downloader conflict is visible instead of ordinary stopped status', async () => {
  const reason = '原下载器仍有托管种子，已暂停新增下载并保留原下载器检查；处理完成后再保存公共设置'
  const data = snapshot()
  data.tasks[0] = { ...data.tasks[0], enabled: false, downloader_conflict: reason }
  const run = workbench({ get: async () => ({ success: true, data }) })
  await run('loadStatus({ loadDetail: false })')
  assert.equal(run('taskStatusText(tasks.value[0])'), reason)
  assert.equal(run('hasTaskError(tasks.value[0])'), true)
  assert.equal(run('selectedState.value.color'), 'warning')
})

await test('settings payload excludes per-site metadata and stale override controls', () => {
  const payload = utils.normalizeSettings({ ...snapshot(), selected_site_ids: [],
    global_defaults: { ...snapshot().global_defaults, tag: 'keep-on-task', rss_support: true,
      timezone_offset: 8, site_hr_active: true, site_skip_tips: true, novice: true },
    apply_defaults_to_existing: false, use_global_defaults: false, override_fields: ['downloader'] })
  assert.deepEqual(payload.selected_site_ids, [])
  for (const key of ['tag', 'rss_support', 'timezone_offset', 'site_hr_active', 'site_skip_tips', 'novice']) {
    assert.equal(Object.hasOwn(payload.global_defaults, key), false, key)
  }
  for (const key of ['tasks', 'options', 'apply_defaults_to_existing', 'use_global_defaults', 'override_fields']) {
    assert.equal(Object.hasOwn(payload, key), false, key)
  }
})

const dialogSource = await readFile(new URL('../plugins.v3/brushflow/src/components/BrushSettingsDialog.vue', import.meta.url), 'utf8')
const dialogScript = dialogSource.match(/<script setup>([\s\S]*?)<\/script>/)[1]
  .replace(/^import[\s\S]*?from ['"][^'"]+['"]\s*$/gm, '')
function dialog(overrides = {}) {
  const data = snapshot()
  const props = { modelValue: true, settings: utils.settingsFromStatus(data), sites: data.options.sites,
    downloaders: data.options.downloaders, ready: true, loading: false, saving: false, error: '', ...overrides }
  const events = []; const watchers = []
  const context = { ...utils, componentProps: props,
    ref: value => ({ value }), computed: getter => ({ get value() { return getter() } }),
    watch(getter, callback, options) { watchers.push(() => callback(getter())); if (options?.immediate) callback(getter()) },
    defineProps: () => props, defineEmits: () => (...event) => events.push(event),
  }
  vm.createContext(context); vm.runInContext(dialogScript, context)
  return { run: expression => vm.runInContext(expression, context), props, events,
    sync: () => watchers.forEach(watch => watch()) }
}

await test('clear selection removes manual focus while preserving common rules', () => {
  const form = dialog()
  form.run('selectSites([])')
  assert.deepEqual(Array.from(form.run('draft.value.selected_site_ids')), [])
  assert.equal(form.run('draft.value.manual_focus_site_id'), null)
  assert.equal(form.run('draft.value.global_defaults.include'), 'KEEP')
})

await test('settings window emits one complete payload without per-site edits', async () => {
  const form = dialog()
  form.run("draft.value.global_defaults.include = 'EDITED'; selectSites([1, 10])")
  await form.run('save()')
  assert.equal(form.events.length, 1)
  assert.equal(form.events[0][0], 'save')
  assert.equal(form.events[0][1].global_defaults.include, 'EDITED')
  assert.deepEqual(Array.from(form.events[0][1].selected_site_ids), [1, 10])
  assert.equal(form.props.settings.global_defaults.include, 'KEEP')
})

await test('unready window and unavailable selected site cannot save', async () => {
  const form = dialog({ ready: false })
  await form.run('save()')
  assert.equal(form.events.length, 0)
  form.props.ready = true
  form.run('selectSites([999])')
  await form.run('save()')
  assert.equal(form.events.length, 0)
  assert.ok(form.run('formError.value'))
})

await test('double save during validation emits only one request', async () => {
  const form = dialog()
  form.run('globalThis.finishValidation = null; formRef.value = { validate: () => new Promise(resolve => { finishValidation = resolve }) }')
  const first = form.run('save()')
  const firstResolver = form.run('finishValidation')
  const second = form.run('save()')
  const secondResolver = form.run('finishValidation')
  firstResolver({ valid: true }); secondResolver({ valid: true })
  await Promise.all([first, second])
  assert.equal(form.events.length, 1)
})

await test('validation finishing after close does not refill or submit the form', async () => {
  const form = dialog()
  form.run('globalThis.finishValidation = null; formRef.value = { validate: () => new Promise(resolve => { finishValidation = resolve }) }')
  const pending = form.run('save()')
  form.props.modelValue = false; form.sync()
  form.run('finishValidation({ valid: true })')
  await pending
  assert.equal(form.events.length, 0)
  assert.equal(form.run('draft.value'), null)
  assert.equal(form.run('formError.value'), '')
})

function focusFixture(form) {
  assert.equal(form.run('typeof trapDialogFocus'), 'function', 'dialog keyboard boundary handler is missing')
  const doc = { activeElement: null, defaultView: { getComputedStyle: node => ({ visibility: node.hidden ? 'hidden' : 'visible' }) } }
  const makeNode = (id, options = {}) => ({ id, tabIndex: 0, ...options,
    matches: () => Boolean(options.disabled), closest: () => options.expanded ? {} : null,
    getClientRects: () => options.hidden ? [] : [{}], focus() { doc.activeElement = this } })
  const first = makeNode('close'); const middle = makeNode('field'); const last = makeNode('save')
  const nodes = [makeNode('hidden', { hidden: true }), first, middle, makeNode('disabled', { disabled: true }), last]
  const card = { ownerDocument: doc, contains: node => nodes.includes(node), querySelectorAll: () => nodes }
  const press = (target, extra = {}) => {
    doc.activeElement = target
    const event = { key: 'Tab', target, currentTarget: card, defaultPrevented: false, shiftKey: false,
      preventDefault() { this.defaultPrevented = true }, ...extra }
    form.run('trapDialogFocus')(event)
    return event
  }
  return { doc, first, middle, last, press }
}

await test('keyboard loops only at the visible enabled dialog boundaries', () => {
  const fixture = focusFixture(dialog())
  assert.equal(fixture.press(fixture.last).defaultPrevented, true)
  assert.equal(fixture.doc.activeElement, fixture.first)
  assert.equal(fixture.press(fixture.first, { shiftKey: true }).defaultPrevented, true)
  assert.equal(fixture.doc.activeElement, fixture.last)
  assert.equal(fixture.press(fixture.middle).defaultPrevented, false)
})

await test('dialog boundary handler leaves autocomplete menu and handled events alone', () => {
  const form = dialog(); const fixture = focusFixture(form)
  fixture.last.closest = () => ({})
  assert.equal(fixture.press(fixture.last).defaultPrevented, false)
  fixture.last.closest = () => null
  const event = fixture.press(fixture.last, { defaultPrevented: true })
  assert.equal(event.defaultPrevented, true)
  assert.equal(fixture.doc.activeElement, fixture.last)
  assert.equal(fixture.press({ id: 'teleported-option' }).defaultPrevented, false)
  form.props.modelValue = false
  assert.equal(fixture.press(fixture.last).defaultPrevented, false)
})

process.exitCode = Number(failures > 0)
