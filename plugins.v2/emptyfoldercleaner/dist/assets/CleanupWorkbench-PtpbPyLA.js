import { importShared } from './__federation_fn_import-JrT3xvdd.js';

const {resolveComponent:_resolveComponent$1,openBlock:_openBlock$1,createBlock:_createBlock$1,createCommentVNode:_createCommentVNode$1,toDisplayString:_toDisplayString$1,createTextVNode:_createTextVNode$1,withCtx:_withCtx$1,createVNode:_createVNode$1,createElementBlock:_createElementBlock$1,createElementVNode:_createElementVNode$1,withModifiers:_withModifiers} = await importShared('vue');


const _hoisted_1$1 = { key: 2 };
const _hoisted_2$1 = { class: "cleanup-settings__switches" };
const _hoisted_3$1 = { class: "cleanup-settings__switches" };
const _hoisted_4$1 = { class: "cleanup-settings__switches" };
const _hoisted_5$1 = { class: "cleanup-settings__numbers" };
const _hoisted_6$1 = { class: "cleanup-hint" };
const _hoisted_7$1 = { class: "cleanup-settings__save" };
const _hoisted_8$1 = {
  role: "status",
  "aria-live": "polite"
};

const {nextTick: nextTick$1,ref: ref$1} = await importShared('vue');


const _sfc_main$1 = {
  __name: 'SettingsForm',
  props: { form: { type:Object, required:true } },
  emits: ['save','retry'],
  setup(__props, { emit: __emit }) {


const emit = __emit;
const root = ref$1(null), fields = ref$1(null);
const cronRule = value => !value || String(value).trim().split(/\s+/).length === 5 || '请填写五段 Cron 表达式，或留空仅手动运行';
const batchRule = value => Number.isInteger(value) && value >= 1 && value <= 100 || '请填写 1 到 100 的整数';
const hoursRule = value => Number.isInteger(value) && value >= 0 && value <= 720 || '请填写 0 到 720 的整数';
async function save() {
 const result=await fields.value?.validate();
 if(result?.valid) emit('save');
 else { await nextTick$1();root.value?.querySelector('.v-input--error input')?.focus(); }
}

return (_ctx, _cache) => {
  const _component_VProgressLinear = _resolveComponent$1("VProgressLinear");
  const _component_VBtn = _resolveComponent$1("VBtn");
  const _component_VAlert = _resolveComponent$1("VAlert");
  const _component_VSwitch = _resolveComponent$1("VSwitch");
  const _component_VTextarea = _resolveComponent$1("VTextarea");
  const _component_VTextField = _resolveComponent$1("VTextField");
  const _component_VForm = _resolveComponent$1("VForm");

  return (_openBlock$1(), _createElementBlock$1("section", {
    ref_key: "root",
    ref: root,
    class: "cleanup-settings",
    "aria-label": "清理设置"
  }, [
    (__props.form.loading)
      ? (_openBlock$1(), _createBlock$1(_component_VProgressLinear, {
          key: 0,
          indeterminate: "",
          "aria-label": "正在读取实际配置"
        }))
      : _createCommentVNode$1("", true),
    (__props.form.error&&!__props.form.ready)
      ? (_openBlock$1(), _createBlock$1(_component_VAlert, {
          key: 1,
          type: "error",
          variant: "tonal",
          role: "alert",
          class: "mb-4"
        }, {
          append: _withCtx$1(() => [
            _createVNode$1(_component_VBtn, {
              variant: "text",
              disabled: __props.form.loading||__props.form.saving,
              onClick: _cache[0] || (_cache[0] = $event => (emit('retry')))
            }, {
              default: _withCtx$1(() => [
                _createTextVNode$1(_toDisplayString$1(__props.form.ready?'保留修改并重新读取':'重新读取'), 1)
              ]),
              _: 1
            }, 8, ["disabled"])
          ]),
          default: _withCtx$1(() => [
            _createTextVNode$1(_toDisplayString$1(__props.form.error) + " ", 1)
          ]),
          _: 1
        }))
      : _createCommentVNode$1("", true),
    (!__props.form.ready && !__props.form.loading && !__props.form.error)
      ? (_openBlock$1(), _createElementBlock$1("p", _hoisted_1$1, "配置尚未读取。"))
      : _createCommentVNode$1("", true),
    (__props.form.ready && __props.form.draft)
      ? (_openBlock$1(), _createBlock$1(_component_VForm, {
          key: 3,
          ref_key: "fields",
          ref: fields,
          onSubmit: _withModifiers(save, ["prevent"])
        }, {
          default: _withCtx$1(() => [
            _createElementVNode$1("fieldset", null, [
              _cache[14] || (_cache[14] = _createElementVNode$1("legend", null, "自动运行与通知", -1)),
              _createElementVNode$1("div", _hoisted_2$1, [
                _createVNode$1(_component_VSwitch, {
                  modelValue: __props.form.draft.enabled,
                  "onUpdate:modelValue": _cache[1] || (_cache[1] = $event => ((__props.form.draft.enabled) = $event)),
                  label: "启用定时清理",
                  color: "primary",
                  "hide-details": ""
                }, null, 8, ["modelValue"]),
                _createVNode$1(_component_VSwitch, {
                  modelValue: __props.form.draft.notify,
                  "onUpdate:modelValue": _cache[2] || (_cache[2] = $event => ((__props.form.draft.notify) = $event)),
                  label: "空文件夹完成通知",
                  color: "primary",
                  "hide-details": ""
                }, null, 8, ["modelValue"])
              ]),
              _cache[15] || (_cache[15] = _createElementVNode$1("p", { class: "cleanup-hint" }, "关闭定时运行后，仍可查看统计、历史及手动执行。", -1))
            ]),
            _createElementVNode$1("fieldset", null, [
              _cache[16] || (_cache[16] = _createElementVNode$1("legend", null, "空文件夹", -1)),
              _createVNode$1(_component_VTextarea, {
                modelValue: __props.form.draft.target_dirs,
                "onUpdate:modelValue": _cache[3] || (_cache[3] = $event => ((__props.form.draft.target_dirs) = $event)),
                label: "扫描目录",
                rows: "3",
                "auto-grow": "",
                hint: "每行一个实际目录。保留目标根目录。",
                "persistent-hint": ""
              }, null, 8, ["modelValue"]),
              _createVNode$1(_component_VTextarea, {
                modelValue: __props.form.draft.exclude_dirs,
                "onUpdate:modelValue": _cache[4] || (_cache[4] = $event => ((__props.form.draft.exclude_dirs) = $event)),
                label: "排除目录",
                rows: "2",
                "auto-grow": "",
                hint: "每行一个目录名或路径片段，匹配的目录及其内容保留。",
                "persistent-hint": ""
              }, null, 8, ["modelValue"]),
              _createElementVNode$1("div", _hoisted_3$1, [
                _createVNode$1(_component_VSwitch, {
                  modelValue: __props.form.draft.recursive,
                  "onUpdate:modelValue": _cache[5] || (_cache[5] = $event => ((__props.form.draft.recursive) = $event)),
                  label: "递归清理新变空的父目录",
                  color: "primary",
                  "hide-details": ""
                }, null, 8, ["modelValue"]),
                _createVNode$1(_component_VSwitch, {
                  modelValue: __props.form.draft.dry_run,
                  "onUpdate:modelValue": _cache[6] || (_cache[6] = $event => ((__props.form.draft.dry_run) = $event)),
                  label: "空文件夹仅预览",
                  color: "primary",
                  "hide-details": ""
                }, null, 8, ["modelValue"])
              ]),
              _cache[17] || (_cache[17] = _createElementVNode$1("p", { class: "cleanup-hint" }, "关闭递归时，仅清理扫描时本来就空的目录；仍会扫描子目录。", -1)),
              _createVNode$1(_component_VTextField, {
                modelValue: __props.form.draft.cron,
                "onUpdate:modelValue": _cache[7] || (_cache[7] = $event => ((__props.form.draft.cron) = $event)),
                label: "空文件夹执行周期",
                rules: [cronRule],
                hint: "五段 Cron 表达式，留空仅手动运行。",
                "persistent-hint": ""
              }, null, 8, ["modelValue", "rules"])
            ]),
            _createElementVNode$1("fieldset", null, [
              _cache[18] || (_cache[18] = _createElementVNode$1("legend", null, "失效记录与残留文件", -1)),
              _cache[19] || (_cache[19] = _createElementVNode$1("p", { class: "cleanup-hint" }, "两项共用核对周期和预览设置。依据实际文件与准确关联处理，未配置媒体服务器仍可核对。有效版本、共享文件、下载或整理中的数据会保留。", -1)),
              _createElementVNode$1("div", _hoisted_4$1, [
                _createVNode$1(_component_VSwitch, {
                  modelValue: __props.form.draft.records_enabled,
                  "onUpdate:modelValue": _cache[8] || (_cache[8] = $event => ((__props.form.draft.records_enabled) = $event)),
                  label: "自动核对记录与残留",
                  color: "primary",
                  "hide-details": ""
                }, null, 8, ["modelValue"]),
                _createVNode$1(_component_VSwitch, {
                  modelValue: __props.form.draft.records_dry_run,
                  "onUpdate:modelValue": _cache[9] || (_cache[9] = $event => ((__props.form.draft.records_dry_run) = $event)),
                  label: "记录与残留仅预览",
                  color: "primary",
                  "hide-details": ""
                }, null, 8, ["modelValue"])
              ]),
              _createVNode$1(_component_VTextField, {
                modelValue: __props.form.draft.records_cron,
                "onUpdate:modelValue": _cache[10] || (_cache[10] = $event => ((__props.form.draft.records_cron) = $event)),
                label: "核对周期",
                rules: [cronRule],
                hint: "五段 Cron 表达式，留空仅手动运行。",
                "persistent-hint": ""
              }, null, 8, ["modelValue", "rules"]),
              _createElementVNode$1("div", _hoisted_5$1, [
                _createVNode$1(_component_VTextField, {
                  modelValue: __props.form.draft.records_grace_hours,
                  "onUpdate:modelValue": _cache[11] || (_cache[11] = $event => ((__props.form.draft.records_grace_hours) = $event)),
                  modelModifiers: { number: true },
                  label: "缺失确认时间（小时）",
                  type: "number",
                  min: "0",
                  max: "720",
                  rules: [hoursRule],
                  hint: "缺失和归属需要在执行前再次核实。",
                  "persistent-hint": ""
                }, null, 8, ["modelValue", "rules"]),
                _createVNode$1(_component_VTextField, {
                  modelValue: __props.form.draft.records_batch_size,
                  "onUpdate:modelValue": _cache[12] || (_cache[12] = $event => ((__props.form.draft.records_batch_size) = $event)),
                  modelModifiers: { number: true },
                  label: "每批最多核对条目",
                  type: "number",
                  min: "1",
                  max: "100",
                  rules: [batchRule]
                }, null, 8, ["modelValue", "rules"])
              ]),
              _createElementVNode$1("p", _hoisted_6$1, "清理不会修改订阅进度或触发补下载。旧“更新订阅缺集进度”配置原值为" + _toDisplayString$1(__props.form.draft.records_reopen_subscription === true ? '开启' : '关闭') + "，仅保留兼容，不授予清理权限。", 1)
            ]),
            (__props.form.error)
              ? (_openBlock$1(), _createBlock$1(_component_VAlert, {
                  key: 0,
                  type: "error",
                  variant: "tonal",
                  role: "alert",
                  class: "mb-4"
                }, {
                  append: _withCtx$1(() => [
                    _createVNode$1(_component_VBtn, {
                      variant: "text",
                      disabled: __props.form.loading||__props.form.saving,
                      onClick: _cache[13] || (_cache[13] = $event => (emit('retry')))
                    }, {
                      default: _withCtx$1(() => _cache[20] || (_cache[20] = [
                        _createTextVNode$1("保留修改并重新读取")
                      ])),
                      _: 1
                    }, 8, ["disabled"])
                  ]),
                  default: _withCtx$1(() => [
                    _createTextVNode$1(_toDisplayString$1(__props.form.error) + " ", 1)
                  ]),
                  _: 1
                }))
              : _createCommentVNode$1("", true),
            _createElementVNode$1("div", _hoisted_7$1, [
              _createVNode$1(_component_VBtn, {
                type: "submit",
                color: "primary",
                loading: __props.form.saving,
                disabled: __props.form.saving||__props.form.loading
              }, {
                default: _withCtx$1(() => _cache[21] || (_cache[21] = [
                  _createTextVNode$1("保存设置")
                ])),
                _: 1
              }, 8, ["loading", "disabled"]),
              _createElementVNode$1("span", _hoisted_8$1, _toDisplayString$1(__props.form.notice), 1)
            ])
          ]),
          _: 1
        }, 512))
      : _createCommentVNode$1("", true)
  ], 512))
}
}

};

// Request ownership is independent from Vue so races can be verified directly.
const defaults = Object.freeze({
  enabled: false, notify: false, recursive: true, cron: '0 2 * * *', target_dirs: '',
  exclude_dirs: '', dry_run: false, records_enabled: false, records_cron: '*/5 * * * *',
  records_dry_run: true, records_batch_size: 100, records_grace_hours: 24,
});
const clone = value => value == null ? value : JSON.parse(JSON.stringify(value));
const display = config => ({ ...clone(defaults), ...clone(config) });
function unwrap(response) {
  const body = response?.data && typeof response.success !== 'boolean' && typeof response.data.success === 'boolean'
    ? response.data : response;
  if (body?.success === false) {
    const error = new Error(body.message || '请求失败，请重试');
    error.data = body.data;
    throw error
  }
  return typeof body?.success === 'boolean' ? body.data : body?.data ?? body
}
function createCleanupState() {
  return {
    view: 'overview', status: null, statusLoading: false, statusError: '', actionBusy: false, actionNotice: '',
    settings: { open: false, loading: false, ready: false, saving: false, baseline: null, draft: null, revision: '', error: '', notice: '' },
    detail: { kind: 'records', loading: false, error: '', items: [], total: 0, page: 1, page_size: 20, snapshot_id: '', scope: 'latest_batch', expanded: '' },
    history: { kind: 'folders', loading: false, error: '', items: [], total: 0, page: 1, page_size: 20 },
  }
}
function createCleanupController(api, state) {
  const base = 'plugin/EmptyFolderCleaner';
  const epochs = { status: 0, settings: 0, detail: 0, history: 0 };
  const requests = new Set();
  const requestSlots = new Map();
  let alive = true, action = null, retryRun = null;
  async function request(method, path, payload) {
    const abort = new AbortController();
    const slot = `${method}:${path.split('?')[0]}`;
    requestSlots.get(slot)?.abort();
    requestSlots.set(slot, abort);
    requests.add(abort);
    let timer, abortListener;
    try {
      const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => { abort.abort(); reject(new Error('请求超时，请重试')); }, 15000);
      });
      const operation = method === 'get' ? api.get(path, { signal: abort.signal }) : api.post(path, payload, { signal: abort.signal });
      const cancelled = new Promise((_, reject) => {
        abortListener = () => reject(new Error('请求已取消'));
        abort.signal.addEventListener('abort', abortListener, { once: true });
      });
      return unwrap(await Promise.race([operation, timeout, cancelled]))
    } finally {
      clearTimeout(timer); requests.delete(abort);
      if (requestSlots.get(slot) === abort) requestSlots.delete(slot);
      if (abortListener) abort.signal.removeEventListener('abort', abortListener);
    }
  }
  const current = (kind, id) => alive && epochs[kind] === id;
  const controller = {
    async loadStatus() {
      const id = ++epochs.status;
      state.statusLoading = !state.status;
      try {
        const result = await request('get', `${base}/status`);
        if (current('status', id)) { state.status = result; state.statusError = ''; }
      } catch (_) {
        if (current('status', id)) state.statusError = '统计读取失败，请重试';
      } finally { if (current('status', id)) state.statusLoading = false; }
    },
    async openSettings() {
      state.view = 'settings'; state.settings.open = true;
      if (!state.settings.ready) await controller.loadSettings();
    },
    async loadSettings({ preserveDraft = true } = {}) {
      if (!state.settings.open || state.settings.saving) return
      const id = ++epochs.settings, form = state.settings;
      const previous = preserveDraft && form.ready ? display(form.baseline) : null;
      form.loading = true; form.error = ''; form.notice = '';
      try {
        const data = await request('get', `${base}/settings`);
        if (!current('settings', id) || !form.open) return
        if (!data?.config || typeof data.revision !== 'string') throw new Error('设置读取不完整，请重试')
        const dirty = previous ? Object.fromEntries(Object.keys(defaults)
          .filter(key => JSON.stringify(form.draft[key]) !== JSON.stringify(previous[key]))
          .map(key => [key, clone(form.draft[key])])) : {};
        form.baseline = clone(data.config); form.draft = {...display(data.config),...dirty}; form.revision = data.revision; form.ready = true;
        if (Object.keys(dirty).length) form.notice = '已读取最新配置并保留本地修改，请核对后再次保存';
      } catch (error) { if (current('settings', id) && form.open) form.error = error.message || '设置读取失败'; }
      finally { if (current('settings', id)) form.loading = false; }
    },
    closeSettings() {
      epochs.settings++;
      requestSlots.get(`get:${base}/settings`)?.abort();
      requestSlots.get(`post:${base}/settings`)?.abort();
      Object.assign(state.settings, {open:false,loading:false,ready:false,saving:false,baseline:null,draft:null,revision:'',error:'',notice:''});
    },
    async saveSettings() {
      const form = state.settings;
      if (!form.open || !form.ready || form.loading || form.saving) return
      const id = epochs.settings, sent = clone(form.draft), baseline = display(form.baseline);
      const changes = Object.fromEntries(Object.keys(defaults).filter(key => JSON.stringify(sent[key]) !== JSON.stringify(baseline[key])).map(key => [key, sent[key]]));
      if (!Object.keys(changes).length) { form.notice = '没有需要保存的修改'; return }
      form.saving = true; form.error = ''; form.notice = '';
      try {
        const data = await request('post', `${base}/settings`, {revision:form.revision,changes});
        if (!current('settings', id) || !form.open) return
        if (!data?.config || typeof data.revision !== 'string') throw new Error('保存结果不完整，请重新读取确认')
        const remaining = Object.fromEntries(Object.keys(defaults).filter(key => JSON.stringify(form.draft[key]) !== JSON.stringify(sent[key])).map(key => [key, clone(form.draft[key])]));
        form.baseline = clone(data.config); form.draft = {...display(data.config),...remaining}; form.revision = data.revision;
        form.notice = Object.keys(remaining).length ? '已保存，后续修改仍保留在表单中' : '设置已保存';
        await controller.loadStatus();
      } catch (error) { if (current('settings', id) && form.open) form.error = error.message || '设置保存失败，修改内容已保留'; }
      finally { if (current('settings', id)) form.saving = false; }
    },
    async loadDetails(kind = state.detail.kind, page = 1, latest = false) {
      state.view = 'details';
      const target = state.detail, id = ++epochs.detail;
      const snapshot = latest || kind !== target.kind ? '' : target.snapshot_id;
      target.kind = kind; target.loading = true; target.error = '';
      try {
        const query = new URLSearchParams({kind,page:String(page),page_size:'20',snapshot_id:snapshot});
        const result = await request('get', `${base}/details?${query}`);
        if (current('detail', id)) Object.assign(target, result);
      } catch (error) { if (current('detail', id)) target.error = error.message || '详情读取失败'; }
      finally { if (current('detail', id)) target.loading = false; }
    },
    async loadHistory(kind = state.history.kind, page = 1) {
      state.view = 'history';
      const target = state.history, id = ++epochs.history;
      target.kind = kind; target.loading = true; target.error = '';
      try {
        const result = await request('get', `${base}/history?${new URLSearchParams({kind,page:String(page),page_size:'20'})}`);
        if (current('history', id)) Object.assign(target, result);
      } catch (error) { if (current('history', id)) target.error = error.message || '历史读取失败'; }
      finally { if (current('history', id)) target.loading = false; }
    },
    run(scope) {
      if (action) return action
      state.actionBusy = true; state.actionNotice = '';
      if (!retryRun || retryRun.scope !== scope) retryRun = {scope,request_id:globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random()}`};
      action = (async () => {
        try {
          const result = await request('post', `${base}/run`, retryRun);
          retryRun = null;
          if (alive) {
            state.actionNotice = !result?.reused ? '运行请求已接收'
              : result.status === 'failed' ? '该请求已运行但失败，可查看执行历史后重新核对'
              : result.status === 'interrupted' ? '上次请求已中断，可查看状态后重新核对'
              : ['queued','running','cancelling'].includes(result.status) ? '已复用正在运行的任务，未创建重复任务'
              : '该运行请求已有结果，未创建重复任务';
            await controller.loadStatus();
          }
        } catch (error) {
          if (error.data?.not_started === true) retryRun = null;
          if (alive) state.actionNotice = error.message || '运行请求未确认，请重试';
        }
        finally { if (alive) state.actionBusy = false; action = null; }
      })();
      return action
    },
    async cancel() {
      if (state.actionBusy || !state.status?.run?.run_id) return
      state.actionBusy = true;
      try {
        await request('post', `${base}/cancel`, {run_id:state.status.run.run_id});
        if (alive) { state.actionNotice = '已请求停止，正在等待当前步骤结束'; await controller.loadStatus(); }
      } catch (error) { if (alive) state.actionNotice = error.message || '停止请求未确认，请重试'; }
      finally { if (alive) state.actionBusy = false; }
    },
    destroy() {
      alive = false;
      Object.keys(epochs).forEach(key => epochs[key]++);
      requests.forEach(item => item.abort()); requests.clear();
      controller.closeSettings();
    },
  };
  return controller
}

const statusNames = {
 queued:'等待开始',running:'正在运行',cancelling:'正在停止',cancelled:'已停止',completed:'已完成',
 review:'已完成，部分保留复核',yielded:'本批达到处理上限，下轮继续',failed:'运行失败',interrupted:'上次运行已中断',disabled:'未启用',
 applied:'已处理',eligible:'预览可处理',waiting:'观察中',needs_review:'需复核',skipped:'已保留',already_clean:'已处理过',
};
const branchNames = {file_only:'失效文件记录',related_history:'关联历史',related_transfer:'失效整理记录',orphan_history:'旧孤儿下载历史',orphan_transfer:'旧孤儿整理历史',source_purge:'残留源文件',residual_task:'残留下载任务'};
const reasons = {
 stability_window:'缺失情况正在观察，达到确认时间后再次核对。',
 confirmed_missing_dry_run:'缺失情况符合条件，当前仅预览，文件、任务与记录均未删除。',
 source_present:'源文件仍然有效，继续保留。',source_missing:'源文件已不存在，继续核对准确关联的记录。',
 destination_present:'已知入库文件仍在，保留入库文件及有效整理历史。',
 other_version_present:'还有有效版本，保留仍被使用的数据。',media_library_present:'媒体库仍存在此资源，保留相关数据。',
 downloader_task_present:'下载器中仍有任务，等待确认任务归属与状态。',transfer_task_active:'正在整理，继续保留。',
 presence_unknown:'文件状态暂时无法确认，保留并等待恢复。',storage_unavailable:'存储尚未就绪，保留文件与记录。',
 downloaders_incomplete:'下载器状态未完整取得，保留并等待重试。',media_lookup_incomplete:'媒体身份或范围尚未完整确认，保留复核。',
 destination_lookup_incomplete:'入库关联尚未完整确认，保留复核。',task_lookup_incomplete:'任务状态尚未完整确认，保留复核。',identity_unverified:'资源身份或归属尚不能准确对应，保留复核。',
 invalid_record:'记录路径或资源身份存在不确定信息，保留复核。',record_changed:'记录已经变化，等待重新核对。',
 observation_failed:'本项读取失败，保留并在后续核对中重试。',record_lookup_incomplete:'关联记录读取不完整，保留并等待重试。',
 record_absent:'这条记录已经不存在。',already_applied:'这条记录已经处理过。',records_updated:'准确关联的失效记录已清理。',
 source_purged:'确认失效的源文件、独占任务和对应记录已清理。',source_shared_by_task:'源文件被其他任务使用，保留文件与任务。',
 torrent_not_exclusive_single_file:'下载任务还有其他文件或无法证明独占，保留任务及共享文件。',torrent_file_incomplete:'文件尚未完整下载或大小不一致，继续保留。',
 task_inventory_incomplete:'下载任务清单读取不完整，保留并等待恢复。',file_inventory_incomplete:'任务文件清单读取不完整，保留并等待恢复。',
 source_signature_unverified:'源文件身份不能稳定确认，保留复核。',source_signature_unavailable:'暂时无法读取源文件身份，保留复核。',
 source_purge_evidence_changed:'执行期间文件或归属发生变化，已停止后续清理，等待复核。',downloader_remove_unconfirmed:'任务移除尚未全部确认，未继续删除文件。',
 source_unlink_unconfirmed:'源文件删除结果尚未确认，保留记录等待恢复处理。',source_purged_history_pending:'源文件已删除，剩余记录会继续核对处理。',
 source_purge_failed:'清理可能部分完成，保留后续处理入口等待恢复。',source_intent_store_failed:'恢复回执保存失败，已停止后续清理。',
 source_intent_revision_changed:'恢复回执与当前文件或记录不一致，保留复核。',cleanup_cancelled:'已请求停止，保留已完成结果和恢复位置。',source_changed:'源文件已变化，重新观察缺失状态。',
 receipt_capacity_wait:'关联恢复队列已达到上限，保留此记录，等待已有条目处理后继续。',
 association_snapshot_unavailable:'无法完整保存此文件的关联关系，保留文件记录等待复核。',
 new_or_changed_reference:'关联记录已新增或发生变化，保留并重新核对。',
 related_records_absent:'准确关联的历史已经不存在。',
 association_unverified:'尚未证明关联历史及文件归属完全一致，继续保留。',
 related_file_present:'源文件或入库文件仍正常存在，保留仍被使用的历史。',
 source_shared_or_signature_unavailable:'源文件还有其他硬链接，或文件身份暂时无法确认，继续保留。',
 task_absent:'对应下载任务已不存在，无需移除。',
 task_evidence_changed:'任务归属或运行状态发生变化，已停止该项操作。',
 residual_task_removed:'准确归属且已完成的残留下载任务已移除，未请求下载器删除目录。',
 related_evidence_changed:'关联文件、历史或任务状态发生变化，已保留等待重新核对。',
 missing_file_row_removed:'源路径确认缺失，已清理这条失效文件记录；有效入库文件与历史保留。',
 download_file_pruned:'源路径确认缺失，已清理这条失效文件记录。',
 related_history_removed:'已清理确认失效且无其他引用的关联历史。',
 task_moving:'下载任务正在搬移或分配文件，暂时保留。',task_checking:'下载任务正在校验，暂时保留。',
 task_queued_download:'任务仍在等待下载，继续保留。',task_downloading:'任务仍在下载，继续保留。',
 task_download_incomplete:'任务尚未完成或文件报错，继续保留。',task_state_unknown:'下载任务的运行状态无法确认，暂时保留。',
 source_shared_reference:'文件仍被其他记录引用，保留文件与关联数据。',conflicting_parent_histories:'关联下载历史存在真实冲突，保留等待复核。',
 unsafe_transfer_mapping:'整理映射未成功或无法精确确认，保留文件与历史。',transfer_mapping_changed:'整理映射发生变化，需要重新核对。',
 related_files_still_registered:'仍有同一任务的文件记录，继续保留关联历史。',receipt_signature_mismatch:'关联恢复回执的校验不一致，停止此项操作。',
 group_path_limit:'单任务关联路径超过本批安全上限，保留等待分批处理。',
 stale_transfer_removed:'已清理准确失效的整理记录，有效文件与其他记录保留。',
 transfer_records_absent:'对应整理记录已经不存在，无需处理。',
 transfer_evidence_incomplete:'整理记录关联证据不完整，保留等待复核。',
 transfer_evidence_changed:'整理记录关联证据已经变化，保留重新核对。',
 orphan_mapping_incomplete:'旧孤儿记录缺少完整且成功的文件映射，保留等待复核。',
 orphan_downloader_unverified:'旧整理记录的下载器归属无法唯一确认，保留等待复核。',
 destination_reappeared:'目标文件已经重新出现，停止清理并保留有效数据。',
};
function errorText(code) {
 if (['batch_time_limit','batch_path_budget','batch_host_budget'].includes(code)) return '本批达到处理上限，已保留完成结果与继续位置，下轮继续核对。'
 const names={record_run_failed:'本次核对未能完成，请确认存储和下载器可用后重新核对。',
 receipt_store_failed:'恢复回执保存或回读失败，已停止后续操作。请恢复插件数据存储后重新核对。',
 candidate_read_failed:'文件记录读取不完整，未推进扫描位置。请恢复服务后重新核对。',
 invalid_record_state:'已保存的运行状态无法验证，已停止清理。请检查插件数据存储，保留现有数据后重试。',
 receipt_ledger_size_limit:'关联恢复队列达到容量上限，未继续清理。请检查保留条目的具体原因。',
 record_state_save_failed:'扫描状态保存失败，请恢复插件数据存储后重新核对。',
 source_intent_store_failed:'文件清理恢复回执保存失败，请恢复插件数据存储后重新核对。'};
 return names[code]||'本次核对未能完成。已完成的结果保留，恢复相关服务后可再次核对。'
}
function reasonText(item) { return reasons[item.reason] || item.message || '保护条件尚未满足，已保留等待后续核对。' }
function number(value) { return typeof value === 'number' && Number.isFinite(value) ? value.toLocaleString('zh-CN') : '—' }
function date(value) {
 if (!value) return '尚无记录'
 if (typeof value === 'string' && !/^\d+(\.\d+)?$/.test(value)) return value
 return new Date(Number(value) * 1000).toLocaleString('zh-CN', { hour12:false })
}

const {resolveComponent:_resolveComponent,openBlock:_openBlock,createBlock:_createBlock,createCommentVNode:_createCommentVNode,toDisplayString:_toDisplayString,createElementVNode:_createElementVNode,createTextVNode:_createTextVNode,withCtx:_withCtx,createVNode:_createVNode,Fragment:_Fragment,createElementBlock:_createElementBlock,unref:_unref,normalizeClass:_normalizeClass,renderList:_renderList} = await importShared('vue');


const _hoisted_1 = { class: "cleanup-master" };
const _hoisted_2 = { class: "cleanup-header" };
const _hoisted_3 = { class: "cleanup-header__title" };
const _hoisted_4 = {
  class: "cleanup-header__actions",
  "aria-label": "清理功能入口"
};
const _hoisted_5 = {
  key: 0,
  class: "cleanup-loading",
  role: "status"
};
const _hoisted_6 = {
  class: "cleanup-run",
  "aria-label": "当前运行状态",
  "aria-live": "polite"
};
const _hoisted_7 = { class: "cleanup-run__schedule" };
const _hoisted_8 = {
  key: 0,
  class: "cleanup-notice",
  role: "status"
};
const _hoisted_9 = { class: "cleanup-metrics" };
const _hoisted_10 = {
  class: "cleanup-metric",
  "aria-labelledby": "folders-title"
};
const _hoisted_11 = { class: "cleanup-metric__heading" };
const _hoisted_12 = { class: "cleanup-metric__number" };
const _hoisted_13 = {
  class: "cleanup-metric",
  "aria-labelledby": "records-title"
};
const _hoisted_14 = { class: "cleanup-metric__heading" };
const _hoisted_15 = { class: "cleanup-metric__number" };
const _hoisted_16 = { class: "cleanup-metric__actions" };
const _hoisted_17 = {
  class: "cleanup-metric",
  "aria-labelledby": "sources-title"
};
const _hoisted_18 = { class: "cleanup-metric__heading" };
const _hoisted_19 = { class: "cleanup-metric__number" };
const _hoisted_20 = {
  class: "cleanup-coverage",
  "aria-label": "核对覆盖与结果统计"
};
const _hoisted_21 = {
  key: 0,
  class: "cleanup-preview"
};
const _hoisted_22 = { key: 0 };
const _hoisted_23 = { key: 1 };
const _hoisted_24 = { class: "cleanup-summary" };
const _hoisted_25 = { class: "cleanup-hint" };
const _hoisted_26 = {
  key: 2,
  "aria-label": "核对详情"
};
const _hoisted_27 = { class: "cleanup-list-tools" };
const _hoisted_28 = { class: "cleanup-hint" };
const _hoisted_29 = {
  key: 2,
  class: "cleanup-loading",
  role: "status"
};
const _hoisted_30 = {
  key: 3,
  class: "cleanup-empty"
};
const _hoisted_31 = {
  key: 4,
  class: "cleanup-detail-list"
};
const _hoisted_32 = { class: "cleanup-item-status" };
const _hoisted_33 = {
  key: 0,
  class: "cleanup-hint"
};
const _hoisted_34 = {
  key: 3,
  "aria-label": "执行历史"
};
const _hoisted_35 = { class: "cleanup-list-tools" };
const _hoisted_36 = {
  key: 1,
  class: "cleanup-loading",
  role: "status"
};
const _hoisted_37 = {
  key: 2,
  class: "cleanup-empty"
};
const _hoisted_38 = {
  key: 3,
  class: "cleanup-detail-list"
};
const _hoisted_39 = { key: 0 };
const _hoisted_40 = { key: 1 };
const _hoisted_41 = { key: 2 };

const {computed,nextTick,onMounted,onUnmounted,reactive,ref,watch} = await importShared('vue');

const _sfc_main = {
  __name: 'CleanupWorkbench',
  props: {api:{type:Object,required:true},initialView:{type:String,default:'overview'},configMode:Boolean,initialConfig:{type:Object,default:()=>({})}},
  emits: ['close'],
  setup(__props, { emit: __emit }) {

const props=__props;
const emit=__emit;
const state=reactive(createCleanupState());
let controller=createCleanupController(props.api,state);
const heading=ref(null),settingsTrigger=ref(null),detailsTrigger=ref(null),sourcesTrigger=ref(null),historyTrigger=ref(null);
let timer,disposed=false,returnFocus=null;
const active=computed(()=>['queued','running','cancelling'].includes(state.status?.run?.status));
const records=computed(()=>state.status?.records||{});
const counts=computed(()=>records.value.counts||{});
const scan=computed(()=>records.value.scan||{});
const folders=computed(()=>state.status?.folders);
const pageCount=computed(()=>Math.max(1,Math.ceil((state.view==='details'?state.detail.total:state.history.total)/20)));
const pageTitle=computed(()=>({settings:'清理设置',details:'核对详情',history:'执行历史'}[state.view]||'清理大师'));
const summary=computed(()=>records.value.summary||{});
async function focusHeading(){await nextTick();heading.value?.focus();}
async function settings(){returnFocus='settings';const pending=controller.openSettings();await focusHeading();await pending;}
async function details(kind){returnFocus=kind==='sources'?'sources':'details';const pending=controller.loadDetails(kind,1,true);await focusHeading();await pending;}
async function history(){returnFocus='history';const pending=controller.loadHistory();await focusHeading();await pending;}
async function back(){
 if(state.view==='settings')controller.closeSettings();
 state.view='overview';await nextTick();
 const target={settings:settingsTrigger.value,details:detailsTrigger.value,sources:sourcesTrigger.value,history:historyTrigger.value}[returnFocus]
 ;(target?.$el||target)?.focus?.();
}
function close(){controller.closeSettings();emit('close');}
async function poll(){
 if(disposed||props.configMode)return
 if(document.visibilityState==='visible')await controller.loadStatus();
 if(!disposed)timer=setTimeout(poll,active.value?2500:15000);
}
function visible(){if(document.visibilityState==='visible'&&!props.configMode)controller.loadStatus();}
watch(()=>props.api,async api=>{
 controller.destroy();
 Object.assign(state,createCleanupState());
 controller=createCleanupController(api,state);
 if(disposed)return
 if(props.initialView==='settings')await controller.openSettings();
 else await controller.loadStatus();
});
onMounted(async()=>{
 if(props.initialView==='settings')await controller.openSettings();
 else await controller.loadStatus();
 if(!disposed&&!props.configMode){timer=setTimeout(poll,2500);document.addEventListener('visibilitychange',visible);}
});
onUnmounted(()=>{disposed=true;clearTimeout(timer);document.removeEventListener('visibilitychange',visible);controller.destroy();});

return (_ctx, _cache) => {
  const _component_VBtn = _resolveComponent("VBtn");
  const _component_VProgressCircular = _resolveComponent("VProgressCircular");
  const _component_VAlert = _resolveComponent("VAlert");
  const _component_VIcon = _resolveComponent("VIcon");
  const _component_VPagination = _resolveComponent("VPagination");

  return (_openBlock(), _createElementBlock("div", _hoisted_1, [
    _createElementVNode("header", _hoisted_2, [
      _createElementVNode("div", _hoisted_3, [
        (state.view!=='overview'&&!__props.configMode)
          ? (_openBlock(), _createBlock(_component_VBtn, {
              key: 0,
              icon: "mdi-arrow-left",
              variant: "text",
              "aria-label": "返回清理统计",
              onClick: back
            }))
          : _createCommentVNode("", true),
        _createElementVNode("h1", {
          ref_key: "heading",
          ref: heading,
          tabindex: "-1"
        }, _toDisplayString(pageTitle.value), 513)
      ]),
      _createElementVNode("nav", _hoisted_4, [
        (state.view==='overview')
          ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
              _createVNode(_component_VBtn, {
                ref_key: "settingsTrigger",
                ref: settingsTrigger,
                variant: "tonal",
                "prepend-icon": "mdi-tune-variant",
                onClick: settings
              }, {
                default: _withCtx(() => _cache[14] || (_cache[14] = [
                  _createTextVNode("设置")
                ])),
                _: 1
              }, 512),
              _createVNode(_component_VBtn, {
                ref_key: "historyTrigger",
                ref: historyTrigger,
                variant: "text",
                "prepend-icon": "mdi-history",
                onClick: history
              }, {
                default: _withCtx(() => _cache[15] || (_cache[15] = [
                  _createTextVNode("执行历史")
                ])),
                _: 1
              }, 512)
            ], 64))
          : _createCommentVNode("", true),
        _createVNode(_component_VBtn, {
          icon: "mdi-close",
          variant: "text",
          "aria-label": "关闭清理大师",
          onClick: close
        })
      ])
    ]),
    (state.view==='overview')
      ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
          (state.statusLoading)
            ? (_openBlock(), _createElementBlock("div", _hoisted_5, [
                _createVNode(_component_VProgressCircular, {
                  indeterminate: "",
                  size: "24"
                }),
                _cache[16] || (_cache[16] = _createTextVNode("正在读取清理统计"))
              ]))
            : _createCommentVNode("", true),
          (state.statusError)
            ? (_openBlock(), _createBlock(_component_VAlert, {
                key: 1,
                type: "error",
                variant: "tonal",
                class: "mb-4"
              }, {
                default: _withCtx(() => [
                  _createTextVNode(_toDisplayString(state.statusError), 1),
                  _createVNode(_component_VBtn, {
                    variant: "text",
                    onClick: _unref(controller).loadStatus
                  }, {
                    default: _withCtx(() => _cache[17] || (_cache[17] = [
                      _createTextVNode("重试")
                    ])),
                    _: 1
                  }, 8, ["onClick"])
                ]),
                _: 1
              }))
            : _createCommentVNode("", true),
          (state.status)
            ? (_openBlock(), _createElementBlock(_Fragment, { key: 2 }, [
                _createElementVNode("section", _hoisted_6, [
                  _createElementVNode("div", null, [
                    _createElementVNode("span", {
                      class: _normalizeClass(["cleanup-status-dot", {'cleanup-status-dot--active':active.value}])
                    }, null, 2),
                    _createTextVNode(_toDisplayString(_unref(statusNames)[state.status.run?.status]||'等待运行'), 1),
                    _createElementVNode("span", _hoisted_7, _toDisplayString(state.status.enabled?'定时清理已启用':'定时清理未启用'), 1)
                  ]),
                  (active.value)
                    ? (_openBlock(), _createBlock(_component_VBtn, {
                        key: 0,
                        variant: "tonal",
                        color: "warning",
                        loading: state.actionBusy,
                        disabled: state.actionBusy||state.status.run?.status==='cancelling',
                        onClick: _unref(controller).cancel
                      }, {
                        default: _withCtx(() => [
                          _createTextVNode(_toDisplayString(state.status.run?.status==='cancelling'?'正在停止':'停止本次运行'), 1)
                        ]),
                        _: 1
                      }, 8, ["loading", "disabled", "onClick"]))
                    : _createCommentVNode("", true)
                ]),
                (state.actionNotice)
                  ? (_openBlock(), _createElementBlock("div", _hoisted_8, _toDisplayString(state.actionNotice), 1))
                  : _createCommentVNode("", true),
                _createElementVNode("div", _hoisted_9, [
                  _createElementVNode("section", _hoisted_10, [
                    _createElementVNode("div", _hoisted_11, [
                      _createVNode(_component_VIcon, {
                        icon: "mdi-folder-outline",
                        "aria-hidden": "true"
                      }),
                      _cache[18] || (_cache[18] = _createElementVNode("h2", { id: "folders-title" }, "空文件夹", -1))
                    ]),
                    _createElementVNode("div", _hoisted_12, [
                      _createTextVNode(_toDisplayString(_unref(number)(folders.value?.removed_count)), 1),
                      _cache[19] || (_cache[19] = _createElementVNode("span", null, "个", -1))
                    ]),
                    _createElementVNode("p", null, _toDisplayString(folders.value?.dry_run?'最近一次预览可清理':'最近一次已清理'), 1),
                    _createElementVNode("dl", null, [
                      _createElementVNode("div", null, [
                        _cache[20] || (_cache[20] = _createElementVNode("dt", null, "扫描目录", -1)),
                        _createElementVNode("dd", null, _toDisplayString(_unref(number)(state.status.configured_folders)) + " 个", 1)
                      ]),
                      _createElementVNode("div", null, [
                        _cache[21] || (_cache[21] = _createElementVNode("dt", null, "最近运行", -1)),
                        _createElementVNode("dd", null, _toDisplayString(_unref(date)(folders.value?.clean_time)), 1)
                      ])
                    ]),
                    _createVNode(_component_VBtn, {
                      variant: "tonal",
                      color: "primary",
                      disabled: active.value||state.actionBusy||!state.status.configured_folders,
                      onClick: _cache[0] || (_cache[0] = $event => (_unref(controller).run('folders')))
                    }, {
                      default: _withCtx(() => _cache[22] || (_cache[22] = [
                        _createTextVNode("扫描空文件夹")
                      ])),
                      _: 1
                    }, 8, ["disabled"])
                  ]),
                  _createElementVNode("section", _hoisted_13, [
                    _createElementVNode("div", _hoisted_14, [
                      _createVNode(_component_VIcon, {
                        icon: "mdi-text-box-check-outline",
                        "aria-hidden": "true"
                      }),
                      _cache[23] || (_cache[23] = _createElementVNode("h2", { id: "records-title" }, "失效记录", -1))
                    ]),
                    _createElementVNode("div", _hoisted_15, [
                      _createTextVNode(_toDisplayString(_unref(number)(counts.value.download_files)), 1),
                      _cache[24] || (_cache[24] = _createElementVNode("span", null, "条", -1))
                    ]),
                    _cache[29] || (_cache[29] = _createElementVNode("p", null, "本批已清理文件记录", -1)),
                    _createElementVNode("dl", null, [
                      _createElementVNode("div", null, [
                        _cache[25] || (_cache[25] = _createElementVNode("dt", null, "本批核对", -1)),
                        _createElementVNode("dd", null, _toDisplayString(_unref(number)(records.value.processed)) + " 条", 1)
                      ]),
                      _createElementVNode("div", null, [
                        _cache[26] || (_cache[26] = _createElementVNode("dt", null, "关联历史已清理", -1)),
                        _createElementVNode("dd", null, _toDisplayString(_unref(number)((counts.value.download_history||0)+(counts.value.transfer_history||0))) + " 条", 1)
                      ])
                    ]),
                    _createElementVNode("div", _hoisted_16, [
                      _createVNode(_component_VBtn, {
                        color: "primary",
                        variant: "tonal",
                        disabled: active.value||state.actionBusy,
                        onClick: _cache[1] || (_cache[1] = $event => (_unref(controller).run('records')))
                      }, {
                        default: _withCtx(() => _cache[27] || (_cache[27] = [
                          _createTextVNode("核对记录与残留")
                        ])),
                        _: 1
                      }, 8, ["disabled"]),
                      _createVNode(_component_VBtn, {
                        ref_key: "detailsTrigger",
                        ref: detailsTrigger,
                        variant: "text",
                        onClick: _cache[2] || (_cache[2] = $event => (details('records')))
                      }, {
                        default: _withCtx(() => _cache[28] || (_cache[28] = [
                          _createTextVNode("查看详情")
                        ])),
                        _: 1
                      }, 512)
                    ])
                  ]),
                  _createElementVNode("section", _hoisted_17, [
                    _createElementVNode("div", _hoisted_18, [
                      _createVNode(_component_VIcon, {
                        icon: "mdi-file-remove-outline",
                        "aria-hidden": "true"
                      }),
                      _cache[30] || (_cache[30] = _createElementVNode("h2", { id: "sources-title" }, "残留文件与任务", -1))
                    ]),
                    _createElementVNode("div", _hoisted_19, [
                      _createTextVNode(_toDisplayString(_unref(number)(counts.value.source_files)), 1),
                      _cache[31] || (_cache[31] = _createElementVNode("span", null, "个文件", -1))
                    ]),
                    _cache[35] || (_cache[35] = _createElementVNode("p", null, "本批已清理残留源文件", -1)),
                    _createElementVNode("dl", null, [
                      _createElementVNode("div", null, [
                        _cache[32] || (_cache[32] = _createElementVNode("dt", null, "下载任务已移除", -1)),
                        _createElementVNode("dd", null, _toDisplayString(_unref(number)(counts.value.tasks)) + " 个", 1)
                      ]),
                      _cache[33] || (_cache[33] = _createElementVNode("div", null, [
                        _createElementVNode("dt", null, "执行方式"),
                        _createElementVNode("dd", null, "与失效记录共同核对")
                      ], -1))
                    ]),
                    _createVNode(_component_VBtn, {
                      ref_key: "sourcesTrigger",
                      ref: sourcesTrigger,
                      variant: "text",
                      onClick: _cache[3] || (_cache[3] = $event => (details('sources')))
                    }, {
                      default: _withCtx(() => _cache[34] || (_cache[34] = [
                        _createTextVNode("查看详情")
                      ])),
                      _: 1
                    }, 512)
                  ])
                ]),
                _createElementVNode("section", _hoisted_20, [
                  _createElementVNode("div", null, [
                    _cache[36] || (_cache[36] = _createElementVNode("h2", null, "核对进度", -1)),
                    (records.value.dry_run)
                      ? (_openBlock(), _createElementBlock("span", _hoisted_21, "仅预览"))
                      : _createCommentVNode("", true)
                  ]),
                  (scan.value.cycle_id)
                    ? (_openBlock(), _createElementBlock("p", _hoisted_22, [
                        _createTextVNode("本轮已核对 " + _toDisplayString(_unref(number)(scan.value.checked)) + " 条" + _toDisplayString(scan.value.complete?'，本轮扫描已完成。':'，扫描中。'), 1),
                        (!scan.value.complete&&scan.value.total==null)
                          ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                              _createTextVNode("总数尚未统计。")
                            ], 64))
                          : _createCommentVNode("", true)
                      ]))
                    : (_openBlock(), _createElementBlock("p", _hoisted_23, "尚无完整扫描统计。手动核对或等待定时运行后更新。")),
                  _createElementVNode("div", _hoisted_24, [
                    _createElementVNode("span", null, [
                      _cache[37] || (_cache[37] = _createTextVNode("本批预览可处理 ")),
                      _createElementVNode("strong", null, _toDisplayString(_unref(number)(summary.value.eligible||0)) + " 项", 1)
                    ]),
                    _createElementVNode("span", null, [
                      _cache[38] || (_cache[38] = _createTextVNode("本批观察中 ")),
                      _createElementVNode("strong", null, _toDisplayString(_unref(number)(summary.value.waiting||0)) + " 项", 1)
                    ]),
                    _createElementVNode("span", null, [
                      _cache[39] || (_cache[39] = _createTextVNode("本批需复核 ")),
                      _createElementVNode("strong", null, _toDisplayString(_unref(number)(summary.value.needs_review||0)) + " 项", 1)
                    ]),
                    _createElementVNode("span", null, [
                      _cache[40] || (_cache[40] = _createTextVNode("观察队列 ")),
                      _createElementVNode("strong", null, _toDisplayString(_unref(number)(records.value.observed_pending||0)) + " 项", 1)
                    ])
                  ]),
                  _createElementVNode("p", _hoisted_25, [
                    _createTextVNode("统计更新：" + _toDisplayString(_unref(date)(records.value.clean_time)), 1),
                    (scan.value.completed_at)
                      ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                          _createTextVNode("；完整扫描：" + _toDisplayString(_unref(date)(scan.value.completed_at)), 1)
                        ], 64))
                      : _createCommentVNode("", true),
                    _cache[41] || (_cache[41] = _createTextVNode("。本批结果与全量扫描进度分别统计。"))
                  ])
                ])
              ], 64))
            : _createCommentVNode("", true)
        ], 64))
      : (state.view==='settings')
        ? (_openBlock(), _createBlock(_sfc_main$1, {
            key: 1,
            form: state.settings,
            onSave: _unref(controller).saveSettings,
            onRetry: _unref(controller).loadSettings
          }, null, 8, ["form", "onSave", "onRetry"]))
        : (state.view==='details')
          ? (_openBlock(), _createElementBlock("section", _hoisted_26, [
              _createElementVNode("div", _hoisted_27, [
                _createElementVNode("div", null, [
                  _createVNode(_component_VBtn, {
                    variant: state.detail.kind==='records'?'tonal':'text',
                    "aria-pressed": state.detail.kind==='records',
                    onClick: _cache[4] || (_cache[4] = $event => (_unref(controller).loadDetails('records',1,true)))
                  }, {
                    default: _withCtx(() => _cache[42] || (_cache[42] = [
                      _createTextVNode("失效记录")
                    ])),
                    _: 1
                  }, 8, ["variant", "aria-pressed"]),
                  _createVNode(_component_VBtn, {
                    variant: state.detail.kind==='sources'?'tonal':'text',
                    "aria-pressed": state.detail.kind==='sources',
                    onClick: _cache[5] || (_cache[5] = $event => (_unref(controller).loadDetails('sources',1,true)))
                  }, {
                    default: _withCtx(() => _cache[43] || (_cache[43] = [
                      _createTextVNode("残留文件与任务")
                    ])),
                    _: 1
                  }, 8, ["variant", "aria-pressed"])
                ]),
                _createVNode(_component_VBtn, {
                  variant: "text",
                  "prepend-icon": "mdi-refresh",
                  disabled: state.detail.loading,
                  onClick: _cache[6] || (_cache[6] = $event => (_unref(controller).loadDetails(state.detail.kind,1,true)))
                }, {
                  default: _withCtx(() => _cache[44] || (_cache[44] = [
                    _createTextVNode("读取最新批次")
                  ])),
                  _: 1
                }, 8, ["disabled"])
              ]),
              _createElementVNode("p", _hoisted_28, "最近一批的逐项结果，共 " + _toDisplayString(_unref(number)(state.detail.total)) + " 项；" + _toDisplayString(_unref(date)(state.detail.clean_time)) + "。翻页保留本批快照，扫描继续进行。", 1),
              (state.detail.error)
                ? (_openBlock(), _createBlock(_component_VAlert, {
                    key: 0,
                    type: "error",
                    variant: "tonal"
                  }, {
                    default: _withCtx(() => [
                      _createTextVNode(_toDisplayString(state.detail.error), 1),
                      _createVNode(_component_VBtn, {
                        variant: "text",
                        onClick: _cache[7] || (_cache[7] = $event => (_unref(controller).loadDetails(state.detail.kind,state.detail.page,false)))
                      }, {
                        default: _withCtx(() => _cache[45] || (_cache[45] = [
                          _createTextVNode("重试")
                        ])),
                        _: 1
                      })
                    ]),
                    _: 1
                  }))
                : _createCommentVNode("", true),
              (state.detail.run_error)
                ? (_openBlock(), _createBlock(_component_VAlert, {
                    key: 1,
                    type: state.detail.status==='yielded'?'info':'warning',
                    variant: "tonal",
                    class: "mb-4"
                  }, {
                    default: _withCtx(() => [
                      _createTextVNode(_toDisplayString(_unref(errorText)(state.detail.run_error)), 1),
                      _createVNode(_component_VBtn, {
                        variant: "text",
                        disabled: active.value||state.actionBusy,
                        onClick: _cache[8] || (_cache[8] = $event => (_unref(controller).run('records')))
                      }, {
                        default: _withCtx(() => [
                          _createTextVNode(_toDisplayString(state.detail.status==='yielded'?'继续核对':'重新核对'), 1)
                        ]),
                        _: 1
                      }, 8, ["disabled"])
                    ]),
                    _: 1
                  }, 8, ["type"]))
                : _createCommentVNode("", true),
              (state.detail.loading)
                ? (_openBlock(), _createElementBlock("div", _hoisted_29, [
                    _createVNode(_component_VProgressCircular, {
                      indeterminate: "",
                      size: "24"
                    }),
                    _cache[46] || (_cache[46] = _createTextVNode("正在读取详情"))
                  ]))
                : (!state.detail.items.length)
                  ? (_openBlock(), _createElementBlock("p", _hoisted_30, "本批暂无此类结果。可读取最新批次或开始核对。"))
                  : (_openBlock(), _createElementBlock("div", _hoisted_31, [
                      (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(state.detail.items, (item, index) => {
                        return (_openBlock(), _createElementBlock("article", {
                          key: `${state.detail.snapshot_id}-${item.key}-${item.branch}-${index}`,
                          class: "cleanup-detail-item"
                        }, [
                          _createElementVNode("div", null, [
                            _createElementVNode("h3", null, _toDisplayString(item.file||`文件记录 ${item.file_id||''}`), 1),
                            _createElementVNode("span", _hoisted_32, _toDisplayString(_unref(statusNames)[item.status]||'需复核'), 1)
                          ]),
                          _createElementVNode("p", null, _toDisplayString(_unref(branchNames)[item.branch]||'关联核对'), 1),
                          _createElementVNode("p", null, _toDisplayString(_unref(reasonText)(item)), 1),
                          (item.eligible_at)
                            ? (_openBlock(), _createElementBlock("p", _hoisted_33, "可再次确认时间：" + _toDisplayString(_unref(date)(item.eligible_at)), 1))
                            : _createCommentVNode("", true)
                        ]))
                      }), 128))
                    ])),
              (pageCount.value>1)
                ? (_openBlock(), _createBlock(_component_VPagination, {
                    key: 5,
                    "model-value": state.detail.page,
                    length: pageCount.value,
                    "total-visible": 3,
                    disabled: state.detail.loading,
                    "aria-label": "详情分页",
                    "onUpdate:modelValue": _cache[9] || (_cache[9] = page=>_unref(controller).loadDetails(state.detail.kind,page,false))
                  }, null, 8, ["model-value", "length", "disabled"]))
                : _createCommentVNode("", true)
            ]))
          : (state.view==='history')
            ? (_openBlock(), _createElementBlock("section", _hoisted_34, [
                _createElementVNode("div", _hoisted_35, [
                  _createElementVNode("div", null, [
                    _createVNode(_component_VBtn, {
                      variant: state.history.kind==='folders'?'tonal':'text',
                      "aria-pressed": state.history.kind==='folders',
                      onClick: _cache[10] || (_cache[10] = $event => (_unref(controller).loadHistory('folders',1)))
                    }, {
                      default: _withCtx(() => _cache[47] || (_cache[47] = [
                        _createTextVNode("空文件夹")
                      ])),
                      _: 1
                    }, 8, ["variant", "aria-pressed"]),
                    _createVNode(_component_VBtn, {
                      variant: state.history.kind==='records'?'tonal':'text',
                      "aria-pressed": state.history.kind==='records',
                      onClick: _cache[11] || (_cache[11] = $event => (_unref(controller).loadHistory('records',1)))
                    }, {
                      default: _withCtx(() => _cache[48] || (_cache[48] = [
                        _createTextVNode("记录与残留")
                      ])),
                      _: 1
                    }, 8, ["variant", "aria-pressed"])
                  ])
                ]),
                (state.history.error)
                  ? (_openBlock(), _createBlock(_component_VAlert, {
                      key: 0,
                      type: "error",
                      variant: "tonal"
                    }, {
                      default: _withCtx(() => [
                        _createTextVNode(_toDisplayString(state.history.error), 1),
                        _createVNode(_component_VBtn, {
                          variant: "text",
                          onClick: _cache[12] || (_cache[12] = $event => (_unref(controller).loadHistory(state.history.kind,state.history.page)))
                        }, {
                          default: _withCtx(() => _cache[49] || (_cache[49] = [
                            _createTextVNode("重试")
                          ])),
                          _: 1
                        })
                      ]),
                      _: 1
                    }))
                  : _createCommentVNode("", true),
                (state.history.loading)
                  ? (_openBlock(), _createElementBlock("div", _hoisted_36, [
                      _createVNode(_component_VProgressCircular, {
                        indeterminate: "",
                        size: "24"
                      }),
                      _cache[50] || (_cache[50] = _createTextVNode("正在读取历史"))
                    ]))
                  : (!state.history.items.length)
                    ? (_openBlock(), _createElementBlock("p", _hoisted_37, "暂无执行历史。完成清理后会保留结果。"))
                    : (_openBlock(), _createElementBlock("div", _hoisted_38, [
                        (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(state.history.items, (item, index) => {
                          return (_openBlock(), _createElementBlock("article", {
                            key: `${state.history.kind}-${item.clean_time}-${index}`,
                            class: "cleanup-detail-item"
                          }, [
                            _createElementVNode("div", null, [
                              _createElementVNode("h3", null, _toDisplayString(item.clean_time||'时间未记录'), 1),
                              _createElementVNode("span", null, _toDisplayString(item.dry_run?'仅预览':'实际执行'), 1)
                            ]),
                            _createElementVNode("p", null, _toDisplayString(_unref(statusNames)[item.status]||'已有历史'), 1),
                            (item.error)
                              ? (_openBlock(), _createElementBlock("p", _hoisted_39, _toDisplayString(_unref(errorText)(item.error)), 1))
                              : _createCommentVNode("", true),
                            (state.history.kind==='folders')
                              ? (_openBlock(), _createElementBlock("p", _hoisted_40, "扫描 " + _toDisplayString(_unref(number)(item.target_dirs)) + " 个目录，" + _toDisplayString(item.dry_run?'预览可清理':'已清理') + " " + _toDisplayString(_unref(number)(item.removed_count)) + " 个空文件夹。", 1))
                              : (_openBlock(), _createElementBlock("p", _hoisted_41, "核对 " + _toDisplayString(_unref(number)(item.processed)) + " 条文件记录，已清理 " + _toDisplayString(_unref(number)(item.counts?.download_files||0)) + " 条文件记录、" + _toDisplayString(_unref(number)(item.counts?.source_files||0)) + " 个文件、" + _toDisplayString(_unref(number)(item.counts?.tasks||0)) + " 个任务。", 1))
                          ]))
                        }), 128))
                      ])),
                (pageCount.value>1)
                  ? (_openBlock(), _createBlock(_component_VPagination, {
                      key: 4,
                      "model-value": state.history.page,
                      length: pageCount.value,
                      "total-visible": 3,
                      disabled: state.history.loading,
                      "aria-label": "历史分页",
                      "onUpdate:modelValue": _cache[13] || (_cache[13] = page=>_unref(controller).loadHistory(state.history.kind,page))
                    }, null, 8, ["model-value", "length", "disabled"]))
                  : _createCommentVNode("", true)
              ]))
            : _createCommentVNode("", true)
  ]))
}
}

};

export { _sfc_main as _ };
