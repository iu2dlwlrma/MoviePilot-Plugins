import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { _ as _export_sfc, n as normalizeSettings, f as formatDateTime, t as taskStateMeta, e as effectiveTask, b as formatRemainingTime, a as formatBytes, c as formatDuration, d as torrentProgress, u as unwrapResponse, s as settingsFromStatus } from './_plugin-vue_export-helper-BtgFjEiW.js';

const {createTextVNode:_createTextVNode$1,resolveComponent:_resolveComponent$1,withCtx:_withCtx$1,createVNode:_createVNode$1,openBlock:_openBlock$1,createElementBlock:_createElementBlock$1,createCommentVNode:_createCommentVNode$1,toDisplayString:_toDisplayString$1,createSlots:_createSlots,createBlock:_createBlock$1,createElementVNode:_createElementVNode$1,renderList:_renderList$1,Fragment:_Fragment$1,withModifiers:_withModifiers$1,withKeys:_withKeys} = await importShared('vue');


const _hoisted_1$1 = {
  key: 0,
  class: "brush-settings__loading",
  role: "status"
};
const _hoisted_2$1 = { class: "brush-settings__section" };
const _hoisted_3$1 = { class: "brush-settings__heading" };
const _hoisted_4$1 = { class: "brush-settings__switches" };
const _hoisted_5$1 = { class: "brush-settings__selection" };
const _hoisted_6$1 = { class: "text-body-2 text-medium-emphasis" };
const _hoisted_7$1 = { class: "brush-settings__heading" };
const _hoisted_8$1 = { class: "text-body-2 text-medium-emphasis" };

const {computed: computed$1,ref: ref$1,watch: watch$1} = await importShared('vue');


const _sfc_main$1 = {
  __name: 'BrushSettingsDialog',
  props: {
  modelValue: { type: Boolean, default: false },
  settings: { type: Object, default: null },
  sites: { type: Array, default: () => [] },
  downloaders: { type: Array, default: () => [] },
  ready: { type: Boolean, default: false },
  loading: { type: Boolean, default: false },
  saving: { type: Boolean, default: false },
  error: { type: String, default: '' },
},
  emits: ['update:modelValue', 'retry', 'save', 'closed'],
  setup(__props, { emit: __emit }) {

const props = __props;
const emit = __emit;
const formRef = ref$1(null);
const draft = ref$1(null);
const formError = ref$1('');
const validating = ref$1(false);
const selectedSites = computed$1(() => draft.value?.selected_site_ids || []);
const manualFocusSites = computed$1(() => props.sites.filter(site => selectedSites.value.includes(Number(site.value))));
const siteOptions = computed$1(() => {
  const known = new Set(props.sites.map(site => Number(site.value)));
  return [...props.sites, ...selectedSites.value.filter(id => !known.has(id))
    .map(id => ({ title: `站点 ${id}（已不可用）`, value: id }))]
});
const canSave = computed$1(() => props.ready && !props.loading && !props.saving && !validating.value && Boolean(draft.value));

const sections = [
  {
    key: 'download', title: '下载与调度', icon: 'mdi-calendar-clock',
    description: '所有选中站点使用同一套参数，下载并发和速度上限仍由下载器管理。',
    fields: [
      { key: 'downloader', label: '下载器', type: 'downloader' },
      { key: 'save_path', label: '保存目录', hint: '留空使用下载器默认目录' },
      { key: 'qb_category', label: 'qBittorrent 分类', hint: '使用 Transmission 时不生效' },
      { key: 'brush_interval', label: '刷流刷新周期（分钟）', type: 'number', min: 1, max: 1440, required: true },
      { key: 'check_interval', label: '状态检查周期（分钟）', type: 'number', min: 1, max: 1440, required: true },
      { key: 'cron', label: 'CRON 表达式', hint: '留空使用固定刷新周期' },
      { key: 'active_time_range', label: '开启时间段', hint: '如 00:00-08:00，留空为全天' },
      { key: 'site_ratio_target', label: '目标站点分享率', type: 'number', min: 0, step: 0.01, hint: '单站模式达标后切换；留空或 0 表示不限' },
      { key: 'rotation_hours', label: '达标后每站驻留时间（小时）', type: 'number', min: 0.01, max: 168, required: true, hint: '单站模式下全部站点达标后生效；旧种继续托管' },
      { key: 'disksize', label: '每站空间上限（GB）', type: 'number', min: 0, hint: '留空或 0 表示不限，仍受总空间上限约束' },
      { key: 'notify', label: '发送通知', type: 'switch' },
    ],
  },
  {
    key: 'selection', title: '选种规则', icon: 'mdi-filter-cog-outline',
    description: '候选先通过以下条件，再按分享率和当前调度顺序分配机会。',
    fields: [
      { key: 'freeleech', label: '促销', type: 'select', items: [{ title: '全部（包括普通）', value: '' }, { title: '免费', value: 'free' }, { title: '2X 免费', value: '2xfree' }] },
      { key: 'hr', label: '排除 H&R', type: 'select', items: [{ title: '是', value: 'yes' }, { title: '否', value: 'no' }] },
      { key: 'size', label: '种子大小（GB）', hint: '单值为下限，或填写范围，如 10-80' },
      { key: 'seeder', label: '做种人数', hint: '单值为上限，或填写范围，如 1-50' },
      { key: 'leecher', label: '下载人数', hint: '单值为最低人数，或填写范围，如 1-100' },
      { key: 'min_peer_seed_ratio', label: '下载人数 / 做种人数下限', type: 'number', min: 0, step: 0.01, hint: '留空或 0 表示不限制' },
      { key: 'pubtime', label: '发布时间（分钟）', hint: '单值为上限，或填写范围，如 5-120' },
      { key: 'include', label: '包含规则', hint: '支持正则表达式，留空不限制' },
      { key: 'exclude', label: '排除规则', hint: '支持正则表达式，留空不限制' },
      { key: 'except_subscribe', label: '排除订阅内容', type: 'switch' },
    ],
  },
  {
    key: 'cleanup', title: '清理与保护', icon: 'mdi-delete-clock-outline',
    description: '促销保护可处理尚未开始的排队任务；其余规则仅处理已开始下载的种子，并遵守 H&R 与保护标签。',
    fields: [
      { key: 'proxy_delete', label: '按站点动态删种', type: 'switch' },
      { key: 'delete_size_range', label: '站点动态删种阈值（GB）', hint: '如 350-500；开启全局动态删种时使用总阈值' },
      { key: 'seed_time', label: '做种时间（小时）', type: 'number', min: 0 },
      { key: 'hr_seed_time', label: 'H&R 做种时间（小时）', type: 'number', min: 0 },
      { key: 'seed_ratio', label: '种子分享率', type: 'number', min: 0 },
      { key: 'seed_size', label: '上传量（GB）', type: 'number', min: 0 },
      { key: 'download_time', label: '下载超时（小时）', type: 'number', min: 0 },
      { key: 'download_min_speed', label: '低速下载阈值（KB/s）', type: 'number', min: 0, hint: '与观察时间同时大于 0 时生效；上传达到此速度时暂留' },
      { key: 'download_slow_minutes', label: '低速观察时间（分钟）', type: 'number', min: 0, hint: '排队、暂停不计时，采样中断重新计算；0 关闭' },
      { key: 'seed_inactivetime', label: '未活动时间（分钟）', type: 'number', min: 0 },
      { key: 'seed_avgspeed', label: '平均上传速度（KB/s）', type: 'number', min: 0 },
      { key: 'delete_except_tags', label: '删除排除标签' },
      { key: 'del_no_free', label: '促销结束前删除未完成任务（包括排队）', type: 'switch' },
      { key: 'promotion_lead_minutes', label: '促销结束前预留时间（分钟）', type: 'number', min: 0, required: true, hint: '默认提前 10 分钟，仍遵守保护标签与 H&R 约束' },
      { key: 'auto_archive_days', label: '已删除记录归档天数', type: 'number', min: 0, hint: '留空或 0 不自动归档' },
    ],
  },
];

watch$1(() => [props.modelValue, props.settings], ([visible, settings]) => {
  draft.value = visible && settings ? JSON.parse(JSON.stringify(settings)) : null;
  formError.value = '';
}, { immediate: true });

function selectSites(values) {
  if (!draft.value) return
  draft.value.selected_site_ids = [...new Set(values.map(Number))];
  if (!draft.value.selected_site_ids.includes(draft.value.manual_focus_site_id)) draft.value.manual_focus_site_id = null;
}

function fieldLabel(field) {
  return field.key === 'proxy_delete' && draft.value?.global_proxy_delete ? '参与全局动态删种' : field.label
}

function fieldRules(field) {
  const rules = [];
  if (field.required || (field.type === 'downloader' && selectedSites.value.length)) {
    rules.push(value => value !== null && value !== '' && value !== undefined || '此项不能为空');
  }
  if (field.type === 'downloader' && selectedSites.value.length) {
    rules.push(value => props.downloaders.some(item => item.value === value) || '下载器不可用，请重新选择');
  }
  if (field.type === 'number') {
    rules.push(value => value === '' || value == null || Number.isFinite(Number(value)) || '请输入有效数字');
    if (field.min != null) rules.push(value => value === '' || value == null || Number(value) >= field.min || `不能小于 ${field.min}`);
    if (field.max != null) rules.push(value => value === '' || value == null || Number(value) <= field.max || `不能大于 ${field.max}`);
  }
  return rules
}

function trapDialogFocus(event) {
  if (!props.modelValue || event.defaultPrevented || event.key !== 'Tab') return
  const card = event.currentTarget;
  if (!card?.contains(event.target) || event.target.closest?.('[role="combobox"][aria-expanded="true"]')) return
  const document = card.ownerDocument;
  const focusable = [...card.querySelectorAll('button, input, select, textarea, a[href], [tabindex]')]
    .filter(element => element.tabIndex >= 0
      && !element.matches(':disabled, [aria-disabled="true"], [inert], [inert] *')
      && element.getClientRects().length
      && document.defaultView.getComputedStyle(element).visibility !== 'hidden');
  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  const destination = event.shiftKey
    ? (document.activeElement === first ? last : null)
    : (document.activeElement === last ? first : null);
  if (destination) {
    event.preventDefault();
    destination.focus({ preventScroll: true });
  }
}

async function save() {
  if (!canSave.value) return
  const currentDraft = draft.value;
  validating.value = true;
  formError.value = '';
  try {
    const result = await formRef.value?.validate();
    if (!props.modelValue || draft.value !== currentDraft || !props.ready || props.loading || props.saving) return
    if (result && !result.valid) return
    if (selectedSites.value.some(id => !props.sites.some(site => Number(site.value) === id))) {
      formError.value = '部分选中站点已不可用，请取消选择后保存';
      return
    }
    emit('save', normalizeSettings(draft.value));
  } catch (err) {
    if (props.modelValue && draft.value === currentDraft) formError.value = err?.message || '请检查配置格式';
  } finally {
    validating.value = false;
  }
}

return (_ctx, _cache) => {
  const _component_VToolbarTitle = _resolveComponent$1("VToolbarTitle");
  const _component_VBtn = _resolveComponent$1("VBtn");
  const _component_VToolbar = _resolveComponent$1("VToolbar");
  const _component_VDivider = _resolveComponent$1("VDivider");
  const _component_VProgressCircular = _resolveComponent$1("VProgressCircular");
  const _component_VAlert = _resolveComponent$1("VAlert");
  const _component_VIcon = _resolveComponent$1("VIcon");
  const _component_VSwitch = _resolveComponent$1("VSwitch");
  const _component_VAutocomplete = _resolveComponent$1("VAutocomplete");
  const _component_VSpacer = _resolveComponent$1("VSpacer");
  const _component_VSelect = _resolveComponent$1("VSelect");
  const _component_VCol = _resolveComponent$1("VCol");
  const _component_VTextField = _resolveComponent$1("VTextField");
  const _component_VRow = _resolveComponent$1("VRow");
  const _component_VForm = _resolveComponent$1("VForm");
  const _component_VCardText = _resolveComponent$1("VCardText");
  const _component_VCardActions = _resolveComponent$1("VCardActions");
  const _component_VCard = _resolveComponent$1("VCard");
  const _component_VDialog = _resolveComponent$1("VDialog");

  return (_openBlock$1(), _createBlock$1(_component_VDialog, {
    "model-value": __props.modelValue,
    persistent: __props.saving,
    scrollable: "",
    "max-width": "960",
    class: "brush-settings-dialog",
    "onUpdate:modelValue": _cache[12] || (_cache[12] = value => emit('update:modelValue', value)),
    onAfterLeave: _cache[13] || (_cache[13] = $event => (emit('closed')))
  }, {
    default: _withCtx$1(() => [
      _createVNode$1(_component_VCard, {
        class: "brush-settings",
        onKeydown: _withKeys(trapDialogFocus, ["tab"])
      }, {
        default: _withCtx$1(() => [
          _createVNode$1(_component_VToolbar, {
            color: "transparent",
            class: "brush-settings__toolbar"
          }, {
            default: _withCtx$1(() => [
              _createVNode$1(_component_VToolbarTitle, null, {
                default: _withCtx$1(() => [...(_cache[14] || (_cache[14] = [
                  _createTextVNode$1("刷流设置", -1)
                ]))]),
                _: 1
              }),
              _createVNode$1(_component_VBtn, {
                icon: "mdi-close",
                variant: "text",
                "aria-label": "关闭刷流设置",
                disabled: __props.saving,
                onClick: _cache[0] || (_cache[0] = $event => (emit('update:modelValue', false)))
              }, null, 8, ["disabled"])
            ]),
            _: 1
          }),
          _createVNode$1(_component_VDivider),
          _createVNode$1(_component_VCardText, { class: "brush-settings__body" }, {
            default: _withCtx$1(() => [
              (__props.loading)
                ? (_openBlock$1(), _createElementBlock$1("div", _hoisted_1$1, [
                    _createVNode$1(_component_VProgressCircular, {
                      indeterminate: "",
                      size: "24",
                      width: "2"
                    }),
                    _cache[15] || (_cache[15] = _createTextVNode$1("正在读取设置…", -1))
                  ]))
                : _createCommentVNode$1("", true),
              (__props.error)
                ? (_openBlock$1(), _createBlock$1(_component_VAlert, {
                    key: 1,
                    type: "error",
                    variant: "tonal",
                    class: "mb-4",
                    role: "alert"
                  }, _createSlots({
                    default: _withCtx$1(() => [
                      _createTextVNode$1(_toDisplayString$1(__props.error) + " ", 1)
                    ]),
                    _: 2
                  }, [
                    (!__props.ready)
                      ? {
                          name: "append",
                          fn: _withCtx$1(() => [
                            _createVNode$1(_component_VBtn, {
                              variant: "text",
                              disabled: __props.loading,
                              onClick: _cache[1] || (_cache[1] = $event => (emit('retry')))
                            }, {
                              default: _withCtx$1(() => [...(_cache[16] || (_cache[16] = [
                                _createTextVNode$1("重试读取", -1)
                              ]))]),
                              _: 1
                            }, 8, ["disabled"])
                          ]),
                          key: "0"
                        }
                      : undefined
                  ]), 1024))
                : _createCommentVNode$1("", true),
              (formError.value)
                ? (_openBlock$1(), _createBlock$1(_component_VAlert, {
                    key: 2,
                    type: "error",
                    variant: "tonal",
                    class: "mb-4",
                    role: "alert"
                  }, {
                    default: _withCtx$1(() => [
                      _createTextVNode$1(_toDisplayString$1(formError.value), 1)
                    ]),
                    _: 1
                  }))
                : _createCommentVNode$1("", true),
              (__props.ready && draft.value)
                ? (_openBlock$1(), _createBlock$1(_component_VForm, {
                    key: 3,
                    ref_key: "formRef",
                    ref: formRef,
                    disabled: __props.saving || __props.loading || validating.value,
                    onSubmit: _withModifiers$1(save, ["prevent"])
                  }, {
                    default: _withCtx$1(() => [
                      _createElementVNode$1("section", _hoisted_2$1, [
                        _createElementVNode$1("div", _hoisted_3$1, [
                          _createVNode$1(_component_VIcon, { icon: "mdi-web" }),
                          _cache[17] || (_cache[17] = _createElementVNode$1("h2", null, "管理站点", -1))
                        ]),
                        _cache[20] || (_cache[20] = _createElementVNode$1("p", { class: "text-body-2 text-medium-emphasis" }, "从 MoviePilot 读取站点，所有选中站点使用下方公共规则。取消选择会停止选种与检查，保留托管记录。", -1)),
                        _createElementVNode$1("div", _hoisted_4$1, [
                          _createVNode$1(_component_VSwitch, {
                            modelValue: draft.value.enabled,
                            "onUpdate:modelValue": _cache[2] || (_cache[2] = $event => ((draft.value.enabled) = $event)),
                            label: "启用插件",
                            color: "primary",
                            "hide-details": "",
                            inset: ""
                          }, null, 8, ["modelValue"]),
                          _createVNode$1(_component_VSwitch, {
                            modelValue: draft.value.show_sidebar_nav,
                            "onUpdate:modelValue": _cache[3] || (_cache[3] = $event => ((draft.value.show_sidebar_nav) = $event)),
                            label: "显示侧栏入口",
                            color: "primary",
                            "hide-details": "",
                            inset: ""
                          }, null, 8, ["modelValue"])
                        ]),
                        _createVNode$1(_component_VAutocomplete, {
                          "model-value": selectedSites.value,
                          items: siteOptions.value,
                          label: "参与刷流的站点",
                          multiple: "",
                          chips: "",
                          "closable-chips": "",
                          "hide-details": "auto",
                          "no-data-text": "MoviePilot 中暂无可用私有站点",
                          "onUpdate:modelValue": selectSites
                        }, null, 8, ["model-value", "items"]),
                        _createElementVNode$1("div", _hoisted_5$1, [
                          _createElementVNode$1("span", _hoisted_6$1, "已选择 " + _toDisplayString$1(selectedSites.value.length) + " / " + _toDisplayString$1(__props.sites.length) + " 个站点", 1),
                          _createVNode$1(_component_VSpacer),
                          _createVNode$1(_component_VBtn, {
                            variant: "text",
                            disabled: __props.saving || validating.value || !__props.sites.length,
                            onClick: _cache[4] || (_cache[4] = $event => (selectSites(__props.sites.map(site => site.value))))
                          }, {
                            default: _withCtx$1(() => [...(_cache[18] || (_cache[18] = [
                              _createTextVNode$1("全选", -1)
                            ]))]),
                            _: 1
                          }, 8, ["disabled"]),
                          _createVNode$1(_component_VBtn, {
                            variant: "text",
                            disabled: __props.saving || validating.value || !selectedSites.value.length,
                            onClick: _cache[5] || (_cache[5] = $event => (selectSites([])))
                          }, {
                            default: _withCtx$1(() => [...(_cache[19] || (_cache[19] = [
                              _createTextVNode$1("清空", -1)
                            ]))]),
                            _: 1
                          }, 8, ["disabled"])
                        ])
                      ]),
                      (_openBlock$1(), _createElementBlock$1(_Fragment$1, null, _renderList$1(sections, (section) => {
                        return _createElementVNode$1("section", {
                          key: section.key,
                          class: "brush-settings__section"
                        }, [
                          _createElementVNode$1("div", _hoisted_7$1, [
                            _createVNode$1(_component_VIcon, {
                              icon: section.icon
                            }, null, 8, ["icon"]),
                            _createElementVNode$1("h2", null, _toDisplayString$1(section.title), 1)
                          ]),
                          _createElementVNode$1("p", _hoisted_8$1, _toDisplayString$1(section.description), 1),
                          (section.key === 'download')
                            ? (_openBlock$1(), _createBlock$1(_component_VRow, { key: 0 }, {
                                default: _withCtx$1(() => [
                                  _createVNode$1(_component_VCol, {
                                    cols: "12",
                                    md: "6"
                                  }, {
                                    default: _withCtx$1(() => [
                                      _createVNode$1(_component_VSelect, {
                                        modelValue: draft.value.rotation_mode,
                                        "onUpdate:modelValue": _cache[6] || (_cache[6] = $event => ((draft.value.rotation_mode) = $event)),
                                        label: "站点轮换模式",
                                        items: [{ title: '多站并行', value: 'parallel' }, { title: '单站达标后轮转', value: 'sequential' }],
                                        hint: "未达标站点优先，全部达标后按驻留时间轮转",
                                        "persistent-hint": ""
                                      }, null, 8, ["modelValue"])
                                    ]),
                                    _: 1
                                  }),
                                  (draft.value.rotation_mode === 'sequential')
                                    ? (_openBlock$1(), _createBlock$1(_component_VCol, {
                                        key: 0,
                                        cols: "12",
                                        md: "6"
                                      }, {
                                        default: _withCtx$1(() => [
                                          _createVNode$1(_component_VSelect, {
                                            modelValue: draft.value.manual_focus_site_id,
                                            "onUpdate:modelValue": _cache[7] || (_cache[7] = $event => ((draft.value.manual_focus_site_id) = $event)),
                                            label: "手动重点站",
                                            items: manualFocusSites.value,
                                            clearable: "",
                                            placeholder: "自动调度",
                                            hint: "选定站点优先，清空后恢复自动调度；保存后生效",
                                            "persistent-hint": ""
                                          }, null, 8, ["modelValue", "items"])
                                        ]),
                                        _: 1
                                      }))
                                    : _createCommentVNode$1("", true),
                                  _createVNode$1(_component_VCol, {
                                    cols: "12",
                                    md: "6"
                                  }, {
                                    default: _withCtx$1(() => [
                                      _createVNode$1(_component_VTextField, {
                                        modelValue: draft.value.global_disksize,
                                        "onUpdate:modelValue": _cache[8] || (_cache[8] = $event => ((draft.value.global_disksize) = $event)),
                                        label: "总空间上限（GB）",
                                        type: "number",
                                        min: "0",
                                        clearable: "",
                                        hint: "所有站点合计；留空或 0 表示不限",
                                        "persistent-hint": "",
                                        rules: fieldRules({ type: 'number', min: 0 })
                                      }, null, 8, ["modelValue", "rules"])
                                    ]),
                                    _: 1
                                  })
                                ]),
                                _: 1
                              }))
                            : _createCommentVNode$1("", true),
                          (section.key === 'cleanup')
                            ? (_openBlock$1(), _createBlock$1(_component_VRow, { key: 1 }, {
                                default: _withCtx$1(() => [
                                  _createVNode$1(_component_VCol, {
                                    cols: "12",
                                    md: "6"
                                  }, {
                                    default: _withCtx$1(() => [
                                      _createVNode$1(_component_VSwitch, {
                                        modelValue: draft.value.global_proxy_delete,
                                        "onUpdate:modelValue": _cache[9] || (_cache[9] = $event => ((draft.value.global_proxy_delete) = $event)),
                                        label: "全局动态删种",
                                        color: "primary",
                                        inset: "",
                                        "hide-details": ""
                                      }, null, 8, ["modelValue"])
                                    ]),
                                    _: 1
                                  }),
                                  (draft.value.global_proxy_delete)
                                    ? (_openBlock$1(), _createBlock$1(_component_VCol, {
                                        key: 0,
                                        cols: "12",
                                        md: "6"
                                      }, {
                                        default: _withCtx$1(() => [
                                          _createVNode$1(_component_VTextField, {
                                            modelValue: draft.value.global_delete_size_range,
                                            "onUpdate:modelValue": _cache[10] || (_cache[10] = $event => ((draft.value.global_delete_size_range) = $event)),
                                            label: "全局动态删种阈值（GB）",
                                            hint: "如 350-500，达到上限后清理至下限",
                                            "persistent-hint": ""
                                          }, null, 8, ["modelValue"])
                                        ]),
                                        _: 1
                                      }))
                                    : _createCommentVNode$1("", true)
                                ]),
                                _: 1
                              }))
                            : _createCommentVNode$1("", true),
                          _createVNode$1(_component_VRow, null, {
                            default: _withCtx$1(() => [
                              (_openBlock$1(true), _createElementBlock$1(_Fragment$1, null, _renderList$1(section.fields, (field) => {
                                return (_openBlock$1(), _createBlock$1(_component_VCol, {
                                  key: field.key,
                                  cols: "12",
                                  md: "6"
                                }, {
                                  default: _withCtx$1(() => [
                                    (field.type === 'switch')
                                      ? (_openBlock$1(), _createBlock$1(_component_VSwitch, {
                                          key: 0,
                                          modelValue: draft.value.global_defaults[field.key],
                                          "onUpdate:modelValue": $event => ((draft.value.global_defaults[field.key]) = $event),
                                          label: fieldLabel(field),
                                          color: "primary",
                                          "hide-details": "",
                                          inset: ""
                                        }, null, 8, ["modelValue", "onUpdate:modelValue", "label"]))
                                      : (['select', 'downloader'].includes(field.type))
                                        ? (_openBlock$1(), _createBlock$1(_component_VSelect, {
                                            key: 1,
                                            modelValue: draft.value.global_defaults[field.key],
                                            "onUpdate:modelValue": $event => ((draft.value.global_defaults[field.key]) = $event),
                                            items: field.type === 'downloader' ? __props.downloaders : field.items,
                                            label: fieldLabel(field),
                                            hint: field.hint,
                                            "persistent-hint": Boolean(field.hint),
                                            rules: fieldRules(field)
                                          }, null, 8, ["modelValue", "onUpdate:modelValue", "items", "label", "hint", "persistent-hint", "rules"]))
                                        : (_openBlock$1(), _createBlock$1(_component_VTextField, {
                                            key: 2,
                                            modelValue: draft.value.global_defaults[field.key],
                                            "onUpdate:modelValue": $event => ((draft.value.global_defaults[field.key]) = $event),
                                            label: fieldLabel(field),
                                            type: field.type || 'text',
                                            min: field.min,
                                            max: field.max,
                                            step: field.step || 'any',
                                            hint: field.hint,
                                            "persistent-hint": Boolean(field.hint),
                                            rules: fieldRules(field),
                                            clearable: !field.required
                                          }, null, 8, ["modelValue", "onUpdate:modelValue", "label", "type", "min", "max", "step", "hint", "persistent-hint", "rules", "clearable"]))
                                  ]),
                                  _: 2
                                }, 1024))
                              }), 128))
                            ]),
                            _: 2
                          }, 1024)
                        ])
                      }), 64))
                    ]),
                    _: 1
                  }, 8, ["disabled"]))
                : _createCommentVNode$1("", true)
            ]),
            _: 1
          }),
          _createVNode$1(_component_VDivider),
          _createVNode$1(_component_VCardActions, { class: "brush-settings__actions" }, {
            default: _withCtx$1(() => [
              _createVNode$1(_component_VSpacer),
              _createVNode$1(_component_VBtn, {
                variant: "text",
                disabled: __props.saving,
                onClick: _cache[11] || (_cache[11] = $event => (emit('update:modelValue', false)))
              }, {
                default: _withCtx$1(() => [...(_cache[21] || (_cache[21] = [
                  _createTextVNode$1("取消", -1)
                ]))]),
                _: 1
              }, 8, ["disabled"]),
              _createVNode$1(_component_VBtn, {
                color: "primary",
                variant: "flat",
                disabled: !canSave.value,
                loading: __props.saving || validating.value,
                onClick: save
              }, {
                default: _withCtx$1(() => [...(_cache[22] || (_cache[22] = [
                  _createTextVNode$1("保存设置", -1)
                ]))]),
                _: 1
              }, 8, ["disabled", "loading"])
            ]),
            _: 1
          })
        ]),
        _: 1
      })
    ]),
    _: 1
  }, 8, ["model-value", "persistent"]))
}
}

};
const BrushSettingsDialog = /*#__PURE__*/_export_sfc(_sfc_main$1, [['__scopeId',"data-v-b14e99a2"]]);

const {resolveComponent:_resolveComponent,createVNode:_createVNode,createElementVNode:_createElementVNode,toDisplayString:_toDisplayString,createTextVNode:_createTextVNode,withCtx:_withCtx,openBlock:_openBlock,createBlock:_createBlock,createCommentVNode:_createCommentVNode,unref:_unref,Fragment:_Fragment,createElementBlock:_createElementBlock,renderList:_renderList,withModifiers:_withModifiers,normalizeClass:_normalizeClass,mergeProps:_mergeProps,normalizeStyle:_normalizeStyle} = await importShared('vue');


const _hoisted_1 = { class: "brushflow-page__header" };
const _hoisted_2 = { class: "brushflow-page__identity" };
const _hoisted_3 = { class: "brushflow-page__actions" };
const _hoisted_4 = { class: "d-flex flex-wrap align-center ga-2 mb-2" };
const _hoisted_5 = { class: "text-body-2" };
const _hoisted_6 = {
  key: 3,
  class: "text-body-2"
};
const _hoisted_7 = {
  key: 0,
  class: "text-body-2 mb-2"
};
const _hoisted_8 = {
  key: 3,
  class: "brushflow-loading"
};
const _hoisted_9 = {
  key: 4,
  class: "brushflow-empty"
};
const _hoisted_10 = {
  key: 5,
  class: "brushflow-content"
};
const _hoisted_11 = { class: "brushflow-stat-grid brushflow-global-stats" };
const _hoisted_12 = { key: 0 };
const _hoisted_13 = { key: 1 };
const _hoisted_14 = { class: "brushflow-panel__head" };
const _hoisted_15 = { class: "text-subtitle-1 font-weight-medium" };
const _hoisted_16 = { class: "text-medium-emphasis" };
const _hoisted_17 = { class: "brushflow-site-actions" };
const _hoisted_18 = { class: "brushflow-site-table-wrap" };
const _hoisted_19 = { class: "brushflow-site-table" };
const _hoisted_20 = ["onClick"];
const _hoisted_21 = { class: "brushflow-site-name" };
const _hoisted_22 = ["aria-current", "onClick"];
const _hoisted_23 = { "data-label": "分享率 / 目标" };
const _hoisted_24 = { key: 0 };
const _hoisted_25 = { key: 1 };
const _hoisted_26 = { "data-label": "下载 / 排队 / 做种" };
const _hoisted_27 = { key: 0 };
const _hoisted_28 = { "data-label": "近 30 分钟上传" };
const _hoisted_29 = {
  "data-label": "运行情况",
  class: "brushflow-site-status"
};
const _hoisted_30 = { key: 0 };
const _hoisted_31 = { key: 1 };
const _hoisted_32 = { class: "brushflow-layout" };
const _hoisted_33 = { class: "brushflow-task-head" };
const _hoisted_34 = { class: "brushflow-task-head__identity" };
const _hoisted_35 = { class: "brushflow-task-head__title" };
const _hoisted_36 = { class: "brushflow-task-head__actions" };
const _hoisted_37 = { class: "brushflow-stat-grid" };
const _hoisted_38 = { class: "brushflow-overview-grid" };
const _hoisted_39 = { class: "brushflow-panel__head" };
const _hoisted_40 = { class: "brushflow-facts" };
const _hoisted_41 = { key: 0 };
const _hoisted_42 = { key: 1 };
const _hoisted_43 = { key: 2 };
const _hoisted_44 = { class: "brushflow-panel__head" };
const _hoisted_45 = { class: "text-body-2 text-medium-emphasis" };
const _hoisted_46 = { class: "brushflow-run-summary" };
const _hoisted_47 = { class: "brushflow-panel__head" };
const _hoisted_48 = { class: "brushflow-panel__title-row" };
const _hoisted_49 = { class: "torrent-title-cell" };
const _hoisted_50 = { class: "brushflow-mobile-torrents" };
const _hoisted_51 = { class: "brushflow-mobile-torrent__head" };
const _hoisted_52 = { class: "brushflow-mobile-torrent__meta" };
const _hoisted_53 = { class: "text-body-2 text-medium-emphasis" };
const _hoisted_54 = {
  key: 0,
  class: "brushflow-table-empty"
};
const _hoisted_55 = { key: 0 };
const _hoisted_56 = { class: "text-body-2" };
const _hoisted_57 = { class: "brushflow-diagnostic-head" };
const _hoisted_58 = { class: "text-subtitle-1 font-weight-medium" };
const _hoisted_59 = { class: "text-body-2 text-medium-emphasis" };
const _hoisted_60 = {
  key: 1,
  class: "brushflow-diagnostic-grid"
};
const _hoisted_61 = { class: "brushflow-pipeline" };
const _hoisted_62 = { class: "brushflow-pipeline__index" };
const _hoisted_63 = {
  key: 0,
  class: "brushflow-reasons"
};
const _hoisted_64 = { class: "brushflow-reason__track" };
const _hoisted_65 = {
  key: 1,
  class: "brushflow-table-empty"
};
const _hoisted_66 = { class: "brushflow-events" };
const _hoisted_67 = {
  key: 0,
  class: "text-error"
};
const _hoisted_68 = {
  key: 0,
  class: "brushflow-table-empty"
};
const _hoisted_69 = { class: "brushflow-facts brushflow-facts--two" };

const {computed,inject,nextTick,onMounted,onUnmounted,ref,watch} = await importShared('vue');


const _sfc_main = {
  __name: 'BrushFlowWorkbench',
  props: {
  api: { type: Object, default: () => ({}) },
  pluginId: { type: String, default: 'OriechoBrushFlow' },
  initialTab: { type: String, default: 'overview' },
  showClose: { type: Boolean, default: false },
  compact: { type: Boolean, default: false },
},
  emits: ['close', 'action'],
  setup(__props, { expose: __expose, emit: __emit }) {

const props = __props;

const emit = __emit;
const loading = ref(true);
const taskLoading = ref(false);
const saving = ref(false);
const error = ref('');
// 首次状态请求完成前，不把默认的停用状态展示给用户。
const statusLoaded = ref(false);
const status = ref({
  enabled: false,
  show_sidebar_nav: true,
  rotation_mode: 'parallel',
  manual_focus_site_id: null,
  summary: {},
  tasks: [],
  options: { sites: [], downloaders: [] },
});
const taskDetail = ref(null);
const selectedTaskId = ref('');
const siteSort = ref('priority');
const detailPanel = ref(null);
const activeTab = ref(props.initialTab);
const torrentState = ref('active');
const torrentPage = ref(1);
const settingsOpen = ref(false);
const settingsTrigger = ref(null);
const settingsLeaving = ref(false);
const settingsLoading = ref(false);
const settingsReady = ref(false);
const settingsError = ref('');
const settingsSnapshot = ref(null);
const settingsSites = ref([]);
const settingsDownloaders = ref([]);
let settingsRequestId = 0;
const hostToast = inject('moviepilot:toast', null);
let refreshTimer;

const pluginBase = computed(() => `plugin/${props.pluginId || 'OriechoBrushFlow'}`);
const tasks = computed(() => status.value.tasks || []);
const metrics = computed(() => status.value.metrics || {});
const downloadSlots = computed(() => status.value.download_slots || {});
const sortedTasks = computed(() => [...tasks.value].sort((left, right) => {
  if (siteSort.value === 'upload') {
    const difference = metricNumber(right.metrics, 'uploaded', -1) - metricNumber(left.metrics, 'uploaded', -1);
    if (difference) return difference
  }
  if (siteSort.value === 'ratio') {
    const difference = comparableRatio(left) - comparableRatio(right);
    if (difference) return difference
  }
  if (siteSort.value === 'error') {
    const difference = Number(hasTaskError(right)) - Number(hasTaskError(left));
    if (difference) return difference
  }
  const enabled = Number(Boolean(right.enabled)) - Number(Boolean(left.enabled));
  if (enabled) return enabled
  if (left.priority_order != null && right.priority_order != null) return left.priority_order - right.priority_order
  return Number(left.priority ?? 2) - Number(right.priority ?? 2)
    || Number(right.ratio_deficit_bytes || 0) - Number(left.ratio_deficit_bytes || 0)
}));
const siteRotation = computed(() => status.value.site_rotation || {});
const manualFocus = computed(() => status.value.manual_focus || null);
const manualFocusNotice = computed(() => {
  const focus = manualFocus.value;
  if (!focus) return ''
  const name = focus.site_name || String(focus.site_id);
  if (focus.state === 'pending') return `已手动指定 ${name}，等待当前页处理完成或调度切换。`
  if (focus.state === 'checking') return `正在检查手动重点站 ${name} 的连接。`
  if (focus.state === 'cooldown') {
    return `手动重点站 ${name} 暂不可用，已跳过本轮${focus.retry_at ? `；下次重试 ${formatDateTime(focus.retry_at * 1000)}` : ''}。`
  }
  return ''
});
const sequentialMode = computed(() => (status.value.rotation_mode || siteRotation.value.mode) === 'sequential');
const ratioPhase = computed(() => siteRotation.value.phase === 'ratio');
const focusSwitchText = computed(() => ({
  manual: '手动指定，清空后恢复自动',
  ratio: '达标后切换',
}[siteRotation.value.phase] || ''));
const rotationRetrying = computed(() => sequentialMode.value && Boolean(siteRotation.value.retry_at));
const selectedTask = computed(() => tasks.value.find(item => item.id === selectedTaskId.value) || null);
const accountRefreshTitle = computed(() => ({
  traffic_unparsed: '账号流量数据未解析',
  account_unrecognized: '账号登录状态未识别',
  request_failed: '账号数据请求失败',
}[selectedTask.value?.account_refresh?.reason] || '账号数据暂不可用'));
const downloaderConflict = computed(() => selectedTask.value?.downloader_conflict || taskDetail.value?.downloader_conflict || '');
const selectedState = computed(() => downloaderConflict.value
  ? { text: '原下载器仍在托管', color: 'warning', icon: 'mdi-alert-outline' }
  : isRetryingTask(selectedTask.value)
  ? { text: '等待站点重试', color: 'warning', icon: 'mdi-refresh' }
  : taskStateMeta(selectedTask.value?.state, sequentialMode.value && selectedTask.value?.is_focus === false));
const rawTaskConfig = computed(() => taskDetail.value?.task?.config || taskDetail.value?.task || {});
const taskConfig = computed(() => taskDetail.value?.effective_config || effectiveTask(rawTaskConfig.value, status.value.global_defaults));
const priorityText = computed(() => taskPriorityText(selectedTask.value));
const taskRuns = computed(() => taskDetail.value?.runs || []);
const latestBrushRun = computed(() => taskRuns.value.find(item => item.kind === 'brush') || null);
const torrentData = computed(() => taskDetail.value?.torrents || { items: [], total: 0, page: 1, page_size: 50 });
const totalTorrentPages = computed(() => Math.max(Math.ceil(torrentData.value.total / torrentData.value.page_size), 1));
const seedingPercent = computed(() => {
  const limit = Number(taskConfig.value.disksize || 0) * 1024 ** 3;
  if (!limit) return 0
  return Math.min(Math.round((Number(selectedTask.value?.seeding_size || 0) * 100) / limit), 100)
});
const reasonEntries = computed(() => {
  const reasons = latestBrushRun.value?.reason_counts || {};
  return Object.entries(reasons)
    .map(([label, count]) => ({ label, count: Number(count || 0) }))
    .sort((left, right) => right.count - left.count)
});
const maxReasonCount = computed(() => Math.max(1, ...reasonEntries.value.map(item => item.count)));
const pipelineStages = computed(() => {
  const run = latestBrushRun.value;
  if (!run) return []
  const sourceCount = Number(run.source_count || 0);
  const candidateCount = Number(run.candidate_count || 0);
  const addedCount = Number(run.added_count || 0);
  return [
    { title: '获取站点种子', detail: taskConfig.value.rss_support ? 'RSS' : '站点列表页', count: sourceCount },
    { title: '排除订阅内容', detail: `排除 ${Number(run.subscription_excluded || 0)} 个`, count: candidateCount },
    { title: '选种条件过滤', detail: `过滤 ${Number(run.filtered_count || 0)} 个`, count: addedCount },
    { title: '添加下载任务', detail: run.success === false ? '执行失败' : '下载器已确认', count: addedCount },
  ]
});

const torrentHeaders = [
  { title: '种子', key: 'title', sortable: false },
  { title: '状态', key: 'status', sortable: false },
  { title: '大小', key: 'size', align: 'end', sortable: false },
  { title: '上传', key: 'uploaded', align: 'end', sortable: false },
  { title: '分享率', key: 'ratio', align: 'end', sortable: false },
  { title: '处理条件', key: 'policy', sortable: false },
];

// 通过宿主注入的 Toast 显示操作结果，不在插件内创建第二套通知层。
function notify(text, color = 'success') {
  const method = ['error', 'info', 'warning', 'success'].includes(color) ? color : 'success';
  if (typeof hostToast?.[method] === 'function') {
    hostToast[method](text);
  } else if (method === 'error') {
    error.value = text;
  }
}

// 从插件状态接口加载全局设置和任务摘要。
async function loadStatus({ preserveSelection = true, loadDetail = true } = {}) {
  loading.value = true;
  error.value = '';
  try {
    const data = unwrapResponse(await props.api.get(`${pluginBase.value}/status`));
    status.value = data || status.value;
    statusLoaded.value = true;
    const selectedStillExists = tasks.value.some(item => item.id === selectedTaskId.value);
    if (!preserveSelection || !selectedStillExists) selectedTaskId.value = sortedTasks.value[0]?.id || '';
    if (loadDetail && selectedTaskId.value) await loadTaskDetail();
  } catch (err) {
    error.value = err?.message || '加载刷流任务失败';
  } finally {
    loading.value = false;
  }
}

// 加载选中任务的配置、诊断记录和当前分页种子。
async function loadTaskDetail() {
  if (!selectedTaskId.value) {
    taskDetail.value = null;
    return
  }
  const taskId = selectedTaskId.value;
  taskLoading.value = true;
  try {
    const query = `state=${torrentState.value}&page=${torrentPage.value}&page_size=50`;
    const detail = unwrapResponse(
      await props.api.get(`${pluginBase.value}/tasks/${taskId}?${query}`),
    );
    if (taskId === selectedTaskId.value) taskDetail.value = detail;
  } catch (err) {
    if (taskId === selectedTaskId.value) error.value = err?.message || '加载任务详情失败';
  } finally {
    if (taskId === selectedTaskId.value) taskLoading.value = false;
  }
}

// 切换任务并从第一页加载对应明细。
async function selectTask(taskId) {
  if (!taskId || taskId === selectedTaskId.value) return
  selectedTaskId.value = taskId;
  taskDetail.value = null;
  torrentState.value = 'active';
  torrentPage.value = 1;
  await loadTaskDetail();
}

async function openTask(taskId) {
  await selectTask(taskId);
  detailPanel.value?.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

// 同时刷新任务摘要和当前任务详情。
async function refreshAll(showMessage = false) {
  await loadStatus();
  if (showMessage) notify('刷流任务数据已刷新');
  emit('action');
}

// 打开设置时重新读取完整配置；轮询不会覆盖正在编辑的草稿。
async function openSettings() {
  if (saving.value) return
  settingsLeaving.value = false;
  settingsOpen.value = true;
  settingsSnapshot.value = null;
  await loadSettings();
}

function closeSettings() {
  if (saving.value) return
  settingsRequestId += 1;
  settingsLeaving.value = settingsOpen.value || settingsLeaving.value;
  settingsOpen.value = false;
  settingsReady.value = false;
  settingsLoading.value = false;
  settingsError.value = '';
  settingsSnapshot.value = null;
  settingsSites.value = [];
  settingsDownloaders.value = [];
}

async function restoreSettingsFocus() {
  await nextTick();
  if (settingsOpen.value || settingsLeaving.value || saving.value) return
  const trigger = settingsTrigger.value?.$el || settingsTrigger.value;
  if (trigger?.isConnected !== false && !trigger?.disabled) trigger?.focus({ preventScroll: true });
}

async function onSettingsClosed() {
  settingsLeaving.value = false;
  await restoreSettingsFocus();
}

async function loadSettings() {
  if (!settingsOpen.value || saving.value) return
  const requestId = ++settingsRequestId;
  settingsLoading.value = true;
  settingsReady.value = false;
  settingsError.value = '';
  try {
    const data = unwrapResponse(await props.api.get(`${pluginBase.value}/status`));
    if (requestId !== settingsRequestId || !settingsOpen.value) return
    const snapshot = settingsFromStatus(data);
    settingsSnapshot.value = snapshot;
    settingsSites.value = data.options.sites.map(site => ({ ...site }));
    settingsDownloaders.value = data.options.downloaders.map(item => ({ ...item }));
    settingsReady.value = true;
    status.value = data;
    statusLoaded.value = true;
  } catch (err) {
    if (requestId === settingsRequestId && settingsOpen.value) {
      settingsError.value = err?.message || '读取刷流设置失败，请重试';
    }
  } finally {
    if (requestId === settingsRequestId) settingsLoading.value = false;
  }
}

async function saveSettings(draft) {
  if (!settingsOpen.value || !settingsReady.value || settingsLoading.value || saving.value) return
  saving.value = true;
  settingsError.value = '';
  try {
    const payload = normalizeSettings(draft);
    const data = unwrapResponse(await props.api.post(`${pluginBase.value}/settings`, payload));
    if (data) status.value = data;
    settingsLeaving.value = true;
    settingsOpen.value = false;
    settingsReady.value = false;
    settingsSnapshot.value = null;
    settingsSites.value = [];
    settingsDownloaders.value = [];
    settingsRequestId += 1;
    await loadStatus();
    notify('刷流设置已保存');
  } catch (err) {
    settingsError.value = err?.message || '保存刷流设置失败，请重试';
  } finally {
    saving.value = false;
    if (!settingsOpen.value) await restoreSettingsFocus();
  }
}

// 切换当前任务启停状态并立即重建宿主调度。
async function toggleSelectedTask() {
  if (!selectedTask.value) return
  saving.value = true;
  try {
    unwrapResponse(
      await props.api.post(`${pluginBase.value}/tasks/${selectedTaskId.value}/state`, {
        enabled: !selectedTask.value.enabled,
      }),
    );
    await loadStatus();
    notify(selectedTask.value?.enabled ? '站点管理已启用' : '已停止选种与检查，下载器任务继续运行');
  } catch (err) {
    notify(err?.message || '更新任务状态失败', 'error');
  } finally {
    saving.value = false;
  }
}

// 提交一次刷流刷新或种子检查操作。
async function runOperation(operation) {
  if (!selectedTaskId.value) return
  saving.value = true;
  try {
    const path = operation === 'brush' ? 'run' : 'check';
    unwrapResponse(await props.api.post(`${pluginBase.value}/tasks/${selectedTaskId.value}/${path}`, {}));
    await loadStatus();
    notify(operation === 'brush' ? '刷流刷新已提交' : '种子检查已提交');
  } catch (err) {
    notify(err?.message || '提交任务失败', 'error');
  } finally {
    saving.value = false;
  }
}

// 切换活跃或已删除种子分页视图。
async function changeTorrentState(value) {
  torrentState.value = value;
  torrentPage.value = 1;
  await loadTaskDetail();
}

// 加载指定页的种子记录。
async function changeTorrentPage(value) {
  torrentPage.value = value;
  await loadTaskDetail();
}

// 根据配置生成当前种子的下一项处理条件摘要。
function torrentPolicy(item) {
  if (item.deleted) return '已删除'
  if (item.hit_and_run && taskConfig.value.hr_seed_time) return `H&R ${taskConfig.value.hr_seed_time} 小时`
  if (taskConfig.value.seed_time) return `${taskConfig.value.seed_time} 小时后检查`
  if (taskConfig.value.seed_ratio) return `分享率 ${taskConfig.value.seed_ratio}`
  return taskConfig.value.proxy_delete ? '动态删种托管' : '等待删除条件'
}

// 返回种子当前下载或做种状态文本。
function torrentStateText(item) {
  const progress = torrentProgress(item);
  if (item.deleted) return '已删除'
  return progress >= 100 ? '做种' : `下载 ${progress}%`
}

// 将站点分享率状态格式化为当前值、无限或暂无数据。
function formatSiteRatio(siteRatio) {
  if (!siteRatio?.available) return '-'
  if (siteRatio.unlimited) return '∞'
  return Number(siteRatio.current || 0).toFixed(2)
}

function taskPriorityText(task) {
  return task?.priority_reason || (task?.priority === -1 ? '手动重点' : task?.priority === 1 ? '分享率未达标' : '普通轮转')
}

function hasTaskError(task) {
  return Boolean(task.downloader_conflict) || isRetryingTask(task) || task.state === 'error' || Boolean(task.last_error) || task.last_run?.success === false || Boolean(task.metrics?.gap)
}

function isRetryingTask(task) {
  return rotationRetrying.value && task?.enabled && task.site_id === siteRotation.value.site_id
}

function comparableRatio(task) {
  if (!task.site_ratio?.available || task.site_ratio.unlimited) return Infinity
  return Number(task.site_ratio.current || 0)
}

function metricNumber(value, key, fallback = null) {
  const number = value?.[key];
  return number == null || !Number.isFinite(Number(number)) ? fallback : Number(number)
}

function metricBytes(value, key = 'uploaded') {
  const number = metricNumber(value, key);
  if (number == null || (key === 'uploaded' && !Number(value?.seconds))) return '—'
  return `${value?.partial && key === 'uploaded' ? '≥ ' : ''}${formatBytes(number)}`
}

function metricCount(value, key) {
  return metricNumber(value, key) ?? '—'
}

function metricWindow(value) {
  const seconds = Number(value?.seconds || 0);
  if (!seconds) return value?.gap ? '采样中断，等待恢复' : '等待累积样本'
  const duration = seconds < 60 ? `${Math.round(seconds)} 秒` : `${(seconds / 60).toFixed(1)} 分钟`;
  const qualifier = value?.gap ? ' · 存在采样缺口' : seconds < 1200 ? ' · 不足 20 分钟' : '';
  return `实测 ${duration}${qualifier}${value?.partial ? ' · 部分任务未统计' : ''}`
}

function sampledTime(value) {
  if (!value) return '等待采样'
  return formatDateTime(typeof value === 'number' && value < 1e12 ? value * 1000 : value)
}

function taskStatusText(task) {
  if (task.downloader_conflict) return task.downloader_conflict
  if (!task.enabled) return '已停止选种与检查'
  if (isRetryingTask(task)) return `站点连接失败，等待重试（${siteRotation.value.retry_count || 0}/5）`
  if (task.last_error || task.last_run?.success === false) return '运行异常，查看诊断'
  return task.metrics?.reason || taskStateMeta(task.state, sequentialMode.value && task.is_focus === false).text
}

watch(
  () => props.initialTab,
  value => {
    if (value) activeTab.value = value;
  },
);

onMounted(async () => {
  await loadStatus({ preserveSelection: false });
  refreshTimer = window.setInterval(() => {
    if (!settingsOpen.value && document.visibilityState === 'visible') loadStatus();
  }, 30000);
});

onUnmounted(() => {
  settingsRequestId += 1;
  settingsSnapshot.value = null;
  if (refreshTimer) window.clearInterval(refreshTimer);
});

__expose({ loadStatus, refreshAll, loading, saving });

return (_ctx, _cache) => {
  const _component_VIcon = _resolveComponent("VIcon");
  const _component_VBtn = _resolveComponent("VBtn");
  const _component_VChip = _resolveComponent("VChip");
  const _component_VAlert = _resolveComponent("VAlert");
  const _component_VSkeletonLoader = _resolveComponent("VSkeletonLoader");
  const _component_VSheet = _resolveComponent("VSheet");
  const _component_VSelect = _resolveComponent("VSelect");
  const _component_VAvatar = _resolveComponent("VAvatar");
  const _component_VTooltip = _resolveComponent("VTooltip");
  const _component_VTab = _resolveComponent("VTab");
  const _component_VTabs = _resolveComponent("VTabs");
  const _component_VDivider = _resolveComponent("VDivider");
  const _component_VProgressLinear = _resolveComponent("VProgressLinear");
  const _component_VBtnToggle = _resolveComponent("VBtnToggle");
  const _component_VDataTable = _resolveComponent("VDataTable");
  const _component_VPagination = _resolveComponent("VPagination");
  const _component_VWindowItem = _resolveComponent("VWindowItem");
  const _component_VWindow = _resolveComponent("VWindow");

  return (_openBlock(), _createElementBlock("div", {
    class: _normalizeClass(["brushflow-page", { 'brushflow-page--compact': __props.compact }])
  }, [
    _createElementVNode("header", _hoisted_1, [
      _createElementVNode("div", _hoisted_2, [
        _createVNode(_component_VIcon, {
          icon: "mdi-sync",
          color: "primary",
          size: "28"
        }),
        _createElementVNode("div", null, [
          _cache[9] || (_cache[9] = _createElementVNode("h1", null, "站点刷流", -1)),
          _createElementVNode("p", null, "公共规则 · 分享率优先 · " + _toDisplayString(sequentialMode.value ? '单站轮换' : '多站并行'), 1)
        ])
      ]),
      _createElementVNode("div", _hoisted_3, [
        _createVNode(_component_VBtn, {
          ref_key: "settingsTrigger",
          ref: settingsTrigger,
          class: "brushflow-settings-trigger",
          color: "primary",
          variant: "tonal",
          "prepend-icon": "mdi-tune-variant",
          "aria-haspopup": "dialog",
          "aria-expanded": settingsOpen.value,
          disabled: saving.value,
          onClick: openSettings
        }, {
          default: _withCtx(() => [...(_cache[10] || (_cache[10] = [
            _createTextVNode("刷流设置", -1)
          ]))]),
          _: 1
        }, 8, ["aria-expanded", "disabled"]),
        (status.value.summary.task_count)
          ? (_openBlock(), _createBlock(_component_VChip, {
              key: 0,
              size: "small",
              variant: "tonal"
            }, {
              default: _withCtx(() => [
                _createTextVNode(_toDisplayString(status.value.summary.running_count ?? status.value.summary.enabled_count ?? 0) + " / " + _toDisplayString(status.value.summary.task_count) + " 运行 ", 1)
              ]),
              _: 1
            }))
          : _createCommentVNode("", true),
        (__props.showClose)
          ? (_openBlock(), _createBlock(_component_VBtn, {
              key: 1,
              icon: "mdi-close",
              variant: "text",
              "aria-label": "关闭",
              onClick: _cache[0] || (_cache[0] = $event => (emit('close')))
            }))
          : _createCommentVNode("", true)
      ])
    ]),
    (error.value)
      ? (_openBlock(), _createBlock(_component_VAlert, {
          key: 0,
          type: "error",
          variant: "tonal",
          closable: "",
          "onClick:close": _cache[1] || (_cache[1] = $event => (error.value = ''))
        }, {
          default: _withCtx(() => [
            _createTextVNode(_toDisplayString(error.value), 1)
          ]),
          _: 1
        }))
      : _createCommentVNode("", true),
    (statusLoaded.value && !status.value.enabled)
      ? (_openBlock(), _createBlock(_component_VAlert, {
          key: 1,
          type: "warning",
          variant: "tonal"
        }, {
          default: _withCtx(() => [...(_cache[11] || (_cache[11] = [
            _createTextVNode(" 插件当前未启用，任务配置与历史仍可查看，启用后才会注册刷新和检查服务。 ", -1)
          ]))]),
          _: 1
        }))
      : _createCommentVNode("", true),
    (statusLoaded.value && sequentialMode.value)
      ? (_openBlock(), _createBlock(_component_VAlert, {
          key: 2,
          type: rotationRetrying.value || manualFocus.value?.state === 'cooldown' ? 'warning' : 'info',
          variant: "tonal"
        }, {
          default: _withCtx(() => [
            _createElementVNode("div", _hoisted_4, [
              _createElementVNode("strong", null, "当前重点站：" + _toDisplayString(siteRotation.value.site_name || '等待选择站点'), 1),
              (rotationRetrying.value)
                ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                    _createVNode(_component_VChip, {
                      size: "small",
                      variant: "tonal"
                    }, {
                      default: _withCtx(() => [
                        _createTextVNode("站点连接失败 · 已重试 " + _toDisplayString(siteRotation.value.retry_count || 0) + "/5", 1)
                      ]),
                      _: 1
                    }),
                    _createElementVNode("span", _hoisted_5, "下次重试 " + _toDisplayString(_unref(formatDateTime)(siteRotation.value.retry_at * 1000)) + "；五次仍失败后跳过", 1)
                  ], 64))
                : (focusSwitchText.value)
                  ? (_openBlock(), _createBlock(_component_VChip, {
                      key: 1,
                      size: "small",
                      variant: "tonal"
                    }, {
                      default: _withCtx(() => [
                        _createTextVNode(_toDisplayString(focusSwitchText.value), 1)
                      ]),
                      _: 1
                    }))
                  : (siteRotation.value.deadline_at)
                    ? (_openBlock(), _createBlock(_component_VChip, {
                        key: 2,
                        size: "small",
                        variant: "tonal"
                      }, {
                        default: _withCtx(() => [
                          _createTextVNode("预计切换 " + _toDisplayString(_unref(formatDateTime)(siteRotation.value.deadline_at)), 1)
                        ]),
                        _: 1
                      }))
                    : _createCommentVNode("", true),
              (!rotationRetrying.value && !focusSwitchText.value && siteRotation.value.deadline_at && siteRotation.value.remaining_seconds != null)
                ? (_openBlock(), _createElementBlock("span", _hoisted_6, _toDisplayString(Number(siteRotation.value.remaining_seconds) > 0 ? '剩余约 ' : '') + _toDisplayString(_unref(formatRemainingTime)(siteRotation.value.remaining_seconds)), 1))
                : _createCommentVNode("", true)
            ]),
            (manualFocusNotice.value)
              ? (_openBlock(), _createElementBlock("div", _hoisted_7, _toDisplayString(manualFocusNotice.value), 1))
              : _createCommentVNode("", true),
            _cache[12] || (_cache[12] = _createTextVNode(" 每次仅当前站新增。自动调度：分享率未达标站点优先，全部达标后轮转；账号数据未知时需先刷新。旧种继续下载、做种并按删除规则清理。 ", -1))
          ]),
          _: 1
        }, 8, ["type"]))
      : _createCommentVNode("", true),
    (loading.value && !tasks.value.length)
      ? (_openBlock(), _createElementBlock("div", _hoisted_8, [
          _createVNode(_component_VSkeletonLoader, { type: "list-item-three-line, list-item-three-line, article" })
        ]))
      : (!tasks.value.length)
        ? (_openBlock(), _createElementBlock("div", _hoisted_9, [
            _createVNode(_component_VIcon, {
              icon: "mdi-sync-off",
              size: "52",
              color: "medium-emphasis"
            }),
            _cache[13] || (_cache[13] = _createElementVNode("div", { class: "text-h6" }, "还没有刷流任务", -1)),
            _cache[14] || (_cache[14] = _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "打开右上方“刷流设置”，选择站点并保存公共规则。", -1))
          ]))
        : (_openBlock(), _createElementBlock("div", _hoisted_10, [
            _createElementVNode("div", _hoisted_11, [
              _createVNode(_component_VSheet, { class: "brushflow-stat app-surface-static" }, {
                default: _withCtx(() => [
                  _cache[15] || (_cache[15] = _createElementVNode("span", null, "近 30 分钟上传", -1)),
                  _createElementVNode("strong", null, _toDisplayString(metricBytes(metrics.value)), 1),
                  _createElementVNode("small", null, _toDisplayString(metricWindow(metrics.value)), 1)
                ]),
                _: 1
              }),
              _createVNode(_component_VSheet, { class: "brushflow-stat app-surface-static" }, {
                default: _withCtx(() => [
                  _cache[16] || (_cache[16] = _createElementVNode("span", null, "当前上传", -1)),
                  _createElementVNode("strong", null, [
                    _createTextVNode(_toDisplayString(metricBytes(metrics.value, 'upspeed')), 1),
                    (metricNumber(metrics.value, 'upspeed') != null)
                      ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                          _createTextVNode("/s")
                        ], 64))
                      : _createCommentVNode("", true)
                  ]),
                  _createElementVNode("small", null, _toDisplayString(metrics.value.sampled_at ? `采样 ${sampledTime(metrics.value.sampled_at)}` : '等待采样'), 1)
                ]),
                _: 1
              }),
              _createVNode(_component_VSheet, { class: "brushflow-stat app-surface-static" }, {
                default: _withCtx(() => [
                  _cache[17] || (_cache[17] = _createElementVNode("span", null, "下载 / 排队 / 做种", -1)),
                  _createElementVNode("strong", null, _toDisplayString(metricCount(metrics.value, 'downloading')) + " / " + _toDisplayString(metricCount(metrics.value, 'queued')) + " / " + _toDisplayString(metricCount(metrics.value, 'seeding')), 1),
                  (downloadSlots.value.budget != null)
                    ? (_openBlock(), _createElementBlock("small", _hoisted_12, [
                        _createTextVNode("下载名额 " + _toDisplayString(downloadSlots.value.active ?? metricCount(metrics.value, 'downloading')) + " / " + _toDisplayString(downloadSlots.value.budget), 1),
                        (downloadSlots.value.limit != null)
                          ? (_openBlock(), _createElementBlock(_Fragment, { key: 0 }, [
                              _createTextVNode(" · 上限 " + _toDisplayString(downloadSlots.value.limit), 1)
                            ], 64))
                          : _createCommentVNode("", true)
                      ]))
                    : (_openBlock(), _createElementBlock("small", _hoisted_13, "托管任务的下载器状态"))
                ]),
                _: 1
              }),
              _createVNode(_component_VSheet, { class: "brushflow-stat app-surface-static" }, {
                default: _withCtx(() => [
                  _cache[18] || (_cache[18] = _createElementVNode("span", null, "托管空间", -1)),
                  _createElementVNode("strong", null, _toDisplayString(_unref(formatBytes)(status.value.summary.seeding_size)), 1),
                  _createElementVNode("small", null, _toDisplayString(status.value.global_disksize ? `上限 ${status.value.global_disksize} GB` : '未设插件空间上限') + " · 含未完成任务预留", 1)
                ]),
                _: 1
              })
            ]),
            _createVNode(_component_VSheet, {
              tag: "section",
              class: "brushflow-panel brushflow-sites app-surface-static"
            }, {
              default: _withCtx(() => [
                _createElementVNode("header", _hoisted_14, [
                  _createElementVNode("div", null, [
                    _createElementVNode("div", _hoisted_15, [
                      _cache[19] || (_cache[19] = _createTextVNode("全部站点 ", -1)),
                      _createElementVNode("span", _hoisted_16, _toDisplayString(tasks.value.length), 1)
                    ]),
                    _cache[20] || (_cache[20] = _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "点击站点查看种子与运行详情", -1))
                  ]),
                  _createElementVNode("div", _hoisted_17, [
                    _createVNode(_component_VSelect, {
                      modelValue: siteSort.value,
                      "onUpdate:modelValue": _cache[2] || (_cache[2] = $event => ((siteSort).value = $event)),
                      items: [
                { title: '调度优先', value: 'priority' },
                { title: '上传量高优先', value: 'upload' },
                { title: '分享率低优先', value: 'ratio' },
                { title: '异常优先', value: 'error' },
              ],
                      label: "显示排序",
                      density: "compact",
                      "hide-details": "",
                      class: "brushflow-site-sort"
                    }, null, 8, ["modelValue"])
                  ])
                ]),
                _createElementVNode("div", _hoisted_18, [
                  _createElementVNode("table", _hoisted_19, [
                    _cache[21] || (_cache[21] = _createElementVNode("thead", null, [
                      _createElementVNode("tr", null, [
                        _createElementVNode("th", null, "站点 / 优先级"),
                        _createElementVNode("th", null, "分享率 / 目标"),
                        _createElementVNode("th", null, "下载 / 排队 / 做种"),
                        _createElementVNode("th", null, "近 30 分钟上传"),
                        _createElementVNode("th", null, "运行情况")
                      ])
                    ], -1)),
                    _createElementVNode("tbody", null, [
                      (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(sortedTasks.value, (task) => {
                        return (_openBlock(), _createElementBlock("tr", {
                          key: task.id,
                          class: _normalizeClass({ 'brushflow-site-row--selected': task.id === selectedTaskId.value }),
                          onClick: $event => (openTask(task.id))
                        }, [
                          _createElementVNode("td", _hoisted_21, [
                            _createElementVNode("button", {
                              type: "button",
                              "aria-current": task.id === selectedTaskId.value ? 'true' : undefined,
                              onClick: _withModifiers($event => (openTask(task.id)), ["stop"])
                            }, _toDisplayString(task.name), 9, _hoisted_22),
                            _createElementVNode("small", null, _toDisplayString(task.downloader_conflict ? '暂停新增，继续检查原下载器' : task.enabled ? taskPriorityText(task) : '站点管理已停用'), 1)
                          ]),
                          _createElementVNode("td", _hoisted_23, [
                            _createElementVNode("strong", null, _toDisplayString(formatSiteRatio(task.site_ratio)) + " / " + _toDisplayString(task.site_ratio?.target ?? '—'), 1),
                            (Number(task.ratio_deficit_bytes) > 0)
                              ? (_openBlock(), _createElementBlock("small", _hoisted_24, "待补上传 " + _toDisplayString(_unref(formatBytes)(task.ratio_deficit_bytes)), 1))
                              : (!task.site_ratio?.available)
                                ? (_openBlock(), _createElementBlock("small", _hoisted_25, "账号数据待刷新"))
                                : _createCommentVNode("", true)
                          ]),
                          _createElementVNode("td", _hoisted_26, [
                            _createElementVNode("strong", null, _toDisplayString(metricCount(task.metrics, 'downloading')) + " / " + _toDisplayString(metricCount(task.metrics, 'queued')) + " / " + _toDisplayString(metricCount(task.metrics, 'seeding')), 1),
                            (metricNumber(task.metrics, 'paused', 0) > 0)
                              ? (_openBlock(), _createElementBlock("small", _hoisted_27, "暂停 " + _toDisplayString(task.metrics.paused) + " 个", 1))
                              : _createCommentVNode("", true)
                          ]),
                          _createElementVNode("td", _hoisted_28, [
                            _createElementVNode("strong", null, _toDisplayString(metricBytes(task.metrics)), 1),
                            _createElementVNode("small", {
                              class: _normalizeClass({ 'text-warning': task.metrics?.gap })
                            }, _toDisplayString(metricWindow(task.metrics)), 3)
                          ]),
                          _createElementVNode("td", _hoisted_29, [
                            _createElementVNode("span", {
                              class: _normalizeClass({ 'text-error': hasTaskError(task) })
                            }, _toDisplayString(taskStatusText(task)), 3),
                            (task.metrics?.gap)
                              ? (_openBlock(), _createElementBlock("small", _hoisted_30, "统计有缺口"))
                              : (task.metrics?.upspeed != null)
                                ? (_openBlock(), _createElementBlock("small", _hoisted_31, "上传 " + _toDisplayString(_unref(formatBytes)(task.metrics.upspeed)) + "/s", 1))
                                : _createCommentVNode("", true)
                          ])
                        ], 10, _hoisted_20))
                      }), 128))
                    ])
                  ])
                ])
              ]),
              _: 1
            }),
            _createElementVNode("div", _hoisted_32, [
              (selectedTask.value)
                ? (_openBlock(), _createElementBlock("main", {
                    key: 0,
                    ref_key: "detailPanel",
                    ref: detailPanel,
                    class: "brushflow-workspace"
                  }, [
                    _createElementVNode("section", _hoisted_33, [
                      _createElementVNode("div", _hoisted_34, [
                        _createVNode(_component_VAvatar, {
                          color: "primary",
                          variant: "tonal",
                          rounded: "",
                          size: "42"
                        }, {
                          default: _withCtx(() => [
                            _createVNode(_component_VIcon, { icon: "mdi-web" })
                          ]),
                          _: 1
                        }),
                        _createElementVNode("div", null, [
                          _createElementVNode("div", _hoisted_35, [
                            _createElementVNode("h2", null, _toDisplayString(selectedTask.value.name), 1),
                            (sequentialMode.value && selectedTask.value.is_focus)
                              ? (_openBlock(), _createBlock(_component_VChip, {
                                  key: 0,
                                  color: "primary",
                                  size: "small",
                                  variant: "tonal"
                                }, {
                                  default: _withCtx(() => [...(_cache[22] || (_cache[22] = [
                                    _createTextVNode("当前重点站", -1)
                                  ]))]),
                                  _: 1
                                }))
                              : _createCommentVNode("", true),
                            _createVNode(_component_VChip, {
                              color: selectedState.value.color,
                              size: "small",
                              variant: "tonal",
                              "prepend-icon": selectedState.value.icon
                            }, {
                              default: _withCtx(() => [
                                _createTextVNode(_toDisplayString(selectedState.value.text), 1)
                              ]),
                              _: 1
                            }, 8, ["color", "prepend-icon"])
                          ]),
                          _createElementVNode("p", null, _toDisplayString(selectedTask.value.site_name) + " · 最近 " + _toDisplayString(selectedTask.value.last_run ? _unref(formatDateTime)(selectedTask.value.last_run.started_at) : '尚未运行') + " · " + _toDisplayString(isRetryingTask(selectedTask.value) ? '下次重试' : '下次') + " " + _toDisplayString(isRetryingTask(selectedTask.value) ? _unref(formatDateTime)(siteRotation.value.retry_at * 1000) : selectedTask.value.next_run_at ? _unref(formatDateTime)(selectedTask.value.next_run_at) : '暂无计划'), 1)
                        ])
                      ]),
                      _createElementVNode("div", _hoisted_36, [
                        _createVNode(_component_VTooltip, { text: "立即刷新" }, {
                          activator: _withCtx(({ props: tipProps }) => [
                            _createVNode(_component_VBtn, _mergeProps(tipProps, {
                              icon: "mdi-sync",
                              variant: "text",
                              disabled: Boolean(downloaderConflict.value),
                              loading: selectedTask.value.operation === 'brush',
                              onClick: _cache[3] || (_cache[3] = $event => (runOperation('brush')))
                            }), null, 16, ["disabled", "loading"])
                          ]),
                          _: 1
                        }),
                        _createVNode(_component_VTooltip, { text: "检查种子" }, {
                          activator: _withCtx(({ props: tipProps }) => [
                            _createVNode(_component_VBtn, _mergeProps(tipProps, {
                              icon: "mdi-progress-check",
                              variant: "text",
                              loading: selectedTask.value.operation === 'check',
                              onClick: _cache[4] || (_cache[4] = $event => (runOperation('check')))
                            }), null, 16, ["loading"])
                          ]),
                          _: 1
                        }),
                        _createVNode(_component_VTooltip, {
                          text: selectedTask.value.enabled ? '停用站点管理：停止选种与检查，下载器任务继续' : '启用站点管理'
                        }, {
                          activator: _withCtx(({ props: tipProps }) => [
                            _createVNode(_component_VBtn, _mergeProps(tipProps, {
                              icon: selectedTask.value.enabled ? 'mdi-stop-circle-outline' : 'mdi-play',
                              "aria-label": selectedTask.value.enabled ? '停用站点管理' : '启用站点管理',
                              disabled: Boolean(downloaderConflict.value),
                              variant: "text",
                              onClick: toggleSelectedTask
                            }), null, 16, ["icon", "aria-label", "disabled"])
                          ]),
                          _: 1
                        }, 8, ["text"])
                      ])
                    ]),
                    (downloaderConflict.value)
                      ? (_openBlock(), _createBlock(_component_VAlert, {
                          key: 0,
                          type: "warning",
                          variant: "tonal",
                          class: "mb-3",
                          role: "alert"
                        }, {
                          default: _withCtx(() => [
                            _createTextVNode(_toDisplayString(downloaderConflict.value), 1)
                          ]),
                          _: 1
                        }))
                      : _createCommentVNode("", true),
                    _createVNode(_component_VTabs, {
                      modelValue: activeTab.value,
                      "onUpdate:modelValue": _cache[5] || (_cache[5] = $event => ((activeTab).value = $event)),
                      color: "primary",
                      class: "brushflow-tabs"
                    }, {
                      default: _withCtx(() => [
                        _createVNode(_component_VTab, { value: "overview" }, {
                          default: _withCtx(() => [...(_cache[23] || (_cache[23] = [
                            _createTextVNode("任务概览", -1)
                          ]))]),
                          _: 1
                        }),
                        _createVNode(_component_VTab, { value: "diagnostics" }, {
                          default: _withCtx(() => [...(_cache[24] || (_cache[24] = [
                            _createTextVNode("运行诊断", -1)
                          ]))]),
                          _: 1
                        }),
                        _createVNode(_component_VTab, { value: "config" }, {
                          default: _withCtx(() => [...(_cache[25] || (_cache[25] = [
                            _createTextVNode("生效规则", -1)
                          ]))]),
                          _: 1
                        })
                      ]),
                      _: 1
                    }, 8, ["modelValue"]),
                    _createVNode(_component_VDivider),
                    _createVNode(_component_VWindow, {
                      modelValue: activeTab.value,
                      "onUpdate:modelValue": _cache[7] || (_cache[7] = $event => ((activeTab).value = $event)),
                      touch: false,
                      class: "brushflow-window"
                    }, {
                      default: _withCtx(() => [
                        _createVNode(_component_VWindowItem, { value: "overview" }, {
                          default: _withCtx(() => [
                            _createElementVNode("div", _hoisted_37, [
                              _createVNode(_component_VSheet, { class: "brushflow-stat app-surface-static" }, {
                                default: _withCtx(() => [
                                  _cache[26] || (_cache[26] = _createElementVNode("span", null, "活跃种子", -1)),
                                  _createElementVNode("strong", null, _toDisplayString(selectedTask.value.statistic.active || 0), 1),
                                  _createElementVNode("small", null, _toDisplayString(selectedTask.value.statistic.unarchived || 0) + " 个待归档", 1)
                                ]),
                                _: 1
                              }),
                              _createVNode(_component_VSheet, { class: "brushflow-stat app-surface-static" }, {
                                default: _withCtx(() => [
                                  _cache[27] || (_cache[27] = _createElementVNode("span", null, "近 30 分钟上传", -1)),
                                  _createElementVNode("strong", null, _toDisplayString(metricBytes(selectedTask.value.metrics)), 1),
                                  _createElementVNode("small", null, _toDisplayString(metricWindow(selectedTask.value.metrics)), 1)
                                ]),
                                _: 1
                              }),
                              _createVNode(_component_VSheet, { class: "brushflow-stat app-surface-static" }, {
                                default: _withCtx(() => [
                                  _cache[28] || (_cache[28] = _createElementVNode("span", null, "托管空间", -1)),
                                  _createElementVNode("strong", null, _toDisplayString(_unref(formatBytes)(selectedTask.value.seeding_size)), 1),
                                  _createElementVNode("small", null, _toDisplayString(taskConfig.value.disksize ? `上限 ${taskConfig.value.disksize} GB` : '未设置体积上限'), 1),
                                  (taskConfig.value.disksize)
                                    ? (_openBlock(), _createBlock(_component_VProgressLinear, {
                                        key: 0,
                                        "model-value": seedingPercent.value,
                                        height: "4",
                                        color: "primary"
                                      }, null, 8, ["model-value"]))
                                    : _createCommentVNode("", true)
                                ]),
                                _: 1
                              }),
                              _createVNode(_component_VSheet, { class: "brushflow-stat app-surface-static" }, {
                                default: _withCtx(() => [
                                  _cache[29] || (_cache[29] = _createElementVNode("span", null, "站点分享率", -1)),
                                  _createElementVNode("strong", null, _toDisplayString(formatSiteRatio(selectedTask.value.site_ratio)), 1),
                                  _createElementVNode("small", null, _toDisplayString(selectedTask.value.site_ratio?.available ? `目标 ${Number(taskConfig.value.site_ratio_target || 0).toFixed(2)} · ${priorityText.value}` : '账号数据待刷新'), 1)
                                ]),
                                _: 1
                              })
                            ]),
                            _createElementVNode("div", _hoisted_38, [
                              _createVNode(_component_VSheet, {
                                tag: "section",
                                class: "brushflow-panel app-surface-static"
                              }, {
                                default: _withCtx(() => [
                                  _createElementVNode("header", _hoisted_39, [
                                    _cache[30] || (_cache[30] = _createElementVNode("div", null, [
                                      _createElementVNode("div", { class: "text-subtitle-1 font-weight-medium" }, "运行状态"),
                                      _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "当前任务调度与核心策略")
                                    ], -1)),
                                    _createVNode(_component_VChip, {
                                      color: selectedState.value.color,
                                      size: "small",
                                      variant: "tonal"
                                    }, {
                                      default: _withCtx(() => [
                                        _createTextVNode(_toDisplayString(selectedState.value.text), 1)
                                      ]),
                                      _: 1
                                    }, 8, ["color"])
                                  ]),
                                  _createElementVNode("dl", _hoisted_40, [
                                    _createElementVNode("div", null, [
                                      _cache[31] || (_cache[31] = _createElementVNode("dt", null, "选种优先级", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(priorityText.value), 1)
                                    ]),
                                    (sequentialMode.value)
                                      ? (_openBlock(), _createElementBlock("div", _hoisted_41, [
                                          _cache[32] || (_cache[32] = _createElementVNode("dt", null, "达标后每站驻留", -1)),
                                          _createElementVNode("dd", null, _toDisplayString(taskConfig.value.rotation_hours ?? 24) + " 小时", 1)
                                        ]))
                                      : _createCommentVNode("", true),
                                    (sequentialMode.value && selectedTask.value.is_focus && focusSwitchText.value)
                                      ? (_openBlock(), _createElementBlock("div", _hoisted_42, [
                                          _cache[33] || (_cache[33] = _createElementVNode("dt", null, "切换条件", -1)),
                                          _createElementVNode("dd", null, _toDisplayString(focusSwitchText.value), 1)
                                        ]))
                                      : (sequentialMode.value && selectedTask.value.is_focus && siteRotation.value.deadline_at)
                                        ? (_openBlock(), _createElementBlock("div", _hoisted_43, [
                                            _cache[34] || (_cache[34] = _createElementVNode("dt", null, "预计切换", -1)),
                                            _createElementVNode("dd", null, _toDisplayString(_unref(formatDateTime)(siteRotation.value.deadline_at)), 1)
                                          ]))
                                        : _createCommentVNode("", true),
                                    _createElementVNode("div", null, [
                                      _cache[35] || (_cache[35] = _createElementVNode("dt", null, "刷新周期", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.cron || `每 ${taskConfig.value.brush_interval} 分钟`), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[36] || (_cache[36] = _createElementVNode("dt", null, "检查周期", -1)),
                                      _createElementVNode("dd", null, "每 " + _toDisplayString(taskConfig.value.check_interval) + " 分钟", 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[37] || (_cache[37] = _createElementVNode("dt", null, "开启时段", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.active_time_range || '全天'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[38] || (_cache[38] = _createElementVNode("dt", null, "站点分享率", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(formatSiteRatio(selectedTask.value.site_ratio)) + " / " + _toDisplayString(taskConfig.value.site_ratio_target || '不限'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[39] || (_cache[39] = _createElementVNode("dt", null, "选种来源", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.rss_support ? 'RSS' : '站点列表页'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[40] || (_cache[40] = _createElementVNode("dt", null, "促销要求", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.freeleech === '2xfree' ? '2X 免费' : taskConfig.value.freeleech === 'free' ? '免费' : '全部'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[41] || (_cache[41] = _createElementVNode("dt", null, "删种策略", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.proxy_delete ? `动态 ${taskConfig.value.delete_size_range || '-' } GB` : '满足任一条件'), 1)
                                    ])
                                  ])
                                ]),
                                _: 1
                              }),
                              _createVNode(_component_VSheet, {
                                tag: "section",
                                class: "brushflow-panel app-surface-static"
                              }, {
                                default: _withCtx(() => [
                                  _createElementVNode("header", _hoisted_44, [
                                    _createElementVNode("div", null, [
                                      _cache[42] || (_cache[42] = _createElementVNode("div", { class: "text-subtitle-1 font-weight-medium" }, "最近一次刷新", -1)),
                                      _createElementVNode("div", _hoisted_45, _toDisplayString(latestBrushRun.value ? `${_unref(formatDateTime)(latestBrushRun.value.started_at)} · ${_unref(formatDuration)(latestBrushRun.value.started_at, latestBrushRun.value.finished_at)}` : '暂无运行记录'), 1)
                                    ]),
                                    (latestBrushRun.value)
                                      ? (_openBlock(), _createBlock(_component_VChip, {
                                          key: 0,
                                          color: latestBrushRun.value.success === false ? 'error' : 'success',
                                          size: "small",
                                          variant: "tonal"
                                        }, {
                                          default: _withCtx(() => [
                                            _createTextVNode(_toDisplayString(latestBrushRun.value.success === false ? '失败' : '完成'), 1)
                                          ]),
                                          _: 1
                                        }, 8, ["color"]))
                                      : _createCommentVNode("", true)
                                  ]),
                                  _createElementVNode("div", _hoisted_46, [
                                    _createElementVNode("div", null, [
                                      _cache[43] || (_cache[43] = _createElementVNode("span", null, "站点候选", -1)),
                                      _createElementVNode("strong", null, _toDisplayString(latestBrushRun.value?.source_count || 0), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[44] || (_cache[44] = _createElementVNode("span", null, "规则过滤", -1)),
                                      _createElementVNode("strong", null, _toDisplayString(latestBrushRun.value?.filtered_count || 0), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[45] || (_cache[45] = _createElementVNode("span", null, "新增下载", -1)),
                                      _createElementVNode("strong", null, _toDisplayString(latestBrushRun.value?.added_count || 0), 1)
                                    ])
                                  ]),
                                  (latestBrushRun.value?.error)
                                    ? (_openBlock(), _createBlock(_component_VAlert, {
                                        key: 0,
                                        type: "error",
                                        variant: "tonal",
                                        density: "compact"
                                      }, {
                                        default: _withCtx(() => [
                                          _createTextVNode(_toDisplayString(latestBrushRun.value.error), 1)
                                        ]),
                                        _: 1
                                      }))
                                    : _createCommentVNode("", true),
                                  _createVNode(_component_VBtn, {
                                    variant: "text",
                                    color: "primary",
                                    "append-icon": "mdi-arrow-right",
                                    onClick: _cache[6] || (_cache[6] = $event => (activeTab.value = 'diagnostics'))
                                  }, {
                                    default: _withCtx(() => [...(_cache[46] || (_cache[46] = [
                                      _createTextVNode(" 查看运行诊断 ", -1)
                                    ]))]),
                                    _: 1
                                  })
                                ]),
                                _: 1
                              })
                            ]),
                            _createVNode(_component_VSheet, {
                              tag: "section",
                              class: "brushflow-panel brushflow-torrents app-surface-static"
                            }, {
                              default: _withCtx(() => [
                                _createElementVNode("header", _hoisted_47, [
                                  _createElementVNode("div", null, [
                                    _createElementVNode("div", _hoisted_48, [
                                      _cache[47] || (_cache[47] = _createElementVNode("span", { class: "text-subtitle-1 font-weight-medium" }, "托管种子", -1)),
                                      _createVNode(_component_VChip, {
                                        size: "x-small",
                                        variant: "tonal"
                                      }, {
                                        default: _withCtx(() => [
                                          _createTextVNode(_toDisplayString(torrentData.value.total), 1)
                                        ]),
                                        _: 1
                                      })
                                    ]),
                                    _cache[48] || (_cache[48] = _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "当前任务独立记录", -1))
                                  ]),
                                  _createVNode(_component_VBtnToggle, {
                                    "model-value": torrentState.value,
                                    mandatory: "",
                                    color: "primary",
                                    density: "compact",
                                    "onUpdate:modelValue": changeTorrentState
                                  }, {
                                    default: _withCtx(() => [
                                      _createVNode(_component_VBtn, { value: "active" }, {
                                        default: _withCtx(() => [...(_cache[49] || (_cache[49] = [
                                          _createTextVNode("活跃", -1)
                                        ]))]),
                                        _: 1
                                      }),
                                      _createVNode(_component_VBtn, { value: "deleted" }, {
                                        default: _withCtx(() => [...(_cache[50] || (_cache[50] = [
                                          _createTextVNode("已删除", -1)
                                        ]))]),
                                        _: 1
                                      }),
                                      _createVNode(_component_VBtn, { value: "all" }, {
                                        default: _withCtx(() => [...(_cache[51] || (_cache[51] = [
                                          _createTextVNode("全部", -1)
                                        ]))]),
                                        _: 1
                                      })
                                    ]),
                                    _: 1
                                  }, 8, ["model-value"])
                                ]),
                                _createVNode(_component_VDataTable, {
                                  class: "brushflow-torrent-table",
                                  headers: torrentHeaders,
                                  items: torrentData.value.items,
                                  loading: taskLoading.value,
                                  "items-per-page": -1,
                                  "hide-default-footer": "",
                                  density: "comfortable"
                                }, {
                                  "item.title": _withCtx(({ item }) => [
                                    _createElementVNode("div", _hoisted_49, [
                                      _createElementVNode("strong", null, _toDisplayString(item.title || '未知种子'), 1),
                                      _createElementVNode("span", null, _toDisplayString(item.site_name) + " · " + _toDisplayString(_unref(formatDateTime)((item.time || 0) * 1000)), 1)
                                    ])
                                  ]),
                                  "item.status": _withCtx(({ item }) => [
                                    _createVNode(_component_VChip, {
                                      size: "small",
                                      color: item.deleted ? 'secondary' : _unref(torrentProgress)(item) >= 100 ? 'success' : 'info',
                                      variant: "tonal"
                                    }, {
                                      default: _withCtx(() => [
                                        _createTextVNode(_toDisplayString(torrentStateText(item)), 1)
                                      ]),
                                      _: 2
                                    }, 1032, ["color"])
                                  ]),
                                  "item.size": _withCtx(({ item }) => [
                                    _createTextVNode(_toDisplayString(_unref(formatBytes)(item.size)), 1)
                                  ]),
                                  "item.uploaded": _withCtx(({ item }) => [
                                    _createTextVNode(_toDisplayString(_unref(formatBytes)(item.uploaded)), 1)
                                  ]),
                                  "item.ratio": _withCtx(({ item }) => [
                                    _createTextVNode(_toDisplayString(Number(item.ratio || 0).toFixed(2)), 1)
                                  ]),
                                  "item.policy": _withCtx(({ item }) => [
                                    _createTextVNode(_toDisplayString(torrentPolicy(item)), 1)
                                  ]),
                                  "no-data": _withCtx(() => [...(_cache[52] || (_cache[52] = [
                                    _createElementVNode("div", { class: "brushflow-table-empty" }, "当前筛选下没有种子记录", -1)
                                  ]))]),
                                  _: 1
                                }, 8, ["items", "loading"]),
                                _createElementVNode("div", _hoisted_50, [
                                  (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(torrentData.value.items, (item) => {
                                    return (_openBlock(), _createElementBlock("article", {
                                      key: `${item.task_id}-${item.title}-${item.time}`,
                                      class: "brushflow-mobile-torrent"
                                    }, [
                                      _createElementVNode("div", _hoisted_51, [
                                        _createElementVNode("strong", null, _toDisplayString(item.title || '未知种子'), 1),
                                        _createVNode(_component_VChip, {
                                          size: "x-small",
                                          variant: "tonal"
                                        }, {
                                          default: _withCtx(() => [
                                            _createTextVNode(_toDisplayString(torrentStateText(item)), 1)
                                          ]),
                                          _: 2
                                        }, 1024)
                                      ]),
                                      _createElementVNode("div", _hoisted_52, [
                                        _createElementVNode("span", null, _toDisplayString(_unref(formatBytes)(item.size)), 1),
                                        _createElementVNode("span", null, "上传 " + _toDisplayString(_unref(formatBytes)(item.uploaded)), 1),
                                        _createElementVNode("span", null, "分享率 " + _toDisplayString(Number(item.ratio || 0).toFixed(2)), 1)
                                      ]),
                                      _createElementVNode("div", _hoisted_53, _toDisplayString(torrentPolicy(item)), 1)
                                    ]))
                                  }), 128)),
                                  (!torrentData.value.items.length)
                                    ? (_openBlock(), _createElementBlock("div", _hoisted_54, "当前筛选下没有种子记录"))
                                    : _createCommentVNode("", true)
                                ]),
                                (totalTorrentPages.value > 1)
                                  ? (_openBlock(), _createBlock(_component_VPagination, {
                                      key: 0,
                                      "model-value": torrentPage.value,
                                      length: totalTorrentPages.value,
                                      "total-visible": 5,
                                      density: "comfortable",
                                      "onUpdate:modelValue": changeTorrentPage
                                    }, null, 8, ["model-value", "length"]))
                                  : _createCommentVNode("", true)
                              ]),
                              _: 1
                            })
                          ]),
                          _: 1
                        }),
                        _createVNode(_component_VWindowItem, { value: "diagnostics" }, {
                          default: _withCtx(() => [
                            (selectedTask.value.account_refresh?.success === false)
                              ? (_openBlock(), _createBlock(_component_VAlert, {
                                  key: 0,
                                  type: "warning",
                                  variant: "tonal",
                                  density: "compact",
                                  title: accountRefreshTitle.value,
                                  class: "mb-4"
                                }, {
                                  default: _withCtx(() => [
                                    _cache[53] || (_cache[53] = _createElementVNode("div", null, "此状态来自账号统计刷新，不是站点连接检测，与下载器种子检查独立。", -1)),
                                    (selectedTask.value.account_refresh.reason === 'traffic_unparsed')
                                      ? (_openBlock(), _createElementBlock("div", _hoisted_55, "站点连接正常并不代表流量字段已取得；未解析的流量按未知处理，不能填 0 或视为达标。"))
                                      : _createCommentVNode("", true),
                                    _createElementVNode("div", _hoisted_56, " 连续失败 " + _toDisplayString(selectedTask.value.account_refresh.failures ?? '未知') + " 次 · 最近刷新 " + _toDisplayString(selectedTask.value.account_refresh.checked_at != null ? _unref(formatDateTime)(selectedTask.value.account_refresh.checked_at * 1000) : '未知') + " · 下次重试 " + _toDisplayString(selectedTask.value.account_refresh.next_attempt != null ? _unref(formatDateTime)(selectedTask.value.account_refresh.next_attempt * 1000) : '未知'), 1)
                                  ]),
                                  _: 1
                                }, 8, ["title"]))
                              : _createCommentVNode("", true),
                            _createElementVNode("div", _hoisted_57, [
                              _createElementVNode("div", null, [
                                _createElementVNode("div", _hoisted_58, _toDisplayString(latestBrushRun.value ? `刷流刷新 #${latestBrushRun.value.id.slice(0, 8)}` : '暂无刷流刷新记录'), 1),
                                _createElementVNode("div", _hoisted_59, _toDisplayString(latestBrushRun.value ? `${_unref(formatDateTime)(latestBrushRun.value.started_at)} · ${_unref(formatDuration)(latestBrushRun.value.started_at, latestBrushRun.value.finished_at)}` : '执行一次任务后将显示筛选流水线和过滤原因'), 1)
                              ]),
                              (latestBrushRun.value)
                                ? (_openBlock(), _createBlock(_component_VChip, {
                                    key: 0,
                                    color: latestBrushRun.value.success === false ? 'error' : 'success',
                                    variant: "tonal"
                                  }, {
                                    default: _withCtx(() => [
                                      _createTextVNode(_toDisplayString(latestBrushRun.value.success === false ? '失败' : '完成'), 1)
                                    ]),
                                    _: 1
                                  }, 8, ["color"]))
                                : _createCommentVNode("", true)
                            ]),
                            (latestBrushRun.value)
                              ? (_openBlock(), _createElementBlock("div", _hoisted_60, [
                                  _createVNode(_component_VSheet, {
                                    tag: "section",
                                    class: "brushflow-panel app-surface-static"
                                  }, {
                                    default: _withCtx(() => [
                                      _cache[54] || (_cache[54] = _createElementVNode("header", { class: "brushflow-panel__head" }, [
                                        _createElementVNode("div", null, [
                                          _createElementVNode("div", { class: "text-subtitle-1 font-weight-medium" }, "选种流水线"),
                                          _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "本轮候选在各阶段的剩余数量")
                                        ])
                                      ], -1)),
                                      _createElementVNode("ol", _hoisted_61, [
                                        (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(pipelineStages.value, (stage, index) => {
                                          return (_openBlock(), _createElementBlock("li", {
                                            key: stage.title
                                          }, [
                                            _createElementVNode("span", _hoisted_62, _toDisplayString(index + 1), 1),
                                            _createElementVNode("div", null, [
                                              _createElementVNode("strong", null, _toDisplayString(stage.title), 1),
                                              _createElementVNode("span", null, _toDisplayString(stage.detail), 1)
                                            ]),
                                            _createVNode(_component_VChip, {
                                              size: "small",
                                              variant: "tonal"
                                            }, {
                                              default: _withCtx(() => [
                                                _createTextVNode(_toDisplayString(stage.count), 1)
                                              ]),
                                              _: 2
                                            }, 1024)
                                          ]))
                                        }), 128))
                                      ])
                                    ]),
                                    _: 1
                                  }),
                                  _createVNode(_component_VSheet, {
                                    tag: "section",
                                    class: "brushflow-panel app-surface-static"
                                  }, {
                                    default: _withCtx(() => [
                                      _cache[55] || (_cache[55] = _createElementVNode("header", { class: "brushflow-panel__head" }, [
                                        _createElementVNode("div", null, [
                                          _createElementVNode("div", { class: "text-subtitle-1 font-weight-medium" }, "过滤原因"),
                                          _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "本轮未进入下载器的候选分布")
                                        ])
                                      ], -1)),
                                      (reasonEntries.value.length)
                                        ? (_openBlock(), _createElementBlock("div", _hoisted_63, [
                                            (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(reasonEntries.value, (item) => {
                                              return (_openBlock(), _createElementBlock("div", {
                                                key: item.label,
                                                class: "brushflow-reason"
                                              }, [
                                                _createElementVNode("div", null, [
                                                  _createElementVNode("span", null, _toDisplayString(item.label), 1),
                                                  _createElementVNode("strong", null, _toDisplayString(item.count), 1)
                                                ]),
                                                _createElementVNode("span", _hoisted_64, [
                                                  _createElementVNode("i", {
                                                    style: _normalizeStyle({ width: `${(item.count / maxReasonCount.value) * 100}%` })
                                                  }, null, 4)
                                                ])
                                              ]))
                                            }), 128))
                                          ]))
                                        : (_openBlock(), _createElementBlock("div", _hoisted_65, "本轮没有记录过滤原因"))
                                    ]),
                                    _: 1
                                  })
                                ]))
                              : _createCommentVNode("", true),
                            _createVNode(_component_VSheet, {
                              tag: "section",
                              class: "brushflow-panel app-surface-static"
                            }, {
                              default: _withCtx(() => [
                                _cache[56] || (_cache[56] = _createElementVNode("header", { class: "brushflow-panel__head" }, [
                                  _createElementVNode("div", null, [
                                    _createElementVNode("div", { class: "text-subtitle-1 font-weight-medium" }, "最近事件"),
                                    _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "刷流刷新与种子检查独立记录")
                                  ])
                                ], -1)),
                                _createElementVNode("div", _hoisted_66, [
                                  (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(taskRuns.value, (run) => {
                                    return (_openBlock(), _createElementBlock("article", {
                                      key: run.id
                                    }, [
                                      _createVNode(_component_VIcon, {
                                        icon: run.success === false ? 'mdi-alert-circle-outline' : run.kind === 'brush' ? 'mdi-sync' : 'mdi-progress-check',
                                        color: run.success === false ? 'error' : run.kind === 'brush' ? 'primary' : 'info'
                                      }, null, 8, ["icon", "color"]),
                                      _createElementVNode("div", null, [
                                        _createElementVNode("strong", null, _toDisplayString(run.kind === 'brush' ? '刷流刷新' : '种子检查'), 1),
                                        _createElementVNode("span", null, _toDisplayString(_unref(formatDateTime)(run.started_at)) + " · " + _toDisplayString(_unref(formatDuration)(run.started_at, run.finished_at)) + " · " + _toDisplayString(run.kind === 'brush' ? `新增 ${run.added_count || 0}，过滤 ${run.filtered_count || 0}` : `活跃 ${run.active_count || 0}，删除 ${run.deleted_count || 0}`), 1),
                                        (run.error)
                                          ? (_openBlock(), _createElementBlock("span", _hoisted_67, _toDisplayString(run.error), 1))
                                          : _createCommentVNode("", true)
                                      ])
                                    ]))
                                  }), 128)),
                                  (!taskRuns.value.length)
                                    ? (_openBlock(), _createElementBlock("div", _hoisted_68, "暂无运行事件"))
                                    : _createCommentVNode("", true)
                                ])
                              ]),
                              _: 1
                            })
                          ]),
                          _: 1
                        }),
                        _createVNode(_component_VWindowItem, { value: "config" }, {
                          default: _withCtx(() => [
                            _createElementVNode("div", null, [
                              _createVNode(_component_VSheet, {
                                tag: "section",
                                class: "brushflow-panel app-surface-static"
                              }, {
                                default: _withCtx(() => [
                                  _cache[74] || (_cache[74] = _createElementVNode("header", { class: "brushflow-panel__head" }, [
                                    _createElementVNode("div", null, [
                                      _createElementVNode("div", { class: "text-subtitle-1 font-weight-medium" }, "公共刷流规则"),
                                      _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "全部选中站点使用同一套规则，可在“刷流设置”中修改。")
                                    ])
                                  ], -1)),
                                  _createElementVNode("dl", _hoisted_69, [
                                    _createElementVNode("div", null, [
                                      _cache[57] || (_cache[57] = _createElementVNode("dt", null, "任务状态", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(downloaderConflict.value ? '暂停新增，继续检查原下载器' : selectedTask.value.enabled ? '管理中' : '已停止选种与检查'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[58] || (_cache[58] = _createElementVNode("dt", null, "通知", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.notify ? '发送' : '关闭'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[59] || (_cache[59] = _createElementVNode("dt", null, "站点", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(selectedTask.value.site_name), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[60] || (_cache[60] = _createElementVNode("dt", null, "下载器", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(selectedTask.value.downloader), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[61] || (_cache[61] = _createElementVNode("dt", null, "种子大小", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.size || '不限'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[62] || (_cache[62] = _createElementVNode("dt", null, "做种人数", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.seeder || '不限'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[63] || (_cache[63] = _createElementVNode("dt", null, "下载人数", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.leecher || '不限'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[64] || (_cache[64] = _createElementVNode("dt", null, "下载 / 做种人数下限", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.min_peer_seed_ratio ?? '不限'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[65] || (_cache[65] = _createElementVNode("dt", null, "发布时间", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.pubtime ? `${taskConfig.value.pubtime} 分钟` : '不限'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[66] || (_cache[66] = _createElementVNode("dt", null, "排除 H&R", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.hr === 'yes' ? '是' : '否'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[67] || (_cache[67] = _createElementVNode("dt", null, "包含规则", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.include || '无'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[68] || (_cache[68] = _createElementVNode("dt", null, "排除规则", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.exclude || '无'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[69] || (_cache[69] = _createElementVNode("dt", null, "保种上限", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.disksize ? `${taskConfig.value.disksize} GB` : '不限'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[70] || (_cache[70] = _createElementVNode("dt", null, "目标分享率", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.site_ratio_target || '不限') + _toDisplayString(sequentialMode.value && ratioPhase.value ? '（达标后切换）' : ''), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[71] || (_cache[71] = _createElementVNode("dt", null, "达标后每站驻留时间", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.rotation_hours ?? 24) + " 小时" + _toDisplayString(sequentialMode.value ? '' : '（单站轮换模式生效）'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[72] || (_cache[72] = _createElementVNode("dt", null, "促销保护", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.del_no_free ? `提前 ${taskConfig.value.promotion_lead_minutes ?? 10} 分钟，包含排队任务` : '关闭'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[73] || (_cache[73] = _createElementVNode("dt", null, "归档天数", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.auto_archive_days || '不自动归档'), 1)
                                    ])
                                  ])
                                ]),
                                _: 1
                              })
                            ])
                          ]),
                          _: 1
                        })
                      ]),
                      _: 1
                    }, 8, ["modelValue"])
                  ], 512))
                : _createCommentVNode("", true)
            ])
          ])),
    _createVNode(BrushSettingsDialog, {
      "model-value": settingsOpen.value,
      settings: settingsSnapshot.value,
      sites: settingsSites.value,
      downloaders: settingsDownloaders.value,
      ready: settingsReady.value,
      loading: settingsLoading.value,
      saving: saving.value,
      error: settingsError.value,
      "onUpdate:modelValue": _cache[8] || (_cache[8] = value => { if (!value) closeSettings(); }),
      onRetry: loadSettings,
      onSave: saveSettings,
      onClosed: onSettingsClosed
    }, null, 8, ["model-value", "settings", "sites", "downloaders", "ready", "loading", "saving", "error"])
  ], 2))
}
}

};
const BrushFlowWorkbench = /*#__PURE__*/_export_sfc(_sfc_main, [['__scopeId',"data-v-1df23108"]]);

export { BrushFlowWorkbench as B };
