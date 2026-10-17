import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { _ as _export_sfc, c as cloneTask, d as taskOverrideFields, e as effectiveTask, g as taskDefaults, h as editableTask, n as normalizeDefaults, i as normalizeTask, f as formatDateTime, t as taskStateMeta, b as formatRemainingTime, a as formatBytes, j as formatDuration, k as torrentProgress, u as unwrapResponse, l as normalizeSettings } from './_plugin-vue_export-helper-CPyOxquJ.js';

const {unref:_unref$1,toDisplayString:_toDisplayString$1,createTextVNode:_createTextVNode$1,resolveComponent:_resolveComponent$1,withCtx:_withCtx$1,createVNode:_createVNode$1,renderList:_renderList$1,Fragment:_Fragment$1,openBlock:_openBlock$1,createElementBlock:_createElementBlock$1,createBlock:_createBlock$1,createCommentVNode:_createCommentVNode$1,createElementVNode:_createElementVNode$1,withModifiers:_withModifiers$1} = await importShared('vue');


const _hoisted_1$1 = {
  key: 0,
  class: "editor-section"
};
const _hoisted_2$1 = { class: "editor-switches" };
const _hoisted_3$1 = {
  key: 1,
  class: "editor-section"
};
const _hoisted_4$1 = { class: "editor-switches" };
const _hoisted_5$1 = { class: "editor-section" };
const _hoisted_6$1 = { class: "editor-section__head" };
const _hoisted_7$1 = { class: "text-subtitle-1 font-weight-medium" };
const _hoisted_8$1 = { class: "text-body-2 text-medium-emphasis" };
const _hoisted_9$1 = {
  key: 2,
  class: "editor-section"
};
const _hoisted_10$1 = { key: 0 };
const _hoisted_11$1 = { key: 1 };

const {computed: computed$1,ref: ref$1,watch: watch$1} = await importShared('vue');

const {useDisplay} = await importShared('vuetify');


const _sfc_main$1 = {
  __name: 'TaskEditorDialog',
  props: {
  modelValue: { type: Boolean, default: false },
  task: { type: Object, default: () => ({}) },
  sites: { type: Array, default: () => [] },
  downloaders: { type: Array, default: () => [] },
  globalDefaults: { type: Object, default: () => ({}) },
  globalMode: { type: Boolean, default: false },
  globalDynamicDelete: { type: Boolean, default: false },
  saving: { type: Boolean, default: false },
},
  emits: ['update:modelValue', 'save'],
  setup(__props, { emit: __emit }) {

const props = __props;

const emit = __emit;
const display = useDisplay();
const formRef = ref$1(null);
const activeTab = ref$1('base');
const localTask = ref$1(cloneTask());
const applyDialog = ref$1(false);
const formError = ref$1('');
const required = value => !!String(value ?? '').trim() || '此项不能为空';
const positive = value => value === '' || value == null || Number(value) > 0 || '请输入大于 0 的数值';

const sections = [
  {
    value: 'base', title: '常用与调度', icon: 'mdi-calendar-clock',
    description: '单站模式先按优先级刷至分享率达标，全部达标后按配置顺序轮转。',
    fields: [
      { key: 'downloader', label: '下载器', type: 'downloader', required: true },
      { key: 'save_path', label: '保存目录', globalHint: '留空使用下载器默认目录' },
      { key: 'brush_interval', label: '刷流刷新周期（分钟）', type: 'number', min: 1, max: 1440, required: true },
      { key: 'check_interval', label: '状态检查周期（分钟）', type: 'number', min: 1, max: 1440, required: true },
      { key: 'rotation_hours', label: '达标后每站驻留时间（小时）', type: 'number', min: 0, max: 168, required: true, hint: '单站模式下全部站点达标后生效，默认 24 小时；旧种继续下载、做种和按规则清理。' },
      { key: 'cron', label: 'CRON 表达式', globalHint: '留空使用固定刷新周期' },
      { key: 'active_time_range', label: '开启时间段', hint: '如 00:00-08:00', globalHint: '如 00:00-08:00，留空为全天' },
      { key: 'site_ratio_target', label: '目标站点分享率', type: 'number', min: 0.01, step: 0.01, hint: '默认 2，单站模式刷至达标后切换下一站' },
      { key: 'disksize', label: '站点空间上限（GB）', type: 'number', min: 0, hint: '0 表示不限，仍受全局空间上限约束', globalHint: '留空或 0 表示不限，仍受全局空间上限约束' },
      { key: 'notify', label: '发送通知', type: 'switch' },
    ],
  },
  {
    value: 'selection', title: '选种规则', icon: 'mdi-filter-cog-outline',
    description: '先满足筛选条件，再按新手考核、分享率未达标、普通轮转分配机会。',
    fields: [
      { key: 'freeleech', label: '促销', type: 'select', items: [{ title: '全部（包括普通）', value: '' }, { title: '免费', value: 'free' }, { title: '2X 免费', value: '2xfree' }] },
      { key: 'hr', label: '排除 H&R', type: 'select', items: [{ title: '是', value: 'yes' }, { title: '否', value: 'no' }] },
      { key: 'size', label: '种子大小（GB）', hint: '单值为上限，或填写范围，如 10-80' },
      { key: 'seeder', label: '做种人数', hint: '单值为上限，或填写范围，如 1-10' },
      { key: 'leecher', label: '下载人数', hint: '单值为最低人数，或填写范围，如 1-100；默认 1' },
      { key: 'min_peer_seed_ratio', label: '下载人数 / 做种人数下限', type: 'number', min: 0, step: 0.01, hint: '0 表示不限制', globalHint: '默认 0.1，留空或 0 不限制' },
      { key: 'pubtime', label: '发布时间（分钟）', hint: '单值或范围，如 5-120' },
      { key: 'include', label: '包含规则', hint: '支持正则表达式' },
      { key: 'exclude', label: '排除规则', hint: '支持正则表达式' },
      { key: 'rss_support', label: '使用 RSS', type: 'switch', siteOnly: true },
      { key: 'except_subscribe', label: '排除订阅', type: 'switch' },
    ],
  },
  {
    value: 'delete', title: '删种规则', icon: 'mdi-delete-clock-outline',
    description: '促销到期保护可删除尚未开始的排队任务；其余删种规则跳过尚未开始下载的任务。',
    fields: [
      { key: 'proxy_delete', label: '动态删种', type: 'switch' },
      { key: 'delete_size_range', label: '动态删种阈值（GB）', hint: '如 350-500；全局托管开启时采用全局阈值' },
      { key: 'seed_time', label: '做种时间（小时）', type: 'number', min: 0 },
      { key: 'hr_seed_time', label: 'H&R 做种时间（小时）', type: 'number', min: 0 },
      { key: 'seed_ratio', label: '种子分享率', type: 'number', min: 0 },
      { key: 'seed_size', label: '上传量（GB）', type: 'number', min: 0 },
      { key: 'download_time', label: '下载超时（小时）', type: 'number', min: 0 },
      { key: 'download_min_speed', label: '低速下载阈值（KB/s）', type: 'number', min: 0, hint: '与观察时间同时大于 0 时生效；0 关闭。仅清理未完成种子，平均上传达到此阈值时暂留。' },
      { key: 'download_slow_minutes', label: '低速观察时间（分钟）', type: 'number', min: 0, hint: '平均下载速度持续低于阈值才清理；排队、暂停不计时，采样缺口重计。0 关闭。' },
      { key: 'seed_inactivetime', label: '未活动时间（分钟）', type: 'number', min: 0 },
      { key: 'seed_avgspeed', label: '平均上传速度（KB/s）', type: 'number', min: 0 },
      { key: 'delete_except_tags', label: '删除排除标签' },
      { key: 'del_no_free', label: '促销结束前删除未完成任务（包括排队）', type: 'switch' },
      { key: 'promotion_lead_minutes', label: '促销结束前预留时间（分钟）', type: 'number', min: 0, required: true, hint: '默认提前 10 分钟检查删除，仍遵守保护标签与 H&R 约束' },
    ],
  },
  {
    value: 'advanced', title: '高级', icon: 'mdi-tune-variant', description: '下载器分类和托管记录归档。',
    fields: [
      { key: 'qb_category', label: 'qBittorrent 分类' },
      { key: 'auto_archive_days', label: '自动归档天数', type: 'number', min: 0 },
    ],
  },
];

const dialogTitle = computed$1(() => props.globalMode ? '全局统一参数' : localTask.value.id ? '编辑站点任务' : '新增站点任务');
const siteName = computed$1(() => props.sites.find(item => item.value === Number(localTask.value.site_id))?.title || '未选择');
const overrideFields = computed$1(() => taskOverrideFields(localTask.value));
const previewTask = computed$1(() => props.globalMode ? localTask.value : effectiveTask({ ...localTask.value, use_global_defaults: true, override_fields: overrideFields.value }, props.globalDefaults));
const scheduleText = computed$1(() => previewTask.value.cron || `每 ${previewTask.value.brush_interval || 10} 分钟`);
const noviceActive = computed$1(() => localTask.value.novice && (!localTask.value.novice_until || new Date(localTask.value.novice_until) > new Date()));

function globalValueText(field) {
  const value = { ...taskDefaults, ...props.globalDefaults }[field.key];
  if (field.type === 'switch') return value ? '开启' : '关闭'
  const items = field.type === 'downloader' ? props.downloaders : field.items;
  const selected = items?.find(item => item.value === value);
  if (selected) return selected.title
  if (value == null || value === '') return ({ downloader: '未选择', save_path: '下载器默认目录', cron: '固定刷新周期', active_time_range: '全天', qb_category: '不指定', delete_except_tags: '无', download_min_speed: '关闭', download_slow_minutes: '关闭' })[field.key] || '不限'
  return String(value)
}

function fieldItems(field) {
  const items = field.type === 'switch' ? [{ title: '开启', value: true }, { title: '关闭', value: false }] : field.type === 'downloader' ? props.downloaders : field.items;
  return items
}

function fieldHint(field) {
  if (props.globalMode) return field.globalHint || field.hint
  return field.hint
}

function fieldLabel(field) {
  return field.key === 'proxy_delete' && props.globalDynamicDelete ? '参与全局动态删种' : field.label
}

function fieldRules(field) {
  const rules = field.required && props.globalMode ? [required] : [];
  if (['site_ratio_target', 'rotation_hours'].includes(field.key)) rules.push(positive);
  if (field.type === 'number') {
    rules.push(value => value === '' || value == null || Number.isFinite(Number(value)) || '请输入有效数字');
    if (field.min != null) rules.push(value => value === '' || value == null || Number(value) >= field.min || `不能小于 ${field.min}`);
    if (field.max != null) rules.push(value => value === '' || value == null || Number(value) <= field.max || `不能大于 ${field.max}`);
  }
  return rules
}

function selectSite(value) {
  localTask.value.site_id = value;
  if (!localTask.value.name) localTask.value.name = `${siteName.value}刷流`;
}

watch$1(() => props.modelValue, visible => {
  if (!visible) return
  localTask.value = props.globalMode
    ? cloneTask({ ...taskDefaults, ...props.task, use_global_defaults: false })
    : editableTask(props.task);
  if (localTask.value.novice_until) {
    const deadline = new Date(localTask.value.novice_until);
    if (!Number.isNaN(deadline.getTime())) {
      localTask.value.novice_until = new Date(deadline.getTime() - deadline.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
    }
  }
  activeTab.value = 'base';
  applyDialog.value = false;
  formError.value = '';
});

function closeDialog() {
  emit('update:modelValue', false);
}

async function saveTask(apply = false) {
  applyDialog.value = false;
  const result = await formRef.value?.validate();
  if (result && !result.valid) return
  formError.value = '';
  try {
    const payload = props.globalMode ? normalizeDefaults(localTask.value) : normalizeTask(previewTask.value);
    if (!payload.downloader) {
      formError.value = '请先在全局统一参数中选择下载器，或为当前站点覆盖下载器。';
      activeTab.value = 'base';
      return
    }
    emit('save', payload, apply);
  } catch (err) {
    formError.value = err?.message || '请检查配置格式';
  }
}

return (_ctx, _cache) => {
  const _component_VToolbarTitle = _resolveComponent$1("VToolbarTitle");
  const _component_VSpacer = _resolveComponent$1("VSpacer");
  const _component_VBtn = _resolveComponent$1("VBtn");
  const _component_VToolbar = _resolveComponent$1("VToolbar");
  const _component_VDivider = _resolveComponent$1("VDivider");
  const _component_VTab = _resolveComponent$1("VTab");
  const _component_VTabs = _resolveComponent$1("VTabs");
  const _component_VAlert = _resolveComponent$1("VAlert");
  const _component_VSelect = _resolveComponent$1("VSelect");
  const _component_VCol = _resolveComponent$1("VCol");
  const _component_VTextField = _resolveComponent$1("VTextField");
  const _component_VRow = _resolveComponent$1("VRow");
  const _component_VSwitch = _resolveComponent$1("VSwitch");
  const _component_VWindowItem = _resolveComponent$1("VWindowItem");
  const _component_VWindow = _resolveComponent$1("VWindow");
  const _component_VSheet = _resolveComponent$1("VSheet");
  const _component_VForm = _resolveComponent$1("VForm");
  const _component_VCardText = _resolveComponent$1("VCardText");
  const _component_VCard = _resolveComponent$1("VCard");
  const _component_VDialog = _resolveComponent$1("VDialog");
  const _component_VCardActions = _resolveComponent$1("VCardActions");

  return (_openBlock$1(), _createElementBlock$1(_Fragment$1, null, [
    _createVNode$1(_component_VDialog, {
      "model-value": __props.modelValue,
      scrollable: "",
      fullscreen: _unref$1(display).smAndDown.value,
      "max-width": "74rem",
      "onUpdate:modelValue": _cache[11] || (_cache[11] = value => emit('update:modelValue', value))
    }, {
      default: _withCtx$1(() => [
        _createVNode$1(_component_VCard, { class: "brushflow-editor" }, {
          default: _withCtx$1(() => [
            _createVNode$1(_component_VToolbar, {
              color: "transparent",
              density: "comfortable",
              class: "brushflow-editor__toolbar"
            }, {
              default: _withCtx$1(() => [
                _createVNode$1(_component_VToolbarTitle, null, {
                  default: _withCtx$1(() => [
                    _createTextVNode$1(_toDisplayString$1(dialogTitle.value), 1)
                  ]),
                  _: 1
                }),
                _createVNode$1(_component_VSpacer),
                _createVNode$1(_component_VBtn, {
                  color: "primary",
                  variant: "flat",
                  "prepend-icon": "mdi-content-save",
                  loading: __props.saving,
                  onClick: _cache[0] || (_cache[0] = $event => (saveTask()))
                }, {
                  default: _withCtx$1(() => [...(_cache[15] || (_cache[15] = [
                    _createTextVNode$1("保存", -1)
                  ]))]),
                  _: 1
                }, 8, ["loading"]),
                _createVNode$1(_component_VBtn, {
                  icon: "mdi-close",
                  variant: "text",
                  "aria-label": "关闭",
                  onClick: closeDialog
                })
              ]),
              _: 1
            }),
            _createVNode$1(_component_VDivider),
            _createVNode$1(_component_VCardText, { class: "brushflow-editor__body" }, {
              default: _withCtx$1(() => [
                _createVNode$1(_component_VForm, {
                  ref_key: "formRef",
                  ref: formRef,
                  class: "brushflow-editor__form",
                  onSubmit: _cache[10] || (_cache[10] = _withModifiers$1($event => (saveTask()), ["prevent"]))
                }, {
                  default: _withCtx$1(() => [
                    _createVNode$1(_component_VTabs, {
                      modelValue: activeTab.value,
                      "onUpdate:modelValue": _cache[1] || (_cache[1] = $event => ((activeTab).value = $event)),
                      direction: _unref$1(display).mdAndUp.value ? 'vertical' : 'horizontal',
                      color: "primary",
                      class: "brushflow-editor__tabs"
                    }, {
                      default: _withCtx$1(() => [
                        (_openBlock$1(), _createElementBlock$1(_Fragment$1, null, _renderList$1(sections, (section) => {
                          return _createVNode$1(_component_VTab, {
                            key: section.value,
                            value: section.value,
                            "prepend-icon": section.icon
                          }, {
                            default: _withCtx$1(() => [
                              _createTextVNode$1(_toDisplayString$1(section.value === 'base' && !__props.globalMode ? '站点与调度' : section.title), 1)
                            ]),
                            _: 2
                          }, 1032, ["value", "prepend-icon"])
                        }), 64))
                      ]),
                      _: 1
                    }, 8, ["modelValue", "direction"]),
                    _createVNode$1(_component_VDivider, {
                      vertical: _unref$1(display).mdAndUp.value
                    }, null, 8, ["vertical"]),
                    _createVNode$1(_component_VWindow, {
                      modelValue: activeTab.value,
                      "onUpdate:modelValue": _cache[9] || (_cache[9] = $event => ((activeTab).value = $event)),
                      touch: false,
                      class: "brushflow-editor__window"
                    }, {
                      default: _withCtx$1(() => [
                        (formError.value)
                          ? (_openBlock$1(), _createBlock$1(_component_VAlert, {
                              key: 0,
                              type: "error",
                              variant: "tonal",
                              class: "mb-4"
                            }, {
                              default: _withCtx$1(() => [
                                _createTextVNode$1(_toDisplayString$1(formError.value), 1)
                              ]),
                              _: 1
                            }))
                          : _createCommentVNode$1("", true),
                        (_openBlock$1(), _createElementBlock$1(_Fragment$1, null, _renderList$1(sections, (section) => {
                          return _createVNode$1(_component_VWindowItem, {
                            key: section.value,
                            value: section.value
                          }, {
                            default: _withCtx$1(() => [
                              (section.value === 'base' && !__props.globalMode)
                                ? (_openBlock$1(), _createElementBlock$1("section", _hoisted_1$1, [
                                    _cache[17] || (_cache[17] = _createElementVNode$1("div", { class: "text-subtitle-1 font-weight-medium" }, "站点信息", -1)),
                                    _createVNode$1(_component_VRow, null, {
                                      default: _withCtx$1(() => [
                                        _createVNode$1(_component_VCol, {
                                          cols: "12",
                                          md: "6"
                                        }, {
                                          default: _withCtx$1(() => [
                                            _createVNode$1(_component_VSelect, {
                                              "model-value": localTask.value.site_id,
                                              items: __props.sites,
                                              label: "站点",
                                              rules: [required],
                                              "onUpdate:modelValue": selectSite
                                            }, null, 8, ["model-value", "items", "rules"])
                                          ]),
                                          _: 1
                                        }),
                                        _createVNode$1(_component_VCol, {
                                          cols: "12",
                                          md: "6"
                                        }, {
                                          default: _withCtx$1(() => [
                                            _createVNode$1(_component_VTextField, {
                                              modelValue: localTask.value.name,
                                              "onUpdate:modelValue": _cache[2] || (_cache[2] = $event => ((localTask.value.name) = $event)),
                                              label: "任务名称",
                                              rules: [required]
                                            }, null, 8, ["modelValue", "rules"])
                                          ]),
                                          _: 1
                                        })
                                      ]),
                                      _: 1
                                    }),
                                    _createElementVNode$1("div", _hoisted_2$1, [
                                      _createVNode$1(_component_VSwitch, {
                                        modelValue: localTask.value.enabled,
                                        "onUpdate:modelValue": _cache[3] || (_cache[3] = $event => ((localTask.value.enabled) = $event)),
                                        label: "启用站点管理",
                                        hint: "关闭后停止选种与检查，下载器任务继续运行。",
                                        "persistent-hint": "",
                                        color: "primary",
                                        inset: ""
                                      }, null, 8, ["modelValue"])
                                    ]),
                                    _createVNode$1(_component_VAlert, {
                                      type: "info",
                                      variant: "tonal",
                                      density: "compact"
                                    }, {
                                      default: _withCtx$1(() => [...(_cache[16] || (_cache[16] = [
                                        _createTextVNode$1("留空使用默认值，填写后仅对此站点生效。", -1)
                                      ]))]),
                                      _: 1
                                    })
                                  ]))
                                : _createCommentVNode$1("", true),
                              (section.value === 'base' && !__props.globalMode)
                                ? (_openBlock$1(), _createElementBlock$1("section", _hoisted_3$1, [
                                    _cache[18] || (_cache[18] = _createElementVNode$1("div", { class: "text-subtitle-1 font-weight-medium" }, "考核与站点保护", -1)),
                                    _createVNode$1(_component_VSwitch, {
                                      modelValue: localTask.value.novice,
                                      "onUpdate:modelValue": _cache[4] || (_cache[4] = $event => ((localTask.value.novice) = $event)),
                                      label: "新手考核优先",
                                      color: "primary",
                                      "hide-details": "",
                                      inset: ""
                                    }, null, 8, ["modelValue"]),
                                    (localTask.value.novice)
                                      ? (_openBlock$1(), _createBlock$1(_component_VTextField, {
                                          key: 0,
                                          modelValue: localTask.value.novice_until,
                                          "onUpdate:modelValue": _cache[5] || (_cache[5] = $event => ((localTask.value.novice_until) = $event)),
                                          type: "datetime-local",
                                          label: "考核截止时间（本地时间，可选）",
                                          clearable: "",
                                          hint: "留空时手动关闭；超过截止时间后自动失去考核优先级。",
                                          "persistent-hint": "",
                                          rules: [value => !value || !Number.isNaN(new Date(value).getTime()) || '请输入有效时间']
                                        }, null, 8, ["modelValue", "rules"]))
                                      : _createCommentVNode$1("", true),
                                    _createElementVNode$1("div", _hoisted_4$1, [
                                      _createVNode$1(_component_VSwitch, {
                                        modelValue: localTask.value.site_hr_active,
                                        "onUpdate:modelValue": _cache[6] || (_cache[6] = $event => ((localTask.value.site_hr_active) = $event)),
                                        label: "全站 H&R",
                                        color: "primary",
                                        "hide-details": "",
                                        inset: ""
                                      }, null, 8, ["modelValue"]),
                                      _createVNode$1(_component_VSwitch, {
                                        modelValue: localTask.value.site_skip_tips,
                                        "onUpdate:modelValue": _cache[7] || (_cache[7] = $event => ((localTask.value.site_skip_tips) = $event)),
                                        label: "自动跳过下载提示",
                                        color: "primary",
                                        "hide-details": "",
                                        inset: ""
                                      }, null, 8, ["modelValue"])
                                    ])
                                  ]))
                                : _createCommentVNode$1("", true),
                              _createElementVNode$1("section", _hoisted_5$1, [
                                _createElementVNode$1("header", _hoisted_6$1, [
                                  _createElementVNode$1("div", null, [
                                    _createElementVNode$1("div", _hoisted_7$1, _toDisplayString$1(section.title), 1),
                                    _createElementVNode$1("div", _hoisted_8$1, _toDisplayString$1(section.description), 1)
                                  ])
                                ]),
                                _createVNode$1(_component_VRow, null, {
                                  default: _withCtx$1(() => [
                                    (_openBlock$1(true), _createElementBlock$1(_Fragment$1, null, _renderList$1(section.fields.filter(field => !__props.globalMode || !field.siteOnly), (field) => {
                                      return (_openBlock$1(), _createBlock$1(_component_VCol, {
                                        key: field.key,
                                        cols: "12",
                                        md: __props.globalMode && field.type === 'switch' ? 12 : 6
                                      }, {
                                        default: _withCtx$1(() => [
                                          ((__props.globalMode || field.siteOnly) && field.type === 'switch')
                                            ? (_openBlock$1(), _createBlock$1(_component_VSwitch, {
                                                key: 0,
                                                modelValue: localTask.value[field.key],
                                                "onUpdate:modelValue": $event => ((localTask.value[field.key]) = $event),
                                                label: fieldLabel(field),
                                                color: "primary",
                                                "hide-details": "",
                                                inset: ""
                                              }, null, 8, ["modelValue", "onUpdate:modelValue", "label"]))
                                            : (['select', 'downloader', 'switch'].includes(field.type))
                                              ? (_openBlock$1(), _createBlock$1(_component_VSelect, {
                                                  key: 1,
                                                  modelValue: localTask.value[field.key],
                                                  "onUpdate:modelValue": $event => ((localTask.value[field.key]) = $event),
                                                  items: fieldItems(field),
                                                  placeholder: __props.globalMode ? undefined : globalValueText(field),
                                                  "persistent-placeholder": !__props.globalMode,
                                                  label: fieldLabel(field),
                                                  hint: fieldHint(field),
                                                  "persistent-hint": !!fieldHint(field),
                                                  rules: fieldRules(field),
                                                  clearable: !__props.globalMode
                                                }, null, 8, ["modelValue", "onUpdate:modelValue", "items", "placeholder", "persistent-placeholder", "label", "hint", "persistent-hint", "rules", "clearable"]))
                                              : (_openBlock$1(), _createBlock$1(_component_VTextField, {
                                                  key: 2,
                                                  modelValue: localTask.value[field.key],
                                                  "onUpdate:modelValue": $event => ((localTask.value[field.key]) = $event),
                                                  type: field.type || 'text',
                                                  label: field.label,
                                                  min: field.min,
                                                  max: field.max,
                                                  step: field.step || 'any',
                                                  placeholder: __props.globalMode ? undefined : globalValueText(field),
                                                  "persistent-placeholder": !__props.globalMode,
                                                  hint: fieldHint(field),
                                                  "persistent-hint": !!fieldHint(field),
                                                  rules: fieldRules(field),
                                                  clearable: !__props.globalMode || !field.required
                                                }, null, 8, ["modelValue", "onUpdate:modelValue", "type", "label", "min", "max", "step", "placeholder", "persistent-placeholder", "hint", "persistent-hint", "rules", "clearable"]))
                                        ]),
                                        _: 2
                                      }, 1032, ["md"]))
                                    }), 128))
                                  ]),
                                  _: 2
                                }, 1024)
                              ]),
                              (__props.globalMode && section.value === 'base')
                                ? (_openBlock$1(), _createElementBlock$1("section", _hoisted_9$1, [
                                    _createVNode$1(_component_VAlert, {
                                      type: "info",
                                      variant: "tonal",
                                      density: "compact"
                                    }, {
                                      default: _withCtx$1(() => [...(_cache[19] || (_cache[19] = [
                                        _createTextVNode$1("普通保存保留站点覆盖。应用到全部站点会清除共享参数覆盖，并保留站点身份、考核和保护设置。", -1)
                                      ]))]),
                                      _: 1
                                    }),
                                    _createVNode$1(_component_VBtn, {
                                      variant: "tonal",
                                      color: "primary",
                                      disabled: __props.saving,
                                      onClick: _cache[8] || (_cache[8] = $event => (applyDialog.value = true))
                                    }, {
                                      default: _withCtx$1(() => [...(_cache[20] || (_cache[20] = [
                                        _createTextVNode$1("保存并应用到全部站点", -1)
                                      ]))]),
                                      _: 1
                                    }, 8, ["disabled"])
                                  ]))
                                : _createCommentVNode$1("", true)
                            ]),
                            _: 2
                          }, 1032, ["value"])
                        }), 64))
                      ]),
                      _: 1
                    }, 8, ["modelValue"]),
                    _createVNode$1(_component_VSheet, {
                      tag: "aside",
                      class: "brushflow-editor__summary"
                    }, {
                      default: _withCtx$1(() => [
                        _cache[32] || (_cache[32] = _createElementVNode$1("div", { class: "text-subtitle-1 font-weight-medium" }, "生效参数预览", -1)),
                        _createElementVNode$1("dl", null, [
                          (!__props.globalMode)
                            ? (_openBlock$1(), _createElementBlock$1("div", _hoisted_10$1, [
                                _cache[21] || (_cache[21] = _createElementVNode$1("dt", null, "站点", -1)),
                                _createElementVNode$1("dd", null, _toDisplayString$1(siteName.value), 1)
                              ]))
                            : _createCommentVNode$1("", true),
                          (!__props.globalMode)
                            ? (_openBlock$1(), _createElementBlock$1("div", _hoisted_11$1, [
                                _cache[22] || (_cache[22] = _createElementVNode$1("dt", null, "新手考核", -1)),
                                _createElementVNode$1("dd", null, _toDisplayString$1(noviceActive.value ? '优先' : localTask.value.novice ? '已到期' : '关闭'), 1)
                              ]))
                            : _createCommentVNode$1("", true),
                          _createElementVNode$1("div", null, [
                            _cache[23] || (_cache[23] = _createElementVNode$1("dt", null, "下载器", -1)),
                            _createElementVNode$1("dd", null, _toDisplayString$1(previewTask.value.downloader || '未选择'), 1)
                          ]),
                          _createElementVNode$1("div", null, [
                            _cache[24] || (_cache[24] = _createElementVNode$1("dt", null, "刷新", -1)),
                            _createElementVNode$1("dd", null, _toDisplayString$1(scheduleText.value), 1)
                          ]),
                          _createElementVNode$1("div", null, [
                            _cache[25] || (_cache[25] = _createElementVNode$1("dt", null, "检查", -1)),
                            _createElementVNode$1("dd", null, "每 " + _toDisplayString$1(previewTask.value.check_interval || 5) + " 分钟", 1)
                          ]),
                          _createElementVNode$1("div", null, [
                            _cache[26] || (_cache[26] = _createElementVNode$1("dt", null, "达标后驻留", -1)),
                            _createElementVNode$1("dd", null, _toDisplayString$1(previewTask.value.rotation_hours ?? 24) + " 小时", 1)
                          ]),
                          _createElementVNode$1("div", null, [
                            _cache[27] || (_cache[27] = _createElementVNode$1("dt", null, "目标分享率", -1)),
                            _createElementVNode$1("dd", null, _toDisplayString$1(previewTask.value.site_ratio_target || '不限'), 1)
                          ]),
                          _createElementVNode$1("div", null, [
                            _cache[28] || (_cache[28] = _createElementVNode$1("dt", null, "下载人数", -1)),
                            _createElementVNode$1("dd", null, _toDisplayString$1(previewTask.value.leecher || '不限'), 1)
                          ]),
                          _createElementVNode$1("div", null, [
                            _cache[29] || (_cache[29] = _createElementVNode$1("dt", null, "下载 / 做种", -1)),
                            _createElementVNode$1("dd", null, _toDisplayString$1(previewTask.value.min_peer_seed_ratio ?? '不限'), 1)
                          ]),
                          _createElementVNode$1("div", null, [
                            _cache[30] || (_cache[30] = _createElementVNode$1("dt", null, "空间上限", -1)),
                            _createElementVNode$1("dd", null, _toDisplayString$1(Number(previewTask.value.disksize) > 0 ? `${previewTask.value.disksize} GB` : '不限'), 1)
                          ]),
                          _createElementVNode$1("div", null, [
                            _cache[31] || (_cache[31] = _createElementVNode$1("dt", null, "促销保护", -1)),
                            _createElementVNode$1("dd", null, _toDisplayString$1(previewTask.value.del_no_free ? `提前 ${previewTask.value.promotion_lead_minutes ?? 10} 分钟` : '关闭'), 1)
                          ])
                        ])
                      ]),
                      _: 1
                    })
                  ]),
                  _: 1
                }, 512)
              ]),
              _: 1
            })
          ]),
          _: 1
        })
      ]),
      _: 1
    }, 8, ["model-value", "fullscreen"]),
    _createVNode$1(_component_VDialog, {
      modelValue: applyDialog.value,
      "onUpdate:modelValue": _cache[14] || (_cache[14] = $event => ((applyDialog).value = $event)),
      "max-width": "30rem"
    }, {
      default: _withCtx$1(() => [
        _createVNode$1(_component_VCard, { title: "应用统一参数到全部站点" }, {
          default: _withCtx$1(() => [
            _createVNode$1(_component_VCardText, null, {
              default: _withCtx$1(() => [...(_cache[33] || (_cache[33] = [
                _createTextVNode$1("所有站点将继承这套全局参数，并清除已有共享参数覆盖。站点名称、启用状态、考核截止时间、时区、H&R、提示跳过和下载器标签保持各站设置。", -1)
              ]))]),
              _: 1
            }),
            _createVNode$1(_component_VCardActions, null, {
              default: _withCtx$1(() => [
                _createVNode$1(_component_VSpacer),
                _createVNode$1(_component_VBtn, {
                  variant: "text",
                  onClick: _cache[12] || (_cache[12] = $event => (applyDialog.value = false))
                }, {
                  default: _withCtx$1(() => [...(_cache[34] || (_cache[34] = [
                    _createTextVNode$1("取消", -1)
                  ]))]),
                  _: 1
                }),
                _createVNode$1(_component_VBtn, {
                  color: "primary",
                  variant: "flat",
                  loading: __props.saving,
                  onClick: _cache[13] || (_cache[13] = $event => (saveTask(true)))
                }, {
                  default: _withCtx$1(() => [...(_cache[35] || (_cache[35] = [
                    _createTextVNode$1("保存并应用", -1)
                  ]))]),
                  _: 1
                }, 8, ["loading"])
              ]),
              _: 1
            })
          ]),
          _: 1
        })
      ]),
      _: 1
    }, 8, ["modelValue"])
  ], 64))
}
}

};
const TaskEditorDialog = /*#__PURE__*/_export_sfc(_sfc_main$1, [['__scopeId',"data-v-d98bc772"]]);

const {resolveComponent:_resolveComponent,createVNode:_createVNode,createElementVNode:_createElementVNode,toDisplayString:_toDisplayString,createTextVNode:_createTextVNode,withCtx:_withCtx,openBlock:_openBlock,createBlock:_createBlock,createCommentVNode:_createCommentVNode,mergeProps:_mergeProps,unref:_unref,Fragment:_Fragment,createElementBlock:_createElementBlock,renderList:_renderList,withModifiers:_withModifiers,normalizeClass:_normalizeClass,normalizeStyle:_normalizeStyle} = await importShared('vue');


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
const _hoisted_55 = { class: "text-body-2" };
const _hoisted_56 = { class: "brushflow-diagnostic-head" };
const _hoisted_57 = { class: "text-subtitle-1 font-weight-medium" };
const _hoisted_58 = { class: "text-body-2 text-medium-emphasis" };
const _hoisted_59 = {
  key: 1,
  class: "brushflow-diagnostic-grid"
};
const _hoisted_60 = { class: "brushflow-pipeline" };
const _hoisted_61 = { class: "brushflow-pipeline__index" };
const _hoisted_62 = {
  key: 0,
  class: "brushflow-reasons"
};
const _hoisted_63 = { class: "brushflow-reason__track" };
const _hoisted_64 = {
  key: 1,
  class: "brushflow-table-empty"
};
const _hoisted_65 = { class: "brushflow-events" };
const _hoisted_66 = {
  key: 0,
  class: "text-error"
};
const _hoisted_67 = {
  key: 0,
  class: "brushflow-table-empty"
};
const _hoisted_68 = { class: "brushflow-config-grid" };
const _hoisted_69 = { class: "brushflow-facts brushflow-facts--two" };
const _hoisted_70 = { key: 0 };
const _hoisted_71 = { class: "brushflow-config-actions" };

const {computed,inject,onMounted,onUnmounted,ref,watch} = await importShared('vue');


const _sfc_main = {
  __name: 'BrushFlowWorkbench',
  props: {
  api: { type: Object, default: () => ({}) },
  pluginId: { type: String, default: 'BrushFlow' },
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
const editorOpen = ref(false);
const editorTask = ref(cloneTask());
const globalEditorOpen = ref(false);
const globalEditorTask = ref({});
const deleteDialog = ref(false);
const clearDialog = ref(false);
const settingsMenu = ref(false);
const settingsDraft = ref({
  enabled: false,
  show_sidebar_nav: true,
  rotation_mode: 'parallel',
  manual_focus_site_id: null,
  global_disksize: null,
  global_defaults: {},
  global_proxy_delete: false,
  global_delete_size_range: null,
});
const hostToast = inject('moviepilot:toast', null);
let refreshTimer;

const pluginBase = computed(() => `plugin/${props.pluginId || 'BrushFlow'}`);
const tasks = computed(() => status.value.tasks || []);
const manualFocusSites = computed(() => {
  const sites = new Map();
  tasks.value.forEach(task => {
    const siteId = Number(task.site_id);
    if (task.enabled && Number.isSafeInteger(siteId) && siteId > 0 && !sites.has(siteId)) {
      sites.set(siteId, { title: task.site_name || task.name || String(siteId), value: siteId });
    }
  });
  return [...sites.values()]
});
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
  novice: '考核优先',
  ratio: '达标后切换',
}[siteRotation.value.phase] || ''));
const rotationRetrying = computed(() => sequentialMode.value && Boolean(siteRotation.value.retry_at));
const selectedTask = computed(() => tasks.value.find(item => item.id === selectedTaskId.value) || null);
const selectedState = computed(() => isRetryingTask(selectedTask.value)
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
async function loadStatus({ preserveSelection = true, loadDetail = true, syncSettings = !settingsMenu.value } = {}) {
  loading.value = true;
  error.value = '';
  try {
    const data = unwrapResponse(await props.api.get(`${pluginBase.value}/status`));
    status.value = data || status.value;
    statusLoaded.value = true;
    if (syncSettings) {
      settingsDraft.value = {
        enabled: Boolean(status.value.enabled),
        show_sidebar_nav: status.value.show_sidebar_nav !== false,
        rotation_mode: status.value.rotation_mode || 'parallel',
        manual_focus_site_id: status.value.manual_focus_site_id ?? null,
        global_disksize: status.value.global_disksize ?? null,
        global_defaults: status.value.global_defaults || {},
        global_proxy_delete: Boolean(status.value.global_proxy_delete),
        global_delete_size_range: status.value.global_delete_size_range ?? null,
      };
    }
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

// 打开一个空白任务草稿。
function openCreateTask() {
  editorTask.value = cloneTask();
  editorOpen.value = true;
}

// 打开当前任务的服务端配置快照。
function openEditTask() {
  if (!rawTaskConfig.value.id || rawTaskConfig.value.id !== selectedTaskId.value) return
  editorTask.value = cloneTask(rawTaskConfig.value);
  editorOpen.value = true;
}

function openGlobalDefaults() {
  globalEditorTask.value = JSON.parse(JSON.stringify(status.value.global_defaults || {}));
  settingsMenu.value = false;
  globalEditorOpen.value = true;
}

async function saveGlobalDefaults(defaults, applyToExisting = false) {
  saving.value = true;
  try {
    const payload = normalizeSettings({
      enabled: status.value.enabled,
      show_sidebar_nav: status.value.show_sidebar_nav,
      rotation_mode: status.value.rotation_mode || 'parallel',
      manual_focus_site_id: status.value.manual_focus_site_id ?? null,
      global_disksize: status.value.global_disksize,
      global_proxy_delete: status.value.global_proxy_delete,
      global_delete_size_range: status.value.global_delete_size_range,
      global_defaults: defaults,
      apply_defaults_to_existing: applyToExisting,
    });
    unwrapResponse(await props.api.post(`${pluginBase.value}/settings`, payload));
    globalEditorOpen.value = false;
    await loadStatus();
    notify(applyToExisting ? '统一参数已应用到全部站点' : '全局统一参数已保存，站点覆盖已保留');
  } catch (err) {
    notify(err?.message || '保存统一参数失败', 'error');
  } finally {
    saving.value = false;
  }
}

// 通过任务 API 创建或更新任务并刷新调度状态。
async function saveTask(task) {
  saving.value = true;
  try {
    const response = task.id
      ? await props.api.put(`${pluginBase.value}/tasks/${task.id}`, task)
      : await props.api.post(`${pluginBase.value}/tasks`, task);
    const detail = unwrapResponse(response);
    editorOpen.value = false;
    selectedTaskId.value = detail?.task?.id || task.id || selectedTaskId.value;
    await loadStatus();
    notify(task.id ? '刷流任务已更新' : '刷流任务已创建');
  } catch (err) {
    notify(err?.message || '保存刷流任务失败', 'error');
  } finally {
    saving.value = false;
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

// 保存全局插件与侧栏入口开关。
async function saveSettings() {
  saving.value = true;
  try {
    const payload = normalizeSettings(settingsDraft.value);
    status.value = unwrapResponse(await props.api.post(`${pluginBase.value}/settings`, payload));
    settingsMenu.value = false;
    await loadStatus();
    notify('全局设置已保存');
  } catch (err) {
    notify(err?.message || '保存全局设置失败', 'error');
  } finally {
    saving.value = false;
  }
}

async function saveManualFocus(value) {
  if (saving.value || !sequentialMode.value) {
    settingsDraft.value.manual_focus_site_id = status.value.manual_focus_site_id ?? null;
    return
  }
  saving.value = true;
  try {
    const payload = { manual_focus_site_id: normalizeSettings({ manual_focus_site_id: value }).manual_focus_site_id };
    status.value = unwrapResponse(await props.api.post(`${pluginBase.value}/focus`, payload));
    settingsDraft.value.manual_focus_site_id = status.value.manual_focus_site_id ?? null;
    notify(payload.manual_focus_site_id == null ? '已恢复自动调度' : manualFocusNotice.value || '手动重点已保存');
  } catch (err) {
    settingsDraft.value.manual_focus_site_id = status.value.manual_focus_site_id ?? null;
    notify(err?.message || '保存手动重点失败', 'error');
  } finally {
    saving.value = false;
  }
}

// 删除当前没有活跃种子的任务。
async function confirmDeleteTask() {
  saving.value = true;
  try {
    unwrapResponse(await props.api.delete(`${pluginBase.value}/tasks/${selectedTaskId.value}`));
    deleteDialog.value = false;
    selectedTaskId.value = '';
    await loadStatus({ preserveSelection: false });
    notify('刷流任务已删除');
  } catch (err) {
    notify(err?.message || '删除刷流任务失败', 'error');
  } finally {
    saving.value = false;
  }
}

// 清空当前任务的统计、诊断与种子记录。
async function confirmClearTask() {
  saving.value = true;
  try {
    taskDetail.value = unwrapResponse(
      await props.api.post(`${pluginBase.value}/tasks/${selectedTaskId.value}/clear`, {}),
    );
    clearDialog.value = false;
    await loadStatus();
    notify('任务数据已清除');
  } catch (err) {
    notify(err?.message || '清除任务数据失败', 'error');
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
  return task?.priority_reason || (task?.priority === 0 ? '新手考核优先' : task?.priority === 1 ? '分享率未达标' : '普通轮转')
}

function hasTaskError(task) {
  return isRetryingTask(task) || task.state === 'error' || Boolean(task.last_error) || task.last_run?.success === false || Boolean(task.metrics?.gap)
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
    if (!editorOpen.value && !globalEditorOpen.value && document.visibilityState === 'visible') loadStatus();
  }, 30000);
});

onUnmounted(() => {
  if (refreshTimer) window.clearInterval(refreshTimer);
});

__expose({ loadStatus, refreshAll, loading, saving });

return (_ctx, _cache) => {
  const _component_VIcon = _resolveComponent("VIcon");
  const _component_VBtn = _resolveComponent("VBtn");
  const _component_VChip = _resolveComponent("VChip");
  const _component_VSwitch = _resolveComponent("VSwitch");
  const _component_VSelect = _resolveComponent("VSelect");
  const _component_VDivider = _resolveComponent("VDivider");
  const _component_VTextField = _resolveComponent("VTextField");
  const _component_VCardText = _resolveComponent("VCardText");
  const _component_VSpacer = _resolveComponent("VSpacer");
  const _component_VCardActions = _resolveComponent("VCardActions");
  const _component_VCard = _resolveComponent("VCard");
  const _component_VMenu = _resolveComponent("VMenu");
  const _component_VAlert = _resolveComponent("VAlert");
  const _component_VSkeletonLoader = _resolveComponent("VSkeletonLoader");
  const _component_VSheet = _resolveComponent("VSheet");
  const _component_VAvatar = _resolveComponent("VAvatar");
  const _component_VTooltip = _resolveComponent("VTooltip");
  const _component_VTab = _resolveComponent("VTab");
  const _component_VTabs = _resolveComponent("VTabs");
  const _component_VProgressLinear = _resolveComponent("VProgressLinear");
  const _component_VBtnToggle = _resolveComponent("VBtnToggle");
  const _component_VDataTable = _resolveComponent("VDataTable");
  const _component_VPagination = _resolveComponent("VPagination");
  const _component_VWindowItem = _resolveComponent("VWindowItem");
  const _component_VWindow = _resolveComponent("VWindow");
  const _component_VDialog = _resolveComponent("VDialog");

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
          _cache[24] || (_cache[24] = _createElementVNode("h1", null, "站点刷流", -1)),
          _createElementVNode("p", null, "统一参数 · 考核与分享率优先 · " + _toDisplayString(sequentialMode.value ? '单站轮换' : '多站并行'), 1)
        ])
      ]),
      _createElementVNode("div", _hoisted_3, [
        _createVNode(_component_VBtn, {
          color: "primary",
          variant: "tonal",
          "prepend-icon": "mdi-tune-variant",
          disabled: !statusLoaded.value || saving.value,
          onClick: openGlobalDefaults
        }, {
          default: _withCtx(() => [...(_cache[25] || (_cache[25] = [
            _createTextVNode("统一参数", -1)
          ]))]),
          _: 1
        }, 8, ["disabled"]),
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
        _createVNode(_component_VMenu, {
          modelValue: settingsMenu.value,
          "onUpdate:modelValue": _cache[7] || (_cache[7] = $event => ((settingsMenu).value = $event)),
          "close-on-content-click": false,
          location: "bottom end"
        }, {
          activator: _withCtx(({ props: menuProps }) => [
            _createVNode(_component_VBtn, _mergeProps(menuProps, {
              icon: "mdi-tune-variant",
              variant: "text",
              "aria-label": "全局设置"
            }), null, 16)
          ]),
          default: _withCtx(() => [
            _createVNode(_component_VCard, {
              class: "brushflow-settings-menu",
              title: "全局设置"
            }, {
              default: _withCtx(() => [
                _createVNode(_component_VCardText, { class: "settings-menu__body" }, {
                  default: _withCtx(() => [
                    _createVNode(_component_VSwitch, {
                      modelValue: settingsDraft.value.enabled,
                      "onUpdate:modelValue": _cache[0] || (_cache[0] = $event => ((settingsDraft.value.enabled) = $event)),
                      label: "启用插件",
                      color: "primary",
                      "hide-details": "",
                      inset: ""
                    }, null, 8, ["modelValue"]),
                    _createVNode(_component_VSwitch, {
                      modelValue: settingsDraft.value.show_sidebar_nav,
                      "onUpdate:modelValue": _cache[1] || (_cache[1] = $event => ((settingsDraft.value.show_sidebar_nav) = $event)),
                      label: "显示侧栏入口",
                      color: "primary",
                      "hide-details": "",
                      inset: ""
                    }, null, 8, ["modelValue"]),
                    _createVNode(_component_VSelect, {
                      modelValue: settingsDraft.value.rotation_mode,
                      "onUpdate:modelValue": _cache[2] || (_cache[2] = $event => ((settingsDraft.value.rotation_mode) = $event)),
                      label: "站点轮换模式",
                      items: [
                  { title: '多站并行', value: 'parallel' },
                  { title: '单站达标后轮转', value: 'sequential' },
                ],
                      hint: "考核优先，其次未达标分享率，全部达标后轮转，默认每站 24 小时。账号数据未知时需先刷新。",
                      "persistent-hint": ""
                    }, null, 8, ["modelValue"]),
                    (settingsDraft.value.rotation_mode === 'sequential')
                      ? (_openBlock(), _createBlock(_component_VSelect, {
                          key: 0,
                          modelValue: settingsDraft.value.manual_focus_site_id,
                          "onUpdate:modelValue": [
                            _cache[3] || (_cache[3] = $event => ((settingsDraft.value.manual_focus_site_id) = $event)),
                            saveManualFocus
                          ],
                          label: "手动重点站",
                          items: manualFocusSites.value,
                          loading: saving.value,
                          disabled: saving.value || !sequentialMode.value,
                          placeholder: "自动调度",
                          hint: "留空自动调度；选定站点优先于自动调度，清空后恢复自动。",
                          clearable: "",
                          "persistent-hint": ""
                        }, null, 8, ["modelValue", "items", "loading", "disabled"]))
                      : _createCommentVNode("", true),
                    _createVNode(_component_VDivider),
                    _createVNode(_component_VSwitch, {
                      modelValue: settingsDraft.value.global_proxy_delete,
                      "onUpdate:modelValue": _cache[4] || (_cache[4] = $event => ((settingsDraft.value.global_proxy_delete) = $event)),
                      label: "全局动态删种",
                      color: "primary",
                      "hide-details": "",
                      inset: ""
                    }, null, 8, ["modelValue"]),
                    (settingsDraft.value.global_proxy_delete)
                      ? (_openBlock(), _createBlock(_component_VTextField, {
                          key: 1,
                          modelValue: settingsDraft.value.global_delete_size_range,
                          "onUpdate:modelValue": _cache[5] || (_cache[5] = $event => ((settingsDraft.value.global_delete_size_range) = $event)),
                          label: "全局动态删种阈值（GB）",
                          placeholder: "50-100",
                          clearable: "",
                          "hide-details": ""
                        }, null, 8, ["modelValue"]))
                      : _createCommentVNode("", true),
                    _createVNode(_component_VDivider),
                    _createVNode(_component_VTextField, {
                      modelValue: settingsDraft.value.global_disksize,
                      "onUpdate:modelValue": _cache[6] || (_cache[6] = $event => ((settingsDraft.value.global_disksize) = $event)),
                      modelModifiers: { number: true },
                      type: "number",
                      min: "0",
                      label: "全局保种体积（GB）",
                      clearable: "",
                      "hide-details": ""
                    }, null, 8, ["modelValue"]),
                    _cache[26] || (_cache[26] = _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "插件在下载器上限内分配下载名额，速度上限在下载器设置。", -1))
                  ]),
                  _: 1
                }),
                _createVNode(_component_VCardActions, null, {
                  default: _withCtx(() => [
                    _createVNode(_component_VSpacer),
                    _createVNode(_component_VBtn, {
                      color: "primary",
                      variant: "flat",
                      loading: saving.value,
                      onClick: saveSettings
                    }, {
                      default: _withCtx(() => [...(_cache[27] || (_cache[27] = [
                        _createTextVNode("保存", -1)
                      ]))]),
                      _: 1
                    }, 8, ["loading"])
                  ]),
                  _: 1
                })
              ]),
              _: 1
            })
          ]),
          _: 1
        }, 8, ["modelValue"]),
        (__props.showClose)
          ? (_openBlock(), _createBlock(_component_VBtn, {
              key: 1,
              icon: "mdi-close",
              variant: "text",
              "aria-label": "关闭",
              onClick: _cache[8] || (_cache[8] = $event => (emit('close')))
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
          "onClick:close": _cache[9] || (_cache[9] = $event => (error.value = ''))
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
          default: _withCtx(() => [...(_cache[28] || (_cache[28] = [
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
            _cache[29] || (_cache[29] = _createTextVNode(" 每次仅当前站新增。自动调度：考核优先，其次未达标分享率，全部达标后轮转；账号数据未知时需先刷新。旧种继续下载、做种并按删除规则清理。 ", -1))
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
            _cache[31] || (_cache[31] = _createElementVNode("div", { class: "text-h6" }, "还没有刷流任务", -1)),
            _cache[32] || (_cache[32] = _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "先设置统一参数，再添加站点；站点规则留空跟随全局，填写后仅此站生效。", -1)),
            _createVNode(_component_VBtn, {
              color: "primary",
              variant: "flat",
              "prepend-icon": "mdi-plus",
              onClick: openCreateTask
            }, {
              default: _withCtx(() => [...(_cache[30] || (_cache[30] = [
                _createTextVNode("创建第一个任务", -1)
              ]))]),
              _: 1
            })
          ]))
        : (_openBlock(), _createElementBlock("div", _hoisted_10, [
            _createElementVNode("div", _hoisted_11, [
              _createVNode(_component_VSheet, { class: "brushflow-stat app-surface-static" }, {
                default: _withCtx(() => [
                  _cache[33] || (_cache[33] = _createElementVNode("span", null, "近 30 分钟上传", -1)),
                  _createElementVNode("strong", null, _toDisplayString(metricBytes(metrics.value)), 1),
                  _createElementVNode("small", null, _toDisplayString(metricWindow(metrics.value)), 1)
                ]),
                _: 1
              }),
              _createVNode(_component_VSheet, { class: "brushflow-stat app-surface-static" }, {
                default: _withCtx(() => [
                  _cache[34] || (_cache[34] = _createElementVNode("span", null, "当前上传", -1)),
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
                  _cache[35] || (_cache[35] = _createElementVNode("span", null, "下载 / 排队 / 做种", -1)),
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
                  _cache[36] || (_cache[36] = _createElementVNode("span", null, "托管空间", -1)),
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
                      _cache[37] || (_cache[37] = _createTextVNode("全部站点 ", -1)),
                      _createElementVNode("span", _hoisted_16, _toDisplayString(tasks.value.length), 1)
                    ]),
                    _cache[38] || (_cache[38] = _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "点击站点查看种子与运行详情", -1))
                  ]),
                  _createElementVNode("div", _hoisted_17, [
                    _createVNode(_component_VSelect, {
                      modelValue: siteSort.value,
                      "onUpdate:modelValue": _cache[10] || (_cache[10] = $event => ((siteSort).value = $event)),
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
                    }, null, 8, ["modelValue"]),
                    _createVNode(_component_VBtn, {
                      color: "primary",
                      variant: "tonal",
                      "prepend-icon": "mdi-plus",
                      onClick: openCreateTask
                    }, {
                      default: _withCtx(() => [...(_cache[39] || (_cache[39] = [
                        _createTextVNode("添加站点", -1)
                      ]))]),
                      _: 1
                    })
                  ])
                ]),
                _createElementVNode("div", _hoisted_18, [
                  _createElementVNode("table", _hoisted_19, [
                    _cache[40] || (_cache[40] = _createElementVNode("thead", null, [
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
                            _createElementVNode("small", null, _toDisplayString(task.enabled ? taskPriorityText(task) : '站点管理已停用'), 1)
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
                                  default: _withCtx(() => [...(_cache[41] || (_cache[41] = [
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
                              loading: selectedTask.value.operation === 'brush',
                              onClick: _cache[11] || (_cache[11] = $event => (runOperation('brush')))
                            }), null, 16, ["loading"])
                          ]),
                          _: 1
                        }),
                        _createVNode(_component_VTooltip, { text: "检查种子" }, {
                          activator: _withCtx(({ props: tipProps }) => [
                            _createVNode(_component_VBtn, _mergeProps(tipProps, {
                              icon: "mdi-progress-check",
                              variant: "text",
                              loading: selectedTask.value.operation === 'check',
                              onClick: _cache[12] || (_cache[12] = $event => (runOperation('check')))
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
                              variant: "text",
                              onClick: toggleSelectedTask
                            }), null, 16, ["icon", "aria-label"])
                          ]),
                          _: 1
                        }, 8, ["text"]),
                        _createVNode(_component_VTooltip, { text: "编辑任务" }, {
                          activator: _withCtx(({ props: tipProps }) => [
                            _createVNode(_component_VBtn, _mergeProps(tipProps, {
                              icon: "mdi-pencil-outline",
                              variant: "text",
                              disabled: taskLoading.value || rawTaskConfig.value.id !== selectedTaskId.value,
                              onClick: openEditTask
                            }), null, 16, ["disabled"])
                          ]),
                          _: 1
                        })
                      ])
                    ]),
                    _createVNode(_component_VTabs, {
                      modelValue: activeTab.value,
                      "onUpdate:modelValue": _cache[13] || (_cache[13] = $event => ((activeTab).value = $event)),
                      color: "primary",
                      class: "brushflow-tabs"
                    }, {
                      default: _withCtx(() => [
                        _createVNode(_component_VTab, { value: "overview" }, {
                          default: _withCtx(() => [...(_cache[42] || (_cache[42] = [
                            _createTextVNode("任务概览", -1)
                          ]))]),
                          _: 1
                        }),
                        _createVNode(_component_VTab, { value: "diagnostics" }, {
                          default: _withCtx(() => [...(_cache[43] || (_cache[43] = [
                            _createTextVNode("运行诊断", -1)
                          ]))]),
                          _: 1
                        }),
                        _createVNode(_component_VTab, { value: "config" }, {
                          default: _withCtx(() => [...(_cache[44] || (_cache[44] = [
                            _createTextVNode("任务配置", -1)
                          ]))]),
                          _: 1
                        })
                      ]),
                      _: 1
                    }, 8, ["modelValue"]),
                    _createVNode(_component_VDivider),
                    _createVNode(_component_VWindow, {
                      modelValue: activeTab.value,
                      "onUpdate:modelValue": _cache[17] || (_cache[17] = $event => ((activeTab).value = $event)),
                      touch: false,
                      class: "brushflow-window"
                    }, {
                      default: _withCtx(() => [
                        _createVNode(_component_VWindowItem, { value: "overview" }, {
                          default: _withCtx(() => [
                            _createElementVNode("div", _hoisted_37, [
                              _createVNode(_component_VSheet, { class: "brushflow-stat app-surface-static" }, {
                                default: _withCtx(() => [
                                  _cache[45] || (_cache[45] = _createElementVNode("span", null, "活跃种子", -1)),
                                  _createElementVNode("strong", null, _toDisplayString(selectedTask.value.statistic.active || 0), 1),
                                  _createElementVNode("small", null, _toDisplayString(selectedTask.value.statistic.unarchived || 0) + " 个待归档", 1)
                                ]),
                                _: 1
                              }),
                              _createVNode(_component_VSheet, { class: "brushflow-stat app-surface-static" }, {
                                default: _withCtx(() => [
                                  _cache[46] || (_cache[46] = _createElementVNode("span", null, "近 30 分钟上传", -1)),
                                  _createElementVNode("strong", null, _toDisplayString(metricBytes(selectedTask.value.metrics)), 1),
                                  _createElementVNode("small", null, _toDisplayString(metricWindow(selectedTask.value.metrics)), 1)
                                ]),
                                _: 1
                              }),
                              _createVNode(_component_VSheet, { class: "brushflow-stat app-surface-static" }, {
                                default: _withCtx(() => [
                                  _cache[47] || (_cache[47] = _createElementVNode("span", null, "托管空间", -1)),
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
                                  _cache[48] || (_cache[48] = _createElementVNode("span", null, "站点分享率", -1)),
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
                                    _cache[49] || (_cache[49] = _createElementVNode("div", null, [
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
                                      _cache[50] || (_cache[50] = _createElementVNode("dt", null, "选种优先级", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(priorityText.value), 1)
                                    ]),
                                    (sequentialMode.value)
                                      ? (_openBlock(), _createElementBlock("div", _hoisted_41, [
                                          _cache[51] || (_cache[51] = _createElementVNode("dt", null, "达标后每站驻留", -1)),
                                          _createElementVNode("dd", null, _toDisplayString(taskConfig.value.rotation_hours ?? 24) + " 小时", 1)
                                        ]))
                                      : _createCommentVNode("", true),
                                    (sequentialMode.value && selectedTask.value.is_focus && focusSwitchText.value)
                                      ? (_openBlock(), _createElementBlock("div", _hoisted_42, [
                                          _cache[52] || (_cache[52] = _createElementVNode("dt", null, "切换条件", -1)),
                                          _createElementVNode("dd", null, _toDisplayString(focusSwitchText.value), 1)
                                        ]))
                                      : (sequentialMode.value && selectedTask.value.is_focus && siteRotation.value.deadline_at)
                                        ? (_openBlock(), _createElementBlock("div", _hoisted_43, [
                                            _cache[53] || (_cache[53] = _createElementVNode("dt", null, "预计切换", -1)),
                                            _createElementVNode("dd", null, _toDisplayString(_unref(formatDateTime)(siteRotation.value.deadline_at)), 1)
                                          ]))
                                        : _createCommentVNode("", true),
                                    _createElementVNode("div", null, [
                                      _cache[54] || (_cache[54] = _createElementVNode("dt", null, "刷新周期", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.cron || `每 ${taskConfig.value.brush_interval} 分钟`), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[55] || (_cache[55] = _createElementVNode("dt", null, "检查周期", -1)),
                                      _createElementVNode("dd", null, "每 " + _toDisplayString(taskConfig.value.check_interval) + " 分钟", 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[56] || (_cache[56] = _createElementVNode("dt", null, "开启时段", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.active_time_range || '全天'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[57] || (_cache[57] = _createElementVNode("dt", null, "站点分享率", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(formatSiteRatio(selectedTask.value.site_ratio)) + " / " + _toDisplayString(taskConfig.value.site_ratio_target || '不限'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[58] || (_cache[58] = _createElementVNode("dt", null, "选种来源", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.rss_support ? 'RSS' : '站点列表页'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[59] || (_cache[59] = _createElementVNode("dt", null, "促销要求", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.freeleech === '2xfree' ? '2X 免费' : taskConfig.value.freeleech === 'free' ? '免费' : '全部'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[60] || (_cache[60] = _createElementVNode("dt", null, "删种策略", -1)),
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
                                      _cache[61] || (_cache[61] = _createElementVNode("div", { class: "text-subtitle-1 font-weight-medium" }, "最近一次刷新", -1)),
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
                                      _cache[62] || (_cache[62] = _createElementVNode("span", null, "站点候选", -1)),
                                      _createElementVNode("strong", null, _toDisplayString(latestBrushRun.value?.source_count || 0), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[63] || (_cache[63] = _createElementVNode("span", null, "规则过滤", -1)),
                                      _createElementVNode("strong", null, _toDisplayString(latestBrushRun.value?.filtered_count || 0), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[64] || (_cache[64] = _createElementVNode("span", null, "新增下载", -1)),
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
                                    onClick: _cache[14] || (_cache[14] = $event => (activeTab.value = 'diagnostics'))
                                  }, {
                                    default: _withCtx(() => [...(_cache[65] || (_cache[65] = [
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
                                      _cache[66] || (_cache[66] = _createElementVNode("span", { class: "text-subtitle-1 font-weight-medium" }, "托管种子", -1)),
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
                                    _cache[67] || (_cache[67] = _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "当前任务独立记录", -1))
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
                                        default: _withCtx(() => [...(_cache[68] || (_cache[68] = [
                                          _createTextVNode("活跃", -1)
                                        ]))]),
                                        _: 1
                                      }),
                                      _createVNode(_component_VBtn, { value: "deleted" }, {
                                        default: _withCtx(() => [...(_cache[69] || (_cache[69] = [
                                          _createTextVNode("已删除", -1)
                                        ]))]),
                                        _: 1
                                      }),
                                      _createVNode(_component_VBtn, { value: "all" }, {
                                        default: _withCtx(() => [...(_cache[70] || (_cache[70] = [
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
                                  "no-data": _withCtx(() => [...(_cache[71] || (_cache[71] = [
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
                                  title: "账号数据刷新失败",
                                  class: "mb-4"
                                }, {
                                  default: _withCtx(() => [
                                    _cache[72] || (_cache[72] = _createElementVNode("div", null, "此状态来自站点账号统计刷新，与下载器种子检查独立。", -1)),
                                    _createElementVNode("div", _hoisted_55, " 连续失败 " + _toDisplayString(selectedTask.value.account_refresh.failures ?? '未知') + " 次 · 最近刷新 " + _toDisplayString(selectedTask.value.account_refresh.checked_at != null ? _unref(formatDateTime)(selectedTask.value.account_refresh.checked_at * 1000) : '未知') + " · 下次重试 " + _toDisplayString(selectedTask.value.account_refresh.next_attempt != null ? _unref(formatDateTime)(selectedTask.value.account_refresh.next_attempt * 1000) : '未知'), 1)
                                  ]),
                                  _: 1
                                }))
                              : _createCommentVNode("", true),
                            _createElementVNode("div", _hoisted_56, [
                              _createElementVNode("div", null, [
                                _createElementVNode("div", _hoisted_57, _toDisplayString(latestBrushRun.value ? `刷流刷新 #${latestBrushRun.value.id.slice(0, 8)}` : '暂无刷流刷新记录'), 1),
                                _createElementVNode("div", _hoisted_58, _toDisplayString(latestBrushRun.value ? `${_unref(formatDateTime)(latestBrushRun.value.started_at)} · ${_unref(formatDuration)(latestBrushRun.value.started_at, latestBrushRun.value.finished_at)}` : '执行一次任务后将显示筛选流水线和过滤原因'), 1)
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
                              ? (_openBlock(), _createElementBlock("div", _hoisted_59, [
                                  _createVNode(_component_VSheet, {
                                    tag: "section",
                                    class: "brushflow-panel app-surface-static"
                                  }, {
                                    default: _withCtx(() => [
                                      _cache[73] || (_cache[73] = _createElementVNode("header", { class: "brushflow-panel__head" }, [
                                        _createElementVNode("div", null, [
                                          _createElementVNode("div", { class: "text-subtitle-1 font-weight-medium" }, "选种流水线"),
                                          _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "本轮候选在各阶段的剩余数量")
                                        ])
                                      ], -1)),
                                      _createElementVNode("ol", _hoisted_60, [
                                        (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(pipelineStages.value, (stage, index) => {
                                          return (_openBlock(), _createElementBlock("li", {
                                            key: stage.title
                                          }, [
                                            _createElementVNode("span", _hoisted_61, _toDisplayString(index + 1), 1),
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
                                      _cache[74] || (_cache[74] = _createElementVNode("header", { class: "brushflow-panel__head" }, [
                                        _createElementVNode("div", null, [
                                          _createElementVNode("div", { class: "text-subtitle-1 font-weight-medium" }, "过滤原因"),
                                          _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "本轮未进入下载器的候选分布")
                                        ])
                                      ], -1)),
                                      (reasonEntries.value.length)
                                        ? (_openBlock(), _createElementBlock("div", _hoisted_62, [
                                            (_openBlock(true), _createElementBlock(_Fragment, null, _renderList(reasonEntries.value, (item) => {
                                              return (_openBlock(), _createElementBlock("div", {
                                                key: item.label,
                                                class: "brushflow-reason"
                                              }, [
                                                _createElementVNode("div", null, [
                                                  _createElementVNode("span", null, _toDisplayString(item.label), 1),
                                                  _createElementVNode("strong", null, _toDisplayString(item.count), 1)
                                                ]),
                                                _createElementVNode("span", _hoisted_63, [
                                                  _createElementVNode("i", {
                                                    style: _normalizeStyle({ width: `${(item.count / maxReasonCount.value) * 100}%` })
                                                  }, null, 4)
                                                ])
                                              ]))
                                            }), 128))
                                          ]))
                                        : (_openBlock(), _createElementBlock("div", _hoisted_64, "本轮没有记录过滤原因"))
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
                                _cache[75] || (_cache[75] = _createElementVNode("header", { class: "brushflow-panel__head" }, [
                                  _createElementVNode("div", null, [
                                    _createElementVNode("div", { class: "text-subtitle-1 font-weight-medium" }, "最近事件"),
                                    _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "刷流刷新与种子检查独立记录")
                                  ])
                                ], -1)),
                                _createElementVNode("div", _hoisted_65, [
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
                                          ? (_openBlock(), _createElementBlock("span", _hoisted_66, _toDisplayString(run.error), 1))
                                          : _createCommentVNode("", true)
                                      ])
                                    ]))
                                  }), 128)),
                                  (!taskRuns.value.length)
                                    ? (_openBlock(), _createElementBlock("div", _hoisted_67, "暂无运行事件"))
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
                            _createElementVNode("div", _hoisted_68, [
                              _createVNode(_component_VSheet, {
                                tag: "section",
                                class: "brushflow-panel app-surface-static"
                              }, {
                                default: _withCtx(() => [
                                  _cache[95] || (_cache[95] = _createElementVNode("header", { class: "brushflow-panel__head" }, [
                                    _createElementVNode("div", null, [
                                      _createElementVNode("div", { class: "text-subtitle-1 font-weight-medium" }, "任务规则"),
                                      _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "当前生效配置")
                                    ])
                                  ], -1)),
                                  _createElementVNode("dl", _hoisted_69, [
                                    _createElementVNode("div", null, [
                                      _cache[76] || (_cache[76] = _createElementVNode("dt", null, "任务状态", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(selectedTask.value.enabled ? '管理中' : '已停止选种与检查'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[77] || (_cache[77] = _createElementVNode("dt", null, "通知", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.notify ? '发送' : '关闭'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[78] || (_cache[78] = _createElementVNode("dt", null, "站点", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(selectedTask.value.site_name), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[79] || (_cache[79] = _createElementVNode("dt", null, "下载器", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(selectedTask.value.downloader), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[80] || (_cache[80] = _createElementVNode("dt", null, "种子大小", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.size || '不限'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[81] || (_cache[81] = _createElementVNode("dt", null, "做种人数", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.seeder || '不限'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[82] || (_cache[82] = _createElementVNode("dt", null, "下载人数", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.leecher || '不限'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[83] || (_cache[83] = _createElementVNode("dt", null, "下载 / 做种人数下限", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.min_peer_seed_ratio ?? '不限'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[84] || (_cache[84] = _createElementVNode("dt", null, "发布时间", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.pubtime ? `${taskConfig.value.pubtime} 分钟` : '不限'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[85] || (_cache[85] = _createElementVNode("dt", null, "排除 H&R", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.hr === 'yes' ? '是' : '否'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[86] || (_cache[86] = _createElementVNode("dt", null, "包含规则", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.include || '无'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[87] || (_cache[87] = _createElementVNode("dt", null, "排除规则", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.exclude || '无'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[88] || (_cache[88] = _createElementVNode("dt", null, "保种上限", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.disksize ? `${taskConfig.value.disksize} GB` : '不限'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[89] || (_cache[89] = _createElementVNode("dt", null, "目标分享率", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.site_ratio_target || '不限') + _toDisplayString(sequentialMode.value && ratioPhase.value ? '（达标后切换）' : ''), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[90] || (_cache[90] = _createElementVNode("dt", null, "达标后每站驻留时间", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.rotation_hours ?? 24) + " 小时" + _toDisplayString(sequentialMode.value ? '' : '（单站轮换模式生效）'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[91] || (_cache[91] = _createElementVNode("dt", null, "新手考核", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(selectedTask.value.novice ? '优先' : taskConfig.value.novice ? '已到期' : '关闭'), 1)
                                    ]),
                                    (taskConfig.value.novice_until)
                                      ? (_openBlock(), _createElementBlock("div", _hoisted_70, [
                                          _cache[92] || (_cache[92] = _createElementVNode("dt", null, "考核截止", -1)),
                                          _createElementVNode("dd", null, _toDisplayString(_unref(formatDateTime)(taskConfig.value.novice_until)), 1)
                                        ]))
                                      : _createCommentVNode("", true),
                                    _createElementVNode("div", null, [
                                      _cache[93] || (_cache[93] = _createElementVNode("dt", null, "促销保护", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.del_no_free ? `提前 ${taskConfig.value.promotion_lead_minutes ?? 10} 分钟，包含排队任务` : '关闭'), 1)
                                    ]),
                                    _createElementVNode("div", null, [
                                      _cache[94] || (_cache[94] = _createElementVNode("dt", null, "归档天数", -1)),
                                      _createElementVNode("dd", null, _toDisplayString(taskConfig.value.auto_archive_days || '不自动归档'), 1)
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
                                  _cache[102] || (_cache[102] = _createElementVNode("header", { class: "brushflow-panel__head" }, [
                                    _createElementVNode("div", null, [
                                      _createElementVNode("div", { class: "text-subtitle-1 font-weight-medium" }, "任务数据"),
                                      _createElementVNode("div", { class: "text-body-2 text-medium-emphasis" }, "以下操作只影响当前任务")
                                    ])
                                  ], -1)),
                                  _createElementVNode("div", _hoisted_71, [
                                    _createElementVNode("div", null, [
                                      _cache[97] || (_cache[97] = _createElementVNode("strong", null, "清除统计与记录", -1)),
                                      _cache[98] || (_cache[98] = _createElementVNode("span", null, "下载器中的任务标签种子会在下次检查时重新纳入", -1)),
                                      _createVNode(_component_VBtn, {
                                        color: "warning",
                                        variant: "tonal",
                                        "prepend-icon": "mdi-eraser",
                                        onClick: _cache[15] || (_cache[15] = $event => (clearDialog.value = true))
                                      }, {
                                        default: _withCtx(() => [...(_cache[96] || (_cache[96] = [
                                          _createTextVNode("清除数据", -1)
                                        ]))]),
                                        _: 1
                                      })
                                    ]),
                                    _createVNode(_component_VDivider),
                                    _createElementVNode("div", null, [
                                      _cache[100] || (_cache[100] = _createElementVNode("strong", null, "删除任务", -1)),
                                      _cache[101] || (_cache[101] = _createElementVNode("span", null, "存在活跃种子时后端会拒绝删除，避免留下失管任务", -1)),
                                      _createVNode(_component_VBtn, {
                                        color: "error",
                                        variant: "tonal",
                                        "prepend-icon": "mdi-delete-outline",
                                        onClick: _cache[16] || (_cache[16] = $event => (deleteDialog.value = true))
                                      }, {
                                        default: _withCtx(() => [...(_cache[99] || (_cache[99] = [
                                          _createTextVNode("删除任务", -1)
                                        ]))]),
                                        _: 1
                                      })
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
    _createVNode(TaskEditorDialog, {
      modelValue: editorOpen.value,
      "onUpdate:modelValue": _cache[18] || (_cache[18] = $event => ((editorOpen).value = $event)),
      task: editorTask.value,
      sites: status.value.options.sites,
      downloaders: status.value.options.downloaders,
      "global-defaults": status.value.global_defaults || {},
      "global-dynamic-delete": Boolean(status.value.global_proxy_delete),
      saving: saving.value,
      onSave: saveTask
    }, null, 8, ["modelValue", "task", "sites", "downloaders", "global-defaults", "global-dynamic-delete", "saving"]),
    _createVNode(TaskEditorDialog, {
      modelValue: globalEditorOpen.value,
      "onUpdate:modelValue": _cache[19] || (_cache[19] = $event => ((globalEditorOpen).value = $event)),
      task: globalEditorTask.value,
      downloaders: status.value.options.downloaders,
      "global-dynamic-delete": Boolean(status.value.global_proxy_delete),
      saving: saving.value,
      "global-mode": "",
      onSave: saveGlobalDefaults
    }, null, 8, ["modelValue", "task", "downloaders", "global-dynamic-delete", "saving"]),
    _createVNode(_component_VDialog, {
      modelValue: deleteDialog.value,
      "onUpdate:modelValue": _cache[21] || (_cache[21] = $event => ((deleteDialog).value = $event)),
      "max-width": "28rem"
    }, {
      default: _withCtx(() => [
        _createVNode(_component_VCard, { title: "删除刷流任务" }, {
          default: _withCtx(() => [
            _createVNode(_component_VCardText, null, {
              default: _withCtx(() => [
                _createTextVNode("确认删除“" + _toDisplayString(selectedTask.value?.name) + "”？存在活跃种子时不会执行删除。", 1)
              ]),
              _: 1
            }),
            _createVNode(_component_VCardActions, null, {
              default: _withCtx(() => [
                _createVNode(_component_VSpacer),
                _createVNode(_component_VBtn, {
                  variant: "text",
                  onClick: _cache[20] || (_cache[20] = $event => (deleteDialog.value = false))
                }, {
                  default: _withCtx(() => [...(_cache[103] || (_cache[103] = [
                    _createTextVNode("取消", -1)
                  ]))]),
                  _: 1
                }),
                _createVNode(_component_VBtn, {
                  color: "error",
                  variant: "flat",
                  loading: saving.value,
                  onClick: confirmDeleteTask
                }, {
                  default: _withCtx(() => [...(_cache[104] || (_cache[104] = [
                    _createTextVNode("删除", -1)
                  ]))]),
                  _: 1
                }, 8, ["loading"])
              ]),
              _: 1
            })
          ]),
          _: 1
        })
      ]),
      _: 1
    }, 8, ["modelValue"]),
    _createVNode(_component_VDialog, {
      modelValue: clearDialog.value,
      "onUpdate:modelValue": _cache[23] || (_cache[23] = $event => ((clearDialog).value = $event)),
      "max-width": "28rem"
    }, {
      default: _withCtx(() => [
        _createVNode(_component_VCard, { title: "清除任务数据" }, {
          default: _withCtx(() => [
            _createVNode(_component_VCardText, null, {
              default: _withCtx(() => [...(_cache[105] || (_cache[105] = [
                _createTextVNode("将清除当前任务的统计、运行诊断、托管和归档记录，下载器内的种子与文件不会删除。", -1)
              ]))]),
              _: 1
            }),
            _createVNode(_component_VCardActions, null, {
              default: _withCtx(() => [
                _createVNode(_component_VSpacer),
                _createVNode(_component_VBtn, {
                  variant: "text",
                  onClick: _cache[22] || (_cache[22] = $event => (clearDialog.value = false))
                }, {
                  default: _withCtx(() => [...(_cache[106] || (_cache[106] = [
                    _createTextVNode("取消", -1)
                  ]))]),
                  _: 1
                }),
                _createVNode(_component_VBtn, {
                  color: "warning",
                  variant: "flat",
                  loading: saving.value,
                  onClick: confirmClearTask
                }, {
                  default: _withCtx(() => [...(_cache[107] || (_cache[107] = [
                    _createTextVNode("清除", -1)
                  ]))]),
                  _: 1
                }, 8, ["loading"])
              ]),
              _: 1
            })
          ]),
          _: 1
        })
      ]),
      _: 1
    }, 8, ["modelValue"])
  ], 2))
}
}

};
const BrushFlowWorkbench = /*#__PURE__*/_export_sfc(_sfc_main, [['__scopeId',"data-v-cf27f386"]]);

export { BrushFlowWorkbench as B };
