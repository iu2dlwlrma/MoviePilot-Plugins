<script setup>
import { computed, ref, watch } from 'vue'
import { useDisplay } from 'vuetify'
import { cloneTask, editableTask, effectiveTask, normalizeDefaults, normalizeTask, taskDefaults, taskOverrideFields } from '../utils'

const props = defineProps({
  modelValue: { type: Boolean, default: false },
  task: { type: Object, default: () => ({}) },
  sites: { type: Array, default: () => [] },
  downloaders: { type: Array, default: () => [] },
  globalDefaults: { type: Object, default: () => ({}) },
  globalMode: { type: Boolean, default: false },
  globalDynamicDelete: { type: Boolean, default: false },
  saving: { type: Boolean, default: false },
})

const emit = defineEmits(['update:modelValue', 'save'])
const display = useDisplay()
const formRef = ref(null)
const activeTab = ref('base')
const localTask = ref(cloneTask())
const applyDialog = ref(false)
const formError = ref('')
const required = value => !!String(value ?? '').trim() || '此项不能为空'
const positive = value => value === '' || value == null || Number(value) > 0 || '请输入大于 0 的数值'

const sections = [
  {
    value: 'base', title: '常用与调度', icon: 'mdi-calendar-clock',
    description: '单站轮换按驻留时间切换。分享率决定优先级，不会提前切走；并发和速度由下载器管理。',
    fields: [
      { key: 'downloader', label: '下载器', type: 'downloader', required: true },
      { key: 'save_path', label: '保存目录', globalHint: '留空使用下载器默认目录' },
      { key: 'brush_interval', label: '刷流刷新周期（分钟）', type: 'number', min: 1, max: 1440, required: true },
      { key: 'check_interval', label: '状态检查周期（分钟）', type: 'number', min: 1, max: 1440, required: true },
      { key: 'rotation_hours', label: '每站驻留时间（小时）', type: 'number', min: 0, max: 168, required: true, hint: '单站轮换模式生效，默认 24 小时；切换后旧种继续按原规则下载、做种和删除。' },
      { key: 'cron', label: 'CRON 表达式', globalHint: '留空使用固定刷新周期' },
      { key: 'active_time_range', label: '开启时间段', hint: '如 00:00-08:00', globalHint: '如 00:00-08:00，留空为全天' },
      { key: 'site_ratio_target', label: '目标站点分享率', type: 'number', min: 0.01, step: 0.01, hint: '默认 2，未达标优先分配选种机会' },
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
]

const dialogTitle = computed(() => props.globalMode ? '全局统一参数' : localTask.value.id ? '编辑站点任务' : '新增站点任务')
const siteName = computed(() => props.sites.find(item => item.value === Number(localTask.value.site_id))?.title || '未选择')
const overrideFields = computed(() => taskOverrideFields(localTask.value))
const previewTask = computed(() => props.globalMode ? localTask.value : effectiveTask({ ...localTask.value, use_global_defaults: true, override_fields: overrideFields.value }, props.globalDefaults))
const scheduleText = computed(() => previewTask.value.cron || `每 ${previewTask.value.brush_interval || 10} 分钟`)
const noviceActive = computed(() => localTask.value.novice && (!localTask.value.novice_until || new Date(localTask.value.novice_until) > new Date()))

function globalValueText(field) {
  const value = { ...taskDefaults, ...props.globalDefaults }[field.key]
  if (field.type === 'switch') return value ? '开启' : '关闭'
  const items = field.type === 'downloader' ? props.downloaders : field.items
  const selected = items?.find(item => item.value === value)
  if (selected) return selected.title
  if (value == null || value === '') return ({ downloader: '未选择', save_path: '下载器默认目录', cron: '固定刷新周期', active_time_range: '全天', qb_category: '不指定', delete_except_tags: '无' })[field.key] || '不限'
  return String(value)
}

function fieldItems(field) {
  const items = field.type === 'switch' ? [{ title: '开启', value: true }, { title: '关闭', value: false }] : field.type === 'downloader' ? props.downloaders : field.items
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
  const rules = field.required && props.globalMode ? [required] : []
  if (['site_ratio_target', 'rotation_hours'].includes(field.key)) rules.push(positive)
  if (field.type === 'number') {
    rules.push(value => value === '' || value == null || Number.isFinite(Number(value)) || '请输入有效数字')
    if (field.min != null) rules.push(value => value === '' || value == null || Number(value) >= field.min || `不能小于 ${field.min}`)
    if (field.max != null) rules.push(value => value === '' || value == null || Number(value) <= field.max || `不能大于 ${field.max}`)
  }
  return rules
}

function selectSite(value) {
  localTask.value.site_id = value
  if (!localTask.value.name) localTask.value.name = `${siteName.value}刷流`
}

watch(() => props.modelValue, visible => {
  if (!visible) return
  localTask.value = props.globalMode
    ? cloneTask({ ...taskDefaults, ...props.task, use_global_defaults: false })
    : editableTask(props.task)
  if (localTask.value.novice_until) {
    const deadline = new Date(localTask.value.novice_until)
    if (!Number.isNaN(deadline.getTime())) {
      localTask.value.novice_until = new Date(deadline.getTime() - deadline.getTimezoneOffset() * 60000).toISOString().slice(0, 16)
    }
  }
  activeTab.value = 'base'
  applyDialog.value = false
  formError.value = ''
})

function closeDialog() {
  emit('update:modelValue', false)
}

async function saveTask(apply = false) {
  applyDialog.value = false
  const result = await formRef.value?.validate()
  if (result && !result.valid) return
  formError.value = ''
  try {
    const payload = props.globalMode ? normalizeDefaults(localTask.value) : normalizeTask(previewTask.value)
    if (!payload.downloader) {
      formError.value = '请先在全局统一参数中选择下载器，或为当前站点覆盖下载器。'
      activeTab.value = 'base'
      return
    }
    emit('save', payload, apply)
  } catch (err) {
    formError.value = err?.message || '请检查配置格式'
  }
}
</script>

<template>
  <VDialog :model-value="modelValue" scrollable :fullscreen="display.smAndDown.value" max-width="74rem" @update:model-value="value => emit('update:modelValue', value)">
    <VCard class="brushflow-editor">
      <VToolbar color="transparent" density="comfortable" class="brushflow-editor__toolbar">
        <VToolbarTitle>{{ dialogTitle }}</VToolbarTitle>
        <VSpacer />
        <VBtn color="primary" variant="flat" prepend-icon="mdi-content-save" :loading="saving" @click="saveTask()">保存</VBtn>
        <VBtn icon="mdi-close" variant="text" aria-label="关闭" @click="closeDialog" />
      </VToolbar>
      <VDivider />
      <VCardText class="brushflow-editor__body">
        <VForm ref="formRef" class="brushflow-editor__form" @submit.prevent="saveTask()">
          <VTabs v-model="activeTab" :direction="display.mdAndUp.value ? 'vertical' : 'horizontal'" color="primary" class="brushflow-editor__tabs">
            <VTab v-for="section in sections" :key="section.value" :value="section.value" :prepend-icon="section.icon">
              {{ section.value === 'base' && !globalMode ? '站点与调度' : section.title }}
            </VTab>
          </VTabs>
          <VDivider :vertical="display.mdAndUp.value" />
          <VWindow v-model="activeTab" :touch="false" class="brushflow-editor__window">
            <VAlert v-if="formError" type="error" variant="tonal" class="mb-4">{{ formError }}</VAlert>
            <VWindowItem v-for="section in sections" :key="section.value" :value="section.value">
              <section v-if="section.value === 'base' && !globalMode" class="editor-section">
                <div class="text-subtitle-1 font-weight-medium">站点信息</div>
                <VRow>
                  <VCol cols="12" md="6"><VSelect :model-value="localTask.site_id" :items="sites" label="站点" :rules="[required]" @update:model-value="selectSite" /></VCol>
                  <VCol cols="12" md="6"><VTextField v-model="localTask.name" label="任务名称" :rules="[required]" /></VCol>
                </VRow>
                <div class="editor-switches">
                  <VSwitch v-model="localTask.enabled" label="启用任务" color="primary" hide-details inset />
                </div>
                <VAlert type="info" variant="tonal" density="compact">留空使用默认值，填写后仅对此站点生效。</VAlert>
              </section>
              <section v-if="section.value === 'base' && !globalMode" class="editor-section">
                <div class="text-subtitle-1 font-weight-medium">考核与站点保护</div>
                <VSwitch v-model="localTask.novice" label="新手考核优先" color="primary" hide-details inset />
                <VTextField v-if="localTask.novice" v-model="localTask.novice_until" type="datetime-local" label="考核截止时间（本地时间，可选）" clearable hint="留空时手动关闭；超过截止时间后自动失去考核优先级。" persistent-hint :rules="[value => !value || !Number.isNaN(new Date(value).getTime()) || '请输入有效时间']" />
                <div class="editor-switches">
                  <VSwitch v-model="localTask.site_hr_active" label="全站 H&R" color="primary" hide-details inset />
                  <VSwitch v-model="localTask.site_skip_tips" label="自动跳过下载提示" color="primary" hide-details inset />
                </div>
              </section>
              <section class="editor-section">
                <header class="editor-section__head"><div><div class="text-subtitle-1 font-weight-medium">{{ section.title }}</div><div class="text-body-2 text-medium-emphasis">{{ section.description }}</div></div></header>
                <VRow>
                  <VCol v-for="field in section.fields.filter(field => !globalMode || !field.siteOnly)" :key="field.key" cols="12" :md="globalMode && field.type === 'switch' ? 12 : 6">
                    <VSwitch v-if="(globalMode || field.siteOnly) && field.type === 'switch'" v-model="localTask[field.key]" :label="fieldLabel(field)" color="primary" hide-details inset />
                    <VSelect v-else-if="['select', 'downloader', 'switch'].includes(field.type)" v-model="localTask[field.key]" :items="fieldItems(field)" :placeholder="globalMode ? undefined : globalValueText(field)" :persistent-placeholder="!globalMode" :label="fieldLabel(field)" :hint="fieldHint(field)" :persistent-hint="!!fieldHint(field)" :rules="fieldRules(field)" :clearable="!globalMode" />
                    <VTextField v-else v-model="localTask[field.key]" :type="field.type || 'text'" :label="field.label" :min="field.min" :max="field.max" :step="field.step || 'any'" :placeholder="globalMode ? undefined : globalValueText(field)" :persistent-placeholder="!globalMode" :hint="fieldHint(field)" :persistent-hint="!!fieldHint(field)" :rules="fieldRules(field)" :clearable="!globalMode || !field.required" />
                  </VCol>
                </VRow>
              </section>
              <section v-if="globalMode && section.value === 'base'" class="editor-section">
                <VAlert type="info" variant="tonal" density="compact">普通保存保留站点覆盖。应用到全部站点会清除共享参数覆盖，并保留站点身份、考核和保护设置。</VAlert>
                <VBtn variant="tonal" color="primary" :disabled="saving" @click="applyDialog = true">保存并应用到全部站点</VBtn>
              </section>
            </VWindowItem>
          </VWindow>
          <VSheet tag="aside" class="brushflow-editor__summary">
            <div class="text-subtitle-1 font-weight-medium">生效参数预览</div>
            <dl>
              <div v-if="!globalMode"><dt>站点</dt><dd>{{ siteName }}</dd></div>
              <div v-if="!globalMode"><dt>新手考核</dt><dd>{{ noviceActive ? '优先' : localTask.novice ? '已到期' : '关闭' }}</dd></div>
              <div><dt>下载器</dt><dd>{{ previewTask.downloader || '未选择' }}</dd></div>
              <div><dt>刷新</dt><dd>{{ scheduleText }}</dd></div>
              <div><dt>检查</dt><dd>每 {{ previewTask.check_interval || 5 }} 分钟</dd></div>
              <div><dt>单站驻留</dt><dd>{{ previewTask.rotation_hours ?? 24 }} 小时</dd></div>
              <div><dt>目标分享率</dt><dd>{{ previewTask.site_ratio_target || '不限' }}</dd></div>
              <div><dt>下载人数</dt><dd>{{ previewTask.leecher || '不限' }}</dd></div>
              <div><dt>下载 / 做种</dt><dd>{{ previewTask.min_peer_seed_ratio ?? '不限' }}</dd></div>
              <div><dt>空间上限</dt><dd>{{ Number(previewTask.disksize) > 0 ? `${previewTask.disksize} GB` : '不限' }}</dd></div>
              <div><dt>促销保护</dt><dd>{{ previewTask.del_no_free ? `提前 ${previewTask.promotion_lead_minutes ?? 10} 分钟` : '关闭' }}</dd></div>
            </dl>
          </VSheet>
        </VForm>
      </VCardText>
    </VCard>
  </VDialog>
  <VDialog v-model="applyDialog" max-width="30rem">
    <VCard title="应用统一参数到全部站点">
      <VCardText>所有站点将继承这套全局参数，并清除已有共享参数覆盖。站点名称、启用状态、考核截止时间、时区、H&R、提示跳过和下载器标签保持各站设置。</VCardText>
      <VCardActions><VSpacer /><VBtn variant="text" @click="applyDialog = false">取消</VBtn><VBtn color="primary" variant="flat" :loading="saving" @click="saveTask(true)">保存并应用</VBtn></VCardActions>
    </VCard>
  </VDialog>
</template>

<style scoped>
.brushflow-editor {
  max-block-size: min(90dvh, 58rem);
}

.brushflow-editor__toolbar {
  flex: 0 0 auto;
  padding-inline: 8px;
  z-index: 4;
  backdrop-filter: blur(var(--transparent-blur, 0px));
  background-color: rgba(var(--v-theme-surface), var(--transparent-opacity-heavy, 1));
}

.brushflow-editor__body {
  padding: 0;
}

.brushflow-editor__form {
  display: grid;
  grid-template-columns: 12rem auto minmax(0, 1fr) minmax(12rem, 0.34fr);
  min-block-size: 34rem;
}

.brushflow-editor__tabs {
  padding: 12px 8px;
}

.brushflow-editor__window {
  min-inline-size: 0;
  padding: 20px;
}

.editor-section {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.editor-section + .editor-section {
  margin-block-start: 28px;
  padding-block-start: 24px;
  border-block-start: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
}

.editor-section__head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}

.editor-switches {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 24px;
}

.brushflow-editor__summary {
  padding: 20px 16px;
  border-inline-start: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
}

.brushflow-editor__summary dl {
  display: grid;
  gap: 12px;
  margin: 18px 0 0;
}

.brushflow-editor__summary dl > div {
  display: flex;
  justify-content: space-between;
  gap: 12px;
}

.brushflow-editor__summary dt {
  color: rgba(var(--v-theme-on-surface), var(--v-medium-emphasis-opacity));
}

.brushflow-editor__summary dd {
  margin: 0;
  text-align: end;
  overflow-wrap: anywhere;
}

@media (max-width: 959px) {
  .brushflow-editor {
    max-block-size: none;
  }

  .brushflow-editor__form {
    grid-template-columns: 1fr;
    min-block-size: 0;
  }

  .brushflow-editor__tabs {
    max-inline-size: 100%;
    padding-block: 0;
    overflow-x: auto;
  }

  .brushflow-editor__window {
    padding: 16px;
  }

  .brushflow-editor__summary {
    border-inline-start: 0;
    border-block-start: 1px solid rgba(var(--v-border-color), var(--v-border-opacity));
  }
}

@media (max-width: 599px) {
  .brushflow-editor__toolbar :deep(.v-toolbar-title) {
    font-size: 1rem;
  }

  .brushflow-editor__toolbar :deep(.v-btn__content) {
    white-space: normal;
  }

}
</style>
