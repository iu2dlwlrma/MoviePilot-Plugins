<script setup>
import { computed, ref, watch } from 'vue'
import { normalizeSettings } from '../utils'

const props = defineProps({
  modelValue: { type: Boolean, default: false },
  settings: { type: Object, default: null },
  sites: { type: Array, default: () => [] },
  downloaders: { type: Array, default: () => [] },
  ready: { type: Boolean, default: false },
  loading: { type: Boolean, default: false },
  saving: { type: Boolean, default: false },
  error: { type: String, default: '' },
})
const emit = defineEmits(['update:modelValue', 'retry', 'save', 'closed'])
const formRef = ref(null)
const draft = ref(null)
const formError = ref('')
const validating = ref(false)
const selectedSites = computed(() => draft.value?.selected_site_ids || [])
const manualFocusSites = computed(() => props.sites.filter(site => selectedSites.value.includes(Number(site.value))))
const siteOptions = computed(() => {
  const known = new Set(props.sites.map(site => Number(site.value)))
  return [...props.sites, ...selectedSites.value.filter(id => !known.has(id))
    .map(id => ({ title: `站点 ${id}（已不可用）`, value: id }))]
})
const canSave = computed(() => props.ready && !props.loading && !props.saving && !validating.value && Boolean(draft.value))

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
]

watch(() => [props.modelValue, props.settings], ([visible, settings]) => {
  draft.value = visible && settings ? JSON.parse(JSON.stringify(settings)) : null
  formError.value = ''
}, { immediate: true })

function selectSites(values) {
  if (!draft.value) return
  draft.value.selected_site_ids = [...new Set(values.map(Number))]
  if (!draft.value.selected_site_ids.includes(draft.value.manual_focus_site_id)) draft.value.manual_focus_site_id = null
}

function fieldLabel(field) {
  return field.key === 'proxy_delete' && draft.value?.global_proxy_delete ? '参与全局动态删种' : field.label
}

function fieldRules(field) {
  const rules = []
  if (field.required || (field.type === 'downloader' && selectedSites.value.length)) {
    rules.push(value => value !== null && value !== '' && value !== undefined || '此项不能为空')
  }
  if (field.type === 'downloader' && selectedSites.value.length) {
    rules.push(value => props.downloaders.some(item => item.value === value) || '下载器不可用，请重新选择')
  }
  if (field.type === 'number') {
    rules.push(value => value === '' || value == null || Number.isFinite(Number(value)) || '请输入有效数字')
    if (field.min != null) rules.push(value => value === '' || value == null || Number(value) >= field.min || `不能小于 ${field.min}`)
    if (field.max != null) rules.push(value => value === '' || value == null || Number(value) <= field.max || `不能大于 ${field.max}`)
  }
  return rules
}

function trapDialogFocus(event) {
  if (!props.modelValue || event.defaultPrevented || event.key !== 'Tab') return
  const card = event.currentTarget
  if (!card?.contains(event.target) || event.target.closest?.('[role="combobox"][aria-expanded="true"]')) return
  const document = card.ownerDocument
  const focusable = [...card.querySelectorAll('button, input, select, textarea, a[href], [tabindex]')]
    .filter(element => element.tabIndex >= 0
      && !element.matches(':disabled, [aria-disabled="true"], [inert], [inert] *')
      && element.getClientRects().length
      && document.defaultView.getComputedStyle(element).visibility !== 'hidden')
  const first = focusable[0]
  const last = focusable[focusable.length - 1]
  const destination = event.shiftKey
    ? (document.activeElement === first ? last : null)
    : (document.activeElement === last ? first : null)
  if (destination) {
    event.preventDefault()
    destination.focus({ preventScroll: true })
  }
}

async function save() {
  if (!canSave.value) return
  const currentDraft = draft.value
  validating.value = true
  formError.value = ''
  try {
    const result = await formRef.value?.validate()
    if (!props.modelValue || draft.value !== currentDraft || !props.ready || props.loading || props.saving) return
    if (result && !result.valid) return
    if (selectedSites.value.some(id => !props.sites.some(site => Number(site.value) === id))) {
      formError.value = '部分选中站点已不可用，请取消选择后保存'
      return
    }
    emit('save', normalizeSettings(draft.value))
  } catch (err) {
    if (props.modelValue && draft.value === currentDraft) formError.value = err?.message || '请检查配置格式'
  } finally {
    validating.value = false
  }
}
</script>

<template>
  <VDialog :model-value="modelValue" :persistent="saving" scrollable max-width="960" class="brush-settings-dialog" @update:model-value="value => emit('update:modelValue', value)" @after-leave="emit('closed')">
    <VCard class="brush-settings" @keydown.tab="trapDialogFocus">
      <VToolbar color="transparent" class="brush-settings__toolbar">
        <VToolbarTitle>刷流设置</VToolbarTitle>
        <VBtn icon="mdi-close" variant="text" aria-label="关闭刷流设置" :disabled="saving" @click="emit('update:modelValue', false)" />
      </VToolbar>
      <VDivider />
      <VCardText class="brush-settings__body">
        <div v-if="loading" class="brush-settings__loading" role="status"><VProgressCircular indeterminate size="24" width="2" />正在读取设置…</div>
        <VAlert v-if="error" type="error" variant="tonal" class="mb-4" role="alert">
          {{ error }}
          <template v-if="!ready" #append><VBtn variant="text" :disabled="loading" @click="emit('retry')">重试读取</VBtn></template>
        </VAlert>
        <VAlert v-if="formError" type="error" variant="tonal" class="mb-4" role="alert">{{ formError }}</VAlert>
        <VForm v-if="ready && draft" ref="formRef" :disabled="saving || loading || validating" @submit.prevent="save">
          <section class="brush-settings__section">
            <div class="brush-settings__heading"><VIcon icon="mdi-web" /><h2>管理站点</h2></div>
            <p class="text-body-2 text-medium-emphasis">从 MoviePilot 读取站点，所有选中站点使用下方公共规则。取消选择会停止选种与检查，保留托管记录。</p>
            <div class="brush-settings__switches">
              <VSwitch v-model="draft.enabled" label="启用插件" color="primary" hide-details inset />
              <VSwitch v-model="draft.show_sidebar_nav" label="显示侧栏入口" color="primary" hide-details inset />
            </div>
            <VAutocomplete :model-value="selectedSites" :items="siteOptions" label="参与刷流的站点" multiple chips closable-chips hide-details="auto" no-data-text="MoviePilot 中暂无可用私有站点" @update:model-value="selectSites" />
            <div class="brush-settings__selection">
              <span class="text-body-2 text-medium-emphasis">已选择 {{ selectedSites.length }} / {{ sites.length }} 个站点</span>
              <VSpacer />
              <VBtn variant="text" :disabled="saving || validating || !sites.length" @click="selectSites(sites.map(site => site.value))">全选</VBtn>
              <VBtn variant="text" :disabled="saving || validating || !selectedSites.length" @click="selectSites([])">清空</VBtn>
            </div>
          </section>
          <section v-for="section in sections" :key="section.key" class="brush-settings__section">
            <div class="brush-settings__heading"><VIcon :icon="section.icon" /><h2>{{ section.title }}</h2></div>
            <p class="text-body-2 text-medium-emphasis">{{ section.description }}</p>
            <VRow v-if="section.key === 'download'">
              <VCol cols="12" md="6"><VSelect v-model="draft.rotation_mode" label="站点轮换模式" :items="[{ title: '多站并行', value: 'parallel' }, { title: '单站达标后轮转', value: 'sequential' }]" hint="未达标站点优先，全部达标后按驻留时间轮转" persistent-hint /></VCol>
              <VCol v-if="draft.rotation_mode === 'sequential'" cols="12" md="6"><VSelect v-model="draft.manual_focus_site_id" label="手动重点站" :items="manualFocusSites" clearable placeholder="自动调度" hint="选定站点优先，清空后恢复自动调度；保存后生效" persistent-hint /></VCol>
              <VCol cols="12" md="6"><VTextField v-model="draft.global_disksize" label="总空间上限（GB）" type="number" min="0" clearable hint="所有站点合计；留空或 0 表示不限" persistent-hint :rules="fieldRules({ type: 'number', min: 0 })" /></VCol>
            </VRow>
            <VRow v-if="section.key === 'cleanup'">
              <VCol cols="12" md="6"><VSwitch v-model="draft.global_proxy_delete" label="全局动态删种" color="primary" inset hide-details /></VCol>
              <VCol v-if="draft.global_proxy_delete" cols="12" md="6"><VTextField v-model="draft.global_delete_size_range" label="全局动态删种阈值（GB）" hint="如 350-500，达到上限后清理至下限" persistent-hint /></VCol>
            </VRow>
            <VRow>
              <VCol v-for="field in section.fields" :key="field.key" cols="12" md="6">
                <VSwitch v-if="field.type === 'switch'" v-model="draft.global_defaults[field.key]" :label="fieldLabel(field)" color="primary" hide-details inset />
                <VSelect v-else-if="['select', 'downloader'].includes(field.type)" v-model="draft.global_defaults[field.key]" :items="field.type === 'downloader' ? downloaders : field.items" :label="fieldLabel(field)" :hint="field.hint" :persistent-hint="Boolean(field.hint)" :rules="fieldRules(field)" />
                <VTextField v-else v-model="draft.global_defaults[field.key]" :label="fieldLabel(field)" :type="field.type || 'text'" :min="field.min" :max="field.max" :step="field.step || 'any'" :hint="field.hint" :persistent-hint="Boolean(field.hint)" :rules="fieldRules(field)" :clearable="!field.required" />
              </VCol>
            </VRow>
          </section>
        </VForm>
      </VCardText>
      <VDivider />
      <VCardActions class="brush-settings__actions">
        <VSpacer />
        <VBtn variant="text" :disabled="saving" @click="emit('update:modelValue', false)">取消</VBtn>
        <VBtn color="primary" variant="flat" :disabled="!canSave" :loading="saving || validating" @click="save">保存设置</VBtn>
      </VCardActions>
    </VCard>
  </VDialog>
</template>

<style scoped>
.brush-settings { max-height: calc(100dvh - 24px); }
.brush-settings__toolbar { flex: 0 0 auto; }
.brush-settings__body { padding: 20px 24px; }
.brush-settings__section + .brush-settings__section { margin-top: 28px; padding-top: 24px; border-top: 1px solid rgba(var(--v-border-color), var(--v-border-opacity)); }
.brush-settings__heading { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; }
.brush-settings__heading h2 { margin: 0; font-size: 1rem; font-weight: 600; }
.brush-settings__section > p { margin-bottom: 16px; }
.brush-settings__switches { display: flex; flex-wrap: wrap; gap: 12px 28px; margin-bottom: 16px; }
.brush-settings__selection { display: flex; align-items: center; flex-wrap: wrap; gap: 4px; }
.brush-settings__loading { display: flex; align-items: center; gap: 12px; padding: 12px 0; }
.brush-settings__actions { flex: 0 0 auto; padding: 12px 16px; }
.brush-settings :deep([class~="v-btn"]) { min-height: 44px; }
.brush-settings :deep([class~="v-label"]) { white-space: normal; }
.brush-settings :deep([class~="v-field"]), .brush-settings :deep([class~="v-col"]) { min-width: 0; }
@media (max-width: 600px) {
  .brush-settings__body { padding: 16px; }
  .brush-settings__switches { display: block; }
  .brush-settings :deep([class~="v-autocomplete__selection"]) { max-width: 100%; }
  .brush-settings :deep([class~="v-chip__content"]) { overflow: hidden; text-overflow: ellipsis; }
  .brush-settings :deep([class~="v-chip"]), .brush-settings :deep([class~="v-chip__close"]) { min-height: 44px; }
  .brush-settings :deep([class~="v-chip__close"]) { min-width: 44px; }
  .brush-settings :deep([class~="v-field__clearable"]), .brush-settings :deep([class~="v-field__append-inner"]) { align-items: center; padding-top: 0; }
  .brush-settings :deep([class~="v-field__clearable"] [role="button"]),
  .brush-settings :deep([class~="v-field__append-inner"] [role="button"]) {
    width: 44px;
    height: 44px;
    min-width: 44px;
    min-height: 44px;
    flex: 0 0 44px;
    font-size: 24px;
  }
}
</style>
