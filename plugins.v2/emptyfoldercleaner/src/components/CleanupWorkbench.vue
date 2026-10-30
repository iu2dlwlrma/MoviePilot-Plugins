<script setup>
import { computed, nextTick, onMounted, onUnmounted, reactive, ref, watch } from 'vue'
import SettingsForm from './SettingsForm.vue'
import { createCleanupController, createCleanupState } from '../controller.js'
import { branchNames, date, errorText, number, reasonText, statusNames } from '../labels.js'
import '../style.css'
const props=defineProps({api:{type:Object,required:true},initialView:{type:String,default:'overview'},configMode:Boolean,initialConfig:{type:Object,default:()=>({})}})
const emit=defineEmits(['close'])
const state=reactive(createCleanupState())
let controller=createCleanupController(props.api,state)
const heading=ref(null),settingsTrigger=ref(null),detailsTrigger=ref(null),sourcesTrigger=ref(null),historyTrigger=ref(null)
let timer,disposed=false,returnFocus=null
const active=computed(()=>['queued','running','cancelling'].includes(state.status?.run?.status))
const records=computed(()=>state.status?.records||{})
const counts=computed(()=>records.value.counts||{})
const scan=computed(()=>records.value.scan||{})
const folders=computed(()=>state.status?.folders)
const pageCount=computed(()=>Math.max(1,Math.ceil((state.view==='details'?state.detail.total:state.history.total)/20)))
const pageTitle=computed(()=>({settings:'清理设置',details:'核对详情',history:'执行历史'}[state.view]||'清理大师'))
const summary=computed(()=>records.value.summary||{})
async function focusHeading(){await nextTick();heading.value?.focus()}
async function settings(){returnFocus='settings';const pending=controller.openSettings();await focusHeading();await pending}
async function details(kind){returnFocus=kind==='sources'?'sources':'details';const pending=controller.loadDetails(kind,1,true);await focusHeading();await pending}
async function history(){returnFocus='history';const pending=controller.loadHistory();await focusHeading();await pending}
async function back(){
 if(state.view==='settings')controller.closeSettings()
 state.view='overview';await nextTick()
 const target={settings:settingsTrigger.value,details:detailsTrigger.value,sources:sourcesTrigger.value,history:historyTrigger.value}[returnFocus]
 ;(target?.$el||target)?.focus?.()
}
function close(){controller.closeSettings();emit('close')}
async function poll(){
 if(disposed||props.configMode)return
 if(document.visibilityState==='visible')await controller.loadStatus()
 if(!disposed)timer=setTimeout(poll,active.value?2500:15000)
}
function visible(){if(document.visibilityState==='visible'&&!props.configMode)controller.loadStatus()}
watch(()=>props.api,async api=>{
 controller.destroy()
 Object.assign(state,createCleanupState())
 controller=createCleanupController(api,state)
 if(disposed)return
 if(props.initialView==='settings')await controller.openSettings()
 else await controller.loadStatus()
})
onMounted(async()=>{
 if(props.initialView==='settings')await controller.openSettings()
 else await controller.loadStatus()
 if(!disposed&&!props.configMode){timer=setTimeout(poll,2500);document.addEventListener('visibilitychange',visible)}
})
onUnmounted(()=>{disposed=true;clearTimeout(timer);document.removeEventListener('visibilitychange',visible);controller.destroy()})
</script>
<template>
 <div class="cleanup-master">
  <header class="cleanup-header">
   <div class="cleanup-header__title">
    <VBtn v-if="state.view!=='overview'&&!configMode" icon="mdi-arrow-left" variant="text" aria-label="返回清理统计" @click="back" />
    <h1 ref="heading" tabindex="-1">{{ pageTitle }}</h1>
   </div>
   <nav class="cleanup-header__actions" aria-label="清理功能入口">
    <template v-if="state.view==='overview'">
     <VBtn ref="settingsTrigger" variant="tonal" prepend-icon="mdi-tune-variant" @click="settings">设置</VBtn>
     <VBtn ref="historyTrigger" variant="text" prepend-icon="mdi-history" @click="history">执行历史</VBtn>
    </template>
    <VBtn icon="mdi-close" variant="text" aria-label="关闭清理大师" @click="close" />
   </nav>
  </header>
  <template v-if="state.view==='overview'">
   <div v-if="state.statusLoading" class="cleanup-loading" role="status"><VProgressCircular indeterminate size="24" />正在读取清理统计</div>
   <VAlert v-if="state.statusError" type="error" variant="tonal" class="mb-4">{{ state.statusError }}<VBtn variant="text" @click="controller.loadStatus">重试</VBtn></VAlert>
   <template v-if="state.status">
    <section class="cleanup-run" aria-label="当前运行状态" aria-live="polite">
     <div><span class="cleanup-status-dot" :class="{'cleanup-status-dot--active':active}" />{{ statusNames[state.status.run?.status]||'等待运行' }}<span class="cleanup-run__schedule">{{ state.status.enabled?'定时清理已启用':'定时清理未启用' }}</span></div>
     <VBtn v-if="active" variant="tonal" color="warning" :loading="state.actionBusy" :disabled="state.actionBusy||state.status.run?.status==='cancelling'" @click="controller.cancel">{{ state.status.run?.status==='cancelling'?'正在停止':'停止本次运行' }}</VBtn>
    </section>
    <div v-if="state.actionNotice" class="cleanup-notice" role="status">{{ state.actionNotice }}</div>
    <div class="cleanup-metrics">
     <section class="cleanup-metric" aria-labelledby="folders-title">
      <div class="cleanup-metric__heading"><VIcon icon="mdi-folder-outline" aria-hidden="true" /><h2 id="folders-title">空文件夹</h2></div>
      <div class="cleanup-metric__number">{{ number(folders?.removed_count) }}<span>个</span></div>
      <p>{{ folders?.dry_run?'最近一次预览可清理':'最近一次已清理' }}</p>
      <dl><div><dt>扫描目录</dt><dd>{{ number(state.status.configured_folders) }} 个</dd></div><div><dt>最近运行</dt><dd>{{ date(folders?.clean_time) }}</dd></div></dl>
      <VBtn variant="tonal" color="primary" :disabled="active||state.actionBusy||!state.status.configured_folders" @click="controller.run('folders')">扫描空文件夹</VBtn>
     </section>
     <section class="cleanup-metric" aria-labelledby="records-title">
      <div class="cleanup-metric__heading"><VIcon icon="mdi-text-box-check-outline" aria-hidden="true" /><h2 id="records-title">失效记录</h2></div>
      <div class="cleanup-metric__number">{{ number(counts.download_files) }}<span>条</span></div>
      <p>本批已清理文件记录</p>
      <dl><div><dt>本批核对</dt><dd>{{ number(records.processed) }} 条</dd></div><div><dt>关联历史已清理</dt><dd>{{ number((counts.download_history||0)+(counts.transfer_history||0)) }} 条</dd></div></dl>
      <div class="cleanup-metric__actions"><VBtn color="primary" variant="tonal" :disabled="active||state.actionBusy" @click="controller.run('records')">核对记录与残留</VBtn><VBtn ref="detailsTrigger" variant="text" @click="details('records')">查看详情</VBtn></div>
     </section>
     <section class="cleanup-metric" aria-labelledby="sources-title">
      <div class="cleanup-metric__heading"><VIcon icon="mdi-file-remove-outline" aria-hidden="true" /><h2 id="sources-title">残留文件与任务</h2></div>
      <div class="cleanup-metric__number">{{ number(counts.source_files) }}<span>个文件</span></div>
      <p>本批已清理残留源文件</p>
      <dl><div><dt>下载任务已移除</dt><dd>{{ number(counts.tasks) }} 个</dd></div><div><dt>执行方式</dt><dd>与失效记录共同核对</dd></div></dl>
      <VBtn ref="sourcesTrigger" variant="text" @click="details('sources')">查看详情</VBtn>
     </section>
    </div>
    <section class="cleanup-coverage" aria-label="核对覆盖与结果统计">
     <div><h2>核对进度</h2><span v-if="records.dry_run" class="cleanup-preview">仅预览</span></div>
     <p v-if="scan.cycle_id">本轮已核对 {{ number(scan.checked) }} 条{{ scan.complete?'，本轮扫描已完成。':'，扫描中。' }}<template v-if="!scan.complete&&scan.total==null">总数尚未统计。</template></p>
     <p v-else>尚无完整扫描统计。手动核对或等待定时运行后更新。</p>
     <div class="cleanup-summary"><span>本批预览可处理 <strong>{{ number(summary.eligible||0) }} 项</strong></span><span>本批观察中 <strong>{{ number(summary.waiting||0) }} 项</strong></span><span>本批需复核 <strong>{{ number(summary.needs_review||0) }} 项</strong></span><span>观察队列 <strong>{{ number(records.observed_pending||0) }} 项</strong></span></div>
     <p class="cleanup-hint">统计更新：{{ date(records.clean_time) }}<template v-if="scan.completed_at">；完整扫描：{{ date(scan.completed_at) }}</template>。本批结果与全量扫描进度分别统计。</p>
    </section>
   </template>
  </template>
  <SettingsForm v-else-if="state.view==='settings'" :form="state.settings" @save="controller.saveSettings" @retry="controller.loadSettings" />
  <section v-else-if="state.view==='details'" aria-label="核对详情">
   <div class="cleanup-list-tools"><div><VBtn :variant="state.detail.kind==='records'?'tonal':'text'" :aria-pressed="state.detail.kind==='records'" @click="controller.loadDetails('records',1,true)">失效记录</VBtn><VBtn :variant="state.detail.kind==='sources'?'tonal':'text'" :aria-pressed="state.detail.kind==='sources'" @click="controller.loadDetails('sources',1,true)">残留文件与任务</VBtn></div><VBtn variant="text" prepend-icon="mdi-refresh" :disabled="state.detail.loading" @click="controller.loadDetails(state.detail.kind,1,true)">读取最新批次</VBtn></div>
   <p class="cleanup-hint">最近一批的逐项结果，共 {{ number(state.detail.total) }} 项；{{ date(state.detail.clean_time) }}。翻页保留本批快照，扫描继续进行。</p>
   <VAlert v-if="state.detail.error" type="error" variant="tonal">{{ state.detail.error }}<VBtn variant="text" @click="controller.loadDetails(state.detail.kind,state.detail.page,false)">重试</VBtn></VAlert>
   <VAlert v-if="state.detail.run_error" :type="state.detail.status==='yielded'?'info':'warning'" variant="tonal" class="mb-4">{{ errorText(state.detail.run_error) }}<VBtn variant="text" :disabled="active||state.actionBusy" @click="controller.run('records')">{{ state.detail.status==='yielded'?'继续核对':'重新核对' }}</VBtn></VAlert>
   <div v-if="state.detail.loading" class="cleanup-loading" role="status"><VProgressCircular indeterminate size="24" />正在读取详情</div>
   <p v-else-if="!state.detail.items.length" class="cleanup-empty">本批暂无此类结果。可读取最新批次或开始核对。</p>
   <div v-else class="cleanup-detail-list"><article v-for="(item,index) in state.detail.items" :key="`${state.detail.snapshot_id}-${item.key}-${item.branch}-${index}`" class="cleanup-detail-item"><div><h3>{{ item.file||`文件记录 ${item.file_id||''}` }}</h3><span class="cleanup-item-status">{{ statusNames[item.status]||'需复核' }}</span></div><p>{{ branchNames[item.branch]||'关联核对' }}</p><p>{{ reasonText(item) }}</p><p v-if="item.eligible_at" class="cleanup-hint">可再次确认时间：{{ date(item.eligible_at) }}</p></article></div>
   <VPagination v-if="pageCount>1" :model-value="state.detail.page" :length="pageCount" :total-visible="3" :disabled="state.detail.loading" aria-label="详情分页" @update:model-value="page=>controller.loadDetails(state.detail.kind,page,false)" />
  </section>
  <section v-else-if="state.view==='history'" aria-label="执行历史">
   <div class="cleanup-list-tools"><div><VBtn :variant="state.history.kind==='folders'?'tonal':'text'" :aria-pressed="state.history.kind==='folders'" @click="controller.loadHistory('folders',1)">空文件夹</VBtn><VBtn :variant="state.history.kind==='records'?'tonal':'text'" :aria-pressed="state.history.kind==='records'" @click="controller.loadHistory('records',1)">记录与残留</VBtn></div></div>
   <VAlert v-if="state.history.error" type="error" variant="tonal">{{ state.history.error }}<VBtn variant="text" @click="controller.loadHistory(state.history.kind,state.history.page)">重试</VBtn></VAlert>
   <div v-if="state.history.loading" class="cleanup-loading" role="status"><VProgressCircular indeterminate size="24" />正在读取历史</div>
   <p v-else-if="!state.history.items.length" class="cleanup-empty">暂无执行历史。完成清理后会保留结果。</p>
   <div v-else class="cleanup-detail-list"><article v-for="(item,index) in state.history.items" :key="`${state.history.kind}-${item.clean_time}-${index}`" class="cleanup-detail-item"><div><h3>{{ item.clean_time||'时间未记录' }}</h3><span>{{ item.dry_run?'仅预览':'实际执行' }}</span></div><p>{{ statusNames[item.status]||'已有历史' }}</p><p v-if="item.error">{{ errorText(item.error) }}</p><p v-if="state.history.kind==='folders'">扫描 {{ number(item.target_dirs) }} 个目录，{{ item.dry_run?'预览可清理':'已清理' }} {{ number(item.removed_count) }} 个空文件夹。</p><p v-else>核对 {{ number(item.processed) }} 条文件记录，已清理 {{ number(item.counts?.download_files||0) }} 条文件记录、{{ number(item.counts?.source_files||0) }} 个文件、{{ number(item.counts?.tasks||0) }} 个任务。</p></article></div>
   <VPagination v-if="pageCount>1" :model-value="state.history.page" :length="pageCount" :total-visible="3" :disabled="state.history.loading" aria-label="历史分页" @update:model-value="page=>controller.loadHistory(state.history.kind,page)" />
  </section>
 </div>
</template>
