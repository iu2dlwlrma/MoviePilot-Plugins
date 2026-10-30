<script setup>
import { nextTick, ref } from 'vue'
defineProps({ form: { type:Object, required:true } })
const emit = defineEmits(['save','retry'])
const root = ref(null), fields = ref(null)
const cronRule = value => !value || String(value).trim().split(/\s+/).length === 5 || '请填写五段 Cron 表达式，或留空仅手动运行'
const batchRule = value => Number.isInteger(value) && value >= 1 && value <= 100 || '请填写 1 到 100 的整数'
const hoursRule = value => Number.isInteger(value) && value >= 0 && value <= 720 || '请填写 0 到 720 的整数'
async function save() {
 const result=await fields.value?.validate()
 if(result?.valid) emit('save')
 else { await nextTick();root.value?.querySelector('.v-input--error input')?.focus() }
}
</script>
<template>
 <section ref="root" class="cleanup-settings" aria-label="清理设置">
  <VProgressLinear v-if="form.loading" indeterminate aria-label="正在读取实际配置" />
  <VAlert v-if="form.error&&!form.ready" type="error" variant="tonal" role="alert" class="mb-4">
   {{ form.error }}
   <template #append><VBtn variant="text" :disabled="form.loading||form.saving" @click="emit('retry')">{{ form.ready?'保留修改并重新读取':'重新读取' }}</VBtn></template>
  </VAlert>
  <p v-if="!form.ready && !form.loading && !form.error">配置尚未读取。</p>
  <VForm v-if="form.ready && form.draft" ref="fields" @submit.prevent="save">
   <fieldset>
    <legend>自动运行与通知</legend>
    <div class="cleanup-settings__switches">
     <VSwitch v-model="form.draft.enabled" label="启用定时清理" color="primary" hide-details />
     <VSwitch v-model="form.draft.notify" label="空文件夹完成通知" color="primary" hide-details />
    </div>
    <p class="cleanup-hint">关闭定时运行后，仍可查看统计、历史及手动执行。</p>
   </fieldset>
   <fieldset>
    <legend>空文件夹</legend>
    <VTextarea v-model="form.draft.target_dirs" label="扫描目录" rows="3" auto-grow hint="每行一个实际目录。保留目标根目录。" persistent-hint />
    <VTextarea v-model="form.draft.exclude_dirs" label="排除目录" rows="2" auto-grow hint="每行一个目录名或路径片段，匹配的目录及其内容保留。" persistent-hint />
    <div class="cleanup-settings__switches">
     <VSwitch v-model="form.draft.recursive" label="递归清理新变空的父目录" color="primary" hide-details />
     <VSwitch v-model="form.draft.dry_run" label="空文件夹仅预览" color="primary" hide-details />
    </div>
    <p class="cleanup-hint">关闭递归时，仅清理扫描时本来就空的目录；仍会扫描子目录。</p>
    <VTextField v-model="form.draft.cron" label="空文件夹执行周期" :rules="[cronRule]" hint="五段 Cron 表达式，留空仅手动运行。" persistent-hint />
   </fieldset>
   <fieldset>
    <legend>失效记录与残留文件</legend>
    <p class="cleanup-hint">两项共用核对周期和预览设置。依据实际文件与准确关联处理，未配置媒体服务器仍可核对。有效版本、共享文件、下载或整理中的数据会保留。</p>
    <div class="cleanup-settings__switches">
     <VSwitch v-model="form.draft.records_enabled" label="自动核对记录与残留" color="primary" hide-details />
     <VSwitch v-model="form.draft.records_dry_run" label="记录与残留仅预览" color="primary" hide-details />
    </div>
    <VTextField v-model="form.draft.records_cron" label="核对周期" :rules="[cronRule]" hint="五段 Cron 表达式，留空仅手动运行。" persistent-hint />
    <div class="cleanup-settings__numbers">
     <VTextField v-model.number="form.draft.records_grace_hours" label="缺失确认时间（小时）" type="number" min="0" max="720" :rules="[hoursRule]" hint="缺失和归属需要在执行前再次核实。" persistent-hint />
     <VTextField v-model.number="form.draft.records_batch_size" label="每批最多核对条目" type="number" min="1" max="100" :rules="[batchRule]" />
    </div>
    <p class="cleanup-hint">清理不会修改订阅进度或触发补下载。旧“更新订阅缺集进度”配置原值为{{ form.draft.records_reopen_subscription === true ? '开启' : '关闭' }}，仅保留兼容，不授予清理权限。</p>
   </fieldset>
   <VAlert v-if="form.error" type="error" variant="tonal" role="alert" class="mb-4">
    {{ form.error }}
    <template #append><VBtn variant="text" :disabled="form.loading||form.saving" @click="emit('retry')">保留修改并重新读取</VBtn></template>
   </VAlert>
   <div class="cleanup-settings__save">
    <VBtn type="submit" color="primary" :loading="form.saving" :disabled="form.saving||form.loading">保存设置</VBtn>
    <span role="status" aria-live="polite">{{ form.notice }}</span>
   </div>
  </VForm>
 </section>
</template>
