<script setup>
import { onMounted } from 'vue'
import CleanupWorkbench from './CleanupWorkbench.vue'
defineProps({ api: { type: Object, required: true }, initialConfig: { type: Object, default: () => ({}) } })
const emit = defineEmits(['close', 'layout'])
onMounted(() => emit('layout', { maxWidth: '72rem' }))
</script>
<template><CleanupWorkbench :api="api" :initial-config="initialConfig" initial-view="settings" config-mode @close="emit('close')" /></template>
