<script setup>
import { onMounted } from 'vue'
import CleanupWorkbench from './CleanupWorkbench.vue'
defineProps({ api: { type: Object, required: true } })
const emit = defineEmits(['action', 'close', 'layout'])
onMounted(() => emit('layout', { maxWidth: '80rem' }))
</script>
<template><CleanupWorkbench :api="api" @close="emit('close')" /></template>
