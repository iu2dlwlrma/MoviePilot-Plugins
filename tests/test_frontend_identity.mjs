import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import vm from 'node:vm'
import * as utils from '../plugins.v3/brushflow/src/utils.js'

const components = new URL('../plugins.v3/brushflow/src/components/', import.meta.url)

async function loadComponent(name, pluginId) {
  const source = await readFile(new URL(name, components), 'utf8')
  const script = source.match(/<script setup>([\s\S]*?)<\/script>/)[1]
    .replace(/^import[\s\S]*?from ['"][^'"]+['"]\s*$/gm, '')
  const calls = []
  const sandbox = {
    ...utils, console, URLSearchParams,
    ref: value => ({ value }),
    computed: getter => ({ get value() { return getter() } }),
    inject: () => null,
    onMounted() {}, onUnmounted() {}, watch() {}, defineExpose() {},
    defineEmits: () => () => {},
    defineProps(definitions) {
      const props = Object.fromEntries(Object.entries(definitions).map(([key, def]) => [key,
        typeof def.default === 'function' ? def.default() : def.default]))
      props.api = { get: async path => { calls.push(path); return { success: true, data: { tasks: [], summary: {} } } } }
      if (pluginId !== undefined) props.pluginId = pluginId
      return props
    },
  }
  vm.createContext(sandbox)
  vm.runInContext(`${script}\nglobalThis.runLoadStatus = loadStatus;`, sandbox, { filename: name })
  await sandbox.runLoadStatus()
  return calls
}

let failures = 0
for (const [label, file, pluginId] of [
  ['workbench default', 'BrushFlowWorkbench.vue', undefined],
  ['workbench empty fallback', 'BrushFlowWorkbench.vue', ''],
  ['dashboard direct endpoint', 'Dashboard.vue', undefined],
]) {
  try {
    assert.deepEqual(await loadComponent(file, pluginId), ['plugin/OriechoBrushFlow/status'])
    console.log(`PASS ${label}`)
  } catch (error) {
    failures += 1
    console.error(`FAIL ${label}: ${error.message}`)
  }
}
process.exitCode = Number(failures > 0)
