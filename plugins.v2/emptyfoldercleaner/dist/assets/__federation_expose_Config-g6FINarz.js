import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { _ as _sfc_main$1 } from './CleanupWorkbench-PtpbPyLA.js';

const {openBlock:_openBlock,createBlock:_createBlock} = await importShared('vue');


const {onMounted} = await importShared('vue');

const _sfc_main = {
  __name: 'Config',
  props: { api: { type: Object, required: true }, initialConfig: { type: Object, default: () => ({}) } },
  emits: ['close', 'layout'],
  setup(__props, { emit: __emit }) {


const emit = __emit;
onMounted(() => emit('layout', { maxWidth: '72rem' }));

return (_ctx, _cache) => {
  return (_openBlock(), _createBlock(_sfc_main$1, {
    api: __props.api,
    "initial-config": __props.initialConfig,
    "initial-view": "settings",
    "config-mode": "",
    onClose: _cache[0] || (_cache[0] = $event => (emit('close')))
  }, null, 8, ["api", "initial-config"]))
}
}

};

export { _sfc_main as default };
