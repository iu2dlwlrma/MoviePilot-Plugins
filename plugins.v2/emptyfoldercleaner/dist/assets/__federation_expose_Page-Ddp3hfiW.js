import { importShared } from './__federation_fn_import-JrT3xvdd.js';
import { _ as _sfc_main$1 } from './CleanupWorkbench-PtpbPyLA.js';

const {openBlock:_openBlock,createBlock:_createBlock} = await importShared('vue');


const {onMounted} = await importShared('vue');

const _sfc_main = {
  __name: 'Page',
  props: { api: { type: Object, required: true } },
  emits: ['action', 'close', 'layout'],
  setup(__props, { emit: __emit }) {


const emit = __emit;
onMounted(() => emit('layout', { maxWidth: '80rem' }));

return (_ctx, _cache) => {
  return (_openBlock(), _createBlock(_sfc_main$1, {
    api: __props.api,
    onClose: _cache[0] || (_cache[0] = $event => (emit('close')))
  }, null, 8, ["api"]))
}
}

};

export { _sfc_main as default };
