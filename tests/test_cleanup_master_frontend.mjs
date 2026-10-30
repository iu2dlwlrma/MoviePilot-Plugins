import assert from 'node:assert/strict'
import { test } from 'node:test'
import { existsSync } from 'node:fs'
const url = new URL('../plugins.v2/emptyfoldercleaner/src/controller.js', import.meta.url)
const deferred = () => { let resolve, reject; const promise = new Promise((a,b) => { resolve=a; reject=b }); return {promise,resolve,reject} }
const original = {enabled:true,cron:'0 0 * * 1',target_dirs:'/old',notify:false,onlyonce:true,records_reopen_subscription:true,future:{token:'retain'}}
const response = (config=original,revision='r1') => ({success:true,data:{config,revision}})
async function create(api) {
  assert.ok(existsSync(url), 'The cleanup controller must implement draft and request ownership')
  const {createCleanupState,createCleanupController}=await import(url)
  const state=createCleanupState()
  return {state,controller:createCleanupController(api,state)}
}

test('status polling preserves the open settings draft and only changed editable keys are posted',async()=>{
 const posts=[]
 const {state,controller}=await create({get:async path=>path.endsWith('/settings')?response():{success:true,data:{enabled:false,records:{processed:100}}},post:async(path,body)=>{posts.push({path,body});return response({...original,notify:true},'r2')}})
 await controller.openSettings()
 state.settings.draft.notify=true
 await controller.loadStatus()
 assert.equal(state.settings.draft.notify,true)
 await controller.saveSettings()
 assert.deepEqual(posts,[{path:'plugin/EmptyFolderCleaner/settings',body:{revision:'r1',changes:{notify:true}}}])
 assert.equal(state.settings.open,true)
 assert.equal(state.settings.revision,'r2')
 assert.equal(state.settings.draft.onlyonce,true)
 assert.equal(state.settings.draft.future.token,'retain')
})

test('typing during a save survives its response and a second save uses the new revision',async()=>{
 const pending=deferred(),posts=[]
 const {state,controller}=await create({get:async()=>response(),post:async(path,body)=>{posts.push(body);return posts.length===1?pending.promise:response({...original,notify:true,target_dirs:'/new'},'r3')}})
 await controller.openSettings();state.settings.draft.notify=true
 const saving=controller.saveSettings();state.settings.draft.target_dirs='/new'
 pending.resolve(response({...original,notify:true},'r2'));await saving
 assert.equal(state.settings.draft.target_dirs,'/new')
 await controller.saveSettings()
 assert.deepEqual(posts[1],{revision:'r2',changes:{target_dirs:'/new'}})
})

test('failed save retains the exact draft and allows retry',async()=>{
 let fail=true
 const {state,controller}=await create({get:async()=>response(),post:async()=>fail?{success:false,message:'配置已变化'}:response({...original,notify:true},'r2')})
 await controller.openSettings();state.settings.draft.notify=true
 await controller.saveSettings();assert.equal(state.settings.error,'配置已变化');assert.equal(state.settings.draft.notify,true)
 fail=false;await controller.saveSettings();assert.equal(state.settings.revision,'r2')
})

test('late settings response cannot refill a closed or destroyed form',async()=>{
 const pending=deferred()
 const {state,controller}=await create({get:async()=>pending.promise})
 const opening=controller.openSettings();controller.closeSettings();pending.resolve(response());await opening
 assert.equal(state.settings.draft,null);assert.equal(state.settings.ready,false)
 controller.destroy();assert.equal(state.settings.baseline,null)
})

test('latest detail responses own their page and pagination pins the existing snapshot',async()=>{
 const old=deferred(),paths=[]
 const {state,controller}=await create({get:async path=>{paths.push(path);if(paths.length===1)return old.promise;return {success:true,data:{items:[{file:'new'}],page:2,page_size:20,total:25,snapshot_id:'fixed',scope:'latest_batch'}}}})
 const first=controller.loadDetails('records',1,true)
 await controller.loadDetails('records',2,false)
 old.resolve({success:true,data:{items:[{file:'old'}],page:1,snapshot_id:'old'}});await first
 assert.equal(state.detail.items[0].file,'new');assert.equal(state.detail.page,2)
 await controller.loadDetails('records',1,false)
 assert.match(paths[2],/snapshot_id=fixed/)
})

test('rapid run clicks reuse one request and cancelled state waits for status',async()=>{
 const pending=deferred(),posts=[]
 const {state,controller}=await create({post:async(path,body)=>{posts.push({path,body});if(path.endsWith('/run'))return pending.promise;return {success:true,data:{status:'cancelling'}}},get:async()=>({success:true,data:{run:{run_id:'job',status:'running'}}})})
 const first=controller.run('records'),second=controller.run('records')
 assert.equal(posts.length,1)
 pending.resolve({success:true,data:{run_id:'job'}});await Promise.all([first,second])
 await controller.cancel()
 assert.equal(posts[1].body.run_id,'job')
 assert.equal(state.status.run.status,'running')
})

test('settings read failure never enables a default config to save',async()=>{
 let posts=0
 const {state,controller}=await create({get:async()=>{throw Error('unavailable')},post:async()=>{posts++}})
 await controller.openSettings();await controller.saveSettings()
 assert.equal(state.settings.ready,false);assert.equal(posts,0)
})

test('conflict recovery reads the new revision while keeping local dirty fields',async()=>{
 let reads=0,posts=0,submitted
 const changed={...original,target_dirs:'/server',future:{token:'new retained'}}
 const {state,controller}=await create({get:async path=>path.endsWith('/settings')?response(reads++?changed:original,reads>1?'r2':'r1'):{success:true,data:{}},post:async(path,body)=>{posts++;submitted=body;return posts===1?{success:false,message:'配置已变化'}:response({...changed,notify:true},'r3')}})
 await controller.openSettings();state.settings.draft.notify=true
 await controller.saveSettings();await controller.loadSettings()
 assert.equal(state.settings.draft.notify,true)
 assert.equal(state.settings.draft.target_dirs,'/server')
 assert.equal(state.settings.draft.future.token,'new retained')
 assert.equal(state.settings.revision,'r2')
 await controller.saveSettings()
 assert.deepEqual(submitted,{revision:'r2',changes:{notify:true}})
})

test('closing a settings form aborts its outstanding request instead of accumulating reads',async()=>{
 let signal
 const {state,controller}=await create({get:async(path,options)=>{signal=options.signal;return new Promise(()=>{})}})
 const opening=controller.openSettings()
 controller.closeSettings()
 assert.equal(signal.aborted,true)
 await opening
 assert.equal(state.settings.draft,null)
 controller.destroy()
})

test('an explicit not-started receipt permits a new click but network failure keeps the retry ID',async()=>{
 const sent=[]
 let mode='not_started'
 const {controller}=await create({get:async()=>({success:true,data:{}}),post:async(path,body)=>{
  sent.push(body.request_id)
  if(mode==='network')throw Error('network unavailable')
  return {success:false,message:'任务未开始',data:{status:'failed',not_started:true}}
 }})
 await controller.run('records');await controller.run('records')
 assert.notEqual(sent[0],sent[1])
 mode='network';await controller.run('records');await controller.run('records')
 assert.equal(sent[2],sent[3])
})
