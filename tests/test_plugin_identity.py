"""Execute isolated identity-dependent plugin methods, with no host imports."""
import ast
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
import unittest
import uuid

ROOT=Path(__file__).resolve().parents[1]/'plugins.v3/brushflow'
tree=ast.parse((ROOT/'__init__.py').read_text())
original=next(node for node in tree.body if isinstance(node,ast.ClassDef) and any(isinstance(base,ast.Name) and base.id=='_PluginBase' for base in node.bases))
methods={'reload','_refresh_scheduler','_task_data_key','_new_run_report','_now_iso','get_render_mode'}
body=[node for node in original.body if isinstance(node,ast.Assign) or isinstance(node,ast.FunctionDef) and node.name in methods]
registered=[]
scheduled=[]
namespace={'_PluginBase':object,'datetime':datetime,'uuid':uuid,'Counter':__import__('collections').Counter,
 'eventmanager':SimpleNamespace(register=lambda event:lambda method:method),
 'EventType':SimpleNamespace(PluginReload='reload'),
 'register_plugin_api':lambda **kwargs:registered.append(kwargs['plugin_id']),
 'Scheduler':lambda:SimpleNamespace(update_plugin_job=lambda plugin_id:scheduled.append(plugin_id))}
extracted=ast.Module(body=[ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0),ast.ClassDef(name=original.name,bases=[ast.Name(id='_PluginBase',ctx=ast.Load())],keywords=[],body=body,decorator_list=[])],type_ignores=[])
exec(compile(ast.fix_missing_locations(extracted),str(ROOT/'__init__.py'),'exec'),namespace)
Plugin=namespace[original.name]

class PluginIdentityTests(unittest.TestCase):
 def setUp(self):
  registered.clear(); scheduled.clear()
 def test_reload_registers_only_personal_plugin_identity(self):
  plugin=Plugin()
  plugin.reload(SimpleNamespace(event_data={'plugin_id':'BrushFlow'}))
  self.assertEqual(registered,[])
  plugin.reload(SimpleNamespace(event_data={'plugin_id':'OriechoBrushFlow'}))
  self.assertEqual(registered,['OriechoBrushFlow'])
  self.assertEqual(scheduled,['OriechoBrushFlow'])
 def test_scheduler_uses_personal_namespace(self):
  Plugin()._refresh_scheduler()
  self.assertEqual(scheduled,['OriechoBrushFlow'])
 def test_task_record_keys_and_self_references_remain_usable(self):
  self.assertEqual(Plugin._task_data_key('existing-id','source_scan'),'task.existing-id.source_scan')
  self.assertEqual(Plugin._new_run_report('brush')['kind'],'brush')
  self.assertEqual(Plugin.get_render_mode(),('vue','dist/assets'))

if __name__=='__main__':unittest.main(verbosity=2)
