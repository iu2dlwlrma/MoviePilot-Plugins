"""Confirm the renamed plugin keeps its existing identity and configuration."""

import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "plugins.v2/emptyfoldercleaner/__init__.py"
ORIGINAL = next(node for node in ast.parse(SOURCE.read_text(encoding="utf-8")).body
                if isinstance(node, ast.ClassDef) and node.name == "EmptyFolderCleaner")
METHODS = {"init_plugin", "get_form", "get_state", "get_service", "_bounded_int", "_load_config"}
CLASS = ast.ClassDef(
    name="EmptyFolderCleaner", bases=[ast.Name(id="_PluginBase", ctx=ast.Load())],
    keywords=[], decorator_list=[],
    body=[node for node in ORIGINAL.body if isinstance(node, ast.Assign) or
          isinstance(node, ast.FunctionDef) and node.name in METHODS],
)
NAMESPACE = {
    "_PluginBase": object,
    "deepcopy": __import__("copy").deepcopy,
    "Event": __import__("threading").Event,
    "Lock": __import__("threading").Lock,
    "Any": object,
    "List": list,
    "Dict": dict,
    "Tuple": tuple,
    "CronTrigger": type("CronTrigger", (), {"from_crontab": staticmethod(lambda value: value)}),
}
exec(compile(ast.fix_missing_locations(ast.Module(body=[CLASS], type_ignores=[])),
             str(SOURCE), "exec"), NAMESPACE)
Plugin = NAMESPACE["EmptyFolderCleaner"]


class CleanupMasterFormTests(unittest.TestCase):
    def test_existing_id_and_settings_are_preserved(self):
        plugin = Plugin()
        self.assertEqual(Plugin.__name__, "EmptyFolderCleaner")
        self.assertEqual(plugin.plugin_config_prefix, "emptyfoldercleaner_")
        self.assertEqual(plugin.plugin_name, "清理大师")
        original = {"enabled": True, "cron": "0 0 * * 1", "target_dirs": "/Data/Downloads",
                    "exclude_dirs": "keep", "recursive": True, "dry_run": False,
                    "records_enabled": True, "records_cron": "0 3 * * *", "future": {"v": 7}}
        plugin._load_config(original)
        self.assertEqual(plugin._cron, "0 0 * * 1")
        self.assertEqual(plugin._target_dirs, "/Data/Downloads")
        self.assertEqual(plugin._exclude_dirs, "keep")
        self.assertTrue(plugin._records_enabled)
        self.assertEqual(plugin._loaded_config, original)
        plugin._loaded_config["future"]["v"] = 8
        self.assertEqual(original["future"]["v"], 7)

    def test_form_defaults_preview_new_record_cleanup(self):
        _, defaults = Plugin().get_form()
        self.assertFalse(defaults["records_enabled"])
        self.assertTrue(defaults["records_dry_run"])
        self.assertFalse(defaults["records_reopen_subscription"])
        self.assertEqual(defaults["records_grace_hours"], 24)
        self.assertEqual(defaults["target_dirs"], "")

    def test_normal_form_cannot_trigger_one_shot_or_grant_subscription_mutation(self):
        form, _ = Plugin().get_form()
        models = {item["props"]["model"] for item in form[0]["content"]}
        self.assertNotIn("onlyonce", models)
        self.assertNotIn("records_reopen_subscription", models)
        self.assertTrue({"target_dirs", "exclude_dirs", "recursive", "notify", "dry_run", "cron"} <= models)

    def test_folder_and_record_jobs_are_independent(self):
        plugin = Plugin()
        plugin._enabled = True
        plugin._target_dirs = "/Data/Downloads"
        plugin._cron = "0 0 * * 1"
        plugin._records_enabled = True
        plugin._records_cron = "0 3 * * *"
        plugin.clean_empty_folders = lambda: None
        plugin.clean_records = lambda: None
        services = plugin.get_service()
        self.assertEqual({job["id"] for job in services},
                         {"EmptyFolderCleaner", "EmptyFolderCleanerRecords"})
        plugin._records_enabled = False
        self.assertEqual([job["id"] for job in plugin.get_service()], ["EmptyFolderCleaner"])

    def test_untrusted_numeric_form_input_is_bounded(self):
        self.assertEqual(Plugin._bounded_int("invalid", 50, 1, 100), 50)
        self.assertEqual(Plugin._bounded_int("0", 50, 1, 100), 1)
        self.assertEqual(Plugin._bounded_int("10000", 50, 1, 100), 100)

    def test_malformed_preview_value_never_enables_record_writes(self):
        for value in (None, 0, "", "false"):
            with self.subTest(value=value):
                plugin = Plugin()
                plugin.stop_service = lambda: None
                plugin.init_plugin({"enabled": False, "records_dry_run": value})
                self.assertTrue(plugin._records_dry_run)
        plugin = Plugin()
        plugin.stop_service = lambda: None
        plugin.init_plugin({"enabled": False, "records_dry_run": False})
        self.assertFalse(plugin._records_dry_run)


if __name__ == "__main__":
    unittest.main()
