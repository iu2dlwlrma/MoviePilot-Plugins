import importlib.util
from pathlib import Path
import unittest


try:
    import pydantic
except ImportError:
    pydantic = None


@unittest.skipUnless(pydantic and hasattr(pydantic, "field_validator"), "需要 Pydantic 2")
class BrushFlowModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).parents[1] / "plugins.v3/brushflow/models.py"
        spec = importlib.util.spec_from_file_location("brushflow_models", path)
        cls.models = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.models)

    def task(self, **values):
        return self.models.BrushTaskPayload(name="站点", site_id=1, **values)

    def test_task_defaults(self):
        task = self.task()
        self.assertTrue(task.use_global_defaults)
        self.assertEqual(task.override_fields, [])
        self.assertFalse(task.novice)
        self.assertIsNone(task.novice_until)
        self.assertEqual(task.seeder, "1-50")
        self.assertEqual(task.leecher, "1")
        self.assertEqual(task.min_peer_seed_ratio, 0.1)
        self.assertEqual(task.site_ratio_target, 2)
        self.assertEqual(task.promotion_lead_minutes, 10)
        self.assertTrue(task.del_no_free)
        self.assertEqual(task.rotation_hours, 24)

    def test_rotation_settings_validation(self):
        self.assertEqual(self.models.BrushFlowSettingsPayload().rotation_mode, "parallel")
        self.assertEqual(self.models.BrushFlowSettingsPayload(rotation_mode="sequential").rotation_mode, "sequential")
        with self.assertRaises(pydantic.ValidationError):
            self.models.BrushFlowSettingsPayload(rotation_mode="unknown")
        for hours in (0, -1, 169, float("nan"), float("inf")):
            with self.subTest(hours=hours), self.assertRaises(pydantic.ValidationError):
                self.task(rotation_hours=hours)

    def test_manual_focus_site_validation(self):
        payload = self.models.BrushFlowSettingsPayload
        self.assertEqual(payload(manual_focus_site_id=2).manual_focus_site_id, 2)
        for value in (None, "", 0, "0"):
            with self.subTest(value=value):
                self.assertIsNone(payload(manual_focus_site_id=value).manual_focus_site_id)
        for value in (-1, 1.5, "unknown", True, float("inf")):
            with self.subTest(value=value), self.assertRaises(pydantic.ValidationError):
                payload(manual_focus_site_id=value)

    def test_downloader_required_only_for_independent_tasks(self):
        self.assertIsNone(self.task(downloader="  ").downloader)
        with self.assertRaises(pydantic.ValidationError):
            self.task(use_global_defaults=False)
        self.assertEqual(self.task(use_global_defaults=False, downloader=" qb ").downloader, "qb")

    def test_identity_stays_required(self):
        for values in ({"site_id": 1}, {"name": "站点"}, {"name": " ", "site_id": 1}):
            with self.subTest(values=values), self.assertRaises(pydantic.ValidationError):
                self.models.BrushTaskPayload(**values)

    def test_novice_date_validation(self):
        for value in (None, "2026-10-11", "2026-10-11T23:59:00+08:00", "2026-10-11T15:59:00Z"):
            self.assertEqual(self.task(novice_until=value).novice_until, value)
        for value in ("2026-02-30", "2026-10-11T25:00:00", "tomorrow"):
            with self.subTest(value=value), self.assertRaises(pydantic.ValidationError):
                self.task(novice_until=value)

    def test_new_filter_validation(self):
        self.assertEqual(self.task(leecher=" 1-10 ").leecher, "1-10")
        self.task(min_peer_seed_ratio=0, promotion_lead_minutes=0)
        for values in ({"leecher": "many"}, {"leecher": "1.5"}, {"leecher": "1-2.5"},
                       {"leecher": "3-1"}, {"min_peer_seed_ratio": -1},
                       {"promotion_lead_minutes": -1}, {"min_peer_seed_ratio": float("nan")}):
            with self.subTest(values=values), self.assertRaises(pydantic.ValidationError):
                self.task(**values)

    def test_slow_download_settings_validate_and_can_be_disabled(self):
        settings = self.models.BrushFlowSettingsPayload(global_defaults={
            "download_min_speed": "100", "download_slow_minutes": "30"})
        self.assertEqual(settings.global_defaults["download_min_speed"], 100)
        self.assertEqual(settings.global_defaults["download_slow_minutes"], 30)
        for field in ("download_min_speed", "download_slow_minutes"):
            for value in (None, "", 0, "0"):
                self.assertIsNone(getattr(self.task(**{field: value}), field))
            for value in (-1, float("nan"), float("inf")):
                with self.subTest(field=field, value=value), self.assertRaises(pydantic.ValidationError):
                    self.task(**{field: value})

    def test_shared_defaults_validate_and_fill_missing_values(self):
        settings = self.models.BrushFlowSettingsPayload(global_defaults={"leecher": "2", "notify": False})
        self.assertEqual(set(settings.global_defaults), self.models.SHARED_TASK_FIELDS)
        self.assertEqual(settings.global_defaults["leecher"], "2")
        self.assertFalse(settings.global_defaults["notify"])
        self.assertEqual(settings.global_defaults["promotion_lead_minutes"], 10)
        self.assertIsNone(settings.global_defaults["downloader"])
        self.assertFalse(settings.apply_defaults_to_existing)
        for values in ({"brush_interval": 0}, {"leecher": "bad"}, {"promotion_lead_minutes": -1}):
            with self.subTest(values=values), self.assertRaises(pydantic.ValidationError):
                self.models.BrushFlowSettingsPayload(global_defaults=values)

    def test_shared_defaults_reject_private_fields(self):
        for key in ("name", "site_id", "enabled", "novice", "novice_until", "tag",
                    "timezone_offset", "site_hr_active", "site_skip_tips"):
            with self.subTest(key=key), self.assertRaises(pydantic.ValidationError):
                self.models.BrushFlowSettingsPayload(global_defaults={key: True})
        with self.assertRaises(pydantic.ValidationError):
            self.task(override_fields=["novice"])
        self.assertEqual(self.task(override_fields=["notify", "notify"]).override_fields, ["notify"])

    def test_old_speed_and_count_fields_are_ignored(self):
        removed = {key: 1 for key in ("maxdlcount", "maxupspeed", "maxdlspeed", "up_speed", "dl_speed")}
        self.assertFalse(set(self.task(**removed).model_dump()).intersection(removed))
        settings = self.models.BrushFlowSettingsPayload(global_maxdlcount=5, global_maxupspeed=1, global_maxdlspeed=1)
        self.assertFalse(any(key in settings.model_dump() for key in
                             ("global_maxdlcount", "global_maxupspeed", "global_maxdlspeed")))


if __name__ == "__main__":
    unittest.main()
