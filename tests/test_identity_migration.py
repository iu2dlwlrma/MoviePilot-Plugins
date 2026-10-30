"""Offline rehearsal of the reviewed SQLite business-data migration procedure.

This is not an application migration entry point. It opens only an in-memory
database; installation receipts are fixtures created by the normal-install step.
The live procedure additionally requires both plugin runtimes to be quiescent.
"""

import json
import sqlite3
import unittest


OLD_ID = "BrushFlow"
NEW_ID = "OriechoBrushFlow"


def rows(db, plugin_id):
    return db.execute(
        "SELECT key, value FROM plugindata WHERE plugin_id = ? ORDER BY key, id",
        (plugin_id,),
    ).fetchall()


def rehearse_copy(db, *, fresh_config='{"enabled": false, "tasks": []}',
                  fail_after=None):
    """Model only the reviewed transaction, with strict fresh-target checks."""
    db.execute("BEGIN IMMEDIATE")
    try:
        hosts = {}
        for plugin_id in (OLD_ID, NEW_ID):
            host = db.execute(
                "SELECT source_plugin_id, is_enabled, config_data "
                "FROM plugininstance WHERE instance_id = ?", (plugin_id,),
            ).fetchone()
            if host is None or host[0] != plugin_id or host[1] != 0:
                raise ValueError("Both physical hosts must exist and be stopped")
            hosts[plugin_id] = host
        if hosts[NEW_ID][2] != fresh_config:
            raise ValueError("Target changed since fresh-install verification")
        target_config = json.loads(hosts[NEW_ID][2])
        if target_config.get("enabled") or target_config.get("tasks"):
            raise ValueError("Target has existing business configuration")

        source_rows = rows(db, OLD_ID)
        if len({key for key, _ in source_rows}) != len(source_rows):
            raise ValueError("Source contains duplicate keys")
        target_rows = rows(db, NEW_ID)
        # Empty installation init_plugin writes precisely this schema marker.
        # No INSERT OR REPLACE: plugindata has no unique (plugin_id, key) index.
        if target_rows not in ([], [("task_data_schema_version", "3")]):
            raise ValueError("Target is not an empty fresh installation")

        db.execute(
            "UPDATE plugininstance SET config_data = ? WHERE instance_id = ?",
            (hosts[OLD_ID][2], NEW_ID),
        )
        if fail_after == "config":
            raise RuntimeError("Injected failure after configuration copy")
        db.execute("DELETE FROM plugindata WHERE plugin_id = ?", (NEW_ID,))
        db.execute(
            "INSERT INTO plugindata (plugin_id, key, value) "
            "SELECT ?, key, value FROM plugindata WHERE plugin_id = ?",
            (NEW_ID, OLD_ID),
        )
        if fail_after == "data":
            raise RuntimeError("Injected failure after business-data copy")
        if rows(db, NEW_ID) != source_rows:
            raise ValueError("Copied business data differs from source")
        db.commit()
    except BaseException:
        db.rollback()
        raise


class IdentityMigrationTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)
        self.db.executescript("""
            CREATE TABLE plugininstance (
                instance_id TEXT UNIQUE, source_plugin_id TEXT,
                is_enabled INTEGER, config_data TEXT, plugin_name TEXT
            );
            CREATE TABLE plugindata (
                id INTEGER PRIMARY KEY, plugin_id TEXT, key TEXT, value TEXT
            );
            CREATE INDEX data_namespace ON plugindata (plugin_id, key);
            CREATE TABLE pluginidentity (plugin_id TEXT, receipt TEXT);
            CREATE TABLE plugininstallation (plugin_id TEXT, receipt TEXT);
            CREATE TABLE systemconfig (key TEXT, value TEXT);
        """)
        tasks = [{"id": f"stable-task-{i}", "enabled": i < 2,
                  "downloader": "existing-downloader", "tag": f"刷流-原标签{i}"}
                 for i in range(9)]
        config = {"enabled": True, "tasks": tasks, "unknown_setting": [1, 2]}
        self.source_config = json.dumps(config, ensure_ascii=False)
        self.db.executemany(
            "INSERT INTO plugininstance VALUES (?, ?, 0, ?, '站点刷流')",
            [(OLD_ID, OLD_ID, self.source_config),
             (NEW_ID, NEW_ID, '{"enabled": false, "tasks": []}')],
        )
        data = {
            "source_budget.1": {"requests": [100, 130], "retry_at": 1000},
            "source_budget.10": {"requests": [120], "last_task_id": tasks[0]["id"]},
            "task_data_schema_version": 3,
            "metrics": {"uploaded": 987654321},
            "download_budget.existing-downloader": {},
            "download_slots": {},
            "future_unknown.key": {"must_preserve": True},
        }
        for task in tasks:
            for suffix in ("torrents", "archived", "unmanaged", "statistic",
                           "runs", "rotation", "metrics", "source_scan"):
                data[f"task.{task['id']}.{suffix}"] = {"original": suffix}
        data["task.stable-task-0.torrents"] = {
            f"hash-{i}": {"task_id": tasks[0]["id"], "tag": tasks[0]["tag"]}
            for i in range(20)
        }
        self.db.executemany(
            "INSERT INTO plugindata (plugin_id, key, value) VALUES (?, ?, ?)",
            [(OLD_ID, key, json.dumps(value, ensure_ascii=False))
             for key, value in data.items()]
            + [(NEW_ID, "task_data_schema_version", "3")],
        )
        for table in ("pluginidentity", "plugininstallation"):
            self.db.executemany(f"INSERT INTO {table} VALUES (?, ?)",
                                [(OLD_ID, "normal-old-receipt"),
                                 (NEW_ID, "normal-new-receipt")])
        self.db.execute("INSERT INTO systemconfig VALUES (?, ?)",
                        ("UserInstalledPlugins", json.dumps([NEW_ID])))
        self.db.commit()

    def snapshot(self):
        return {table: self.db.execute(f"SELECT * FROM {table}").fetchall()
                for table in ("plugininstance", "plugindata", "pluginidentity",
                              "plugininstallation", "systemconfig")}

    def test_copies_every_key_and_preserves_source_and_installation_receipts(self):
        before = self.snapshot()
        original = rows(self.db, OLD_ID)
        rehearse_copy(self.db)
        self.assertEqual(rows(self.db, OLD_ID), original)
        self.assertEqual(rows(self.db, NEW_ID), original)
        self.assertIn("future_unknown.key", dict(rows(self.db, NEW_ID)))
        after = self.snapshot()
        for table in ("pluginidentity", "plugininstallation", "systemconfig"):
            self.assertEqual(after[table], before[table])
        source, target = after["plugininstance"]
        self.assertEqual(source, before["plugininstance"][0])
        self.assertEqual(target[:3], (NEW_ID, NEW_ID, 0))
        self.assertEqual(target[3], self.source_config)
        tasks = json.loads(target[3])["tasks"]
        self.assertEqual((len(tasks), sum(t["enabled"] for t in tasks)), (9, 2))
        torrents = json.loads(dict(rows(self.db, NEW_ID))["task.stable-task-0.torrents"])
        self.assertEqual(len(torrents), 20)
        self.assertEqual({t["tag"] for t in torrents.values()}, {"刷流-原标签0"})

    def test_rejects_second_migration_without_changing_any_data(self):
        rehearse_copy(self.db)
        before = self.snapshot()
        with self.assertRaises(ValueError):
            rehearse_copy(self.db)
        self.assertEqual(self.snapshot(), before)

    def test_rejects_target_business_data_and_duplicate_markers(self):
        for key, value in (("source_budget.1", "{}"),
                           ("task_data_schema_version", "3")):
            with self.subTest(key=key):
                cursor = self.db.execute(
                    "INSERT INTO plugindata (plugin_id, key, value) VALUES (?, ?, ?)",
                    (NEW_ID, key, value))
                self.db.commit()
                before = self.snapshot()
                with self.assertRaises(ValueError):
                    rehearse_copy(self.db)
                self.assertEqual(self.snapshot(), before)
                self.db.execute("DELETE FROM plugindata WHERE id = ?", (cursor.lastrowid,))
                self.db.commit()

    def test_rejects_duplicate_source_keys(self):
        self.db.execute("INSERT INTO plugindata (plugin_id, key, value) VALUES (?, ?, ?)",
                        (OLD_ID, "source_budget.1", "{}"))
        self.db.commit()
        before = self.snapshot()
        with self.assertRaises(ValueError):
            rehearse_copy(self.db)
        self.assertEqual(self.snapshot(), before)

    def test_rejects_changed_target_configuration(self):
        self.db.execute("UPDATE plugininstance SET config_data = ? WHERE instance_id = ?",
                        ('{"enabled": false, "tasks": [], "custom": true}', NEW_ID))
        self.db.commit()
        before = self.snapshot()
        with self.assertRaises(ValueError):
            rehearse_copy(self.db)
        self.assertEqual(self.snapshot(), before)

    def test_rolls_back_configuration_and_data_on_failure(self):
        for phase in ("config", "data"):
            with self.subTest(phase=phase):
                before = self.snapshot()
                with self.assertRaises(RuntimeError):
                    rehearse_copy(self.db, fail_after=phase)
                self.assertEqual(self.snapshot(), before)

    def test_rejects_running_source_or_target_host(self):
        for plugin_id in (OLD_ID, NEW_ID):
            with self.subTest(plugin_id=plugin_id):
                self.db.execute("UPDATE plugininstance SET is_enabled = 1 WHERE instance_id = ?",
                                (plugin_id,))
                self.db.commit()
                before = self.snapshot()
                with self.assertRaises(ValueError):
                    rehearse_copy(self.db)
                self.assertEqual(self.snapshot(), before)
                self.db.execute("UPDATE plugininstance SET is_enabled = 0 WHERE instance_id = ?",
                                (plugin_id,))
                self.db.commit()

    def test_accepts_completely_empty_target_namespace(self):
        self.db.execute("DELETE FROM plugindata WHERE plugin_id = ?", (NEW_ID,))
        self.db.commit()
        rehearse_copy(self.db)
        self.assertEqual(rows(self.db, NEW_ID), rows(self.db, OLD_ID))


if __name__ == "__main__":
    unittest.main(verbosity=2)
