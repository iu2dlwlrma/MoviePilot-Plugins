"""Exercise the release workflow's actual checks without publishing anything."""

import json
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = (
    ("release.yml", "OriechoBrushFlow", "brushflow"),
    ("site-statistic-release.yml", "OriechoSiteStatistic", "sitestatistic"),
)


def workflow_python(filename, *, guard=False):
    """Read a Python heredoc from its real GitHub Actions run block."""
    source = (ROOT / ".github" / "workflows" / filename).read_text(encoding="utf-8")
    runs = re.findall(r"(?m)^        run: \|\n((?:          .*\n|\n)+)", source)
    for run in runs:
        script = textwrap.dedent(run)
        for match in re.finditer(
            r"(?m)^[ \t]*python3 ([^\n]*)<<'PY'\n(.*?)^PY[ \t]*$", script, re.DOTALL
        ):
            if ("$tag" in match.group(1)) == guard:
                return match.group(2)
    raise AssertionError(f"No {'immutability' if guard else 'validation'} script in {filename}")


class PublishedVersionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="site-statistic-release-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.git("init", "--quiet")
        self.manifest = {
            "OriechoBrushFlow": {"version": "7.3.1", "name": "站点刷流", "release": True},
            "OriechoSiteStatistic": {"version": "1.0.0", "name": "站点数据统计", "release": True},
        }
        for _, _, directory in WORKFLOWS:
            plugin = self.root / "plugins.v3" / directory
            plugin.mkdir(parents=True)
            (plugin / "__init__.py").write_text("version = 'unchanged'\n", encoding="utf-8")
        self.write_manifest()
        self.commit()
        self.git("tag", "OriechoBrushFlow_v7.3.1")
        self.git("tag", "OriechoSiteStatistic_v1.0.0")

    def git(self, *args):
        return subprocess.run(
            ["git", "-c", "user.name=Release Test", "-c", "user.email=release-test@example.invalid", *args],
            cwd=self.root, check=True, text=True, capture_output=True,
        )

    def write_manifest(self, *, sort_keys=False):
        (self.root / "package.v3.json").write_text(
            json.dumps(self.manifest, ensure_ascii=False, indent=2, sort_keys=sort_keys), encoding="utf-8"
        )

    def commit(self):
        self.git("add", ".")
        self.git("commit", "--quiet", "-m", "release test fixture")

    def run_guard(self, filename, plugin_id, directory):
        version = "7.3.1" if plugin_id == "OriechoBrushFlow" else "1.0.0"
        return subprocess.run(
            [sys.executable, "-", f"{plugin_id}_v{version}", plugin_id, f"plugins.v3/{directory}"],
            input=workflow_python(filename, guard=True), cwd=self.root, text=True, capture_output=True,
        )

    def test_unrelated_manifest_entries_do_not_invalidate_existing_releases(self):
        # Comparing the whole shared manifest would reject this independent plugin.
        self.manifest["AnotherPlugin"] = {"name": "独立插件", "version": "9.0.0"}
        self.write_manifest(sort_keys=True)
        self.commit()
        for workflow in WORKFLOWS:
            with self.subTest(workflow=workflow[0]):
                result = self.run_guard(*workflow)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_changing_own_manifest_entry_requires_a_new_version(self):
        # A guard that only compares source directories would incorrectly allow this.
        for _, plugin_id, _ in WORKFLOWS:
            self.manifest[plugin_id]["name"] = "changed published metadata"
        self.write_manifest()
        self.commit()
        for workflow in WORKFLOWS:
            with self.subTest(workflow=workflow[0]):
                result = self.run_guard(*workflow)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("increase the version", result.stdout + result.stderr)

    def test_other_plugin_source_does_not_invalidate_a_release(self):
        # Each release must remain independent of the other plugin's source tree.
        changed = self.root / "plugins.v3" / "brushflow" / "__init__.py"
        changed.write_text("version = 'a separate brush-flow change'\n", encoding="utf-8")
        self.commit()
        result = self.run_guard(*WORKFLOWS[1])
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_changing_own_source_requires_a_new_version(self):
        # A guard that only compares the manifest would incorrectly allow this.
        for _, _, directory in WORKFLOWS:
            (self.root / "plugins.v3" / directory / "__init__.py").write_text(
                "version = 'changed without a new release'\n", encoding="utf-8"
            )
        self.commit()
        for workflow in WORKFLOWS:
            with self.subTest(workflow=workflow[0]):
                result = self.run_guard(*workflow)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("increase the version", result.stdout + result.stderr)


class StatisticReleaseInputsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="site-statistic-inputs-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.plugin = self.root / "plugins.v3" / "sitestatistic"
        self.plugin.mkdir(parents=True)
        self.source = (
            "class OriechoSiteStatistic:\n"
            "    plugin_version = '1.0.0'\n"
            "    plugin_name = '站点数据统计'\n"
            "    plugin_author = 'oriecho'\n"
            "    author_url = 'https://github.com/iu2dlwlrma'\n"
            "    plugin_config_prefix = 'oriechositestatistic_'\n"
        )
        (self.plugin / "__init__.py").write_text(self.source, encoding="utf-8")
        (self.plugin / "monthly.py").write_text("def month_data():\n    return []\n", encoding="utf-8")
        (self.plugin / "README.md").write_text("# 站点数据统计\n", encoding="utf-8")
        (self.root / "LICENSE").write_text("test license\n", encoding="utf-8")
        (self.plugin / "LICENSE").write_text("test license\n", encoding="utf-8")
        (self.root / "package.v3.json").write_text(json.dumps({
            "OriechoSiteStatistic": {
                "version": "1.0.0", "name": "站点数据统计", "author": "oriecho",
                "author_url": "https://github.com/iu2dlwlrma", "level": 2,
                "release": True, "system_version": ">=3.0.0",
            },
        }, ensure_ascii=False), encoding="utf-8")

    def validate(self):
        return subprocess.run(
            [sys.executable, "-"], input=workflow_python("site-statistic-release.yml"),
            cwd=self.root, text=True, capture_output=True,
        )

    def test_native_json_plugin_releases_without_frontend_dist(self):
        result = self.validate()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertFalse((self.plugin / "dist").exists())

    def test_missing_release_inputs_are_rejected(self):
        for filename in ("README.md", "LICENSE", "monthly.py"):
            with self.subTest(filename=filename):
                path = self.plugin / filename
                content = path.read_bytes()
                path.unlink()
                try:
                    self.assertNotEqual(self.validate().returncode, 0)
                finally:
                    path.write_bytes(content)

    def test_identity_drift_is_rejected(self):
        replacements = (
            ("'1.0.0'", "'1.0.1'"),
            ("'站点数据统计'", "'different name'"),
            ("'oriecho'", "'different author'"),
            ("'https://github.com/iu2dlwlrma'", "'https://github.com/someone-else'"),
            ("'oriechositestatistic_'", "'sitestatistic_'"),
        )
        for old, new in replacements:
            with self.subTest(identity=old):
                (self.plugin / "__init__.py").write_text(self.source.replace(old, new), encoding="utf-8")
                self.assertNotEqual(self.validate().returncode, 0)

    def test_changed_license_is_rejected(self):
        (self.plugin / "LICENSE").write_text("a different license\n", encoding="utf-8")
        self.assertNotEqual(self.validate().returncode, 0)


if __name__ == "__main__":
    unittest.main()
