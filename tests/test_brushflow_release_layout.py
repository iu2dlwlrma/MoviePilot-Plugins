"""Run the real brush-flow release validation against the unprefixed source layout."""
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parents[1]


def release_validation():
    source = (ROOT / '.github/workflows/release.yml').read_text(encoding='utf-8')
    for run in re.findall(r'(?m)^        run: \|\n((?:          .*\n|\n)+)', source):
        match = re.search(r"(?m)^python3 - <<'PY'\n(.*?)^PY$", textwrap.dedent(run), re.DOTALL)
        if match:
            return match.group(1)
    raise AssertionError('Missing release input validation')


class BrushFlowReleaseLayoutTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='brushflow-release-inputs-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.plugin = self.root / 'plugins.v3/brushflow'
        (self.plugin / 'dist/assets').mkdir(parents=True)
        (self.plugin / '__init__.py').write_text(
            "class OriechoBrushFlow:\n"
            "    plugin_version = '7.3.2'\n"
            "    plugin_name = '站点刷流'\n"
            "    plugin_author = 'oriecho'\n"
            "    author_url = 'https://github.com/iu2dlwlrma'\n",
            encoding='utf-8',
        )
        (self.root / 'LICENSE').write_text('fixture license\n', encoding='utf-8')
        for relative in ('README.md', 'LICENSE', 'dist/assets/remoteEntry.js'):
            (self.plugin / relative).write_text('fixture license\n', encoding='utf-8')
        (self.root / 'package.v3.json').write_text(json.dumps({
            'OriechoBrushFlow': {
                'version': '7.3.2', 'name': '站点刷流', 'author': 'oriecho',
                'author_url': 'https://github.com/iu2dlwlrma', 'release': True, 'level': 2,
            },
        }, ensure_ascii=False), encoding='utf-8')

    def validate(self):
        return subprocess.run(
            [sys.executable, '-'], input=release_validation(), cwd=self.root,
            capture_output=True, text=True,
        )

    def test_unprefixed_directory_releases_with_existing_runtime_identity(self):
        # Deriving the source path from OriechoBrushFlow.lower() must fail this case.
        result = self.validate()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_missing_frontend_entry_is_rejected(self):
        (self.plugin / 'dist/assets/remoteEntry.js').unlink()
        self.assertNotEqual(self.validate().returncode, 0)

    def test_changed_license_is_rejected(self):
        (self.plugin / 'LICENSE').write_text('incorrect license\n', encoding='utf-8')
        self.assertNotEqual(self.validate().returncode, 0)


if __name__ == '__main__':
    unittest.main()
