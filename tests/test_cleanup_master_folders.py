"""Exercise folder cleanup without importing the MoviePilot runtime."""

import ast
import errno
from pathlib import Path
from threading import Event
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch


SOURCE = Path(__file__).resolve().parents[1] / "plugins.v2/emptyfoldercleaner/__init__.py"
TREE = ast.parse(SOURCE.read_text(encoding="utf-8"))
ORIGINAL = next(node for node in TREE.body if isinstance(node, ast.ClassDef) and node.name == "EmptyFolderCleaner")
METHODS = {"__is_excluded", "__rmdir_nofollow", "__remove_empty_folders"}
EXTRACTED = ast.Module(
    body=[ast.ClassDef(
        name="FolderCleaner",
        bases=[],
        keywords=[],
        body=[node for node in ORIGINAL.body if isinstance(node, ast.FunctionDef) and node.name in METHODS],
        decorator_list=[],
    )],
    type_ignores=[],
)


class FakeLogger:
    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: None


NAMESPACE = {"Path": Path, "Event": Event, "os": os, "errno": errno, "shutil": shutil,
             "logger": FakeLogger(), "Tuple": tuple, "List": list}
exec(compile(ast.fix_missing_locations(EXTRACTED), str(SOURCE), "exec"), NAMESPACE)
FolderCleaner = NAMESPACE["FolderCleaner"]


class FolderCleanupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.cleaner = FolderCleaner()
        self.cleaner._exclude_dirs = ""
        self.cleaner._dry_run = False
        self.cleaner._recursive = True
        self.cleaner._event = Event()

    def test_removes_nested_empty_directories_but_preserves_root(self):
        (self.root / "a" / "b").mkdir(parents=True)
        count, paths = self.cleaner._FolderCleaner__remove_empty_folders(self.root)
        self.assertEqual(count, 2)
        self.assertEqual(set(paths), {str(self.root / "a"), str(self.root / "a" / "b")})
        self.assertTrue(self.root.is_dir())

    def test_excluded_child_preserves_itself_and_parent(self):
        (self.root / "a" / "keep").mkdir(parents=True)
        self.cleaner._exclude_dirs = "keep"
        count, _ = self.cleaner._FolderCleaner__remove_empty_folders(self.root)
        self.assertEqual(count, 0)
        self.assertTrue((self.root / "a" / "keep").is_dir())

    def test_symlink_is_never_followed_or_removed(self):
        with tempfile.TemporaryDirectory() as external:
            (self.root / "a").mkdir()
            (self.root / "a" / "link").symlink_to(external, target_is_directory=True)
            count, _ = self.cleaner._FolderCleaner__remove_empty_folders(self.root)
            self.assertEqual(count, 0)
            self.assertTrue((self.root / "a" / "link").is_symlink())
            self.assertTrue(Path(external).is_dir())

    def test_file_added_during_cleanup_is_not_deleted(self):
        (self.root / "a").mkdir()
        real_rmdir = os.rmdir

        def concurrent_create(path, *, dir_fd=None):
            if path == "a":
                (self.root / "a" / "new.mkv").write_text("keep")
            return real_rmdir(path, dir_fd=dir_fd)

        with patch.object(os, "rmdir", side_effect=concurrent_create):
            count, _ = self.cleaner._FolderCleaner__remove_empty_folders(self.root)
        self.assertEqual(count, 0)
        self.assertEqual((self.root / "a" / "new.mkv").read_text(), "keep")

    def test_dry_run_counts_nested_empty_folders_without_mutation(self):
        (self.root / "a" / "b").mkdir(parents=True)
        self.cleaner._dry_run = True
        count, _ = self.cleaner._FolderCleaner__remove_empty_folders(self.root)
        self.assertEqual(count, 2)
        self.assertTrue((self.root / "a" / "b").is_dir())

    def test_non_recursive_mode_keeps_parents_that_were_not_initially_empty(self):
        (self.root / "a" / "b").mkdir(parents=True)
        self.cleaner._recursive = False
        count, _ = self.cleaner._FolderCleaner__remove_empty_folders(self.root)
        self.assertEqual(count, 1)
        self.assertTrue((self.root / "a").is_dir())
        self.assertFalse((self.root / "a" / "b").exists())

    def test_ancestor_symlink_swap_cannot_delete_external_directory(self):
        (self.root / "a" / "empty").mkdir(parents=True)
        with tempfile.TemporaryDirectory() as external:
            outside = Path(external)
            (outside / "empty").mkdir()
            real_rmdir = os.rmdir
            swapped = False

            def swap_before_delete(path, *, dir_fd=None):
                nonlocal swapped
                if path == "empty" and not swapped:
                    swapped = True
                    (self.root / "a").rename(self.root / "saved")
                    (self.root / "a").symlink_to(outside, target_is_directory=True)
                return real_rmdir(path, dir_fd=dir_fd)

            with patch.object(os, "rmdir", side_effect=swap_before_delete):
                self.cleaner._FolderCleaner__remove_empty_folders(self.root)
            self.assertTrue((outside / "empty").is_dir())


if __name__ == "__main__":
    unittest.main()
