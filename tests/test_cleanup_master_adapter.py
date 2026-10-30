"""Exercise host boundaries with real files and deterministic external responses."""
from dataclasses import replace
from collections import UserList
import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace, ModuleType
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1] / "plugins.v2/emptyfoldercleaner"
PACKAGE = "cleanup_adapter_tests"
package = ModuleType(PACKAGE)
package.__path__ = [str(ROOT)]
sys.modules[PACKAGE] = package


def load(name):
    spec = importlib.util.spec_from_file_location(PACKAGE + "." + name, ROOT / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    setattr(package, name, module)
    return module


kernel = load("record_cleanup")
adapter = load("host_adapter")
P = kernel.Presence


def candidate(**kwargs):
    values = dict(file_id=7, revision="rev-1", download_hash="a" * 40,
                  downloader="qb", source_path="/data/source/a.mkv",
                  destination_paths=("/data/library/a.mkv",), media_source="themoviedb",
                  media_id="206484", media_type="电视剧", season=4, episodes=(4,),
                  storage="local", identity_verified=True, review_reason="",
                  destination_lookup_complete=True, subscribe_id=11,
                  subscription_retry_needed=True, transfer_id=5)
    values.update(kwargs)
    return SimpleNamespace(**values)


class PathProbeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.trusted = adapter.TrustedRoot(str(self.root), self.root.stat().st_dev, str(self.root), "test")
        self.probe = adapter.LocalPathProbe([self.trusted], mount_check=lambda _root: True)

    def test_confirms_only_missing_paths_below_healthy_root(self):
        result = self.probe.check(str(self.root / "gone" / "a.mkv"))
        self.assertEqual(result.presence, P.MISSING)
        self.assertTrue(result.ready)

    def test_existing_file_is_protected(self):
        file = self.root / "a.mkv"
        file.write_bytes(b"media")
        self.assertEqual(self.probe.check(str(file)).presence, P.PRESENT)

    def test_symlink_escape_never_proves_missing(self):
        (self.root / "link").symlink_to("/definitely-absent")
        self.assertEqual(self.probe.check(str(self.root / "link/a.mkv")).presence, P.UNKNOWN)

    def test_mount_loss_never_proves_missing(self):
        probe = adapter.LocalPathProbe([self.trusted], mount_check=lambda _root: False)
        self.assertEqual(probe.check(str(self.root / "a.mkv")).presence, P.UNKNOWN)

    def test_changed_device_never_proves_missing(self):
        root = replace(self.trusted, device=self.trusted.device + 1)
        probe = adapter.LocalPathProbe([root], mount_check=lambda _root: True)
        self.assertEqual(probe.check(str(self.root / "a.mkv")).presence, P.UNKNOWN)

    def test_lexical_traversal_and_root_itself_are_not_media(self):
        for path in (str(self.root), str(self.root / ".." / "gone"), "relative.mkv"):
            with self.subTest(path=path):
                self.assertEqual(self.probe.check(path).presence, P.UNKNOWN)

    def test_permission_failure_is_unknown_not_missing(self):
        with patch.object(adapter.os, "lstat", side_effect=PermissionError("denied")):
            self.assertEqual(self.probe.check(str(self.root / "a.mkv")).presence, P.UNKNOWN)

    def test_host_directory_roots_cover_source_and_library_without_assuming_paths(self):
        rows = [SimpleNamespace(storage="local", download_path="/downloads",
                                library_storage="local", library_path="/library"),
                SimpleNamespace(storage="alist", download_path="/remote",
                                library_storage="local", library_path="/library")]
        helper = SimpleNamespace(get_dirs=lambda: rows)
        self.assertEqual(adapter.configured_local_roots(helper), ("/downloads", "/library"))

    def test_two_distinct_trusted_roots_can_both_prove_missing(self):
        source, library = self.root / "source", self.root / "library"
        source.mkdir()
        library.mkdir()
        roots = [adapter.TrustedRoot(str(p), p.stat().st_dev, str(p), "test") for p in (source, library)]
        probe = adapter.LocalPathProbe(roots, mount_check=lambda _: True)
        self.assertEqual(probe.check(str(source / "a.mkv")).presence, P.MISSING)
        self.assertEqual(probe.check(str(library / "a.mkv")).presence, P.MISSING)

    def test_exact_unlink_checks_full_signature_and_removes_only_the_file(self):
        file = self.root / "a.mkv"
        file.write_bytes(b"media")
        sibling = self.root / "keep.mkv"
        sibling.write_bytes(b"keep")
        observation = self.probe.check(str(file))
        self.assertEqual(len(observation.signature), 7)
        self.assertTrue(self.probe.unlink_exact(str(file), observation.signature))
        self.assertFalse(file.exists())
        self.assertEqual(sibling.read_bytes(), b"keep")

    def test_exact_unlink_rejects_changed_file_shared_inode_and_symlink(self):
        file = self.root / "a.mkv"
        file.write_bytes(b"media")
        signature = self.probe.check(str(file)).signature
        file.write_bytes(b"changed")
        self.assertFalse(self.probe.unlink_exact(str(file), signature))
        link = self.root / "alias.mkv"
        os.link(file, link)
        self.assertFalse(self.probe.unlink_exact(str(file), self.probe.check(str(file)).signature))
        link.unlink()
        signature = self.probe.check(str(file)).signature
        file.rename(link)
        file.symlink_to(link)
        self.assertFalse(self.probe.unlink_exact(str(file), signature))
        self.assertEqual(link.read_bytes(), b"changed")

    def test_exact_unlink_rejects_parent_symlink_and_lost_mount(self):
        directory = self.root / "directory"
        directory.mkdir()
        file = directory / "a.mkv"
        file.write_bytes(b"media")
        signature = self.probe.check(str(file)).signature
        directory.rename(self.root / "moved")
        directory.symlink_to(self.root / "moved", target_is_directory=True)
        self.assertFalse(self.probe.unlink_exact(str(file), signature))
        self.assertTrue(file.exists())
        self.probe.mount_check = lambda _: False
        self.assertFalse(self.probe.unlink_exact(str(file), signature))


class Helper:
    def __init__(self, services):
        self.services = services

    def get_configs(self, include_disabled=False):
        if not include_disabled:
            raise AssertionError("must enumerate disabled configurations too")
        return {name: service.config for name, service in self.services.items()}

    def get_service(self, name):
        return self.services.get(name)


class Downloader:
    def __init__(self, tasks=(), error=False):
        self.tasks, self.error = list(tasks), error
        self.calls = []
        self.files = None
        self.file_calls = []
        self.deleted = []
        self.delete_success = True
        self.delete_visible = True

    def get_torrents(self, **kwargs):
        self.calls.append(kwargs)
        return self.tasks, self.error

    def get_files(self, tid):
        self.file_calls.append(tid)
        return self.files

    def delete_torrents(self, *, delete_file, ids):
        self.deleted.append((delete_file, tuple(ids)))
        if self.delete_success and self.delete_visible:
            self.tasks[:] = [row for row in self.tasks if row.get("hash") not in ids]
        return self.delete_success


def downloader_service(tasks=(), *, error=False, enabled=True, kind="qbittorrent"):
    return SimpleNamespace(config=SimpleNamespace(enabled=enabled, type=kind),
                           instance=Downloader(tasks, error), type=kind)


class DownloaderTests(unittest.TestCase):
    def test_completed_shared_task_is_inactive_but_not_removable(self):
        probe, service = self.purge_probe(files=[dict(name="show/a.mkv", size=5, progress=1),
                                                 dict(name="show/b.mkv", size=5, progress=1)])
        self.assertEqual(probe.check_activity("a" * 40, ("/data/show/a.mkv",), "qb"), (True, False))
        self.assertFalse(probe.plan_purge("a" * 40, "/data/show/a.mkv", None, downloader="qb").complete)
        service.instance.tasks[0]["state"] = "checkingUP"
        self.assertEqual(probe.check_activity("a" * 40, ("/data/show/a.mkv",), "qb"), (True, True))
        self.assertEqual(probe.activity_reason, "task_checking")

    def test_same_hash_on_another_downloader_with_distinct_paths_is_preserved(self):
        probe, own = self.purge_probe()
        other = downloader_service([dict(hash="a" * 40, content_path="/other/copy.mkv", save_path="/other", state="uploading")])
        other.instance.files = [dict(name="copy.mkv", size=5, progress=1)]
        probe.helper = Helper({"qb": own, "other": other})
        plan = probe.plan_purge("a" * 40, "/data/show/a.mkv", 5, downloader="qb")
        self.assertTrue(plan.complete)
        self.assertEqual([task.service for task in plan.tasks], ["qb"])
        self.assertTrue(probe.remove_exclusive_tasks("a" * 40, "/data/show/a.mkv", 5, plan, downloader="qb"))
        self.assertEqual(other.instance.deleted, [])
        self.assertEqual(probe.check_scoped("a" * 40, ("/data/show/a.mkv",), "qb"), (True, False))
    def test_preview_inventory_is_reused_but_mutation_inventory_is_fresh(self):
        probe, service = self.purge_probe()
        probe.begin_batch()
        for _ in range(5):
            self.assertEqual(probe.check_cached("a" * 40, ("/data/show/a.mkv",)), (True, True))
        self.assertEqual(len(service.instance.calls), 1)
        probe.plan_purge("a" * 40, "/data/show/a.mkv", 5)
        self.assertEqual(len(service.instance.calls), 2)
        probe.end_batch()
    def purge_probe(self, *, files=None, task_hash="a" * 40):
        service = downloader_service([dict(hash=task_hash, content_path="/data/show", save_path="/data", state="uploading", progress=1.0)])
        service.instance.files = files if files is not None else [dict(name="show/a.mkv", size=5, progress=1.0)]
        return adapter.DownloaderProbe(Helper({"qb": service})), service

    def test_purge_plan_requires_single_complete_exact_file_and_confirms_task_removal(self):
        probe, service = self.purge_probe()
        plan = probe.plan_purge("a" * 40, "/data/show/a.mkv", 5)
        self.assertTrue(plan.complete)
        self.assertEqual(len(plan.tasks), 1)
        self.assertTrue(probe.remove_exclusive_tasks("a" * 40, "/data/show/a.mkv", 5, plan))
        self.assertEqual(service.instance.deleted, [(False, ("a" * 40,))])
        self.assertEqual(service.instance.tasks, [])

    def test_completed_file_does_not_authorize_active_or_unknown_task_removal(self):
        for state in ("moving", "checkingUP", "downloading", "queuedDL", "unknown", None):
            with self.subTest(state=state):
                probe, service = self.purge_probe()
                service.instance.tasks[0]["state"] = state
                self.assertFalse(probe.plan_purge("a" * 40, "/data/show/a.mkv", 5).complete)
                self.assertEqual(service.instance.deleted, [])

    def test_missing_source_can_remove_only_exact_completed_single_file_task(self):
        probe, service = self.purge_probe()
        plan = probe.plan_purge("a" * 40, "/data/show/a.mkv", None)
        self.assertTrue(plan.complete)
        self.assertTrue(probe.remove_exclusive_tasks("a" * 40, "/data/show/a.mkv", None, plan))
        self.assertEqual(service.instance.deleted, [(False, ("a" * 40,))])

    def test_purge_rejects_multi_file_shared_hash_wrong_size_incomplete_and_unknown_files(self):
        cases = ([dict(name="show/a.mkv", size=5, progress=1), dict(name="show/b.mkv", size=5, progress=1)],
                 [dict(name="show/a.mkv", size=6, progress=1)],
                 [dict(name="show/a.mkv", size=5, progress=0.5)],
                 [dict(name="show/a.mkv", size=5)],
                 [dict(name="../show/a.mkv", size=5, progress=1)])
        for files in cases:
            probe, service = self.purge_probe(files=files)
            self.assertFalse(probe.plan_purge("a" * 40, "/data/show/a.mkv", 5).complete)
            self.assertEqual(service.instance.deleted, [])
        probe, _ = self.purge_probe(task_hash="b" * 40)
        self.assertFalse(probe.plan_purge("a" * 40, "/data/show/a.mkv", 5).complete)

    def test_purge_cannot_trust_delete_acknowledgement_or_stale_task_plan(self):
        for visible in (True, False):
            probe, service = self.purge_probe()
            plan = probe.plan_purge("a" * 40, "/data/show/a.mkv", 5)
            if visible:
                service.instance.files.append(dict(name="show/new.mkv", size=3, progress=1))
            else:
                service.instance.delete_visible = False
            self.assertFalse(probe.remove_exclusive_tasks("a" * 40, "/data/show/a.mkv", 5, plan))

    def test_purge_checks_all_clients_and_transmission_complete_bytes(self):
        probe, qb = self.purge_probe()
        tb = downloader_service([dict(hash="a" * 40, content_path="/data/show", save_path="/data", status="seeding")], kind="transmission")
        tb.instance.files = [SimpleNamespace(name="show/a.mkv", size=5, completed=5)]
        probe.helper = Helper({"qb": qb, "tb": tb})
        plan = probe.plan_purge("a" * 40, "/data/show/a.mkv", 5)
        self.assertTrue(plan.complete)
        self.assertEqual(len(plan.tasks), 2)
        self.assertTrue(probe.remove_exclusive_tasks("a" * 40, "/data/show/a.mkv", 5, plan))
        self.assertEqual(qb.instance.deleted[0][0], False)
        self.assertEqual(tb.instance.deleted[0][0], False)

    def test_purge_rejects_invalid_hash_and_disabled_or_incomplete_client(self):
        probe, service = self.purge_probe()
        self.assertFalse(probe.plan_purge("not-a-hash", "/data/show/a.mkv", 5).complete)
        for invalid in (downloader_service(enabled=False), downloader_service(error=True)):
            probe.helper = Helper({"qb": service, "unavailable": invalid})
            self.assertFalse(probe.plan_purge("a" * 40, "/data/show/a.mkv", 5).complete)
        self.assertEqual(service.instance.deleted, [])

    def test_purge_300_unrelated_task_summaries_do_not_fetch_their_files(self):
        probe, service = self.purge_probe()
        service.instance.tasks += [dict(hash=f"{index:040x}", content_path=f"/unrelated/{index}/b.mkv",
                                       save_path="/unrelated") for index in range(1, 302)]
        plan = probe.plan_purge("a" * 40, "/data/show/a.mkv", 5)
        self.assertTrue(plan.complete)
        self.assertEqual(service.instance.file_calls, ["a" * 40])

    def test_purge_unknown_summary_and_too_many_related_file_queries_are_retained(self):
        probe, service = self.purge_probe()
        service.instance.tasks.append(dict(hash="b" * 40))
        self.assertFalse(probe.plan_purge("a" * 40, "/data/show/a.mkv", 5).complete)
        probe, service = self.purge_probe()
        service.instance.tasks = [dict(hash=f"{index:040x}", content_path="/data/show", save_path="/data")
                                  for index in range(1, 34)]
        service.instance.files = [dict(name="show/other.mkv", size=3, progress=1)]
        self.assertFalse(probe.plan_purge("a" * 40, "/data/show/a.mkv", 5).complete)
        self.assertLessEqual(len(service.instance.file_calls), 32)
    def test_queries_all_tasks_without_tags_or_status_filter(self):
        service = downloader_service()
        probe = adapter.DownloaderProbe(Helper({"qb": service}))
        self.assertEqual(probe.check("a" * 40, ("/data/a.mkv",)), (True, False))
        self.assertEqual(service.instance.calls, [dict(ids=None, status=None, tags=None)])

    def test_seeding_error_or_unlabelled_matching_task_protects_record(self):
        row = dict(hash="a" * 40, content_path="/data/a.mkv", state="error", tags="")
        probe = adapter.DownloaderProbe(Helper({"qb": downloader_service([row])}))
        self.assertEqual(probe.check("a" * 40, ("/data/a.mkv",)), (True, True))

    def test_another_hash_using_exact_same_file_is_protected(self):
        row = dict(hash="b" * 40, content_path="/data/show", save_path="/data")
        service = downloader_service([row])
        service.instance.files = [dict(name="show/a.mkv")]
        probe = adapter.DownloaderProbe(Helper({"qb": service}))
        self.assertEqual(probe.check("a" * 40, ("/data/show/a.mkv",)), (True, True))

    def test_shared_torrent_root_does_not_protect_another_episode(self):
        row = dict(hash="b" * 40, content_path="/data/show", save_path="/data")
        service = downloader_service([row])
        service.instance.files = [dict(name="show/e13.mkv")]
        probe = adapter.DownloaderProbe(Helper({"qb": service}))
        self.assertEqual(probe.check("a" * 40, ("/data/show/e06.mkv",)), (True, False))

    def test_shared_root_with_unavailable_files_remains_unknown(self):
        row = dict(hash="b" * 40, content_path="/data/show", save_path="/data")
        probe = adapter.DownloaderProbe(Helper({"qb": downloader_service([row])}))
        self.assertEqual(probe.check("a" * 40, ("/data/show/e06.mkv",)), (False, None))

    def test_native_qb_userlist_tasks_and_files_are_supported(self):
        service = downloader_service()
        service.instance.tasks = UserList([dict(hash="b" * 40, content_path="/data/show", save_path="/data")])
        service.instance.files = UserList([dict(name="show/e13.mkv")])
        probe = adapter.DownloaderProbe(Helper({"qb": service}))
        self.assertEqual(probe.check("a" * 40, ("/data/show/e06.mkv",)), (True, False))

    def test_disabled_failed_and_malformed_instances_are_incomplete(self):
        for service in (downloader_service(enabled=False), downloader_service(error=True),
                        downloader_service([dict(hash="b" * 40)]),
                        downloader_service(kind="unsupported")):
            with self.subTest(service=service):
                result = adapter.DownloaderProbe(Helper({"qb": service})).check("a" * 40, ("/data/a",))
                self.assertFalse(result[0])

    def test_inventory_limit_does_not_silently_truncate(self):
        rows = [dict(hash=str(i) * 40, content_path="/other/" + str(i)) for i in range(2)]
        probe = adapter.DownloaderProbe(Helper({"qb": downloader_service(rows)}), max_tasks=1)
        self.assertEqual(probe.check("a" * 40, ("/data/a",)), (False, None))


class EmbyTests(unittest.TestCase):
    def probe(self, response):
        self.requests = []
        def read(_config, path, params):
            self.requests.append((path, params))
            return response(path, params)
        service = SimpleNamespace(config=SimpleNamespace(enabled=True, type="emby", config={}))
        return adapter.EmbyExactProbe(Helper({"emby": service}), read_json=read, page_size=2, max_items=6)

    def test_explicit_complete_zero_series_is_missing(self):
        probe = self.probe(lambda *_: {"Items": [], "TotalRecordCount": 0})
        self.assertEqual(probe.check(candidate()), (P.MISSING, True))
        self.assertEqual(self.requests[0][0], "Items")
        self.assertEqual(self.requests[0][1]["AnyProviderIdEquals"], "tmdb.206484")

    def test_exact_episode_with_path_is_present(self):
        def response(_path, params):
            if params["IncludeItemTypes"] == "Series":
                return {"Items": [dict(Id="series", Type="Series", ProviderIds={"Tmdb": "206484"})], "TotalRecordCount": 1}
            return {"Items": [dict(Id="episode", Type="Episode", SeriesId="series", ParentIndexNumber=4,
                                   IndexNumber=4, Path="/media/a.mkv")], "TotalRecordCount": 1}
        self.assertEqual(self.probe(response).check(candidate()), (P.PRESENT, True))

    def test_complete_other_episodes_do_not_count_as_requested_episode(self):
        def response(_path, params):
            if params["IncludeItemTypes"] == "Series":
                return {"Items": [dict(Id="series", Type="Series", ProviderIds={"Tmdb": "206484"})], "TotalRecordCount": 1}
            return {"Items": [dict(Id="e5", Type="Episode", SeriesId="series", ParentIndexNumber=4,
                                   IndexNumber=5, Path="/media/e5.mkv")], "TotalRecordCount": 1}
        self.assertEqual(self.probe(response).check(candidate()), (P.MISSING, True))

    def test_missing_count_wrong_identity_and_truncated_pages_are_unknown(self):
        invalid = [dict(Items=[]), dict(Items=[], TotalRecordCount=1),
                   dict(Items=[dict(Id="s", Type="Series", ProviderIds={"Tmdb": "other"})], TotalRecordCount=1)]
        for payload in invalid:
            with self.subTest(payload=payload):
                self.assertEqual(self.probe(lambda *_, p=payload: p).check(candidate()), (P.UNKNOWN, False))

    def test_episode_missing_identity_fields_is_unknown(self):
        def response(_path, params):
            if params["IncludeItemTypes"] == "Series":
                return dict(Items=[dict(Id="s", Type="Series", ProviderIds={"Tmdb": "206484"})], TotalRecordCount=1)
            return dict(Items=[dict(Id="e", Type="Episode", IndexNumber=4)], TotalRecordCount=1)
        self.assertEqual(self.probe(response).check(candidate()), (P.UNKNOWN, False))

    def test_no_configured_server_is_unknown(self):
        self.assertEqual(adapter.EmbyExactProbe(Helper({})).check(candidate()), (P.UNKNOWN, False))

    def test_movie_uses_exact_provider_and_accepts_complete_absence(self):
        movie = candidate(media_type="电影", season=None, episodes=())
        probe = self.probe(lambda *_: dict(Items=[], TotalRecordCount=0))
        self.assertEqual(probe.check(movie), (P.MISSING, True))
        self.assertEqual(self.requests[0][1]["IncludeItemTypes"], "Movie")
        payload = dict(Items=[dict(Id="m", Type="Movie", ProviderIds={"Tmdb": "206484"})], TotalRecordCount=1)
        self.assertEqual(self.probe(lambda *_: payload).check(movie), (P.PRESENT, True))
        payload["Items"][0]["ProviderIds"]["Tmdb"] = "other"
        self.assertEqual(self.probe(lambda *_: payload).check(movie), (P.UNKNOWN, False))

    def test_source_purge_does_not_infer_deletion_from_missing_series_mapping_or_movie_id(self):
        for item in (candidate(), candidate(media_type="电影", season=None, episodes=())):
            probe = self.probe(lambda *_: dict(Items=[], TotalRecordCount=0))
            self.assertEqual(probe.check(item), (P.MISSING, True))
            self.assertEqual(probe.check_purge(item), (P.UNKNOWN, False))

    def test_source_purge_requires_exact_series_and_remaining_actual_episodes_in_same_season(self):
        def response(_path, params):
            if params["IncludeItemTypes"] == "Series":
                return dict(Items=[dict(Id="s", Type="Series", ProviderIds={"Tmdb": "206484"})], TotalRecordCount=1)
            return dict(Items=[dict(Id="e", Type="Episode", SeriesId="s", ParentIndexNumber=4,
                                   IndexNumber=5, Path="/media/e5.mkv")], TotalRecordCount=1)
        probe = self.probe(response)
        self.assertEqual(probe.check_purge(candidate()), (P.MISSING, True))
        self.assertEqual(probe.check_purge(candidate(season=3)), (P.UNKNOWN, False))

    def test_complete_pagination_checks_later_matching_episode(self):
        def response(_path, params):
            if params["IncludeItemTypes"] == "Series":
                return dict(Items=[dict(Id="s", Type="Series", ProviderIds={"Tmdb": "206484"})], TotalRecordCount=1)
            episodes = [dict(Id="e" + str(i), Type="Episode", SeriesId="s", ParentIndexNumber=4,
                             IndexNumber=i, Path="/media/e.mkv") for i in (1, 2, 4)]
            start = params["StartIndex"]
            return dict(Items=episodes[start:start + params["Limit"]], TotalRecordCount=3)
        self.assertEqual(self.probe(response).check(candidate()), (P.PRESENT, True))
        self.assertEqual([params["StartIndex"] for _, params in self.requests], [0, 0, 2])
        self.assertEqual(self.probe(response).season_episodes(candidate()), ((1, 2, 4), True))

    def test_duplicate_page_or_changed_total_is_unknown(self):
        for change_total in (False, True):
            def response(_path, params):
                if params["IncludeItemTypes"] == "Series":
                    return dict(Items=[dict(Id="s", Type="Series", ProviderIds={"Tmdb": "206484"})], TotalRecordCount=1)
                page = [dict(Id="e" + str(i), Type="Episode", SeriesId="s", ParentIndexNumber=4,
                             IndexNumber=i, Path="/media/e.mkv") for i in (1, 2)]
                return dict(Items=page, TotalRecordCount=4 if change_total and params["StartIndex"] else 3)
            with self.subTest(change_total=change_total):
                self.assertEqual(self.probe(response).check(candidate()), (P.UNKNOWN, False))


class EmbyTransportTests(unittest.TestCase):
    def test_transport_confines_auth_to_header_and_closes_response(self):
        calls = {}
        class Response:
            status_code = 200
            def iter_content(self, chunk_size):
                yield b'{"Items":[],"TotalRecordCount":0}'
        class Context:
            def __enter__(self):
                return Response()
            def __exit__(self, *_):
                calls["closed"] = True
        class Reader:
            def __init__(self, **kwargs):
                calls["options"] = kwargs
            def get_stream(self, url, **kwargs):
                calls["url"], calls["request"] = url, kwargs
                return Context()
        module = ModuleType("app.adapters.network.http")
        module.RequestUtils = Reader
        service = SimpleNamespace(instance=SimpleNamespace(_host="http://configured.example/", _apikey="fake-secret"))
        with patch.dict(sys.modules, {"app.adapters.network.http": module}):
            result = adapter._emby_json(service, "Items", {"Limit": 1})
        self.assertEqual(result, {"Items": [], "TotalRecordCount": 0})
        self.assertEqual(calls["url"], "http://configured.example/emby/Items")
        self.assertEqual(calls["options"]["headers"]["X-Emby-Token"], "fake-secret")
        self.assertFalse(calls["request"]["allow_redirects"])
        self.assertLessEqual(calls["options"]["timeout"], 10)
        self.assertTrue(calls["closed"])

    def test_unconfigured_or_credential_bearing_origin_is_rejected(self):
        module = ModuleType("app.adapters.network.http")
        module.RequestUtils = lambda **_: self.fail("must not request an invalid origin")
        with patch.dict(sys.modules, {"app.adapters.network.http": module}):
            for host in ("http://user:password@example/", "http://example/?token=secret", "file:///tmp/media"):
                service = SimpleNamespace(instance=SimpleNamespace(_host=host, _apikey="fake"))
                with self.subTest(host=host), self.assertRaises(ValueError):
                    adapter._emby_json(service, "Items", {})


class Gateway:
    def __init__(self):
        self.candidate = candidate()
        self.applied = []
        self.ids = [7, 9, 12]
        self.retry_options = []
        self.resolved_counts = []

    def list_cleanup_candidates(self, after_id=0, limit=100):
        return [candidate(file_id=i) for i in self.ids if i > after_id][:limit]

    def get_cleanup_candidate(self, file_id):
        return self.candidate if file_id == 7 else None

    def get_cleanup_protection(self, current, limit=500):
        return SimpleNamespace(other_version_paths=(), other_versions_complete=True, transfer_task_active=False)

    def resolve_lack(self, current, present_episodes):
        return 12

    def apply(self, current, evidence, other_paths, *, allow_subscription_retry=False, resolved_lack_episode=None):
        self.applied.append((current.file_id, current.revision))
        self.retry_options.append(allow_subscription_retry)
        self.resolved_counts.append(resolved_lack_episode)
        return SimpleNamespace(status="applied", reason="cleaned")


class AdapterTests(unittest.TestCase):
    def build(self):
        self.gateway = Gateway()
        paths = SimpleNamespace(check=lambda path: adapter.PathObservation(P.MISSING, True))
        downloads = SimpleNamespace(check=lambda *_: (True, False))
        emby = SimpleNamespace(check=lambda *_: (P.MISSING, True), season_episodes=lambda *_: ((1, 2, 3), True))
        emby.check_purge = lambda current: emby.check(current)
        return adapter.MoviePilotCleanupAdapter(self.gateway, paths, downloads, emby)

    def test_cursor_visits_all_records_and_rewinds_at_end(self):
        subject = self.build()
        first = subject.list_candidates(cursor=0, limit=2)
        self.assertEqual([r.record_id for r in first.records], [7, 9])
        second = subject.list_candidates(cursor=first.cursor, limit=2)
        self.assertEqual([r.record_id for r in second.records], [12])
        self.assertEqual(second.cursor, 0)

    def test_changed_revision_is_not_reinterpreted_as_same_missing_record(self):
        subject = self.build()
        record = subject.list_candidates(limit=1).records[0]
        self.gateway.candidate.revision = "rev-2"
        evidence = subject.observe(record)
        self.assertEqual(evidence.revision, "rev-2")
        self.assertFalse(evidence.identity_verified)

    def test_active_task_and_new_file_prevent_mutation_even_after_old_proof(self):
        subject = self.build()
        record = subject.list_candidates(limit=1).records[0]
        proof = subject.observe(record)
        subject.paths = SimpleNamespace(check=lambda _: adapter.PathObservation(P.PRESENT, True))
        result = subject.mutate(record, proof, kernel.CleanupOptions())
        self.assertEqual(result.status, "needs_review")
        self.assertEqual(self.gateway.applied, [])

    def test_mutation_submits_current_stable_id_revision_and_never_retry_count(self):
        subject = self.build()
        record = subject.list_candidates(limit=1).records[0]
        result = subject.mutate(record, subject.observe(record), kernel.CleanupOptions())
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.gateway.applied, [(7, "rev-1")])

    def test_existing_source_or_target_skips_external_reads_only_during_observation(self):
        for present_path in ("/data/source/a.mkv", "/data/library/a.mkv"):
            subject = self.build()
            record = subject.list_candidates(limit=1).records[0]
            subject.paths.check = lambda path: adapter.PathObservation(P.PRESENT if path == present_path else P.MISSING, True)
            with patch.object(subject.downloaders, "check", side_effect=AssertionError("must skip downloaders")), \
                 patch.object(subject.media, "check", side_effect=AssertionError("must skip media")):
                evidence = subject.observe(record)
            self.assertEqual(kernel._decision(record, evidence)[0], "skipped")
            self.assertFalse(evidence.downloaders_complete)

    def test_unknown_local_path_never_requests_external_absence_or_becomes_eligible(self):
        subject = self.build()
        record = subject.list_candidates(limit=1).records[0]
        subject.paths.check = lambda _: adapter.PathObservation(P.UNKNOWN, False)
        with patch.object(subject.downloaders, "check", side_effect=AssertionError("skip unknown storage")):
            evidence = subject.observe(record)
        self.assertEqual(kernel._decision(record, evidence)[0], "needs_review")

    def test_full_mutation_rechecks_external_evidence_after_fast_observation(self):
        subject = self.build()
        record = subject.list_candidates(limit=1).records[0]
        subject.paths.check = lambda _: adapter.PathObservation(P.PRESENT, True)
        evidence = subject.observe(record)
        with patch.object(subject.downloaders, "check", return_value=(True, False)) as check:
            self.assertEqual(subject.mutate(record, evidence, kernel.CleanupOptions()).status, "needs_review")
        check.assert_called_once()
        self.assertEqual(self.gateway.applied, [])

    def test_webhook_is_only_a_bounded_recheck_hint(self):
        subject = self.build()
        self.assertTrue(subject.webhook_hint({"Event": "library.itemremoved"}))
        self.assertEqual(self.gateway.applied, [])
        self.assertFalse(subject.webhook_hint({"Event": "playback.start"}))

    def test_legacy_retry_option_never_reopens_subscription(self):
        subject = self.build()
        record = subject.list_candidates(limit=1).records[0]
        subject.mutate(record, subject.observe(record), kernel.CleanupOptions())
        subject.mutate(record, subject.observe(record), kernel.CleanupOptions(allow_subscription_retry=True))
        self.assertEqual(self.gateway.retry_options, [False, False])
        self.assertEqual(self.gateway.resolved_counts, [None, None])

    def test_subscription_season_lookup_is_never_needed_for_record_cleanup(self):
        subject = self.build()
        record = subject.list_candidates(limit=1).records[0]
        subject.media.season_episodes = lambda *_: ((), False)
        result = subject.mutate(record, subject.observe(record), kernel.CleanupOptions(allow_subscription_retry=True))
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.gateway.retry_options, [False])

    def test_retry_option_without_subscription_cleans_records_without_resolving_progress(self):
        subject = self.build()
        self.gateway.candidate.subscribe_id = None
        self.gateway.candidate.subscription_retry_needed = False
        subject.media.season_episodes = lambda *_: self.fail("no subscription to resolve")
        record = subject.list_candidates(limit=1).records[0]
        result = subject.mutate(record, subject.observe(record), kernel.CleanupOptions(allow_subscription_retry=True))
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.gateway.retry_options, [False])
        self.assertEqual(self.gateway.resolved_counts, [None])

    def test_existing_subscription_without_target_note_cleans_without_rewriting_progress(self):
        subject = self.build()
        self.gateway.candidate.subscription_retry_needed = False
        subject.media.season_episodes = lambda *_: self.fail("no episode to reopen")
        record = subject.list_candidates(limit=1).records[0]
        result = subject.mutate(record, subject.observe(record), kernel.CleanupOptions(allow_subscription_retry=True))
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.gateway.retry_options, [False])
        self.assertEqual(self.gateway.resolved_counts, [None])

    def test_missing_sibling_version_cleans_current_before_retrying_subscription(self):
        subject = self.build()
        self.gateway.get_cleanup_protection = lambda *_args, **_kwargs: SimpleNamespace(
            other_version_paths=("/data/source/sibling.mkv", "/data/library/sibling.mkv"),
            other_versions_complete=True, transfer_task_active=False)
        subject.media.season_episodes = lambda *_: self.fail("wait for final version")
        record = subject.list_candidates(limit=1).records[0]
        result = subject.mutate(record, subject.observe(record), kernel.CleanupOptions(allow_subscription_retry=True))
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.gateway.retry_options, [False])
        self.assertEqual(self.gateway.resolved_counts, [None])

    def test_unknown_subscription_retry_state_is_irrelevant_when_never_written(self):
        subject = self.build()
        del self.gateway.candidate.subscription_retry_needed
        record = subject.list_candidates(limit=1).records[0]
        result = subject.mutate(record, subject.observe(record), kernel.CleanupOptions(allow_subscription_retry=True))
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.gateway.retry_options, [False])

    def destination_subject(self):
        subject = self.build()
        self.gateway.repairs = []
        def repair(current, evidence, other_paths):
            self.gateway.repairs.append((current.file_id, current.revision, current.transfer_id))
            return kernel.MutationResult("applied", "host_confirmed")
        self.gateway.repair = repair
        subject.paths.check = lambda path: adapter.PathObservation(
            P.PRESENT if path == self.gateway.candidate.source_path else P.MISSING, True)
        subject.downloaders.check = lambda *_: (True, True)
        record = subject.list_candidates(limit=1).records[0]
        return subject, record

    def test_destination_repair_keeps_source_and_seeding_tasks_and_uses_separate_command(self):
        subject, record = self.destination_subject()
        evidence = subject.observe_destination(record)
        self.assertEqual(evidence.source, P.PRESENT)
        self.assertTrue(evidence.downloader_task_present)
        result = subject.repair_destination(record, evidence, kernel.CleanupOptions())
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.gateway.repairs, [(7, "rev-1", 5)])
        self.assertEqual(self.gateway.applied, [])

    def test_present_destination_skips_external_reads_without_relaxing_repair(self):
        subject, record = self.destination_subject()
        subject.paths.check = lambda _: adapter.PathObservation(P.PRESENT, True)
        with patch.object(subject.downloaders, "check", side_effect=AssertionError("must skip downloaders")):
            evidence = subject.observe_destination(record)
        self.assertEqual(kernel.destination_decision(record, evidence)[0], "skipped")
        with patch.object(subject.downloaders, "check", return_value=(True, True)) as check:
            self.assertEqual(subject.repair_destination(record, evidence).status, "needs_review")
        check.assert_called_once()
        self.assertEqual(self.gateway.repairs, [])

    def test_destination_repair_requires_unique_legacy_target_and_transfer_id(self):
        for field, value in (("transfer_id", None), ("transfer_id", 0),
                             ("destination_paths", ()),
                             ("destination_paths", ("/data/a", "/data/b"))):
            with self.subTest(field=field, value=value):
                subject, _record = self.destination_subject()
                setattr(self.gateway.candidate, field, value)
                record = adapter._record(self.gateway.candidate)
                self.assertFalse(subject.observe_destination(record).identity_verified)
                self.assertEqual(subject.repair_destination(record).status, "needs_review")
                self.assertEqual(self.gateway.repairs, [])

    def test_destination_observation_without_exact_transfer_skips_external_reads(self):
        subject, _record = self.destination_subject()
        self.gateway.candidate.transfer_id = None
        self.gateway.candidate.destination_paths = ()
        record = adapter._record(self.gateway.candidate)
        with patch.object(subject.downloaders, "check", side_effect=AssertionError("no repair target")):
            evidence = subject.observe_destination(record)
        self.assertFalse(evidence.identity_verified)
        self.assertEqual(evidence.source, P.PRESENT)

    def test_destination_repair_rechecks_source_target_media_and_task_health(self):
        changes = (
            lambda s: setattr(s.paths, "check", lambda _: adapter.PathObservation(P.MISSING, True)),
            lambda s: setattr(s.paths, "check", lambda _: adapter.PathObservation(P.PRESENT, True)),
            lambda s: setattr(s.media, "check", lambda _: (P.PRESENT, True)),
            lambda s: setattr(s.media, "check", lambda _: (P.UNKNOWN, False)),
            lambda s: setattr(s.downloaders, "check", lambda *_: (False, None)),
            lambda s: setattr(self.gateway, "get_cleanup_protection", lambda *_args, **_kwargs:
                              SimpleNamespace(other_version_paths=(), other_versions_complete=True,
                                              transfer_task_active=True)),
        )
        for change in changes:
            subject, record = self.destination_subject()
            old = subject.observe_destination(record)
            change(subject)
            self.assertEqual(subject.repair_destination(record, old).status, "needs_review")
            self.assertEqual(self.gateway.repairs, [])

    def test_destination_repair_rejects_changed_revision_and_generic_success_dict(self):
        subject, record = self.destination_subject()
        self.gateway.candidate.revision = "new"
        self.assertEqual(subject.repair_destination(record).status, "needs_review")
        self.assertEqual(self.gateway.repairs, [])
        subject, record = self.destination_subject()
        self.gateway.repair = lambda *_: {"success": True}
        self.assertEqual(subject.repair_destination(record).status, "needs_review")

    def test_legacy_subscription_snapshot_is_not_queried_or_written(self):
        subject = self.build()
        record = subject.list_candidates(limit=1).records[0]
        subject.media.season_episodes = lambda *_: ((4,), True)
        result = subject.mutate(record, subject.observe(record), kernel.CleanupOptions(allow_subscription_retry=True))
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.gateway.retry_options, [False])

    def test_empty_destination_is_only_eligible_with_explicit_lookup_completion(self):
        subject = self.build()
        self.gateway.candidate.destination_paths = ()
        self.gateway.list_cleanup_candidates = lambda **_: [self.gateway.candidate]
        record = subject.list_candidates(limit=1).records[0]
        self.gateway.candidate.destination_lookup_complete = False
        result = subject.mutate(record, subject.observe(record), kernel.CleanupOptions())
        self.assertEqual(result.status, "needs_review")
        self.assertEqual(self.gateway.applied, [])

    def test_host_proof_includes_presence_and_maps_committed_cleaned_result(self):
        subject = self.build()
        record = subject.list_candidates(limit=1).records[0]
        evidence = subject.observe(record)
        bridge = adapter.MoviePilotGateway.__new__(adapter.MoviePilotGateway)
        def proof_type(**values):
            self.assertIs(values["record_present"], True)
            self.assertEqual(values["source_path"], "/data/source/a.mkv")
            self.assertEqual(values["other_version_paths"], ())
            return SimpleNamespace(**values)
        bridge.proof_type = proof_type
        bridge.module = SimpleNamespace(apply_cleanup_candidate=lambda *args, **kwargs: SimpleNamespace(
            status="cleaned", reason="", file_marked_missing=True))
        result = bridge.apply(candidate(), evidence, ())
        self.assertEqual(result.status, "applied")

    def test_destination_gateway_uses_precise_command_and_requires_transfer_deleted(self):
        subject, record = self.destination_subject()
        evidence = subject.observe_destination(record)
        bridge = adapter.MoviePilotGateway.__new__(adapter.MoviePilotGateway)
        bridge.proof_type = lambda **values: SimpleNamespace(**values)
        calls = []
        def repair(current, proof):
            calls.append((current.transfer_id, proof.source, proof.destination_paths, proof.record_present))
            return SimpleNamespace(status="cleaned", transfer_deleted=True)
        bridge.module = SimpleNamespace(repair_missing_destination=repair)
        self.assertEqual(bridge.repair(candidate(), evidence, ()).status, "applied")
        self.assertEqual(calls, [(5, "present", ("/data/library/a.mkv",), True)])
        bridge.module.repair_missing_destination = lambda *_: SimpleNamespace(status="cleaned", transfer_deleted=False)
        self.assertEqual(bridge.repair(candidate(), evidence, ()).status, "needs_review")


class SourcePurgeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source/a.mkv"
        self.source.parent.mkdir()
        self.source.write_bytes(b"media")
        self.target = self.root / "library/a.mkv"
        trusted = adapter.TrustedRoot(str(self.root), self.root.stat().st_dev, str(self.root), "test")
        self.paths = adapter.LocalPathProbe([trusted], mount_check=lambda _: True)
        self.gateway = Gateway()
        self.gateway.candidate = candidate(source_path=str(self.source), destination_paths=(str(self.target),),
                                           subscribe_id=None, subscription_retry_needed=False)
        self.ownership = True
        self.gateway.get_cleanup_protection = lambda current, limit=500, include_source_ownership=False: SimpleNamespace(
            other_version_paths=(), other_versions_complete=True, transfer_task_active=False,
            source_exclusive=self.ownership and include_source_ownership,
            hash_exclusive=self.ownership and include_source_ownership)
        self.service = downloader_service([dict(hash="a" * 40, content_path=str(self.source), save_path=str(self.root), state="uploading")])
        self.service.instance.files = [dict(name="source/a.mkv", size=5, progress=1.0)]
        self.downloaders = adapter.DownloaderProbe(Helper({"qb": self.service}))
        self.media = SimpleNamespace(check=lambda _: (P.MISSING, True))
        self.media.check_purge = lambda current: self.media.check(current)
        self.subject = adapter.MoviePilotCleanupAdapter(self.gateway, self.paths, self.downloaders, self.media)
        self.record = adapter._record(self.gateway.candidate)
        self.options = kernel.CleanupOptions(preserve_source_tasks=False, allow_subscription_retry=False)

    def proof(self):
        result = self.subject.observe_destination(self.record)
        self.assertEqual(len(result.source_signature), 7)
        return result

    def test_purge_removes_only_exclusive_task_then_exact_source_then_host_history(self):
        proof = self.proof()
        original = self.gateway.apply
        def apply(*args, **kwargs):
            self.assertFalse(self.source.exists())
            self.assertEqual(self.service.instance.tasks, [])
            self.assertFalse(kwargs["allow_subscription_retry"])
            return original(*args, **kwargs)
        self.gateway.apply = apply
        result = self.subject.purge_source(self.record, proof, self.options)
        self.assertEqual(result, kernel.MutationResult("applied", "source_purged"))
        self.assertEqual(self.service.instance.deleted, [(False, ("a" * 40,))])
        self.assertEqual(self.gateway.applied, [(7, "rev-1")])
        self.assertTrue(self.source.parent.exists())

    def test_purge_requires_explicit_authorization_and_full_ownership(self):
        proof = self.proof()
        self.assertEqual(self.subject.purge_source(self.record, proof, kernel.CleanupOptions()).status, "needs_review")
        self.ownership = False
        self.assertEqual(self.subject.purge_source(self.record, proof, self.options).status, "needs_review")
        self.assertTrue(self.source.exists())
        self.assertEqual(self.service.instance.deleted, [])
        self.assertEqual(self.gateway.applied, [])

    def test_purge_rejects_replaced_source_and_shared_hardlink_before_removing_tasks(self):
        proof = self.proof()
        self.source.write_bytes(b"changed")
        self.assertEqual(self.subject.purge_source(self.record, proof, self.options).status, "needs_review")
        proof = self.proof()
        os.link(self.source, self.root / "shared.mkv")
        self.assertEqual(self.subject.purge_source(self.record, proof, self.options).status, "needs_review")
        self.assertEqual(self.service.instance.deleted, [])
        self.assertEqual(self.gateway.applied, [])

    def test_purge_stops_on_task_delete_failure_without_unlinking_or_cleaning_history(self):
        proof = self.proof()
        self.service.instance.delete_success = False
        self.assertEqual(self.subject.purge_source(self.record, proof, self.options).status, "needs_review")
        self.assertTrue(self.source.exists())
        self.assertEqual(self.gateway.applied, [])

    def test_purge_rechecks_emby_after_task_removal_and_keeps_source_on_reappearance(self):
        proof = self.proof()
        self.media.check = lambda _: (P.PRESENT if self.service.instance.deleted else P.MISSING, True)
        self.assertEqual(self.subject.purge_source(self.record, proof, self.options).status, "needs_review")
        self.assertTrue(self.source.exists())
        self.assertEqual(self.gateway.applied, [])

    def test_purge_does_not_clear_history_if_exact_unlink_fails(self):
        proof = self.proof()
        with patch.object(self.paths, "unlink_exact", return_value=False):
            result = self.subject.purge_source(self.record, proof, self.options)
        self.assertEqual(result.status, "needs_review")
        self.assertTrue(self.source.exists())
        self.assertEqual(self.gateway.applied, [])

    def test_database_failure_after_physical_removal_is_reported_for_later_full_missing_retry(self):
        proof = self.proof()
        self.gateway.apply = lambda *_args, **_kwargs: kernel.MutationResult("needs_review", "cas_changed")
        result = self.subject.purge_source(self.record, proof, self.options)
        self.assertEqual(result.status, "needs_review")
        self.assertEqual(result.reason, "source_purged_history_pending")
        self.assertFalse(self.source.exists())

    def test_partial_task_removal_retries_safely_without_duplicate_first_client_delete(self):
        tb = downloader_service([dict(hash="a" * 40, content_path=str(self.source), save_path=str(self.root), status="seeding")], kind="transmission")
        tb.instance.files = [SimpleNamespace(name="source/a.mkv", size=5, completed=5)]
        tb.instance.delete_success = False
        self.downloaders.helper = Helper({"qb": self.service, "tb": tb})
        proof = self.proof()
        self.assertEqual(self.subject.purge_source(self.record, proof, self.options).status, "needs_review")
        self.assertEqual(len(self.service.instance.deleted), 1)
        self.assertTrue(self.source.exists())
        self.assertEqual(self.gateway.applied, [])
        tb.instance.delete_success = True
        self.assertEqual(self.subject.purge_source(self.record, proof, self.options).status, "applied")
        self.assertEqual(len(self.service.instance.deleted), 1)
        self.assertFalse(self.source.exists())

    def test_source_change_during_inventory_is_detected_before_any_task_removal(self):
        proof = self.proof()
        original = self.downloaders.plan_purge
        def inventory(*args):
            result = original(*args)
            self.source.write_bytes(b"newer")
            os.utime(self.source, ns=(proof.source_signature[4] + 1000000000,) * 2)
            return result
        with patch.object(self.downloaders, "plan_purge", side_effect=inventory):
            result = self.subject.purge_source(self.record, proof, self.options)
        self.assertEqual(result.status, "needs_review")
        self.assertEqual(self.source.read_bytes(), b"newer")
        self.assertEqual(self.service.instance.deleted, [])

    def test_source_change_after_task_removal_is_retained_without_history_cleanup(self):
        proof = self.proof()
        original = self.service.instance.delete_torrents
        def remove(**kwargs):
            result = original(**kwargs)
            self.source.write_bytes(b"newer")
            os.utime(self.source, ns=(proof.source_signature[4] + 1000000000,) * 2)
            return result
        self.service.instance.delete_torrents = remove
        result = self.subject.purge_source(self.record, proof, self.options)
        self.assertEqual(result.status, "needs_review")
        self.assertEqual(self.source.read_bytes(), b"newer")
        self.assertEqual(self.gateway.applied, [])

    def test_plugin_local_facade_maps_actual_proof_and_exact_delete_result(self):
        core = load("host_cleanup")
        current = core.CleanupCandidate(**dict(vars(self.gateway.candidate), history_id=1))
        owner = self
        class Port:
            def get_candidate(self, file_id):
                return current if file_id == current.file_id else None

            def get_protection(self, candidate, limit, include_source_ownership=False):
                return core.CleanupProtection((), True, False, include_source_ownership, include_source_ownership)

            def apply(self, candidate, proof, *, allow_subscription_retry=False, resolved_lack_episode=None):
                owner.assertIsInstance(proof, core.CleanupProof)
                owner.assertTrue(core.proof_allows_cleanup(proof, revision=current.revision))
                owner.assertFalse(owner.source.exists())
                owner.assertFalse(allow_subscription_retry)
                return core.CleanupResult("cleaned", file_marked_missing=True, history_deleted=True, transfer_deleted=True)

        core.configure_cleanup_service(Port())
        self.addCleanup(core.reset_cleanup_service)
        self.subject.gateway = adapter.MoviePilotGateway()
        proof = self.proof()
        self.assertEqual(self.subject.purge_source(self.record, proof, self.options).status, "applied")


class FileOnlyAdapterTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(callable(getattr(adapter.MoviePilotCleanupAdapter, "list_file_only_candidates", None)))
        self.current = SimpleNamespace(file_id=7, revision="file-rev", download_hash="a" * 40,
                                       downloader="qb", source_path="/data/source/a.mkv", storage="local",
                                       identity_verified=True, review_reason="")
        self.applied = []
        self.rows = [self.current]
        def apply(current, proof):
            self.applied.append((current, proof))
            return kernel.MutationResult("applied", "file_record_removed")
        self.gateway = SimpleNamespace(
            list_file_only_candidates=lambda after_id=0, limit=100: [row for row in self.rows if row.file_id > after_id][:limit],
            get_file_only_candidate=lambda file_id: self.current if file_id == 7 else None,
            get_file_only_protection=lambda _: SimpleNamespace(transfer_task_active=False),
            apply_file_only=apply,
        )
        self.checked_paths = []
        self.task_queries = []
        def check_path(path):
            self.checked_paths.append(path)
            return adapter.PathObservation(P.MISSING, True)
        def check_tasks(download_hash, paths):
            self.task_queries.append((download_hash, paths))
            return True, False
        # A file-only action cannot access library or subscription capabilities.
        self.subject = adapter.MoviePilotCleanupAdapter(
            self.gateway, SimpleNamespace(check=check_path), SimpleNamespace(check=check_tasks), object())

    def item(self):
        page = self.subject.list_file_only_candidates(limit=1)
        self.assertTrue(page.complete)
        return page.records[0]

    def test_missing_pack_file_needs_no_media_identity_or_library_request(self):
        item = self.item()
        self.assertEqual(item.kind, "download_file_only")
        evidence = self.subject.observe_file_only(item)
        self.assertIsNone(kernel.file_only_decision(item, evidence))
        result = self.subject.mutate_file_only(item, evidence, kernel.CleanupOptions(allow_subscription_retry=True))
        self.assertEqual(result.status, "applied")
        self.assertEqual(len(self.applied), 1)
        self.assertEqual(self.applied[0][0].file_id, 7)
        self.assertEqual(self.applied[0][1].revision, "file-rev")
        self.assertEqual(set(self.checked_paths), {"/data/source/a.mkv"})
        self.assertEqual(set(self.task_queries), {("a" * 40, ("/data/source/a.mkv",))})

    def test_file_cursor_is_bounded_ordered_and_rotates(self):
        self.rows = [SimpleNamespace(**{**vars(self.current), "file_id": value}) for value in (7, 9, 12)]
        first = self.subject.list_file_only_candidates(limit=2)
        self.assertEqual([row.record_id for row in first.records], [7, 9])
        second = self.subject.list_file_only_candidates(cursor=first.cursor, limit=2)
        self.assertEqual([row.record_id for row in second.records], [12])
        self.assertEqual(second.cursor, 0)
        self.rows.reverse()
        bad = self.subject.list_file_only_candidates(limit=2)
        self.assertFalse(bad.complete)
        self.assertEqual(bad.cursor, 0)

    def test_changed_revision_missing_health_or_active_task_never_mutates(self):
        item = self.item()
        proof = self.subject.observe_file_only(item)
        self.current.revision = "changed"
        self.assertEqual(self.subject.mutate_file_only(item, proof, kernel.CleanupOptions()).status, "needs_review")
        self.current.revision = "file-rev"
        for task_result in ((False, None), (True, True)):
            self.subject.downloaders.check = lambda *_: task_result
            self.assertEqual(self.subject.mutate_file_only(item, proof, kernel.CleanupOptions()).status, "needs_review")
        self.subject.downloaders.check = lambda *_: (True, False)
        self.subject.paths.check = lambda _: adapter.PathObservation(P.MISSING, False)
        self.assertEqual(self.subject.mutate_file_only(item, proof, kernel.CleanupOptions()).status, "needs_review")
        self.assertEqual(self.applied, [])

    def test_source_present_or_unknown_skips_remote_queries(self):
        item = self.item()
        for presence in (P.PRESENT, P.UNKNOWN):
            self.subject.paths.check = lambda _: adapter.PathObservation(presence, presence is P.PRESENT)
            evidence = self.subject.observe_file_only(item)
            self.assertEqual(evidence.source, presence)
            self.assertFalse(evidence.downloaders_complete)
        self.assertEqual(self.task_queries, [])

    def test_source_reappearing_during_task_inventory_prevents_file_prune(self):
        item = self.item()
        proof = self.subject.observe_file_only(item)
        def tasks(*_):
            self.subject.paths.check = lambda _: adapter.PathObservation(P.PRESENT, True)
            return True, False
        self.subject.downloaders.check = tasks
        self.assertEqual(self.subject.mutate_file_only(item, proof, kernel.CleanupOptions()).status, "needs_review")
        self.assertEqual(self.applied, [])

    def test_transfer_lease_and_cross_kind_record_are_rejected(self):
        item = self.item()
        self.gateway.get_file_only_protection = lambda _: SimpleNamespace(transfer_task_active=True)
        proof = self.subject.observe_file_only(item)
        self.assertEqual(self.subject.mutate_file_only(item, proof, kernel.CleanupOptions()).status, "needs_review")
        self.assertEqual(self.subject.mutate_file_only(replace(item, kind="download_file"), proof,
                                                      kernel.CleanupOptions()).status, "needs_review")
        self.assertEqual(self.applied, [])

    def test_database_failure_is_not_reported_as_a_completed_prune(self):
        item = self.item()
        self.gateway.apply_file_only = lambda *_: kernel.MutationResult("needs_review", "stale")
        result = self.subject.mutate_file_only(item, self.subject.observe_file_only(item), kernel.CleanupOptions())
        self.assertEqual(result.status, "needs_review")


class ReconciliationBatchTests(unittest.TestCase):
    def build(self):
        self.assertTrue(callable(getattr(adapter.MoviePilotCleanupAdapter, "reconcile_batch", None)))
        self.current = SimpleNamespace(file_id=7, revision="file-rev", download_hash="a" * 40,
            downloader="qb", source_path="/data/source/a.mkv", storage="local", identity_verified=True, review_reason="")
        self.receipt = dict(version=1, key="group", revision="group-rev", complete=True, equivalent_parents=True,
            downloader="qb", download_hash="a" * 40,
            files=[dict(id=7, fullpath=self.current.source_path, downloader="qb", download_hash="a" * 40, state=1)],
            histories=[dict(id=1)], transfers=[dict(id=5, src=self.current.source_path, dest="/data/library/a.mkv")])
        self.present = True
        self.library_present = True
        self.removed = []
        self.persisted = []
        def prune(*_):
            self.assertTrue(self.persisted[-1]["cleanup_v2"]["receipts"])
            self.present = False
            self.removed.append("file")
            return kernel.MutationResult("applied", "file_record_removed", {"download_files": 1})
        def history(receipt, proof):
            self.removed.append("history")
            return kernel.MutationResult("applied", "related_history_removed", {"download_history": 1, "transfer_history": 1})
        gateway = SimpleNamespace(
            list_file_only_candidates=lambda after_id=0, limit=100: [self.current] if self.present and after_id < 7 else [],
            get_file_only_candidate=lambda _: self.current if self.present else None,
            get_file_only_protection=lambda _: SimpleNamespace(transfer_task_active=False),
            apply_file_only=prune, prepare_receipt=lambda _: self.receipt,
            receipt_view=lambda _: dict(valid=True, revision="live", files=[self.receipt["files"][0]] if self.present else [],
                histories=self.receipt["histories"], transfers=self.receipt["transfers"], pending=False, done=False),
            apply_receipt=history, source_ownership=lambda *_: True)
        paths = SimpleNamespace(check=lambda path: adapter.PathObservation(
            P.PRESENT if "/library/" in path and self.library_present else P.MISSING, True))
        self.subject = adapter.MoviePilotCleanupAdapter(gateway, paths,
            SimpleNamespace(check=lambda *_: (True, False)), object())
        return self.subject

    def run_batch(self, state, now, **kwargs):
        from copy import deepcopy
        def persist(snapshot):
            self.persisted.append(deepcopy(snapshot))
        return self.subject.reconcile_batch(state, persist=persist, now=now, dry_run=False,
            options=kernel.CleanupOptions(stability_seconds=0), **kwargs)

    def test_file_prune_keeps_durable_parent_entry_until_library_later_disappears(self):
        self.build()
        first = self.run_batch({}, 100)
        second = self.run_batch(first.state, 101)
        self.assertEqual(self.removed, ["file"])
        self.assertEqual(second.report["counts"]["download_files"], 1)
        self.assertTrue(second.state["cleanup_v2"]["receipts"])
        self.library_present = False
        third = self.run_batch(second.state, 102)
        self.assertNotIn("history", self.removed)
        fourth = self.run_batch(third.state, 103)
        self.assertEqual(self.removed, ["file", "history"])
        self.assertEqual(fourth.report["counts"]["download_history"], 1)
        self.assertFalse(fourth.state["cleanup_v2"]["receipts"])

    def test_failed_receipt_store_prevents_any_database_mutation(self):
        self.build()
        first = self.run_batch({}, 100)
        def fail(_):
            raise OSError("unavailable")
        result = self.subject.reconcile_batch(first.state, persist=fail, now=101, dry_run=False,
            options=kernel.CleanupOptions(stability_seconds=0))
        self.assertEqual(result.report["status"], "failed")
        self.assertEqual(self.removed, [])

    def test_long_lived_receipts_are_independent_of_observation_pool_and_preserve_legacy(self):
        self.build()
        from copy import deepcopy
        receipts = {str(index): dict(self.receipt, serial=index + 1) for index in range(2200)}
        state = {"version": 1, "legacy": {"keep": True}, "purged_source": {"keep": {"phase": "intent"}},
                 "cleanup_v2": {"version": 2, "receipts": receipts, "next_serial": 2201}}
        result = self.run_batch(deepcopy(state), 10_000_000)
        self.assertGreaterEqual(len(result.state["cleanup_v2"]["receipts"]), 2200)
        self.assertEqual(result.state["legacy"], state["legacy"])
        self.assertEqual(result.state["purged_source"], state["purged_source"])

    def test_source_budget_is_persistent_and_report_is_bounded(self):
        self.build()
        self.present = False
        self.receipt["files"] = [dict(id=index + 1, fullpath=f"/data/source/{index}.mkv") for index in range(350)]
        self.receipt.update(serial=1, source_cursor=0)
        state = {"cleanup_v2": {"version": 2, "receipts": {"group": self.receipt}}}
        first = self.run_batch(state, 100)
        self.assertEqual(first.state["cleanup_v2"]["receipts"]["group"]["source_cursor"], 100)
        self.assertFalse(first.report["scan"]["complete"])
        second = self.run_batch(first.state, 101)
        self.assertEqual(second.state["cleanup_v2"]["receipts"]["group"]["source_cursor"], 200)
        self.assertLessEqual(len(second.report["items"]), 400)

    def test_actual_path_budget_yields_and_resumes_without_repeating_finished_sources(self):
        self.build()
        self.present = False
        self.receipt["files"] = [dict(id=index + 1, fullpath=f"/data/source/{index}.mkv") for index in range(10)]
        self.receipt.update(serial=1, source_cursor=0)
        state = {"cleanup_v2": {"version": 2, "receipts": {"group": self.receipt}}}
        import importlib
        recon = importlib.import_module(PACKAGE + ".reconcile")
        original = self.subject.paths.check
        calls = []
        def probe(path):
            calls.append(path)
            return original(path)
        self.subject.paths.check = probe
        saw_yield = False
        with patch.object(recon, "MAX_PATH_PROBES", 70):
            for now in range(100, 106):
                before = len(calls)
                result = self.run_batch(state, now)
                self.assertLessEqual(len(calls) - before, 70)
                self.assertEqual(len(calls) - before, result.report["budget"]["path_probes"])
                saw_yield |= result.report["status"] == "yielded"
                state = result.state
                if result.report["scan"]["complete"]:
                    break
        self.assertTrue(saw_yield)
        self.assertTrue(result.report["scan"]["complete"])

    def test_runtime_budget_is_not_swallowed_by_observer_failure(self):
        self.build()
        import importlib
        recon = importlib.import_module(PACKAGE + ".reconcile")
        clock = [0]
        probes = []
        original = self.subject.paths.check
        def slow(path):
            probes.append(path)
            clock[0] = 100
            return original(path)
        self.subject.paths.check = slow
        with patch.object(recon.time, "monotonic", side_effect=lambda: clock[0]):
            result = self.run_batch({}, 100)
        self.assertEqual(result.report["status"], "yielded")
        self.assertEqual(result.report["error"], "batch_time_limit")
        self.assertEqual(len(probes), 1)
        self.assertEqual(self.removed, [])

    def test_file_cursor_advances_finished_prefix_when_budget_interrupts_a_page(self):
        self.build()
        import importlib
        recon = importlib.import_module(PACKAGE + ".reconcile")
        rows = {value: SimpleNamespace(**{**vars(self.current), "file_id": value, "source_path": f"/data/source/{value}.mkv"})
                for value in (7, 9, 12)}
        self.subject.gateway.list_file_only_candidates = lambda after_id=0, limit=100: [row for value, row in rows.items() if value > after_id][:limit]
        self.subject.gateway.get_file_only_candidate = lambda value: rows.get(value)
        self.receipt["files"] = [dict(id=value, fullpath=row.source_path) for value, row in rows.items()]
        clock, paths = [0], []
        def probe(path):
            paths.append(path)
            clock[0] += 3
            return adapter.PathObservation(P.MISSING, True)
        self.subject.paths.check = probe
        with patch.object(recon.time, "monotonic", side_effect=lambda: clock[0]), patch.object(recon, "MAX_BATCH_SECONDS", 5):
            first = self.run_batch({}, 100)
            self.assertEqual(first.report["status"], "yielded")
            self.assertEqual(first.state["cleanup_v2"]["file_cursor"], 7)
            self.assertEqual(first.report["processed"], 1)
            clock[0] = 0
            paths.clear()
            second = self.run_batch(first.state, 101)
            self.assertEqual(paths[0], "/data/source/9.mkv")
            self.assertEqual(second.state["cleanup_v2"]["file_cursor"], 9)


class ReconciliationSourceTests(unittest.TestCase):
    def setUp(self):
        from copy import deepcopy
        import hashlib
        import json
        self.fixture = SourcePurgeTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        f = self.fixture
        self.present = True
        self.history_present = True
        self.fail_child = False
        file = dict(id=7, downloader="qb", download_hash="a" * 40, fullpath=str(f.source),
                    savepath=str(f.root), filepath="source/a.mkv", torrentname="fixture", state=1)
        revision = hashlib.sha256(json.dumps([file], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        self.child = SimpleNamespace(file_id=7, downloader="qb", download_hash="a" * 40, source_path=str(f.source),
                                     revision=revision, storage="local", identity_verified=True, review_reason="")
        self.receipt = dict(version=1, key="group", revision="group-rev", complete=True, equivalent_parents=True,
            downloader="qb", download_hash="a" * 40, files=[file], histories=[dict(id=1)],
            transfers=[dict(id=5, src=str(f.source), dest=str(f.target))])
        def prune(*_):
            if self.fail_child:
                return kernel.MutationResult("needs_review", "database_failed")
            self.assertFalse(f.source.exists())
            self.present = False
            return kernel.MutationResult("applied", "file_record_removed", {"download_files": 1})
        def history(*_):
            self.history_present = False
            return kernel.MutationResult("applied", "related_history_removed", {"download_history": 1, "transfer_history": 1})
        gateway = SimpleNamespace(
            list_file_only_candidates=lambda after_id=0, limit=100: [self.child] if self.present and after_id < 7 else [],
            get_file_only_candidate=lambda _: self.child if self.present else None,
            get_file_only_protection=lambda _: SimpleNamespace(transfer_task_active=False),
            prepare_receipt=lambda _: deepcopy(self.receipt), apply_file_only=prune,
            receipt_view=lambda _: dict(valid=True, revision="live", files=[file] if self.present else [],
                histories=[dict(id=1)] if self.history_present else [],
                transfers=self.receipt["transfers"] if self.history_present else [],
                pending=False, mappings_safe=True, done=not self.present and not self.history_present),
            source_ownership=lambda *_: True, apply_receipt=history)
        self.subject = adapter.MoviePilotCleanupAdapter(gateway, f.paths, f.downloaders, object())
        self.states = []

    def run_batch(self, state, now, **kwargs):
        from copy import deepcopy
        def persist(value):
            self.states.append(deepcopy(value))
        return self.subject.reconcile_batch(state, persist=persist, now=now, dry_run=False,
            options=kernel.CleanupOptions(stability_seconds=0, allow_subscription_retry=True), **kwargs)

    def test_source_cleanup_is_independent_of_emby_and_preserves_another_copy(self):
        keep = self.fixture.root / "another-version.mkv"
        keep.write_bytes(b"another copy")
        first = self.run_batch({}, 100)
        self.assertTrue(self.fixture.source.exists())
        second = self.run_batch(first.state, 101)
        self.assertFalse(self.fixture.source.exists())
        self.assertEqual(keep.read_bytes(), b"another copy")
        self.assertEqual(second.report["counts"]["source_files"], 1)
        self.assertEqual(second.report["counts"]["tasks"], 1)
        self.assertEqual(second.report["counts"]["download_files"], 1)
        self.assertFalse(self.present)
        self.assertTrue(second.state["cleanup_v2"]["receipts"])

    def test_details_retain_specific_lifecycle_blocker(self):
        self.fixture.service.instance.tasks[0]["state"] = "checkingUP"
        first = self.run_batch({}, 100)
        second = self.run_batch(first.state, 101)
        reasons = [item["reason"] for item in second.report["items"] if item["branch"] == "source_purge"]
        self.assertEqual(reasons, ["task_checking"])
        self.assertTrue(self.fixture.source.exists())
        self.assertEqual(self.fixture.service.instance.deleted, [])

    def test_source_delete_database_failure_recovers_from_persisted_receipt(self):
        first = self.run_batch({}, 100)
        self.fail_child = True
        second = self.run_batch(first.state, 101)
        self.assertFalse(self.fixture.source.exists())
        self.assertTrue(self.present)
        self.assertEqual(second.report["counts"]["source_files"], 1)
        receipt = next(iter(second.state["cleanup_v2"]["receipts"].values()))
        self.assertEqual(receipt["physical_intents"][str(self.fixture.source)]["phase"], "source_deleted")
        self.fail_child = False
        third = self.run_batch(second.state, 102)
        fourth = self.run_batch(third.state, 103)
        self.assertFalse(self.present)
        self.assertEqual(len(self.fixture.service.instance.deleted), 1)

    def test_interrupted_source_intent_never_authorizes_a_replacement_file(self):
        first = self.run_batch({}, 100)
        self.fixture.service.instance.delete_success = False
        second = self.run_batch(first.state, 101)
        self.fixture.source.write_bytes(b"replacement")
        self.fixture.service.instance.files[0]["size"] = len(b"replacement")
        self.fixture.service.instance.delete_success = True
        third = self.run_batch(second.state, 102)
        fourth = self.run_batch(third.state, 103)
        self.assertEqual(self.fixture.source.read_bytes(), b"replacement")
        self.assertIn("source_intent_revision_changed", [item["reason"] for item in fourth.report["items"]])

    def test_host_adapter_build_does_not_import_or_construct_a_media_server(self):
        modules = {name: ModuleType(name) for name in ("app", "app.application", "app.application.directory", "app.application.downloader")}
        modules["app.application.directory"].DirectoryHelper = lambda: SimpleNamespace(get_dirs=lambda: [])
        modules["app.application.downloader"].DownloaderHelper = lambda: Helper({"qb": downloader_service()})
        with patch.dict(sys.modules, modules):
            subject = adapter.build_host_adapter()
        self.assertIsInstance(subject, adapter.MoviePilotCleanupAdapter)

    def test_hardlink_or_cancellation_after_task_command_preserves_source(self):
        link = self.fixture.root / "keep-link.mkv"
        os.link(self.fixture.source, link)
        first = self.run_batch({}, 100)
        second = self.run_batch(first.state, 101)
        self.assertTrue(self.fixture.source.exists())
        self.assertEqual(self.fixture.service.instance.deleted, [])
        link.unlink()
        third = self.run_batch(second.state, 102)
        cancelled = [False]
        original = self.fixture.service.instance.delete_torrents
        def remove(**kwargs):
            result = original(**kwargs)
            cancelled[0] = True
            return result
        self.fixture.service.instance.delete_torrents = remove
        fourth = self.run_batch(third.state, 103, stop_requested=lambda: cancelled[0])
        self.assertEqual(fourth.report["status"], "cancelled")
        self.assertTrue(self.fixture.source.exists())
        self.assertTrue(self.present)


if __name__ == "__main__":
    unittest.main()
