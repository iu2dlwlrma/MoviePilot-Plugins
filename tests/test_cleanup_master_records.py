"""Safety contracts for records-only cleanup, without the MoviePilot runtime."""

from dataclasses import replace
import importlib.util
import json
from pathlib import Path
import sys
from threading import Event, Thread
import unittest


SOURCE = Path(__file__).resolve().parents[1] / "plugins.v2/emptyfoldercleaner/record_cleanup.py"
SPEC = importlib.util.spec_from_file_location("cleanup_records_under_test", SOURCE)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
CleanupRecord = MODULE.CleanupRecord
CleanupEvidence = MODULE.CleanupEvidence
CleanupOptions = MODULE.CleanupOptions
RecordCleanupEngine = MODULE.RecordCleanupEngine
MutationResult = MODULE.MutationResult
Presence = MODULE.Presence
destination_decision = MODULE.destination_decision

START = 1_000_000.0
DAY = 86_400.0
SIGNATURE = (1, 100, 33188, 1024, 900, 900, 1)


def record(**changes):
    value = CleanupRecord(
        key="download_file:17:path-fingerprint", revision="revision-1",
        source_path="/downloads/show/S01E04.mkv",
        destination_paths=("/library/show/Season 1/S01E04.mkv",),
        media_source="themoviedb", media_id="123", media_type="电视剧",
        season=1, episodes=(4,), record_id=17,
    )
    return replace(value, **changes)


def absent(**changes):
    value = CleanupEvidence(
        source=Presence.MISSING, destinations=(Presence.MISSING,),
        other_versions=Presence.MISSING, media_library=Presence.MISSING,
        destination_lookup_complete=True, media_lookup_complete=True, downloaders_complete=True,
        downloader_task_present=False, transfer_task_active=False,
        storage_ready=True, identity_verified=True, revision="revision-1",
    )
    return replace(value, **changes)


class RecordCleanupTests(unittest.TestCase):
    def test_options_cannot_disable_stability_or_remove_resource_bounds(self):
        for changes in (
            {"stability_seconds": 0}, {"stability_seconds": DAY - 1},
            {"stability_seconds": float("nan")}, {"batch_size": 0},
            {"batch_size": 101}, {"pending_limit": 2001},
            {"preserve_source_tasks": "false"},
        ):
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):
                    CleanupOptions(**changes)

    def test_default_dry_run_requires_a_full_day_then_reports_without_mutating(self):
        calls = []
        engine = RecordCleanupEngine(lambda _: absent(), lambda *args: calls.append(args))
        first = engine.run([record()], {}, now=START)
        self.assertEqual(first.results[0].status, "waiting")
        early = engine.run([record()], first.state, now=START + DAY - 1)
        self.assertEqual(early.results[0].status, "waiting")
        mature = engine.run([record()], early.state, now=START + DAY)
        self.assertEqual(mature.results[0].status, "eligible")
        self.assertEqual(calls, [])
        self.assertEqual(first.state["candidates"][record().key]["first_seen"], START)

    def test_any_actual_copy_or_existing_media_item_prevents_cleanup(self):
        for changes in (
            {"source": Presence.PRESENT},
            {"destinations": (Presence.PRESENT,)},
            {"other_versions": Presence.PRESENT},
            {"media_library": Presence.PRESENT},
        ):
            with self.subTest(changes=changes):
                result = RecordCleanupEngine(lambda _: absent(**changes)).run([record()], {}, now=START)
                self.assertEqual(result.results[0].status, "skipped")
                self.assertEqual(result.state["candidates"], {})

    def test_unavailable_io_or_incomplete_lookups_never_count_as_absence(self):
        for changes in (
            {"source": Presence.UNKNOWN}, {"source": False},
            {"destinations": (Presence.UNKNOWN,)}, {"destinations": ()},
            {"other_versions": Presence.UNKNOWN}, {"media_library": Presence.UNKNOWN},
            {"destination_lookup_complete": False}, {"media_lookup_complete": False}, {"downloaders_complete": False},
            {"downloader_task_present": None}, {"transfer_task_active": None},
            {"storage_ready": False}, {"identity_verified": False},
            {"record_present": None}, {"revision": "changed"},
        ):
            with self.subTest(changes=changes):
                result = RecordCleanupEngine(lambda _: absent(**changes)).run([record()], {}, now=START)
                self.assertEqual(result.results[0].status, "needs_review")
                self.assertEqual(result.state["candidates"], {})

    def test_remote_storage_and_ambiguous_episode_are_not_candidates(self):
        for changes in (
            {"source_path": "relative/file.mkv"},
            {"source_path": "/downloads/../media/file.mkv"},
            {"storage": "cloud"}, {"episodes": ()}, {"episodes": (True,)},
            {"season": None}, {"media_id": ""}, {"revision": ""},
            {"record_id": None},
        ):
            with self.subTest(changes=changes):
                result = RecordCleanupEngine(lambda _: absent()).run([record(**changes)], {}, now=START)
                self.assertEqual(result.results[0].status, "needs_review")

    def test_never_transferred_file_requires_proof_of_complete_empty_target_lookup(self):
        never_transferred = record(destination_paths=())
        for evidence in (
            absent(destinations=(), destination_lookup_complete=False),
            absent(destinations=(), media_lookup_complete=False),
            absent(destinations=(), media_library=Presence.UNKNOWN),
            absent(destinations=(), other_versions=Presence.UNKNOWN),
        ):
            with self.subTest(evidence=evidence):
                result = RecordCleanupEngine(lambda _: evidence).run([never_transferred], {}, now=START)
                self.assertEqual(result.results[0].status, "needs_review")
        engine = RecordCleanupEngine(lambda _: absent(destinations=()))
        first = engine.run([never_transferred], {}, now=START)
        mature = engine.run([never_transferred], first.state, now=START + DAY)
        self.assertEqual(first.results[0].status, "waiting")
        self.assertEqual(mature.results[0].status, "eligible")

    def test_seeding_paused_or_active_task_protects_even_when_user_allows_source_cleanup(self):
        options = CleanupOptions(preserve_source_tasks=False, allow_subscription_retry=True)
        for changes in ({"downloader_task_present": True}, {"transfer_task_active": True}):
            with self.subTest(changes=changes):
                engine = RecordCleanupEngine(lambda _: absent(**changes))
                result = engine.run([record()], {}, now=START, dry_run=False, options=options)
                self.assertEqual(result.results[0].status, "skipped")

    def test_reappearance_or_io_outage_resets_entire_observation_period(self):
        current = [absent()]
        engine = RecordCleanupEngine(lambda _: current[0])
        first = engine.run([record()], {}, now=START)
        for interrupt in (absent(source=Presence.PRESENT), absent(storage_ready=False)):
            current[0] = interrupt
            reset = engine.run([record()], first.state, now=START + DAY)
            current[0] = absent()
            again = engine.run([record()], reset.state, now=START + DAY + 1)
            self.assertEqual(again.results[0].status, "waiting")
            self.assertEqual(again.state["candidates"][record().key]["first_seen"], START + DAY + 1)

    def test_record_revision_change_and_backwards_clock_restart_observation(self):
        engine = RecordCleanupEngine(lambda r: absent(revision=r.revision))
        first = engine.run([record()], {}, now=START)
        changed = engine.run([record(revision="revision-2")], first.state, now=START + DAY)
        self.assertEqual(changed.results[0].status, "waiting")
        backwards = engine.run([record()], first.state, now=START - 1)
        self.assertEqual(backwards.results[0].status, "waiting")
        self.assertEqual(backwards.state["candidates"][record().key]["first_seen"], START - 1)

    def test_no_mutation_adapter_means_review_not_applied(self):
        engine = RecordCleanupEngine(lambda _: absent())
        first = engine.run([record()], {}, now=START)
        result = engine.run([record()], first.state, now=START + DAY, dry_run=False)
        self.assertEqual(result.results[0].status, "needs_review")
        self.assertEqual(result.results[0].reason, "mutation_adapter_unavailable")

    def test_apply_rechecks_live_evidence_and_never_mutates_a_restored_file(self):
        observed = iter((absent(), absent(), absent(destinations=(Presence.PRESENT,))))
        calls = []
        engine = RecordCleanupEngine(lambda _: next(observed), lambda *args: calls.append(args))
        first = engine.run([record()], {}, now=START)
        result = engine.run([record()], first.state, now=START + DAY, dry_run=False)
        self.assertEqual(result.results[0].status, "skipped")
        self.assertEqual(calls, [])
        self.assertEqual(result.state["candidates"], {})

    def test_recheck_revision_change_prevents_commit(self):
        observed = iter((absent(), absent(), absent(revision="revision-2")))
        calls = []
        engine = RecordCleanupEngine(lambda _: next(observed), lambda *args: calls.append(args))
        first = engine.run([record()], {}, now=START)
        result = engine.run([record()], first.state, now=START + DAY, dry_run=False)
        self.assertEqual(result.results[0].status, "needs_review")
        self.assertEqual(calls, [])

    def test_applied_record_is_idempotent_with_persisted_state_and_duplicate_inputs(self):
        calls = []

        def mutate(item, evidence, options):
            calls.append((item.key, evidence.revision, options.preserve_source_tasks,
                          options.allow_subscription_retry))
            return MutationResult(status="applied", reason="records_updated")

        engine = RecordCleanupEngine(lambda _: absent(), mutate)
        first = engine.run([record()], {}, now=START)
        result = engine.run([record(), record()], first.state, now=START + DAY, dry_run=False)
        self.assertEqual([r.status for r in result.results], ["applied", "already_clean"])
        self.assertEqual(calls, [(record().key, "revision-1", True, False)])
        restored_state = json.loads(json.dumps(result.state))
        reopened = RecordCleanupEngine(lambda _: absent(), mutate)
        repeated = reopened.run([record()], restored_state, now=START + 2 * DAY, dry_run=False)
        self.assertEqual(repeated.results[0].status, "already_clean")
        self.assertEqual(len(calls), 1)

    def test_missing_host_record_is_already_clean_without_any_mutation(self):
        engine = RecordCleanupEngine(lambda _: absent(record_present=False))
        result = engine.run([record()], {}, now=START, dry_run=False)
        self.assertEqual(result.results[0].status, "already_clean")

    def test_observer_and_mutation_exceptions_are_redacted_and_retain_records(self):
        def fail(*_):
            raise PermissionError("https://example.invalid/?secret=do-not-report")

        failed_observe = RecordCleanupEngine(fail).run([record()], {}, now=START)
        self.assertEqual(failed_observe.results[0].status, "needs_review")
        self.assertNotIn("secret", repr(failed_observe))
        engine = RecordCleanupEngine(lambda _: absent(), fail)
        first = engine.run([record()], {}, now=START)
        failed_apply = engine.run([record()], first.state, now=START + DAY, dry_run=False)
        self.assertEqual(failed_apply.results[0].status, "needs_review")
        self.assertNotIn("secret", repr(failed_apply))
        self.assertEqual(failed_apply.state["completed"], {})

    def test_unconfirmed_host_result_does_not_turn_http_success_into_cleanup_success(self):
        for outcome in (None, True, {"success": True}, MutationResult("needs_review", "active_lease")):
            with self.subTest(outcome=outcome):
                engine = RecordCleanupEngine(lambda _: absent(), lambda *_: outcome)
                first = engine.run([record()], {}, now=START)
                result = engine.run([record()], first.state, now=START + DAY, dry_run=False)
                self.assertEqual(result.results[0].status, "needs_review")
                self.assertEqual(result.state["completed"], {})

    def test_concurrent_engine_instances_are_nonblocking_and_do_not_consume_input(self):
        entered, release = Event(), Event()
        outputs = []

        def observe(_):
            entered.set()
            self.assertTrue(release.wait(3))
            return absent()

        worker = Thread(target=lambda: outputs.append(
            RecordCleanupEngine(observe).run([record()], {}, now=START)))
        worker.start()
        self.addCleanup(lambda: release.set())
        try:
            self.assertTrue(entered.wait(3))

            def forbidden_input():
                raise AssertionError("busy run consumed the candidate source")
                yield record()

            busy = RecordCleanupEngine(lambda _: absent()).run(forbidden_input(), {}, now=START)
            self.assertTrue(busy.busy)
            self.assertEqual(busy.results, ())
        finally:
            release.set()
            worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(outputs), 1)

    def test_batch_bound_limits_probes_and_does_not_scan_the_whole_candidate_source(self):
        produced, observed = [], []

        def records():
            for i in range(20):
                produced.append(i)
                yield record(key=f"file:{i}")

        def observe(item):
            observed.append(item.key)
            return absent()

        result = RecordCleanupEngine(observe).run(
            records(), {}, now=START, options=CleanupOptions(batch_size=2, pending_limit=2))
        self.assertEqual(observed, ["file:0", "file:1"])
        self.assertLessEqual(len(produced), 3)
        self.assertEqual(len(result.state["candidates"]), 2)
        self.assertTrue(result.truncated)

    def test_pending_capacity_does_not_allow_untracked_immediate_apply(self):
        engine = RecordCleanupEngine(lambda _: absent())
        options = CleanupOptions(batch_size=1, pending_limit=1)
        first = engine.run([record(key="first")], {}, now=START, options=options)
        second = engine.run([record(key="second")], first.state, now=START + DAY, options=options)
        self.assertEqual(second.results[0].status, "waiting")
        self.assertEqual(set(second.state["candidates"]), {"first"})
        self.assertEqual(second.results[0].reason, "candidate_capacity_wait")
        verified = engine.run([record(key="first")], second.state, now=START + DAY + 1, options=options)
        admitted = engine.run([record(key="second")], verified.state, now=START + DAY + 2, options=options)
        self.assertEqual(set(admitted.state["candidates"]), {"second"})
        verified_second = engine.run([record(key="second")], admitted.state,
                                     now=START + 2 * DAY + 2, options=options)
        returned = engine.run([record(key="first")], verified_second.state,
                              now=START + 2 * DAY + 3, options=options)
        self.assertEqual(returned.results[0].status, "waiting")
        self.assertEqual(returned.results[0].first_seen, START + 2 * DAY + 3)

    def test_malformed_persisted_timestamps_cannot_bypass_observation(self):
        engine = RecordCleanupEngine(lambda _: absent())
        first = engine.run([record()], {}, now=START)
        for invalid in (float("nan"), float("inf"), -1, "yesterday", True):
            state = json.loads(json.dumps(first.state))
            state["candidates"][record().key]["first_seen"] = invalid
            result = engine.run([record()], state, now=START + DAY)
            self.assertEqual(result.results[0].status, "waiting")


class DestinationRecordTests(unittest.TestCase):
    def test_surviving_source_requires_stable_signature_before_controlled_purge(self):
        calls = []
        candidate = record(key=record().key + ":destination")
        evidence = absent(source=Presence.PRESENT, downloader_task_present=True, source_signature=SIGNATURE)
        engine = RecordCleanupEngine(
            lambda _: evidence,
            lambda *args: (calls.append(args), MutationResult("applied", "host_confirmed"))[1],
            decision=destination_decision,
        )
        options = CleanupOptions(preserve_source_tasks=False)
        first = engine.run([candidate], {}, now=START, dry_run=False, options=options)
        self.assertEqual(first.results[0].status, "waiting")
        self.assertEqual(calls, [])
        mature = engine.run([candidate], first.state, now=START + DAY, dry_run=False, options=options)
        self.assertEqual(mature.results[0].status, "applied")
        self.assertEqual(len(calls), 1)

    def test_source_purge_rejects_absent_or_malformed_signature(self):
        for signature in ((), None, (1, 2), (1, 0, 33188, 1024, 1, 1, 1),
                          (1, 1, 33188, -1, 1, 1, 1), (1, 1, 33188, 1024, 1, 1, False)):
            with self.subTest(signature=signature):
                evidence = absent(source=Presence.PRESENT, source_signature=signature)
                result = RecordCleanupEngine(lambda _: evidence, decision=destination_decision).run(
                    [record()], {}, now=START)
                self.assertEqual(result.results[0].reason, "source_signature_unavailable")
                self.assertEqual(result.state["candidates"], {})

    def test_source_replacement_restarts_day_and_json_signature_roundtrip_is_stable(self):
        current = [absent(source=Presence.PRESENT, source_signature=SIGNATURE)]
        calls = []
        engine = RecordCleanupEngine(lambda _: current[0], lambda *args: calls.append(args),
                                     decision=destination_decision)
        first = engine.run([record()], {}, now=START)
        changed = list(SIGNATURE)
        changed[1] += 1
        current[0] = absent(source=Presence.PRESENT, source_signature=changed)
        replaced = engine.run([record()], json.loads(json.dumps(first.state)), now=START + DAY,
                              dry_run=False)
        self.assertEqual(replaced.results[0].status, "waiting")
        self.assertEqual(replaced.results[0].first_seen, START + DAY)
        current[0] = absent(source=Presence.PRESENT, source_signature=tuple(changed))
        stable = engine.run([record()], replaced.state, now=START + 2 * DAY)
        self.assertEqual(stable.results[0].status, "eligible")
        self.assertEqual(calls, [])

    def test_completed_source_purge_does_not_mask_a_recreated_file(self):
        current = [absent(source=Presence.PRESENT, source_signature=SIGNATURE)]
        calls = []
        def mutate(*args):
            calls.append(args)
            return MutationResult("applied", "source_purged")
        engine = RecordCleanupEngine(lambda _: current[0], mutate, decision=destination_decision)
        first = engine.run([record()], {}, now=START)
        applied = engine.run([record()], first.state, now=START + DAY, dry_run=False)
        self.assertEqual(applied.results[0].status, "applied")
        repeated = engine.run([record()], applied.state, now=START + DAY + 1, dry_run=False)
        self.assertEqual(repeated.results[0].status, "already_clean")
        current[0] = absent(source=Presence.PRESENT, source_signature=(1, 101, *SIGNATURE[2:]))
        recreated = engine.run([record()], repeated.state, now=START + DAY + 2, dry_run=False)
        self.assertEqual(recreated.results[0].status, "waiting")
        self.assertEqual(len(calls), 1)

    def test_source_replacement_during_final_observation_never_commits(self):
        changed = (*SIGNATURE[:3], 2048, *SIGNATURE[4:])
        observations = iter((
            absent(source=Presence.PRESENT, source_signature=SIGNATURE),
            absent(source=Presence.PRESENT, source_signature=SIGNATURE),
            absent(source=Presence.PRESENT, source_signature=changed),
        ))
        calls = []
        engine = RecordCleanupEngine(lambda _: next(observations), lambda *args: calls.append(args),
                                     decision=destination_decision)
        first = engine.run([record()], {}, now=START)
        result = engine.run([record()], first.state, now=START + DAY, dry_run=False)
        self.assertEqual(result.results[0].status, "waiting")
        self.assertEqual(result.results[0].reason, "source_changed")
        self.assertEqual(result.results[0].first_seen, START + DAY)
        self.assertEqual(calls, [])

    def test_existing_library_or_unknown_status_blocks_transfer_repair(self):
        candidate = record(key=record().key + ":destination")
        for evidence, status in (
            (absent(source=Presence.MISSING), "skipped"),
            (absent(source=Presence.PRESENT, destinations=(Presence.PRESENT,)), "skipped"),
            (absent(source=Presence.PRESENT, media_library=Presence.PRESENT), "skipped"),
            (absent(source=Presence.PRESENT, other_versions=Presence.PRESENT), "skipped"),
            (absent(source=Presence.PRESENT, media_library=Presence.UNKNOWN), "needs_review"),
            (absent(source=Presence.PRESENT, transfer_task_active=True), "skipped"),
            (absent(source=Presence.PRESENT, downloaders_complete=False), "needs_review"),
            (absent(source=Presence.PRESENT, storage_ready=False), "needs_review"),
        ):
            with self.subTest(evidence=evidence):
                result = RecordCleanupEngine(lambda _: evidence, decision=destination_decision).run(
                    [candidate], {}, now=START)
                self.assertEqual(result.results[0].status, status)

    def test_transfer_repair_requires_an_exact_destination(self):
        candidate = record(key=record().key + ":destination", destination_paths=())
        result = RecordCleanupEngine(
            lambda _: absent(source=Presence.PRESENT, destinations=()),
            decision=destination_decision,
        ).run([candidate], {}, now=START)
        self.assertEqual(result.results[0].reason, "no_transfer_destination")


class ObservationExpiryTests(unittest.TestCase):
    def test_over_2000_candidates_and_42_hour_page_cycle_keep_first_observations_until_revisited(self):
        options = CleanupOptions(batch_size=100, pending_limit=2000)
        engine = RecordCleanupEngine(lambda _: absent())
        records = [record(key=f"file:{i}", record_id=i + 1) for i in range(2100)]
        state = {}
        # Each page is two hours apart: the first complete cycle takes 42h.
        for page in range(21):
            run = engine.run(records[page * 100:(page + 1) * 100], state,
                             now=START + page * 7200, options=options)
            state = run.state
        self.assertEqual(set(state["candidates"]), {item.key for item in records[:2000]})
        self.assertTrue(all(result.reason == "candidate_capacity_wait" for result in run.results))
        self.assertTrue(all(value["observations"] == 1 for value in state["candidates"].values()))

        # The retained records get their second observation before overflow can
        # rotate them. The final page then starts its own independent window.
        eligible = set()
        for page in range(21):
            run = engine.run(records[page * 100:(page + 1) * 100], state,
                             now=START + (21 + page) * 7200, options=options)
            state = run.state
            eligible.update(result.key for result in run.results if result.status == "eligible")
        self.assertEqual(eligible, {item.key for item in records[:2000]})
        self.assertTrue(all(result.status == "waiting" for result in run.results))
        self.assertEqual(len(state["candidates"]), 2000)
        last_start = START + 41 * 7200
        for item in records[2000:]:
            self.assertEqual(state["candidates"][item.key]["first_seen"], last_start)
        final = engine.run(records[2000:], state, now=last_start + DAY, options=options)
        self.assertTrue(all(result.status == "eligible" for result in final.results))

    def test_two_early_observations_do_not_allow_rotation_without_eligible_report(self):
        options = CleanupOptions(batch_size=1, pending_limit=1)
        engine = RecordCleanupEngine(lambda _: absent())
        first = engine.run([record(key="first")], {}, now=START, options=options)
        early = engine.run([record(key="first")], first.state, now=START + 3600, options=options)
        incoming = engine.run([record(key="second")], early.state, now=START + DAY, options=options)
        self.assertEqual(set(incoming.state["candidates"]), {"first"})
        self.assertEqual(incoming.results[0].reason, "candidate_capacity_wait")

    def test_capacity_rotation_does_not_thrash_young_observations_or_starve_later_pages(self):
        options = CleanupOptions(batch_size=1, pending_limit=1)
        engine = RecordCleanupEngine(lambda _: absent())
        first = engine.run([record(key="first")], {}, now=START, options=options)
        early = engine.run([record(key="second")], first.state, now=START + 3600, options=options)
        self.assertEqual(set(early.state["candidates"]), {"first"})
        self.assertEqual(early.results[0].reason, "candidate_capacity_wait")
        mature = engine.run([record(key="first")], early.state, now=START + DAY, options=options)
        self.assertEqual(mature.results[0].status, "eligible")
        second = engine.run([record(key="second")], mature.state, now=START + DAY + 1, options=options)
        self.assertEqual(set(second.state["candidates"]), {"second"})
        self.assertEqual(second.results[0].status, "waiting")
        second_mature = engine.run([record(key="second")], second.state, now=START + 2 * DAY + 1, options=options)
        self.assertEqual(second_mature.results[0].status, "eligible")


    def test_removed_database_row_cannot_exhaust_candidate_capacity_forever(self):
        options = CleanupOptions(batch_size=1, pending_limit=1)
        engine = RecordCleanupEngine(lambda item: absent(revision=item.revision))
        old = engine.run([record()], {}, now=START, options=options)
        another = record(key="download_file:18:new", record_id=18)
        later = engine.run([another], old.state, now=START + MODULE.MAX_STALE_SECONDS + 1,
                           options=options)
        self.assertEqual(later.results[0].status, "waiting")
        self.assertNotIn(record().key, later.state["candidates"])
        self.assertEqual(later.state["candidates"][another.key]["first_seen"], START + MODULE.MAX_STALE_SECONDS + 1)


if __name__ == "__main__":
    unittest.main()
