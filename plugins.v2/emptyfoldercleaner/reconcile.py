"""Plugin-owned reconciliation with durable, exact association receipts.

Receipts have their own bounded ledger; ordinary library copies may keep a
receipt alive indefinitely. They never authorize physical deletion by themselves:
every mutation re-reads the host rows, task inventory and local paths.
"""
from copy import deepcopy
from dataclasses import dataclass, replace
import hashlib
import json
import math
import re
import time

from .record_cleanup import (CleanupEvidence, CleanupOptions, CleanupRecord, MutationResult,
                             Presence, RecordCleanupEngine, file_only_decision)
from .host_adapter import CandidatePage, PurgePlan, _exclusive_signature


MAX_RECEIPTS = 20000
MAX_LEDGER_BYTES = 64 * 1024 * 1024
COUNT_KEYS = ("download_files", "download_history", "transfer_history", "source_files", "tasks")
MAX_GROUP_PATHS = 1000
MAX_PATH_PROBES = 12000
MAX_HOST_CALLS = 1500
MAX_BATCH_SECONDS = 45


class BatchYield(RuntimeError):
    pass


@dataclass(frozen=True)
class ReconcileBatchResult:
    state: dict
    report: dict


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def _sources(receipt):
    return tuple(sorted({row["fullpath"] for row in receipt["files"]}
                        | {row["src"] for row in receipt["transfers"]}))


def _record(receipt, kind, source, destinations=()):
    return CleanupRecord(key=kind + ":" + str(receipt["serial"]) + ":" + _digest(source),
        revision=receipt["revision"], source_path=source, destination_paths=tuple(destinations),
        media_source="", media_id="", media_type="", record_id=receipt["serial"], kind=kind)


def _task_snapshot(plan):
    return [dict(service=task.service, download_hash=task.download_hash, path=task.path, size=task.size)
            for task in plan.tasks]


def _base_decision(record, evidence):
    reason = evidence.blocking_reason if re.fullmatch(r"[a-z][a-z0-9_]{0,79}", evidence.blocking_reason or "") else ""
    if evidence.record_present is False:
        return "already_clean", "related_records_absent"
    if evidence.record_present is not True or evidence.revision != record.revision:
        return "needs_review", reason or "new_or_changed_reference"
    if evidence.transfer_task_active is True:
        return "skipped", "transfer_task_active"
    if evidence.identity_verified is not True:
        return "needs_review", reason or "association_unverified"
    if evidence.storage_ready is not True:
        return "needs_review", "storage_unavailable"
    if evidence.downloaders_complete is not True or evidence.transfer_task_active is not False:
        return "needs_review", reason or "task_inventory_incomplete"
    return None


def _history_decision(record, evidence):
    base = _base_decision(record, evidence)
    if base:
        return base
    if evidence.source is Presence.PRESENT or Presence.PRESENT in evidence.destinations:
        return "skipped", "related_file_present"
    if evidence.source is not Presence.MISSING or any(item is not Presence.MISSING for item in evidence.destinations):
        return "needs_review", "presence_unknown"
    if evidence.downloader_task_present is not False:
        return "skipped", "downloader_task_present"
    return None


def _physical_decision(record, evidence):
    base = _base_decision(record, evidence)
    if base:
        return base
    if evidence.source is Presence.MISSING:
        return "skipped", "source_missing"
    if evidence.source is not Presence.PRESENT:
        return "needs_review", "presence_unknown"
    if Presence.PRESENT in evidence.destinations:
        return "skipped", "destination_present"
    if not evidence.destinations or any(item is not Presence.MISSING for item in evidence.destinations):
        return "needs_review", "destination_lookup_incomplete"
    if not _exclusive_signature(evidence.source_signature):
        return "needs_review", "source_shared_or_signature_unavailable"
    return None


def _task_decision(record, evidence):
    base = _base_decision(record, evidence)
    if base:
        return base
    if evidence.source is not Presence.MISSING:
        return "skipped", "source_present" if evidence.source is Presence.PRESENT else "presence_unknown"
    if evidence.downloader_task_present is not True:
        return "skipped", "task_absent"
    return None


def _transfer_decision(record, evidence):
    base = _base_decision(record, evidence)
    if base:
        return base
    if evidence.source not in (Presence.PRESENT, Presence.MISSING):
        return "needs_review", "presence_unknown"
    if not evidence.destinations or any(item is not Presence.MISSING for item in evidence.destinations):
        return ("skipped", "destination_present") if Presence.PRESENT in evidence.destinations else ("needs_review", "presence_unknown")
    if evidence.downloader_task_present is not False:
        return "skipped", evidence.blocking_reason or "downloader_task_present"
    return None


def reconcile_batch(adapter, state, *, persist, now=None, dry_run=True, options=None, stop_requested=None):
    now = time.time() if now is None else now
    options = replace(options or CleanupOptions(), allow_subscription_retry=False)
    user_stop = stop_requested or (lambda: False)
    monotonic_start = time.monotonic()
    budget_error = ""
    budget_counts = dict(path_probes=0, host_calls=0)
    def stop():
        return bool(user_stop() or budget_error or time.monotonic() - monotonic_start >= MAX_BATCH_SECONDS)
    current = deepcopy(state) if isinstance(state, dict) else {}
    report = dict(status="running", dry_run=dry_run is not False, processed=0, items=[], summary={},
        counts={name: 0 for name in COUNT_KEYS}, observed_pending=0, cursor_before=0, cursor_after=0, cycle_complete=False)
    processed_files = set()
    store_failed = False
    last_saved = None
    previous_cancel = getattr(adapter.downloaders, "stop_requested", None)
    original_paths, original_gateway = adapter.paths, adapter.gateway
    adapter.downloaders.stop_requested = stop
    if hasattr(adapter.downloaders, "begin_batch"):
        adapter.downloaders.begin_batch()

    def save():
        nonlocal store_failed, last_saved
        snapshot = deepcopy(current)
        if snapshot == last_saved:
            return
        try:
            persist(snapshot)
            last_saved = deepcopy(snapshot)
        except Exception:
            store_failed = True
            raise

    def cancelled():
        if user_stop():
            raise InterruptedError("cleanup_cancelled")
        if budget_error:
            raise BatchYield(budget_error)
        if time.monotonic() - monotonic_start >= MAX_BATCH_SECONDS:
            raise BatchYield("batch_time_limit")
        if store_failed:
            raise OSError("receipt_store_failed")

    def charge(name, maximum):
        nonlocal budget_error
        cancelled()
        if budget_counts[name] >= maximum:
            budget_error = "batch_path_budget" if name == "path_probes" else "batch_host_budget"
            raise BatchYield(budget_error)
        budget_counts[name] += 1

    class Paths:
        def check(self, path):
            charge("path_probes", MAX_PATH_PROBES)
            return original_paths.check(path)
        def unlink_exact(self, *args):
            cancelled()
            return original_paths.unlink_exact(*args)
    class Gateway:
        def __getattr__(self, name):
            value = getattr(original_gateway, name)
            if not callable(value):
                return value
            def call(*args, **kwargs):
                charge("host_calls", MAX_HOST_CALLS)
                return value(*args, **kwargs)
            return call
    adapter.paths, adapter.gateway = Paths(), Gateway()

    try:
        if not isinstance(state, (dict, type(None))):
            raise ValueError("invalid_record_state")
        if current.get("version", 1) != 1:
            raise ValueError("invalid_record_state")
        if type(now) not in (int, float) or not math.isfinite(now) or now < 0:
            raise ValueError("invalid_observation_time")
        current.setdefault("version", 1)
        work = current.setdefault("cleanup_v2", {"version": 2})
        if not isinstance(work, dict) or work.get("version") != 2:
            raise ValueError("invalid_reconciliation_state")
        receipts = work.setdefault("receipts", {})
        if not isinstance(receipts, dict) or len(receipts) > MAX_RECEIPTS:
            raise ValueError("invalid_receipt_ledger")
        ledger_bytes = len(json.dumps(receipts, ensure_ascii=False).encode())
        if ledger_bytes > MAX_LEDGER_BYTES:
            raise ValueError("receipt_ledger_size_limit")
        serials = [entry.get("serial", 0) for entry in receipts.values() if isinstance(entry, dict)]
        if len(serials) != len(receipts) or any(type(value) is not int or value <= 0 for value in serials) or len(set(serials)) != len(serials):
            raise ValueError("invalid_receipt_serial")
        work.setdefault("next_serial", max(serials, default=0) + 1)
        work.setdefault("file_cursor", 0)
        work.setdefault("receipt_cursor", 0)
        work.setdefault("orphan_history_cursor", 0)
        work.setdefault("orphan_transfer_cursor", 0)
        if any(type(work[field]) is not int or work[field] < 0 for field in ("file_cursor", "receipt_cursor", "next_serial")):
            raise ValueError("invalid_reconciliation_cursor")
        scan = work.get("scan")
        if not isinstance(scan, dict) or scan.get("complete") is True:
            previous_id = scan.get("cycle_id", 0) if isinstance(scan, dict) else 0
            scan = dict(cycle_id=previous_id + 1, checked=0, complete=False, started_at=now, completed_at=None, total=None,
                        files_complete=False, receipts_complete=False, orphan_history_complete=False, orphan_transfer_complete=False)
            work["scan"] = scan
            work["file_cursor"] = work["receipt_cursor"] = 0
            work["orphan_history_cursor"] = work["orphan_transfer_cursor"] = 0
        report["cursor_before"] = work["file_cursor"]
        cancelled()
        page = (CandidatePage((), 0, True) if scan.get("files_complete") is True else
                adapter.list_file_only_candidates(cursor=work["file_cursor"], limit=options.batch_size))
        if not isinstance(page, CandidatePage) or not page.complete or page.reason:
            raise ValueError("candidate_read_failed")
        report["listed"] = len(page.records)
        prepared = {}
        def prepare_for_record(record):
            nonlocal ledger_bytes
            cancelled()
            try:
                group_key = _digest([record.downloader, record.download_hash])
                receipt = receipts.get(group_key) or prepared.get(group_key)
                if receipt is None:
                    receipt = adapter.gateway.prepare_receipt(record.record_id)
                    prepared[group_key] = receipt
                if not isinstance(receipt, dict) or receipt.get("complete") is not True or not receipt.get("key"):
                    raise ValueError("association_snapshot_unavailable")
                key = receipt["key"]
                if key not in receipts:
                    if len(receipts) >= MAX_RECEIPTS:
                        report["items"].append(dict(key=record.key, resource_key=_digest(record.source_path), file_id=record.record_id, file=record.source_path.rsplit("/", 1)[-1],
                            branch="file_only", status="needs_review", reason="receipt_capacity_wait", first_seen=None, eligible_at=None))
                        return None
                    receipt = deepcopy(receipt)
                    receipt.update(serial=work["next_serial"], created_at=now, deleted_file_ids=[], physical_intents={})
                    work["next_serial"] += 1
                    receipts[key] = receipt
                    scan["receipts_complete"] = False
                    ledger_bytes += len(json.dumps(receipt, ensure_ascii=False).encode())
                    if ledger_bytes > MAX_LEDGER_BYTES:
                        receipts.pop(key)
                        raise ValueError("receipt_ledger_size_limit")
                return receipts[key]
            except (ValueError, KeyError, TypeError):
                report["items"].append(dict(key=record.key, resource_key=_digest(record.source_path), file_id=record.record_id, file=record.source_path.rsplit("/", 1)[-1],
                    branch="file_only", status="needs_review", reason="association_snapshot_unavailable", first_seen=None, eligible_at=None))
                return None

        def mark_file_done(record):
            if record.record_id > work["file_cursor"]:
                work["file_cursor"] = record.record_id
                scan["checked"] += 1

        def run_one(record, namespace, observer, mutation, decision):
            cancelled()
            observations = work.setdefault("observations", {})
            def observe(item):
                cancelled()
                return observer(item)
            def mutate(item, evidence, policy):
                cancelled()
                return mutation(item, evidence, policy)
            result = RecordCleanupEngine(observe, mutate, decision).run((record,), observations.get(namespace),
                now=now, dry_run=dry_run, options=options)
            if result.busy or result.truncated:
                raise RuntimeError("record_engine_busy")
            item = result.results[0]
            if stop() and item.status not in ("applied", "already_clean"):
                cancelled()
            observations[namespace] = result.state
            report["items"].append(dict(key=item.key, resource_key=_digest(record.source_path), file_id=record.record_id,
                file=record.source_path.rsplit("/", 1)[-1], branch=namespace, status=item.status, reason=item.reason,
                first_seen=item.first_seen, eligible_at=item.eligible_at))
            for name, count in item.counts.items():
                if name in report["counts"] and type(count) is int and count >= 0:
                    report["counts"][name] += count
            if namespace == "file_only":
                processed_files.add(record.record_id)
                report["processed"] = len(processed_files)
                mark_file_done(record)
            cancelled()
            return item

        for record in page.records:
            receipt = prepare_for_record(record)
            if receipt is None:
                mark_file_done(record)
                continue
            def prune(item, evidence, policy, receipt=receipt):
                view = adapter.gateway.receipt_view(receipt)
                original = next((row for row in receipt["files"] if row["id"] == item.record_id), None)
                if (not view.get("valid") or original is None or original.get("fullpath") != item.source_path):
                    return MutationResult("needs_review", "new_or_changed_reference")
                # The complete association snapshot, including prior committed
                # prefix progress, must survive before this exact child CAS.
                save()
                outcome = adapter.mutate_file_only(item, evidence, policy)
                if outcome.status in ("applied", "already_clean"):
                    removed = receipt.setdefault("deleted_file_ids", [])
                    if item.record_id not in removed:
                        removed.append(item.record_id)
                return outcome
            run_one(record, "file_only", adapter.observe_file_only, prune, file_only_decision)

        work["file_cursor"] = page.cursor
        scan["files_complete"] = page.cursor == 0
        save()

        for kind in ("history", "transfer"):
            cancelled()
            cursor_key, complete_key = "orphan_" + kind + "_cursor", "orphan_" + kind + "_complete"
            listing = getattr(adapter.gateway, "list_orphan_receipts", None)
            if listing is None:
                scan[complete_key] = True
                continue
            if scan.get(complete_key):
                continue
            orphan_page = listing(after_id=work[cursor_key], limit=options.batch_size, kind=kind)
            if not isinstance(orphan_page, dict) or orphan_page.get("complete") is not True:
                raise ValueError("orphan_read_failed")
            entries = orphan_page.get("entries", ())
            if not isinstance(entries, (list, tuple)) or len(entries) > options.batch_size:
                raise ValueError("orphan_page_unbounded")
            for entry in entries:
                cancelled()
                receipt = entry.get("receipt")
                if receipt is not None and receipt.get("key") not in receipts:
                    if len(receipts) >= MAX_RECEIPTS:
                        receipt = None
                        entry = dict(entry, reason="receipt_capacity_wait")
                    else:
                        receipt = deepcopy(receipt)
                        receipt.update(serial=work["next_serial"], created_at=now, deleted_file_ids=[], physical_intents={})
                        size = len(json.dumps(receipt, ensure_ascii=False).encode())
                        if ledger_bytes + size > MAX_LEDGER_BYTES:
                            raise ValueError("receipt_ledger_size_limit")
                        ledger_bytes += size
                        work["next_serial"] += 1
                        receipts[receipt["key"]] = receipt
                        scan["receipts_complete"] = False
                if receipt is None:
                    source = entry.get("source_path", "")
                    report["items"].append(dict(key="orphan_" + kind + ":" + str(entry["record_id"]), resource_key=_digest(source or entry["record_id"]),
                        file_id=entry["record_id"], file=source.rsplit("/", 1)[-1], branch="orphan_" + kind,
                        status="needs_review", reason=entry.get("reason", "orphan_mapping_incomplete"), first_seen=None, eligible_at=None))
                work[cursor_key] = entry["record_id"]
                scan["checked"] += 1
            work[cursor_key] = orphan_page["cursor"]
            scan[complete_key] = orphan_page["cursor"] == 0
            save()

        # A stable serial cursor provides a durable entry after all child rows
        # disappear. Retained library copies do not age out of this ledger.
        ordered = sorted(((key, value) for key, value in receipts.items()
                          if value["serial"] > work["receipt_cursor"]), key=lambda pair: pair[1]["serial"])
        selected = ordered[:options.batch_size] if not scan.get("receipts_complete") else []
        source_budget = options.batch_size
        incomplete_serials = []
        for key, receipt in selected:
            cancelled()
            work["receipt_cursor"] = receipt["serial"] - 1
            sources = _sources(receipt)
            if not sources:
                continue
            if len(sources) + len(receipt["transfers"]) > MAX_GROUP_PATHS:
                report["items"].append(dict(key="related_history:" + str(receipt["serial"]),
                    resource_key=_digest(sources[0]), file_id=receipt["serial"], file=sources[0].rsplit("/", 1)[-1],
                    branch="related_history", status="needs_review", reason="group_path_limit", first_seen=None, eligible_at=None))
                work["receipt_cursor"] = receipt["serial"]
                continue
            view = adapter.gateway.receipt_view(receipt)
            if not view.get("valid"):
                refresher = getattr(adapter.gateway, "propose_receipt_refresh", None)
                refreshed = refresher(receipt) if refresher else None
                if refreshed is not None:
                    activity = getattr(adapter.downloaders, "check_activity", None)
                    refreshed_sources = _sources(refreshed)
                    if activity:
                        complete, active = activity(refreshed["download_hash"], refreshed_sources, refreshed["downloader"], cached=False)
                    else:
                        complete, active = adapter.downloaders.check(refreshed["download_hash"], refreshed_sources)
                    if complete is True and active is False and all(adapter.paths.check(path).ready for path in refreshed_sources):
                        refreshed["refreshed_at"] = now
                        receipts[key] = receipt = refreshed
                        sources = _sources(receipt)
                        # File-only fingerprints intentionally ignore unrelated
                        # metadata. A legal group completion still starts a new
                        # observation epoch for every retained original child.
                        ids = {row["id"] for row in receipt["files"]}
                        file_state = work.setdefault("observations", {}).get("file_only", {})
                        for name in ("candidates", "completed"):
                            entries = file_state.get(name, {})
                            for observation_key in list(entries):
                                if any(observation_key.startswith("download_file_only:" + str(file_id) + ":") for file_id in ids):
                                    entries.pop(observation_key)
                        save()
                        view = adapter.gateway.receipt_view(receipt)
            if view.get("valid") and view.get("pending") is False:
                live_ids = {row["id"] for row in view.get("transfers", ())}
                for intent_source, intent in receipt.get("transfer_intents", {}).items():
                    if not isinstance(intent, dict) or intent.get("phase") not in ("intent", "committed", "recovered"):
                        continue
                    intended_ids = intent.get("ids", ())
                    original_rows = [row for row in receipt["transfers"] if row["id"] in intended_ids]
                    if (not intended_ids or len(set(intended_ids)) != len(intended_ids) or len(original_rows) != len(intended_ids)
                            or any(row["src"] != intent_source for row in original_rows)
                            or _digest(original_rows) != intent.get("mapping_revision")
                            or any(row_id in live_ids for row_id in intended_ids)):
                        continue
                    probe = adapter.paths.check(intent_source)
                    same_source = (probe.ready and (intent.get("source_presence") == "missing" and probe.presence is Presence.MISSING
                        or intent.get("source_presence") == "present" and probe.presence is Presence.PRESENT
                        and tuple(probe.signature) == tuple(intent.get("source_signature", ()))))
                    activity = getattr(adapter.downloaders, "check_activity", None)
                    complete, active = (activity(receipt["download_hash"], (intent_source,), receipt["downloader"], cached=False)
                                        if activity else adapter.downloaders.check(receipt["download_hash"], (intent_source,)))
                    if not same_source or complete is not True or active is not False:
                        continue
                    removed = receipt.setdefault("removed_transfer_ids", [])
                    changed = any(row_id not in removed for row_id in intended_ids)
                    removed.extend(row_id for row_id in intended_ids if row_id not in removed)
                    intent["phase"] = "recovered"
                    if intent.get("source_presence") == "present":
                        receipt.setdefault("retained_source_signatures", {})[intent_source] = list(probe.signature)
                    if changed:
                        save()
            if view.get("valid") and view.get("done"):
                receipts.pop(key, None)
                work["receipt_cursor"] = receipt["serial"]
                save()
                continue
            source_cursor = receipt.get("source_cursor", 0)
            if type(source_cursor) is not int or not 0 <= source_cursor < len(sources):
                source_cursor = 0
            source_done = receipt.get("source_cycle_id") == scan["cycle_id"]
            source_page = () if source_done else sources[source_cursor:source_cursor + source_budget]
            source_budget -= len(source_page)
            for source_index, source in enumerate(source_page, source_cursor):
                cancelled()
                mappings = tuple(row["dest"] for row in receipt["transfers"] if row["src"] == source)
                if MAX_PATH_PROBES - budget_counts["path_probes"] < 6 * (1 + len(mappings)) + 10:
                    raise BatchYield("batch_path_budget")
                def local_view(record, physical=False, fresh=False):
                    live = adapter.gateway.receipt_view(receipt) if fresh else view
                    local = adapter.paths.check(source)
                    targets = tuple(adapter.paths.check(path) for path in record.destination_paths)
                    valid = (live.get("valid") is True and live.get("mappings_safe", True) is True
                             and receipt.get("equivalent_parents") is True)
                    if physical:
                        live_ids = {row["id"] for row in live.get("transfers", ())}
                        removed = receipt.get("removed_transfer_ids", ())
                        own_mappings = [row for row in receipt["transfers"] if row["src"] == source]
                        valid = valid and all(row["id"] in live_ids or row["id"] in removed for row in own_mappings)
                        if any(row["id"] in removed for row in own_mappings) and local.presence is Presence.PRESENT:
                            valid = valid and tuple(receipt.get("retained_source_signatures", {}).get(source, ())) == tuple(local.signature)
                    evidence = CleanupEvidence(source=local.presence, destinations=tuple(item.presence for item in targets),
                        revision=record.revision, record_present=True if live.get("valid") else None,
                        identity_verified=valid, storage_ready=all(item.ready for item in (local,) + targets),
                        transfer_task_active=live.get("pending"), source_signature=local.signature,
                        blocking_reason=(live.get("reason", "") if not live.get("valid") else
                            "conflicting_parent_histories" if not receipt.get("equivalent_parents") else
                            "unsafe_transfer_mapping" if not live.get("mappings_safe", True) else
                            "transfer_mapping_changed" if physical and not valid else ""))
                    if (not evidence.storage_ready or not valid or live.get("pending") is not False
                            or physical and (local.presence is not Presence.PRESENT or Presence.PRESENT in evidence.destinations)
                            or not physical and local.presence is not Presence.MISSING):
                        return evidence, None
                    scoped = getattr(adapter.downloaders, "check_scoped" if fresh else "check_scoped_cached", None)
                    if scoped:
                        complete, present = scoped(receipt["download_hash"], (source,), receipt["downloader"])
                    else:
                        check = adapter.downloaders.check if fresh else getattr(adapter.downloaders, "check_cached", adapter.downloaders.check)
                        complete, present = check(receipt["download_hash"], (source,))
                    plan = PurgePlan(True, (), "") if complete and present is False else None
                    if complete and present is True:
                        planner = getattr(adapter.downloaders, "plan_purge" if fresh else "plan_purge_cached", None)
                        if planner is None:
                            planner = getattr(adapter.downloaders, "plan_purge", None)
                        if planner:
                            plan = planner(receipt["download_hash"], source, local.signature[3] if physical and local.signature else None,
                                           downloader=receipt["downloader"])
                    ownership = adapter.gateway.source_ownership(receipt, source)
                    return replace(evidence, downloaders_complete=complete is True and plan is not None and plan.complete,
                        downloader_task_present=present, identity_verified=valid and ownership is True,
                        blocking_reason=("source_shared_reference" if ownership is not True else
                            plan.reason if plan is not None and not plan.complete else "")), plan

                task_record = _record(receipt, "residual_task", source)
                task_local = adapter.paths.check(source)
                if task_local.presence is Presence.MISSING:
                    def observe_task(record):
                        return local_view(record)[0]
                    def remove_task(record, evidence, policy):
                        fresh, plan = local_view(record, fresh=True)
                        if _task_decision(record, fresh) or plan is None or not plan.complete:
                            return MutationResult("needs_review", "task_evidence_changed")
                        if stop():
                            return MutationResult("needs_review", "cleanup_cancelled")
                        last = adapter.paths.check(source)
                        if not last.ready or last.presence is not Presence.MISSING:
                            return MutationResult("needs_review", "source_reappeared")
                        receipt.setdefault("task_intents", {})[source] = dict(tasks=_task_snapshot(plan), phase="intent")
                        save()
                        cancelled()
                        okay = adapter.downloaders.remove_exclusive_tasks(receipt["download_hash"], source, None, plan,
                                                                         downloader=receipt["downloader"])
                        count = len(getattr(adapter.downloaders, "last_removed", ()))
                        receipt["task_intents"][source]["phase"] = "removed" if okay else "unconfirmed"
                        try:
                            save()
                        except Exception:
                            return MutationResult("needs_review", "receipt_store_failed", {"tasks": count})
                        return MutationResult("applied" if okay else "needs_review",
                            "residual_task_removed" if okay else "downloader_remove_unconfirmed", {"tasks": count})
                    run_one(task_record, "residual_task", observe_task, remove_task, _task_decision)
                if mappings:
                    physical_record = _record(receipt, "source_purge", source, mappings)
                    def observe_physical(record):
                        return local_view(record, True)[0]
                    def purge(record, evidence, policy):
                        fresh, plan = local_view(record, True, fresh=True)
                        if (_physical_decision(record, fresh) or plan is None or not plan.complete
                                or tuple(fresh.source_signature) != tuple(evidence.source_signature)):
                            return MutationResult("needs_review", "source_purge_evidence_changed")
                        signature = tuple(fresh.source_signature)
                        counts = {}
                        existing = receipt.setdefault("physical_intents", {}).get(source)
                        if existing is not None and tuple(existing.get("signature", ())) != signature:
                            return MutationResult("needs_review", "source_intent_revision_changed")
                        receipt["physical_intents"][source] = dict(signature=list(signature), tasks=_task_snapshot(plan), phase="intent")
                        try:
                            save()
                        except Exception:
                            return MutationResult("needs_review", "receipt_store_failed", counts)
                        if stop():
                            return MutationResult("needs_review", "cleanup_cancelled", counts)
                        okay = adapter.downloaders.remove_exclusive_tasks(receipt["download_hash"], source, signature[3], plan,
                                                                         downloader=receipt["downloader"])
                        counts = {"tasks": len(getattr(adapter.downloaders, "last_removed", ()))}
                        if not okay:
                            return MutationResult("needs_review", "downloader_remove_unconfirmed", counts)
                        if stop():
                            return MutationResult("needs_review", "cleanup_cancelled", counts)
                        latest, latest_plan = local_view(record, True, fresh=True)
                        if (_physical_decision(record, latest) or latest.downloader_task_present is not False
                                or tuple(latest.source_signature) != signature):
                            return MutationResult("needs_review", "source_purge_evidence_changed", counts)
                        if not adapter.paths.unlink_exact(source, signature):
                            return MutationResult("needs_review", "source_unlink_unconfirmed", counts)
                        counts["source_files"] = 1
                        receipt["physical_intents"][source]["phase"] = "source_deleted"
                        try:
                            save()
                        except Exception:
                            return MutationResult("needs_review", "receipt_store_failed", counts)
                        # Our own signed unlink supplies the missing-source fact;
                        # a second grace period must not strand its exact rows.
                        for file in receipt["files"]:
                            if file["fullpath"] == source:
                                if stop():
                                    return MutationResult("needs_review", "cleanup_cancelled", counts)
                                child = adapter.gateway.get_file_only_candidate(file["id"])
                                if child is not None:
                                    if (child.source_path != source or child.download_hash != receipt["download_hash"]
                                            or child.downloader != receipt["downloader"] or child.revision != _digest([file])):
                                        return MutationResult("needs_review", "source_purged_history_pending", counts)
                                    from .host_adapter import _file_record
                                    child_record = _file_record(child)
                                    outcome = adapter.mutate_file_only(child_record, adapter.observe_file_only(child_record), options)
                                    for name, count in outcome.counts.items():
                                        counts[name] = counts.get(name, 0) + count
                                    if outcome.status not in ("applied", "already_clean"):
                                        return MutationResult("needs_review", "source_purged_history_pending", counts)
                        return MutationResult("applied", "source_purged", counts)
                    run_one(physical_record, "source_purge", observe_physical, purge, _physical_decision)
                transfer_writer = getattr(adapter.gateway, "apply_transfer_scope", None)
                if mappings and transfer_writer is not None:
                    transfer_record = _record(receipt, "related_transfer", source, tuple(sorted(set(mappings))))
                    def observe_transfer(record, fresh=False):
                        live = adapter.gateway.receipt_view(receipt) if fresh else view
                        live_rows = [row for row in live.get("transfers", ()) if row["src"] == source]
                        if live.get("valid") and not live_rows:
                            return CleanupEvidence(record_present=False)
                        local = adapter.paths.check(source)
                        targets = tuple(adapter.paths.check(path) for path in record.destination_paths)
                        healthy = all(item.ready for item in (local,) + targets)
                        activity = getattr(adapter.downloaders, "check_activity", None)
                        if healthy and activity:
                            complete, active = activity(receipt["download_hash"], (source,), receipt["downloader"], cached=not fresh)
                        elif healthy:
                            complete, active = adapter.downloaders.check(receipt["download_hash"], (source,))
                        else:
                            complete, active = False, None
                        return CleanupEvidence(source=local.presence, destinations=tuple(item.presence for item in targets),
                            revision=record.revision, record_present=True if live.get("valid") else None,
                            identity_verified=(live.get("valid") is True and live.get("mappings_safe", True) is True
                                               and receipt.get("equivalent_parents") is True),
                            storage_ready=healthy, downloaders_complete=complete is True, downloader_task_present=active,
                            transfer_task_active=live.get("pending"), source_signature=local.signature,
                            blocking_reason=getattr(adapter.downloaders, "activity_reason", "") if active else live.get("reason", ""))
                    def remove_transfer(record, evidence, policy):
                        fresh = observe_transfer(record, fresh=True)
                        if (_transfer_decision(record, fresh) or fresh.source != evidence.source
                                or fresh.source is Presence.PRESENT and tuple(fresh.source_signature) != tuple(evidence.source_signature)):
                            return MutationResult("needs_review", "transfer_evidence_changed")
                        if fresh.source is Presence.PRESENT:
                            receipt.setdefault("retained_source_signatures", {})[source] = list(fresh.source_signature)
                        if any(not (probe.ready and probe.presence is Presence.MISSING)
                               for probe in (adapter.paths.check(path) for path in record.destination_paths)):
                            return MutationResult("needs_review", "destination_reappeared")
                        cancelled()
                        live = adapter.gateway.receipt_view(receipt)
                        ids = [row["id"] for row in live.get("transfers", ()) if row["src"] == source]
                        original_rows = [row for row in receipt["transfers"] if row["id"] in ids]
                        receipt.setdefault("transfer_intents", {})[source] = dict(ids=ids, mapping_revision=_digest(original_rows),
                            receipt_revision=receipt["revision"], source_presence=fresh.source.value,
                            source_signature=list(fresh.source_signature), phase="intent")
                        save()
                        confirmed = observe_transfer(record, fresh=True)
                        if (_transfer_decision(record, confirmed) or confirmed.source != fresh.source
                                or fresh.source is Presence.PRESENT and tuple(confirmed.source_signature) != tuple(fresh.source_signature)):
                            return MutationResult("needs_review", "transfer_evidence_changed")
                        cancelled()
                        live = adapter.gateway.receipt_view(receipt)
                        outcome = transfer_writer(receipt, source, dict(revision=live.get("revision"), source=fresh.source.value,
                            destination_paths=record.destination_paths, destinations_missing=True, storage_ready=True, tasks_inactive=True))
                        if outcome.status == "applied":
                            removed = receipt.setdefault("removed_transfer_ids", [])
                            removed.extend(row_id for row_id in ids if row_id not in removed)
                            receipt["transfer_intents"][source]["phase"] = "committed"
                            try:
                                save()
                            except Exception:
                                return MutationResult("needs_review", "receipt_store_failed", outcome.counts)
                        return outcome
                    run_one(transfer_record, "related_transfer", observe_transfer, remove_transfer, _transfer_decision)
                receipt["source_cursor"] = (source_index + 1) % len(sources)
                if receipt["source_cursor"] == 0:
                    receipt["source_cycle_id"] = scan["cycle_id"]
            if source_page:
                receipt["source_cursor"] = (source_cursor + len(source_page)) % len(sources)
            if not source_done and len(source_page) < len(sources) - source_cursor:
                incomplete_serials.append(receipt["serial"])

            destinations = tuple(sorted({row["dest"] for row in receipt["transfers"]}))
            if MAX_PATH_PROBES - budget_counts["path_probes"] < 5 * (len(sources) + len(destinations)) + 10:
                raise BatchYield("batch_path_budget")
            history_record = _record(receipt, "related_history", sources[0], destinations)
            def observe_history(record, fresh=False):
                live = adapter.gateway.receipt_view(receipt) if fresh else view
                if live.get("valid") and live.get("done"):
                    return CleanupEvidence(record_present=False)
                local = tuple(adapter.paths.check(path) for path in sources)
                targets = tuple(adapter.paths.check(path) for path in destinations)
                all_paths = sources + destinations
                healthy = all(item.ready for item in local + targets)
                scoped = getattr(adapter.downloaders, "check_scoped" if fresh else "check_scoped_cached", None)
                if healthy and scoped:
                    complete, present = scoped(receipt["download_hash"], all_paths, receipt["downloader"])
                else:
                    check = adapter.downloaders.check if fresh else getattr(adapter.downloaders, "check_cached", adapter.downloaders.check)
                    complete, present = check(receipt["download_hash"], all_paths) if healthy else (False, None)
                presence = (Presence.PRESENT if any(item.presence is Presence.PRESENT for item in local)
                            else Presence.UNKNOWN if any(item.presence is Presence.UNKNOWN for item in local) else Presence.MISSING)
                return CleanupEvidence(source=presence, destinations=tuple(item.presence for item in targets),
                    revision=record.revision, record_present=True if live.get("valid") else None,
                    identity_verified=(live.get("valid") is True and not live.get("files")
                        and live.get("mappings_safe", True) is True and receipt.get("equivalent_parents") is True),
                    storage_ready=healthy, downloaders_complete=complete is True,
                    downloader_task_present=present, transfer_task_active=live.get("pending"),
                    blocking_reason=(live.get("reason", "") if not live.get("valid") else
                        "related_files_still_registered" if live.get("files") else
                        "conflicting_parent_histories" if receipt.get("equivalent_parents") is not True else
                        "unsafe_transfer_mapping" if not live.get("mappings_safe", True) else ""))
            def remove_history(record, evidence, policy):
                fresh = observe_history(record, fresh=True)
                if _history_decision(record, fresh):
                    return MutationResult("needs_review", "related_evidence_changed")
                if any(not (probe.ready and probe.presence is Presence.MISSING)
                       for probe in (adapter.paths.check(path) for path in sources + destinations)):
                    return MutationResult("needs_review", "related_file_reappeared")
                cancelled()
                live = adapter.gateway.receipt_view(receipt)
                result = adapter.gateway.apply_receipt(receipt, dict(sources_missing=True, destinations_missing=True,
                    storage_ready=True, tasks_absent=True, revision=live.get("revision")))
                if result.status in ("applied", "already_clean"):
                    receipts.pop(key, None)
                    try:
                        save()
                    except Exception:
                        return MutationResult("needs_review", "receipt_store_failed", result.counts)
                return result
            item = run_one(history_record, "related_history", observe_history, remove_history, _history_decision)
            if item.status == "already_clean":
                receipts.pop(key, None)
                save()
            if receipt.get("source_cycle_id") == scan["cycle_id"] or key not in receipts:
                work["receipt_cursor"] = receipt["serial"]
            if incomplete_serials:
                break

        cancelled()
        remaining_serials = {value["serial"] for value in receipts.values()}
        incomplete_serials = [value for value in incomplete_serials if value in remaining_serials]
        if incomplete_serials:
            work["receipt_cursor"] = min(incomplete_serials) - 1
        elif selected and work["receipt_cursor"] == selected[-1][1]["serial"]:
            work["receipt_cursor"] = selected[-1][1]["serial"] if len(ordered) > len(selected) else 0
        elif not ordered:
            work["receipt_cursor"] = 0
        scan["receipts_complete"] = not incomplete_serials and work["receipt_cursor"] == 0
        scan["complete"] = (scan["files_complete"] and scan["receipts_complete"]
                            and scan.get("orphan_history_complete") is True and scan.get("orphan_transfer_complete") is True)
        if scan["complete"]:
            scan["completed_at"] = now
        report.update(cursor_after=work["file_cursor"], cycle_complete=scan["complete"])
        report["status"] = "review" if any(item["status"] == "needs_review" for item in report["items"]) else "completed"
        save()
    except InterruptedError:
        report["status"] = "cancelled"
        try:
            save()
        except Exception:
            report.update(status="failed", error="receipt_store_failed")
    except BatchYield as error:
        report.update(status="yielded", error=str(error))
        try:
            save()
        except Exception:
            report.update(status="failed", error="receipt_store_failed")
    except Exception as error:
        report.update(status="failed", error="receipt_store_failed" if store_failed else
            (str(error) if isinstance(error, ValueError) and str(error).replace("_", "").isalnum() else "record_run_failed"))
    finally:
        adapter.paths, adapter.gateway = original_paths, original_gateway
        if previous_cancel is not None:
            adapter.downloaders.stop_requested = previous_cancel
        if hasattr(adapter.downloaders, "end_batch"):
            adapter.downloaders.end_batch()
        grouped = {}
        report["action_summary"] = {}
        for item in report["items"]:
            status = item["status"]
            report["action_summary"][status] = report["action_summary"].get(status, 0) + 1
            grouped.setdefault(item.get("resource_key", item["key"]), set()).add(status)
        for statuses in grouped.values():
            for status in ("applied", "eligible", "waiting", "needs_review", "already_clean", "skipped"):
                if status in statuses:
                    report["summary"][status] = report["summary"].get(status, 0) + 1
                    break
        work = current.get("cleanup_v2", {})
        observations = work.get("observations", {}) if isinstance(work, dict) else {}
        report["observed_pending"] = sum(len(value.get("candidates", {})) for value in observations.values() if isinstance(value, dict))
        scan = work.get("scan", {}) if isinstance(work, dict) else {}
        report["scan"] = {key: scan.get(key) for key in ("cycle_id", "checked", "complete", "started_at", "completed_at", "total")}
        report["receipts_pending"] = len(work.get("receipts", {})) if isinstance(work, dict) else 0
        report["cursor_after"] = work.get("file_cursor", 0) if isinstance(work, dict) else 0
        report["budget"] = dict(budget_counts, elapsed_seconds=round(time.monotonic() - monotonic_start, 3))
    return ReconcileBatchResult(current, report)
