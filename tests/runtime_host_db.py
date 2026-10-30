"""使用隔离数据库验证失效媒体记录清理的事务边界。"""

from dataclasses import replace
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from threading import Event, Thread, current_thread
from types import ModuleType

plugin_directory = Path(os.environ.get(
    "CLEANUP_PLUGIN_PATH",
    Path(__file__).resolve().parents[1] / "plugins.v2" / "emptyfoldercleaner",
))
plugin_package = ModuleType("cleanup_plugin")
plugin_package.__path__ = [str(plugin_directory)]
sys.modules["cleanup_plugin"] = plugin_package

from cleanup_plugin.host_cleanup import CleanupProof, proof_allows_cleanup
from app.db.base import Base
from app.db.models.downloadhistory import DownloadFiles, DownloadHistory
from app.db.models.subscribe import Subscribe
from app.db.models.transferhistory import TransferHistory
from app.db.models.transferpending import TransferPending
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool


def make_database(engine=None):
    """创建与生产隔离的内存或临时文件表及一集已登记样本。"""
    if engine is None:
        engine = create_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
    Base.metadata.create_all(
        engine,
        tables=[
            DownloadFiles.__table__,
            DownloadHistory.__table__,
            Subscribe.__table__,
            TransferHistory.__table__,
            TransferPending.__table__,
        ],
    )
    with Session(engine) as session:
        session.add_all([
            DownloadFiles(id=1, downloader="qb", download_hash="hash-04", fullpath="/downloads/S04E04.mkv", savepath="/downloads", state=1),
            DownloadHistory(id=10, path="/downloads", type="电视剧", title="诛仙", media_source="tmdb", media_id="123", seasons="S04", episodes="E04", downloader="qb", download_hash="hash-04"),
            TransferHistory(id=20, src="/downloads/S04E04.mkv", src_storage="local", dest="/library/S04E04.mkv", dest_storage="local", type="电视剧", media_source="tmdb", media_id="123", seasons="S04", episodes="E04", download_hash="hash-04", status=True),
            Subscribe(id=30, name="诛仙", type="电视剧", media_source="tmdb", media_id="123", season=4, note=[4, 5, 13], lack_episode=10, total_episode=26, start_episode=1, state="R"),
        ])
        session.commit()
    return engine


def make_movie_database():
    """把同一隔离样本改为无季集且具有稳定媒体 ID 的单部电影。"""
    engine = make_database()
    with Session(engine) as session:
        file = session.get(DownloadFiles, 1)
        file.fullpath = "/downloads/Movie.mkv"
        history = session.get(DownloadHistory, 10)
        history.type = "电影"
        history.media_id = "456"
        history.seasons = None
        history.episodes = None
        transfer = session.get(TransferHistory, 20)
        transfer.src = "/downloads/Movie.mkv"
        transfer.dest = "/library/Movie.mkv"
        transfer.type = "电影"
        transfer.media_id = "456"
        transfer.seasons = None
        transfer.episodes = None
        session.commit()
    return engine


def test_movie_candidate_and_protection_use_exact_media_identity():
    """单部电影候选应按来源和 ID 枚举其它版本，无季集不报错。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, get_cleanup_protection

    engine = make_movie_database()
    try:
        with Session(engine) as session:
            session.add_all([
                DownloadHistory(
                    id=11, path="/downloads/other", type="电影", title="同片其他版本",
                    media_source="tmdb", media_id="456", downloader="qb", download_hash="hash-movie-v2",
                ),
                DownloadFiles(
                    id=2, downloader="qb", download_hash="hash-movie-v2",
                    fullpath="/downloads/other/Movie-v2.mkv", savepath="/downloads/other", state=1,
                ),
                TransferHistory(
                    id=21, src="/downloads/other/Movie-v2.mkv", src_storage="local",
                    dest="/library/Movie-v2.mkv", dest_storage="local", type="电影",
                    media_source="tmdb", media_id="456", download_hash="hash-movie-v2", status=True,
                ),
            ])
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        assert candidate.identity_verified is True
        assert candidate.media_type == "电影"
        assert candidate.season is None and candidate.episodes == ()
        protection = get_cleanup_protection(candidate, session_factory=lambda: Session(engine))
        assert protection.other_versions_complete is True
        assert protection.other_version_paths == (
            "/downloads/other/Movie-v2.mkv", "/library/Movie-v2.mkv",
        )
    finally:
        engine.dispose()


def test_movie_full_missing_and_transfer_only_paths_are_separate():
    """电影可安全清失效下载，源仍做种时仅清缺失的旧库回执。"""
    from cleanup_plugin.host_db import (
        apply_cleanup_candidate, get_cleanup_candidate, repair_missing_destination,
    )

    full_engine = make_movie_database()
    try:
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(full_engine))
        assert candidate is not None
        result = apply_cleanup_candidate(
            candidate, missing_proof(candidate), session_factory=lambda: Session(full_engine),
        )
        assert result.status == "cleaned"
        with Session(full_engine) as session:
            assert session.get(DownloadFiles, 1) is None
            assert session.get(DownloadHistory, 10) is None
            assert session.get(TransferHistory, 20) is None
    finally:
        full_engine.dispose()

    transfer_engine = make_movie_database()
    try:
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(transfer_engine))
        assert candidate is not None
        result = repair_missing_destination(
            candidate, missing_destination_proof(candidate),
            session_factory=lambda: Session(transfer_engine),
        )
        assert result.status == "cleaned" and result.transfer_deleted is True
        with Session(transfer_engine) as session:
            assert session.get(DownloadFiles, 1).state == 1
            assert session.get(DownloadHistory, 10) is not None
            assert session.get(TransferHistory, 20) is None
    finally:
        transfer_engine.dispose()


def test_candidate_uses_exact_media_and_episode_identity():
    """候选必须绑定精确下载、整理和订阅身份。"""
    from cleanup_plugin.host_db import get_cleanup_candidate

    engine = make_database()
    try:
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        assert candidate.file_id == 1
        assert candidate.source_path == "/downloads/S04E04.mkv"
        assert candidate.destination_paths == ("/library/S04E04.mkv",)
        assert candidate.media_source == "themoviedb"
        assert candidate.media_id == "123"
        assert candidate.season == 4
        assert candidate.episodes == (4,)
        assert candidate.history_id == 10
        assert candidate.transfer_id == 20
        assert candidate.subscribe_id == 30
        assert candidate.subscription_retry_needed is True
        assert candidate.identity_verified is True
        assert len(candidate.revision) == 64
    finally:
        engine.dispose()


def test_protection_finds_other_versions_and_parent_pending_task():
    """同集其他路径及祖先目录整理任务均阻止误删订阅事实。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, get_cleanup_protection

    engine = make_database()
    try:
        with Session(engine) as session:
            session.add_all([
                DownloadFiles(id=2, downloader="qb", download_hash="hash-04-v2", fullpath="/downloads/S04E04-v2.mkv", savepath="/downloads", state=1),
                DownloadHistory(id=11, path="/downloads", type="电视剧", title="诛仙", media_source="tmdb", media_id="123", seasons="S04", episodes="E04", downloader="qb", download_hash="hash-04-v2"),
                TransferHistory(id=21, src="/downloads/S04E04-v2.mkv", src_storage="local", dest="/library/S04E04-v2.mkv", dest_storage="local", type="电视剧", media_source="tmdb", media_id="123", seasons="S04", episodes="E04", download_hash="hash-04-v2", status=True),
                TransferPending(id=31, task_id="pending-parent", storage="local", src_path="/downloads", state="accepted", input_version=1, planning_input={"fixture": True}, input_fingerprint="fixture", updated_at="2026-01-01 00:00:00", execution_state="not_started"),
            ])
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        protection = get_cleanup_protection(candidate, session_factory=lambda: Session(engine))
        assert protection.other_version_paths == ("/downloads/S04E04-v2.mkv", "/library/S04E04-v2.mkv")
        assert protection.other_versions_complete is True
        assert protection.transfer_task_active is True
    finally:
        engine.dispose()


def test_nonlocal_transfer_and_unknown_sibling_storage_block_cleanup():
    """非本地源/目标及同集其它版本的未知存储不能当作本地缺失。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, get_cleanup_protection

    engine = make_database()
    try:
        with Session(engine) as session:
            session.get(TransferHistory, 20).dest_storage = "rclone"
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        assert candidate.identity_verified is False
        with Session(engine) as session:
            session.get(TransferHistory, 20).dest_storage = "local"
            session.add(TransferHistory(
                id=21, src="/downloads/S04E04-v2.mkv", src_storage="local",
                dest="/library/S04E04-v2.mkv", dest_storage=None,
                type="电视剧", media_source="tmdb", media_id="123",
                seasons="S04", episodes="E04", download_hash="hash-04-v2", status=True,
            ))
            session.commit()
        current = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert current is not None and current.identity_verified is True
        protection = get_cleanup_protection(current, session_factory=lambda: Session(engine))
        assert protection.other_versions_complete is False
    finally:
        engine.dispose()


def test_source_purge_protection_rejects_shared_path_hash_and_transfer_references():
    """源实体清理只允许独占的路径与 Hash，不能碰其他媒体的登记。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, get_cleanup_protection

    engine = make_database()
    try:
        initial = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert initial is not None
        default = get_cleanup_protection(initial, session_factory=lambda: Session(engine))
        assert default.source_exclusive is False and default.hash_exclusive is False

        def protection():
            candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
            assert candidate is not None
            return get_cleanup_protection(
                candidate, include_source_ownership=True, session_factory=lambda: Session(engine),
            )

        assert protection().source_exclusive is True
        assert protection().hash_exclusive is True
        with Session(engine) as session:
            session.add(DownloadFiles(id=2, downloader="qb", download_hash="other", fullpath="/downloads/S04E04.mkv", state=1))
            session.commit()
        assert protection().source_exclusive is False
        assert protection().hash_exclusive is True
        with Session(engine) as session:
            session.get(DownloadFiles, 2).fullpath = "/downloads/other.mkv"
            session.get(DownloadFiles, 2).download_hash = "hash-04"
            session.commit()
        assert protection().source_exclusive is True
        assert protection().hash_exclusive is False
        with Session(engine) as session:
            session.get(DownloadFiles, 2).state = 0
            session.commit()
        assert protection().hash_exclusive is False
        with Session(engine) as session:
            session.add(TransferHistory(
                id=21, src="/downloads/else.mkv", src_storage="local",
                dest="/downloads/S04E04.mkv", dest_storage="local", type="电影",
                media_source="tmdb", media_id="456", download_hash="another", status=True,
            ))
            session.commit()
        assert protection().source_exclusive is False
        with Session(engine) as session:
            session.get(TransferHistory, 21).dest = "/library/else.mkv"
            session.get(TransferHistory, 21).download_hash = "hash-04"
            session.commit()
        assert protection().hash_exclusive is False
        with Session(engine) as session:
            session.delete(session.get(TransferHistory, 21))
            session.add(TransferPending(
                id=31, task_id="pending-parent", storage="local", src_path="/downloads",
                state="accepted", input_version=1, planning_input={"fixture": True},
                input_fingerprint="fixture", updated_at="2026-01-01 00:00:00", execution_state="not_started",
            ))
            session.commit()
        assert protection().source_exclusive is False
    finally:
        engine.dispose()


def test_source_purge_protection_rejects_stale_candidate():
    """源实体删除前的宿主快照变化必须使旧候选失效。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, get_cleanup_protection

    engine = make_database()
    try:
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        with Session(engine) as session:
            session.get(TransferHistory, 20).mode = "link"
            session.commit()
        protection = get_cleanup_protection(
            candidate, include_source_ownership=True, session_factory=lambda: Session(engine),
        )
        assert protection.source_exclusive is False
        assert protection.hash_exclusive is False
    finally:
        engine.dispose()


def test_source_purge_protection_retains_old_inactive_same_path_record():
    """即使旧下载文件行是 state=0，也不能据此删除仍被引用的实体路径。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, get_cleanup_protection

    engine = make_database()
    try:
        with Session(engine) as session:
            session.add(DownloadFiles(
                id=2, downloader="qb", download_hash="old-hash",
                fullpath="/downloads/S04E04.mkv", state=0,
            ))
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        protection = get_cleanup_protection(
            candidate, include_source_ownership=True, session_factory=lambda: Session(engine),
        )
        assert protection.source_exclusive is False
        assert protection.hash_exclusive is True
    finally:
        engine.dispose()


def test_source_purge_protection_rejects_other_download_history_same_source_path():
    """即使另一下载历史没有文件行，精确相同的源路径仍不属于本候选独占。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, get_cleanup_protection

    engine = make_database()
    try:
        with Session(engine) as session:
            session.add(DownloadHistory(
                id=11, path="/downloads/S04E04.mkv", type="电影", title="其它媒体",
                media_source="tmdb", media_id="999", downloader="qb", download_hash="other-hash",
            ))
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None and candidate.identity_verified is True
        protection = get_cleanup_protection(
            candidate, include_source_ownership=True, session_factory=lambda: Session(engine),
        )
        assert protection.source_exclusive is False
        assert protection.hash_exclusive is True
    finally:
        engine.dispose()


def missing_proof(candidate, *, other_version_paths=()):
    """建立仅包含合成路径、无外部 I/O 的完整缺失证据。"""
    return CleanupProof(
        source="missing",
        destinations=tuple("missing" for _ in candidate.destination_paths),
        other_versions="missing",
        media_library="missing",
        media_lookup_complete=True,
        downloaders_complete=True,
        downloader_task_present=False,
        transfer_task_active=False,
        storage_ready=True,
        identity_verified=True,
        revision=candidate.revision,
        record_present=True,
        source_path=candidate.source_path,
        destination_paths=candidate.destination_paths,
        other_version_paths=other_version_paths,
        destination_lookup_complete=candidate.destination_lookup_complete,
    )


def missing_destination_proof(candidate):
    """建立做种源仍存在、精确库目标与 Emby 集均缺失的证据。"""
    return replace(
        missing_proof(candidate),
        source="present",
        downloader_task_present=True,
    )


def test_missing_destination_repair_preserves_download_and_subscription():
    """库目标被手动删除时只移除旧整理回执，保留源、做种及订阅进度。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, repair_missing_destination

    engine = make_database()
    try:
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        result = repair_missing_destination(
            candidate, missing_destination_proof(candidate), session_factory=lambda: Session(engine),
        )
        assert result.status == "cleaned"
        assert result.transfer_deleted is True
        assert result.file_marked_missing is False
        assert result.history_deleted is False
        assert result.note_updated is False
        with Session(engine) as session:
            assert session.get(TransferHistory, 20) is None
            assert session.get(DownloadFiles, 1).state == 1
            assert session.get(DownloadHistory, 10) is not None
            assert session.get(Subscribe, 30).note == [4, 5, 13]
            assert session.get(Subscribe, 30).lack_episode == 10
    finally:
        engine.dispose()


def test_missing_destination_repair_requires_exact_complete_external_absence():
    """源不在、库目标仍在或 Emby 季集不确定时均不得删回执。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, repair_missing_destination

    engine = make_database()
    try:
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        proof = missing_destination_proof(candidate)
        unsafe = (
            replace(proof, source="missing"),
            replace(proof, destinations=("present",)),
            replace(proof, destinations=("unknown",)),
            replace(proof, media_library="present"),
            replace(proof, media_lookup_complete=False),
            replace(proof, downloader_task_present=None),
            replace(proof, destination_lookup_complete=False),
            replace(proof, storage_ready=False),
            replace(proof, destination_paths=("/library/other.mkv",)),
        )
        for altered in unsafe:
            assert repair_missing_destination(
                candidate, altered, session_factory=lambda: Session(engine),
            ).status == "needs_review"
        with Session(engine) as session:
            assert session.get(TransferHistory, 20) is not None
    finally:
        engine.dispose()


def test_missing_destination_repair_without_transfer_keeps_everything():
    """无精确整理回执时，库目标修复不得触碰下载历史或订阅。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, repair_missing_destination

    engine = make_database()
    try:
        with Session(engine) as session:
            session.delete(session.get(TransferHistory, 20))
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None and candidate.transfer_id is None
        assert repair_missing_destination(
            candidate, missing_destination_proof(candidate), session_factory=lambda: Session(engine),
        ).status == "needs_review"
        with Session(engine) as session:
            assert session.get(DownloadFiles, 1).state == 1
            assert session.get(DownloadHistory, 10) is not None
            assert session.get(Subscribe, 30).note == [4, 5, 13]
    finally:
        engine.dispose()


def test_missing_destination_repair_rechecks_pending_and_durable_tasks():
    """库目标缺失也不能绕过事务内的持久整理任务保护。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, repair_missing_destination

    engine = make_database()
    try:
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        with Session(engine) as session:
            session.add(TransferPending(
                id=31, task_id="pending-parent", storage="local", src_path="/downloads",
                state="accepted", input_version=1, planning_input={"fixture": True},
                input_fingerprint="fixture", updated_at="2026-01-01 00:00:00",
                execution_state="not_started",
            ))
            session.commit()
        assert repair_missing_destination(
            candidate, missing_destination_proof(candidate), session_factory=lambda: Session(engine),
        ).status == "needs_review"
        with Session(engine) as session:
            session.query(TransferPending).delete()
            session.get(TransferHistory, 20).transfer_task_id = "durable-task"
            session.commit()
        refreshed = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert refreshed is not None
        assert repair_missing_destination(
            refreshed, missing_destination_proof(refreshed), session_factory=lambda: Session(engine),
        ).status == "needs_review"
        with Session(engine) as session:
            assert session.get(TransferHistory, 20) is not None
    finally:
        engine.dispose()


def test_missing_destination_repair_rejects_changed_transfer_revision():
    """观察后整理目标被更改时旧凭据必须失效。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, repair_missing_destination

    engine = make_database()
    try:
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        with Session(engine) as session:
            session.get(TransferHistory, 20).dest = "/library/restored.mkv"
            session.commit()
        assert repair_missing_destination(
            candidate, missing_destination_proof(candidate), session_factory=lambda: Session(engine),
        ).status == "stale"
        with Session(engine) as session:
            assert session.get(TransferHistory, 20) is not None
    finally:
        engine.dispose()


def test_transfer_status_change_invalidates_observation_revision():
    """旧回执状态变化必须生成新候选版本并使先前证据失效。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, repair_missing_destination

    engine = make_database()
    try:
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        with Session(engine) as session:
            session.get(TransferHistory, 20).status = False
            session.commit()
        changed = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert changed is not None
        assert changed.revision != candidate.revision
        assert repair_missing_destination(
            candidate, missing_destination_proof(candidate), session_factory=lambda: Session(engine),
        ).status == "stale"
        with Session(engine) as session:
            assert session.get(TransferHistory, 20) is not None
    finally:
        engine.dispose()


def test_cleanup_marks_only_target_and_keeps_subscription_when_retry_disabled():
    """E04 失效只清其精确行，E05/E13 与订阅进度保持原状。"""
    from cleanup_plugin.host_db import apply_cleanup_candidate, get_cleanup_candidate

    engine = make_database()
    try:
        with Session(engine) as session:
            session.add_all([
                DownloadFiles(id=2, downloader="qb", download_hash="hash-05", fullpath="/downloads/S04E05.mkv", savepath="/downloads", state=1),
                DownloadHistory(id=11, path="/downloads", type="电视剧", title="诛仙", media_source="tmdb", media_id="123", seasons="S04", episodes="E05", downloader="qb", download_hash="hash-05"),
                TransferHistory(id=21, src="/downloads/S04E05.mkv", src_storage="local", dest="/library/S04E05.mkv", dest_storage="local", type="电视剧", media_source="tmdb", media_id="123", seasons="S04", episodes="E05", download_hash="hash-05", status=True),
                DownloadFiles(id=3, downloader="qb", download_hash="hash-13", fullpath="/downloads/S04E13.mkv", savepath="/downloads", state=1),
                DownloadHistory(id=12, path="/downloads", type="电视剧", title="诛仙", media_source="tmdb", media_id="123", seasons="S04", episodes="E13", downloader="qb", download_hash="hash-13"),
                TransferHistory(id=22, src="/downloads/S04E13.mkv", src_storage="local", dest="/library/S04E13.mkv", dest_storage="local", type="电视剧", media_source="tmdb", media_id="123", seasons="S04", episodes="E13", download_hash="hash-13", status=True),
            ])
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        result = apply_cleanup_candidate(candidate, missing_proof(candidate), allow_subscription_retry=False, session_factory=lambda: Session(engine))
        assert result.status == "cleaned"
        assert result.file_marked_missing and result.history_deleted and result.transfer_deleted
        assert result.note_updated is False
        with Session(engine) as session:
            assert session.get(DownloadFiles, 1) is None
            assert session.get(DownloadHistory, 10) is None
            assert session.get(TransferHistory, 20) is None
            assert [session.get(DownloadFiles, row_id).state for row_id in (2, 3)] == [1, 1]
            assert all(session.get(DownloadHistory, row_id) is not None for row_id in (11, 12))
            assert session.get(Subscribe, 30).note == [4, 5, 13]
            assert session.get(Subscribe, 30).lack_episode == 10
    finally:
        engine.dispose()


def test_subscription_retry_needs_resolved_progress_before_any_write():
    """无法可靠重算缺集数时，重开订阅请求必须整笔保留。"""
    from cleanup_plugin.host_db import apply_cleanup_candidate, get_cleanup_candidate

    engine = make_database()
    try:
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        result = apply_cleanup_candidate(candidate, missing_proof(candidate), allow_subscription_retry=True, session_factory=lambda: Session(engine))
        assert result.status == "needs_review"
        with Session(engine) as session:
            assert session.get(DownloadFiles, 1).state == 1
            assert session.get(DownloadHistory, 10) is not None
            assert session.get(Subscribe, 30).note == [4, 5, 13]
    finally:
        engine.dispose()


def test_subscription_without_target_note_cleans_without_progress_resolution():
    """订阅存在但目标集不在合法 note 时，无须重算缺集数也能清失效历史。"""
    from cleanup_plugin.host_db import apply_cleanup_candidate, get_cleanup_candidate

    engine = make_database()
    try:
        with Session(engine) as session:
            session.get(Subscribe, 30).note = [5, 13]
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        assert candidate.identity_verified is True
        assert candidate.subscription_retry_needed is False
        result = apply_cleanup_candidate(
            candidate, missing_proof(candidate), allow_subscription_retry=True,
            session_factory=lambda: Session(engine),
        )
        assert result.status == "cleaned" and result.note_updated is False
        with Session(engine) as session:
            assert session.get(Subscribe, 30).note == [5, 13]
            assert session.get(Subscribe, 30).lack_episode == 10
            assert session.get(DownloadFiles, 1) is None
    finally:
        engine.dispose()


def test_malformed_subscription_note_is_never_silently_classified_as_no_retry():
    """含其它非整数集号的 note 不能绕开进度保护而只清历史。"""
    from cleanup_plugin.host_db import apply_cleanup_candidate, get_cleanup_candidate

    engine = make_database()
    try:
        with Session(engine) as session:
            session.get(Subscribe, 30).note = [4, "5", 13]
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        assert candidate.identity_verified is False
        assert apply_cleanup_candidate(
            candidate, missing_proof(candidate), allow_subscription_retry=False,
            session_factory=lambda: Session(engine),
        ).status == "needs_review"
        with Session(engine) as session:
            assert session.get(DownloadFiles, 1).state == 1
    finally:
        engine.dispose()


def test_subscription_retry_removes_only_confirmed_episode_and_updates_progress():
    """完整证据下仅移除 E04，缺集数与 note 在同一事务变更。"""
    from cleanup_plugin.host_db import apply_cleanup_candidate, get_cleanup_candidate

    engine = make_database()
    try:
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        result = apply_cleanup_candidate(candidate, missing_proof(candidate), allow_subscription_retry=True, resolved_lack_episode=11, session_factory=lambda: Session(engine))
        assert result.status == "cleaned"
        assert result.note_updated is True
        with Session(engine) as session:
            assert session.get(Subscribe, 30).note == [5, 13]
            assert session.get(Subscribe, 30).lack_episode == 11
    finally:
        engine.dispose()


def test_same_hash_active_sibling_prevents_note_retry_and_rolls_back_file_state():
    """同 Hash 仍有活动文件时，不可只改本行 state 又误删订阅集号。"""
    from cleanup_plugin.host_db import apply_cleanup_candidate, get_cleanup_candidate

    engine = make_database()
    try:
        with Session(engine) as session:
            session.add(DownloadFiles(
                id=2, downloader="qb", download_hash="hash-04",
                fullpath="/downloads/S04E04.mkv", savepath="/downloads", state=1,
            ))
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None and candidate.subscription_retry_needed is True
        result = apply_cleanup_candidate(
            candidate, missing_proof(candidate), allow_subscription_retry=True,
            resolved_lack_episode=11, session_factory=lambda: Session(engine),
        )
        assert result.status == "needs_review"
        with Session(engine) as session:
            assert session.get(DownloadFiles, 1).state == 1
            assert session.get(DownloadFiles, 2).state == 1
            assert session.get(DownloadHistory, 10) is not None
            assert session.get(Subscribe, 30).note == [4, 5, 13]
            assert session.get(Subscribe, 30).lack_episode == 10
    finally:
        engine.dispose()


def test_exact_delete_preserves_legacy_inactive_sibling_and_parent_history():
    """清理目标文件行时不误删同 Hash 的旧 state=0 行或使其父历史孤立。"""
    from cleanup_plugin.host_db import apply_cleanup_candidate, get_cleanup_candidate

    engine = make_database()
    try:
        with Session(engine) as session:
            session.add(DownloadFiles(
                id=2, downloader="qb", download_hash="hash-04",
                fullpath="/downloads/old-state-zero.mkv", savepath="/downloads", state=0,
            ))
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        result = apply_cleanup_candidate(
            candidate, missing_proof(candidate), allow_subscription_retry=True,
            session_factory=lambda: Session(engine),
        )
        assert result.status == "cleaned" and result.file_marked_missing is True
        assert result.history_deleted is False and result.note_updated is False
        with Session(engine) as session:
            assert session.get(DownloadFiles, 1) is None
            assert session.get(DownloadFiles, 2).state == 0
            assert session.get(DownloadHistory, 10) is not None
            assert session.get(Subscribe, 30).note == [4, 5, 13]
    finally:
        engine.dispose()


def test_two_missing_versions_clean_separately_then_reopen_note_once():
    """同集两版都已缺失时先清各自历史，最后一版才移除订阅集号。"""
    from cleanup_plugin.host_db import (
        apply_cleanup_candidate, get_cleanup_candidate, get_cleanup_protection,
    )

    engine = make_database()
    try:
        with Session(engine) as session:
            session.add_all([
                DownloadFiles(
                    id=2, downloader="qb", download_hash="hash-04-v2",
                    fullpath="/downloads/S04E04-v2.mkv", savepath="/downloads", state=1,
                ),
                DownloadHistory(
                    id=11, path="/downloads", type="电视剧", title="诛仙",
                    media_source="tmdb", media_id="123", seasons="S04", episodes="E04",
                    downloader="qb", download_hash="hash-04-v2",
                ),
                TransferHistory(
                    id=21, src="/downloads/S04E04-v2.mkv", src_storage="local",
                    dest="/library/S04E04-v2.mkv", dest_storage="local", type="电视剧",
                    media_source="tmdb", media_id="123", seasons="S04", episodes="E04",
                    download_hash="hash-04-v2", status=True,
                ),
            ])
            session.commit()
        first = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert first is not None and first.subscription_retry_needed is True
        first_paths = get_cleanup_protection(first, session_factory=lambda: Session(engine)).other_version_paths
        assert first_paths == ("/downloads/S04E04-v2.mkv", "/library/S04E04-v2.mkv")
        result = apply_cleanup_candidate(
            first, missing_proof(first, other_version_paths=first_paths),
            allow_subscription_retry=True, resolved_lack_episode=11,
            session_factory=lambda: Session(engine),
        )
        assert result.status == "cleaned" and result.file_marked_missing is True
        assert result.note_updated is False
        with Session(engine) as session:
            assert session.get(DownloadFiles, 1) is None
            assert session.get(DownloadHistory, 10) is None
            assert session.get(Subscribe, 30).note == [4, 5, 13]
        second = get_cleanup_candidate(2, session_factory=lambda: Session(engine))
        assert second is not None and second.subscription_retry_needed is True
        assert get_cleanup_protection(second, session_factory=lambda: Session(engine)).other_version_paths == ()
        result = apply_cleanup_candidate(
            second, missing_proof(second), allow_subscription_retry=True,
            resolved_lack_episode=11, session_factory=lambda: Session(engine),
        )
        assert result.status == "cleaned" and result.note_updated is True
        with Session(engine) as session:
            assert session.get(DownloadFiles, 2) is None
            assert session.get(Subscribe, 30).note == [5, 13]
            assert session.get(Subscribe, 30).lack_episode == 11
    finally:
        engine.dispose()


def test_sqlite_write_lock_prevents_parent_and_note_race_in_both_commands():
    """真实临时 SQLite 双连接下，候选复核期间外部写父历史或 note 不得插队。"""
    from cleanup_plugin import host_db as module

    for destination_only in (False, True):
        with TemporaryDirectory(prefix="moviepilot-cleanup-") as directory:
            db_path = Path(directory) / "isolated.db"
            engine = make_database(create_engine(
                "sqlite+pysqlite:///" + str(db_path),
                connect_args={"check_same_thread": False, "timeout": 0.15},
            ))
            try:
                candidate = module.get_cleanup_candidate(1, session_factory=lambda: Session(engine))
                assert candidate is not None
                proof = missing_destination_proof(candidate) if destination_only else missing_proof(candidate)
                entered, release = Event(), Event()
                outcomes = []
                original = module._candidate
                paused = [False]

                def pause_after_lock(session, file_id):
                    """只在写命令第一次读取候选时暂停，暴露真实并发窗口。"""
                    if current_thread().name == "cleanup-test-worker" and not paused[0]:
                        paused[0] = True
                        entered.set()
                        if not release.wait(3):
                            raise RuntimeError("并发测试未解除暂停")
                    return original(session, file_id)

                def run_cleanup():
                    """在独立连接执行待验证的原子清理命令。"""
                    try:
                        if destination_only:
                            outcomes.append(module.repair_missing_destination(
                                candidate, proof, session_factory=lambda: Session(engine),
                            ))
                        else:
                            outcomes.append(module.apply_cleanup_candidate(
                                candidate, proof, session_factory=lambda: Session(engine),
                            ))
                    except Exception as error:
                        outcomes.append(error)

                module._candidate = pause_after_lock
                worker = Thread(target=run_cleanup, name="cleanup-test-worker")
                try:
                    worker.start()
                    assert entered.wait(3)
                    with Session(engine) as competing:
                        parent = competing.get(DownloadHistory, 10)
                        subscription = competing.get(Subscribe, 30)
                        parent.media_id = "changed"
                        subscription.note = [5, 13]
                        try:
                            competing.commit()
                        except OperationalError:
                            competing.rollback()
                        else:
                            raise AssertionError("清理事务读候选前未持有 SQLite 写锁")
                finally:
                    release.set()
                    worker.join(timeout=3)
                    module._candidate = original
                assert not worker.is_alive()
                assert len(outcomes) == 1
                assert not isinstance(outcomes[0], Exception)
                assert outcomes[0].status == "cleaned"
                with Session(engine) as session:
                    assert session.get(Subscribe, 30).note == [4, 5, 13]
                    if destination_only:
                        assert session.get(DownloadHistory, 10).media_id == "123"
                        assert session.get(TransferHistory, 20) is None
                    else:
                        assert session.get(DownloadFiles, 1) is None
                        assert session.get(DownloadHistory, 10) is None
            finally:
                engine.dispose()


def test_changed_path_and_present_source_leave_every_row_untouched():
    """陈旧 DB 版本及源文件仍存在的证据均须拒绝清理。"""
    from cleanup_plugin.host_db import apply_cleanup_candidate, get_cleanup_candidate

    engine = make_database()
    try:
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        present = replace(missing_proof(candidate), source="present")
        assert apply_cleanup_candidate(candidate, present, session_factory=lambda: Session(engine)).status == "needs_review"
        with Session(engine) as session:
            session.get(DownloadFiles, 1).fullpath = "/downloads/restored-S04E04.mkv"
            session.commit()
        assert apply_cleanup_candidate(candidate, missing_proof(candidate), session_factory=lambda: Session(engine)).status == "stale"
        with Session(engine) as session:
            assert session.get(DownloadFiles, 1).state == 1
            assert session.get(DownloadHistory, 10) is not None
            assert session.get(TransferHistory, 20) is not None
            assert session.get(Subscribe, 30).note == [4, 5, 13]
    finally:
        engine.dispose()


def test_pending_task_recheck_blocks_stale_external_absence_claim():
    """适配层误称无整理任务时，核心事务复查 Pending 后仍需拒绝。"""
    from cleanup_plugin.host_db import apply_cleanup_candidate, get_cleanup_candidate

    engine = make_database()
    try:
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        with Session(engine) as session:
            session.add(TransferPending(id=31, task_id="pending-parent", storage="local", src_path="/downloads", state="accepted", input_version=1, planning_input={"fixture": True}, input_fingerprint="fixture", updated_at="2026-01-01 00:00:00", execution_state="not_started"))
            session.commit()
        result = apply_cleanup_candidate(candidate, missing_proof(candidate), session_factory=lambda: Session(engine))
        assert result.status == "needs_review"
        with Session(engine) as session:
            assert session.get(DownloadFiles, 1).state == 1
            assert session.get(TransferHistory, 20) is not None
    finally:
        engine.dispose()


def test_durable_transfer_history_is_never_auto_removed():
    """失败整理任务绑定的回执只由原生状态机处理。"""
    from cleanup_plugin.host_db import apply_cleanup_candidate, get_cleanup_candidate

    engine = make_database()
    try:
        with Session(engine) as session:
            session.get(TransferHistory, 20).transfer_task_id = "durable-task"
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None and candidate.review_reason == "durable_transfer_task"
        result = apply_cleanup_candidate(candidate, missing_proof(candidate), session_factory=lambda: Session(engine))
        assert result.status == "needs_review"
        with Session(engine) as session:
            assert session.get(TransferHistory, 20).transfer_task_id == "durable-task"
            assert session.get(DownloadFiles, 1).state == 1
    finally:
        engine.dispose()


def test_settlement_revision_or_failed_transfer_is_not_legacy_cleanup():
    """残留 durable 结算版本与失败回执不能被当作成功旧历史删除。"""
    from cleanup_plugin.host_db import (
        apply_cleanup_candidate, get_cleanup_candidate, repair_missing_destination,
    )

    engine = make_database()
    try:
        with Session(engine) as session:
            session.get(TransferHistory, 20).transfer_settlement_revision = 3
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        assert apply_cleanup_candidate(
            candidate, missing_proof(candidate), session_factory=lambda: Session(engine),
        ).status == "needs_review"
        assert repair_missing_destination(
            candidate, missing_destination_proof(candidate), session_factory=lambda: Session(engine),
        ).status == "needs_review"
        with Session(engine) as session:
            transfer = session.get(TransferHistory, 20)
            assert transfer is not None
            transfer.transfer_settlement_revision = None
            transfer.status = False
            session.commit()
        failed = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert failed is not None
        assert apply_cleanup_candidate(
            failed, missing_proof(failed), session_factory=lambda: Session(engine),
        ).status == "needs_review"
        assert repair_missing_destination(
            failed, missing_destination_proof(failed), session_factory=lambda: Session(engine),
        ).status == "needs_review"
        with Session(engine) as session:
            assert session.get(TransferHistory, 20) is not None
            assert session.get(DownloadFiles, 1).state == 1
    finally:
        engine.dispose()


def test_transfer_history_embedded_paths_must_match_exact_file():
    """旧回执若暗含另一个源或库路径，不能按单路径证据删除整行。"""
    from cleanup_plugin.host_db import (
        apply_cleanup_candidate, get_cleanup_candidate, repair_missing_destination,
    )

    engine = make_database()
    try:
        with Session(engine) as session:
            session.get(TransferHistory, 20).files = [
                "/downloads/S04E04.mkv", "/downloads/extra.mkv",
            ]
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None and candidate.identity_verified is False
        assert apply_cleanup_candidate(
            candidate, missing_proof(candidate), session_factory=lambda: Session(engine),
        ).status == "needs_review"
        with Session(engine) as session:
            transfer = session.get(TransferHistory, 20)
            transfer.files = []
            transfer.dest_fileitem = {"path": "/library/other.mkv"}
            session.commit()
        changed = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert changed is not None and changed.identity_verified is False
        assert repair_missing_destination(
            changed, missing_destination_proof(changed), session_factory=lambda: Session(engine),
        ).status == "needs_review"
        with Session(engine) as session:
            assert session.get(TransferHistory, 20) is not None
            assert session.get(DownloadFiles, 1).state == 1
    finally:
        engine.dispose()


def test_single_digit_legacy_episode_format_and_untransferred_file():
    """单数字旧格式可清；从未整理的已失效文件不凭空要求目标路径。"""
    from cleanup_plugin.host_db import apply_cleanup_candidate, get_cleanup_candidate

    engine = make_database()
    try:
        with Session(engine) as session:
            session.get(DownloadHistory, 10).seasons = "S4"
            session.get(DownloadHistory, 10).episodes = "E4"
            session.get(TransferHistory, 20).seasons = "S4"
            session.get(TransferHistory, 20).episodes = "E4"
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None and candidate.identity_verified
        assert apply_cleanup_candidate(
            candidate, missing_proof(candidate), session_factory=lambda: Session(engine),
        ).status == "cleaned"
    finally:
        engine.dispose()

    engine = make_database()
    try:
        with Session(engine) as session:
            session.delete(session.get(TransferHistory, 20))
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        assert candidate.destination_paths == ()
        assert candidate.destination_lookup_complete is True
        assert apply_cleanup_candidate(
            candidate, missing_proof(candidate), session_factory=lambda: Session(engine),
        ).status == "cleaned"
        with Session(engine) as session:
            assert session.get(DownloadFiles, 1) is None
            assert session.get(DownloadHistory, 10) is None
    finally:
        engine.dispose()


def test_nonoverlapping_season_pack_does_not_block_exact_episode():
    """可证明不含 E04 的季包历史不能令全部单集候选永久待审。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, get_cleanup_protection

    engine = make_database()
    try:
        with Session(engine) as session:
            session.add_all([
                DownloadHistory(id=11, path="/downloads/pack", type="电视剧", title="诛仙", media_source="tmdb", media_id="123", seasons="S04", episodes="E01-E03", downloader="qb", download_hash="pack-early"),
                DownloadHistory(id=12, path="/downloads/pack", type="电视剧", title="诛仙", media_source="tmdb", media_id="123", seasons="S04", episodes="E06-E13", downloader="qb", download_hash="pack-later"),
            ])
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        protection = get_cleanup_protection(candidate, session_factory=lambda: Session(engine))
        assert protection.other_versions_complete is True
        assert protection.other_version_paths == ()
    finally:
        engine.dispose()


def test_overlapping_season_pack_requires_review():
    """可能包含目标 E04 但无法逐文件匹配的范围必须待审。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, get_cleanup_protection

    engine = make_database()
    try:
        with Session(engine) as session:
            session.add(DownloadHistory(id=11, path="/downloads/pack", type="电视剧", title="诛仙", media_source="tmdb", media_id="123", seasons="S04", episodes="E01-E13", downloader="qb", download_hash="pack-overlap"))
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        protection = get_cleanup_protection(candidate, session_factory=lambda: Session(engine))
        assert protection.other_versions_complete is False
    finally:
        engine.dispose()


def test_string_note_does_not_rewrite_unrelated_episode_types():
    """旧 note 含字符串集号时待审，不能顺手重写其他集的 JSON 类型。"""
    from cleanup_plugin.host_db import apply_cleanup_candidate, get_cleanup_candidate

    engine = make_database()
    try:
        with Session(engine) as session:
            session.get(Subscribe, 30).note = [4, "5", 13]
            session.commit()
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        result = apply_cleanup_candidate(candidate, missing_proof(candidate), allow_subscription_retry=True, resolved_lack_episode=11, session_factory=lambda: Session(engine))
        assert result.status == "needs_review"
        with Session(engine) as session:
            assert session.get(Subscribe, 30).note == [4, "5", 13]
            assert session.get(DownloadFiles, 1).state == 1
    finally:
        engine.dispose()


def test_resolver_uses_complete_season_view_after_exact_note_prune():
    """缺集数按目标季真实媒体集与删去 E04 后的 note 并集计算。"""
    from cleanup_plugin.host_db import get_cleanup_candidate, resolve_cleanup_lack_episode

    engine = make_database()
    try:
        candidate = get_cleanup_candidate(1, session_factory=lambda: Session(engine))
        assert candidate is not None
        present = (1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13)
        assert resolve_cleanup_lack_episode(candidate, present, session_factory=lambda: Session(engine)) == 14
        assert resolve_cleanup_lack_episode(candidate, present + (4,), session_factory=lambda: Session(engine)) is None
        assert resolve_cleanup_lack_episode(candidate, present, season_lookup_complete=False, session_factory=lambda: Session(engine)) is None
        with Session(engine) as session:
            session.get(Subscribe, 30).total_episode = 25
            session.commit()
        assert resolve_cleanup_lack_episode(candidate, present, session_factory=lambda: Session(engine)) is None
    finally:
        engine.dispose()


def test_application_facade_uses_injected_isolated_service():
    """插件公开入口使用隔离测试服务，生产则懒加载本插件 DB 端口。"""
    from cleanup_plugin.host_cleanup import (
        configure_cleanup_service,
        get_cleanup_candidate as public_get_candidate,
        get_cleanup_protection as public_get_protection,
        list_cleanup_candidates as public_list_candidates,
        repair_missing_destination as public_repair_destination,
        reset_cleanup_service,
    )
    from cleanup_plugin.host_db import SqlAlchemyCleanupService

    engine = make_database()
    try:
        configure_cleanup_service(SqlAlchemyCleanupService(session_factory=lambda: Session(engine)))
        rows = public_list_candidates(limit=10)
        assert len(rows) == 1 and rows[0].file_id == 1
        assert public_get_candidate(1) == rows[0]
        assert public_get_protection(rows[0]).source_exclusive is False
        assert public_get_protection(rows[0], include_source_ownership=True).source_exclusive is True
        assert public_repair_destination(
            rows[0], missing_destination_proof(rows[0]),
        ).transfer_deleted is True
    finally:
        reset_cleanup_service()
        engine.dispose()


def test_unknown_presence_requires_review():
    """任何源文件未知状态都不得成为自动删除凭据。"""
    proof = CleanupProof(
        source="unknown",
        destinations=("missing",),
        other_versions="missing",
        media_library="missing",
        media_lookup_complete=True,
        downloaders_complete=True,
        downloader_task_present=False,
        transfer_task_active=False,
        storage_ready=True,
        identity_verified=True,
        revision="revision-1",
        record_present=True,
    )
    assert proof_allows_cleanup(proof, revision="revision-1") is False


def test_empty_destinations_need_complete_lookup():
    """未整理记录可以处理，但必须证实目标枚举完整。"""
    proof = CleanupProof(
        source="missing",
        destinations=(),
        other_versions="missing",
        media_library="missing",
        media_lookup_complete=True,
        downloaders_complete=True,
        downloader_task_present=False,
        transfer_task_active=False,
        storage_ready=True,
        identity_verified=True,
        revision="revision-2",
        record_present=True,
        destination_lookup_complete=True,
    )
    assert proof_allows_cleanup(proof, revision="revision-2") is True
    assert proof_allows_cleanup(replace(proof, destination_lookup_complete=False), revision="revision-2") is False


if __name__ == "__main__":
    test_movie_candidate_and_protection_use_exact_media_identity()
    test_movie_full_missing_and_transfer_only_paths_are_separate()
    test_missing_destination_repair_preserves_download_and_subscription()
    test_missing_destination_repair_requires_exact_complete_external_absence()
    test_missing_destination_repair_without_transfer_keeps_everything()
    test_missing_destination_repair_rechecks_pending_and_durable_tasks()
    test_missing_destination_repair_rejects_changed_transfer_revision()
    test_transfer_status_change_invalidates_observation_revision()
    test_unknown_presence_requires_review()
    test_empty_destinations_need_complete_lookup()
    test_candidate_uses_exact_media_and_episode_identity()
    test_protection_finds_other_versions_and_parent_pending_task()
    test_nonlocal_transfer_and_unknown_sibling_storage_block_cleanup()
    test_source_purge_protection_rejects_shared_path_hash_and_transfer_references()
    test_source_purge_protection_rejects_stale_candidate()
    test_source_purge_protection_retains_old_inactive_same_path_record()
    test_source_purge_protection_rejects_other_download_history_same_source_path()
    test_cleanup_marks_only_target_and_keeps_subscription_when_retry_disabled()
    test_subscription_retry_needs_resolved_progress_before_any_write()
    test_subscription_without_target_note_cleans_without_progress_resolution()
    test_malformed_subscription_note_is_never_silently_classified_as_no_retry()
    test_subscription_retry_removes_only_confirmed_episode_and_updates_progress()
    test_same_hash_active_sibling_prevents_note_retry_and_rolls_back_file_state()
    test_exact_delete_preserves_legacy_inactive_sibling_and_parent_history()
    test_two_missing_versions_clean_separately_then_reopen_note_once()
    test_sqlite_write_lock_prevents_parent_and_note_race_in_both_commands()
    test_changed_path_and_present_source_leave_every_row_untouched()
    test_pending_task_recheck_blocks_stale_external_absence_claim()
    test_durable_transfer_history_is_never_auto_removed()
    test_settlement_revision_or_failed_transfer_is_not_legacy_cleanup()
    test_transfer_history_embedded_paths_must_match_exact_file()
    test_single_digit_legacy_episode_format_and_untransferred_file()
    test_nonoverlapping_season_pack_does_not_block_exact_episode()
    test_overlapping_season_pack_requires_review()
    test_string_note_does_not_rewrite_unrelated_episode_types()
    test_resolver_uses_complete_season_view_after_exact_note_prune()
    test_application_facade_uses_injected_isolated_service()
