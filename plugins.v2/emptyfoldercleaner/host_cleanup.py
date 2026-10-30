"""失效下载记录清理的纯业务合同与证据判定。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol


Presence = Literal["present", "missing", "unknown"]


@dataclass(frozen=True, slots=True)
class CleanupCandidate:
    """下载文件及其精确关联的只读清理候选。

    :param file_id: 下载文件行主键
    :param source_path: 该行登记的源文件路径
    :param download_hash: 下载任务 Hash
    :param downloader: 下载器名称
    :param destination_paths: 精确关联整理记录的目标路径
    :param destination_lookup_complete: 整理目标枚举是否完整
    :param media_source: 规范媒体数据来源
    :param media_id: 数据来源中的媒体 ID
    :param media_type: 媒体类型
    :param season: 严格解析的季号
    :param episodes: 严格解析的集号；自动清理仅处理单集
    :param storage: 源存储标识
    :param history_id: 精确关联的下载历史 ID
    :param transfer_id: 精确关联的旧整理历史 ID
    :param subscribe_id: 唯一关联的订阅 ID
    :param subscription_retry_needed: 已核验的订阅 note 是否精确含该集
    :param identity_verified: DB 内媒体身份与季集是否一致
    :param revision: 对关联数据库事实计算的不透明版本
    :param review_reason: 阻止自动清理的数据库歧义原因
    """

    file_id: int
    source_path: str
    download_hash: str
    downloader: str | None
    destination_paths: tuple[str, ...]
    destination_lookup_complete: bool
    media_source: str | None
    media_id: str | None
    media_type: str | None
    season: int | None
    episodes: tuple[int, ...]
    storage: str
    history_id: int | None
    transfer_id: int | None
    subscribe_id: int | None
    subscription_retry_needed: bool
    identity_verified: bool
    revision: str
    review_reason: str | None = None


@dataclass(frozen=True, slots=True)
class CleanupProtection:
    """候选同集的其他版本和持久任务保护视图。

    :param other_version_paths: 除本候选外登记的源与库文件路径
    :param other_versions_complete: 同集关联查询是否未截断且无歧义
    :param transfer_task_active: 同一路径或祖先目录有无持久整理任务
    :param source_exclusive: 源路径不被其他下载文件、整理历史或持久任务引用
    :param hash_exclusive: Hash 不被其他下载文件或媒体历史引用
    """

    other_version_paths: tuple[str, ...]
    other_versions_complete: bool
    transfer_task_active: bool | None
    source_exclusive: bool = False
    hash_exclusive: bool = False


@dataclass(frozen=True, slots=True)
class CleanupResult:
    """一次清理命令的提交结果。

    :param status: cleaned、needs_review、stale 或 not_found
    :param reason: 未变更或部分保留的原因
    :param file_marked_missing: 兼容结果字段，表示指定下载文件行已精确删除
    :param history_deleted: 父下载历史是否删除
    :param transfer_deleted: 旧整理历史是否删除
    :param note_updated: 订阅 note 是否移除该集
    :param subscription_refresh_needed: 是否需由主程序重算缺集数
    """

    status: Literal["cleaned", "needs_review", "stale", "not_found"]
    reason: str = ""
    file_marked_missing: bool = False
    history_deleted: bool = False
    transfer_deleted: bool = False
    note_updated: bool = False
    subscription_refresh_needed: bool = False


@dataclass(frozen=True, slots=True)
class CleanupProof:
    """一次完整观察所得的清理证据。

    :param source: 源文件存在性，只有 missing 可清理
    :param destinations: 每个已知库目标路径的存在性，顺序与候选一致
    :param other_versions: 同集其他已知版本的存在性
    :param media_library: 媒体服务器精确季集项的存在性
    :param media_lookup_complete: 媒体服务器查询是否完整且季集映射可信
    :param downloaders_complete: 所有相关下载器的任务列表是否成功读取
    :param downloader_task_present: 任意状态的下载任务是否仍存在
    :param transfer_task_active: 持久整理任务是否仍存在或活动
    :param storage_ready: 所有相关存储是否正常可查询
    :param identity_verified: 媒体身份与季集是否经过严格核验
    :param revision: 观察时的数据库候选版本
    :param record_present: 下载文件记录是否仍存在
    :param source_path: 观察的源文件路径
    :param destination_paths: 观察的目标文件路径，顺序与候选一致
    :param other_version_paths: 观察的其他版本文件路径
    :param destination_lookup_complete: 已知目标路径枚举是否完整
    """

    source: Presence
    destinations: tuple[Presence, ...]
    other_versions: Presence
    media_library: Presence
    media_lookup_complete: bool
    downloaders_complete: bool
    downloader_task_present: bool | None
    transfer_task_active: bool | None
    storage_ready: bool
    identity_verified: bool
    revision: str
    record_present: bool | None
    source_path: str = ""
    destination_paths: tuple[str, ...] = ()
    other_version_paths: tuple[str, ...] = ()
    destination_lookup_complete: bool = False


def proof_allows_cleanup(proof: CleanupProof, *, revision: str) -> bool:
    """仅在所有外部读取均完整且确认缺失时允许清理。"""
    return bool(
        revision
        and proof.revision == revision
        and proof.record_present is True
        and proof.source == "missing"
        and proof.destination_lookup_complete is True
        and all(status == "missing" for status in proof.destinations)
        and proof.other_versions == "missing"
        and proof.media_library == "missing"
        and proof.media_lookup_complete is True
        and proof.downloaders_complete is True
        and proof.downloader_task_present is False
        and proof.transfer_task_active is False
        and proof.storage_ready is True
        and proof.identity_verified is True
    )


def proof_allows_destination_repair(proof: CleanupProof, *, revision: str) -> bool:
    """仅确认源可用而精确库目标和媒体集均缺失时允许移除旧整理回执。"""
    return bool(
        revision
        and proof.revision == revision
        and proof.record_present is True
        and proof.source == "present"
        and bool(proof.destinations)
        and proof.destination_lookup_complete is True
        and all(status == "missing" for status in proof.destinations)
        and proof.other_versions == "missing"
        and proof.media_library == "missing"
        and proof.media_lookup_complete is True
        and proof.downloaders_complete is True
        and type(proof.downloader_task_present) is bool
        and proof.transfer_task_active is False
        and proof.storage_ready is True
        and proof.identity_verified is True
    )


class CleanupServicePort(Protocol):
    """宿主启动组合根注入的失效记录清理数据端口。"""

    def list_candidates(self, after_id: int = 0, limit: int = 100) -> list[CleanupCandidate]:
        """按下载文件主键有界扫描候选。"""
        ...

    def get_candidate(self, file_id: int) -> CleanupCandidate | None:
        """读取一条下载文件的精确候选。"""
        ...

    def get_protection(
        self,
        candidate: CleanupCandidate,
        limit: int = 500,
        include_source_ownership: bool = False,
    ) -> CleanupProtection:
        """读取同集其他版本与持久任务保护视图。"""
        ...

    def resolve_lack_episode(
        self,
        candidate: CleanupCandidate,
        present_episodes: tuple[int, ...],
        season_lookup_complete: bool = True,
    ) -> int | None:
        """按完整季视图计算候选清理后的缺集数。"""
        ...

    def apply(
        self,
        candidate: CleanupCandidate,
        proof: CleanupProof,
        *,
        allow_subscription_retry: bool = False,
        resolved_lack_episode: int | None = None,
    ) -> CleanupResult:
        """在同一事务中复核并提交单条候选清理。"""
        ...

    def repair_missing_destination(
        self,
        candidate: CleanupCandidate,
        proof: CleanupProof,
    ) -> CleanupResult:
        """源仍存在时，只移除精确缺失库目标的旧整理回执。"""
        ...


_configured_cleanup_service: CleanupServicePort | None = None


def configure_cleanup_service(service: CleanupServicePort) -> None:
    """由启动组合根登记本生命周期的清理服务。"""
    global _configured_cleanup_service
    _configured_cleanup_service = service


def reset_cleanup_service() -> None:
    """清除关停生命周期的清理服务引用。"""
    global _configured_cleanup_service
    _configured_cleanup_service = None


def _service() -> CleanupServicePort:
    """取得隔离测试端口，或按需构造插件自身的数据库适配端口。"""
    if _configured_cleanup_service is not None:
        return _configured_cleanup_service
    from .host_db import SqlAlchemyCleanupService

    return SqlAlchemyCleanupService()


def list_cleanup_candidates(after_id: int = 0, limit: int = 100) -> list[CleanupCandidate]:
    """供插件有界扫描下载文件候选。"""
    return _service().list_candidates(after_id, limit)


def get_cleanup_candidate(file_id: int) -> CleanupCandidate | None:
    """供插件读取一条数据库候选。"""
    return _service().get_candidate(file_id)


def get_cleanup_protection(
    candidate: CleanupCandidate,
    limit: int = 500,
    *,
    include_source_ownership: bool = False,
) -> CleanupProtection:
    """供插件读取同集保护；到期物理删除前可额外核验源独占。"""
    return _service().get_protection(candidate, limit, include_source_ownership)


def resolve_cleanup_lack_episode(
    candidate: CleanupCandidate,
    present_episodes: tuple[int, ...],
    *,
    season_lookup_complete: bool = True,
) -> int | None:
    """供插件按完整季实际媒体集解析可提交的缺集数。"""
    return _service().resolve_lack_episode(candidate, present_episodes, season_lookup_complete)


def apply_cleanup_candidate(
    candidate: CleanupCandidate,
    proof: CleanupProof,
    *,
    allow_subscription_retry: bool = False,
    resolved_lack_episode: int | None = None,
) -> CleanupResult:
    """供插件提交已二次确认缺失的候选，原子删除其精确文件行与孤立历史。"""
    return _service().apply(
        candidate, proof,
        allow_subscription_retry=allow_subscription_retry,
        resolved_lack_episode=resolved_lack_episode,
    )


def repair_missing_destination(
    candidate: CleanupCandidate,
    proof: CleanupProof,
) -> CleanupResult:
    """供插件以完整证据原子移除失效整理回执，保留下载与订阅事实。"""
    return _service().repair_missing_destination(candidate, proof)


__all__ = [
    "CleanupCandidate", "CleanupProof", "CleanupProtection", "CleanupResult",
    "CleanupServicePort", "apply_cleanup_candidate", "configure_cleanup_service",
    "get_cleanup_candidate", "get_cleanup_protection", "list_cleanup_candidates",
    "proof_allows_cleanup", "proof_allows_destination_repair", "repair_missing_destination",
    "reset_cleanup_service", "resolve_cleanup_lack_episode",
]
