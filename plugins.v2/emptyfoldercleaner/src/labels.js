export const statusNames = {
 queued:'等待开始',running:'正在运行',cancelling:'正在停止',cancelled:'已停止',completed:'已完成',
 review:'已完成，部分保留复核',yielded:'本批达到处理上限，下轮继续',failed:'运行失败',interrupted:'上次运行已中断',disabled:'未启用',
 applied:'已处理',eligible:'预览可处理',waiting:'观察中',needs_review:'需复核',skipped:'已保留',already_clean:'已处理过',
}
export const branchNames = {file_only:'失效文件记录',related_history:'关联历史',related_transfer:'失效整理记录',orphan_history:'旧孤儿下载历史',orphan_transfer:'旧孤儿整理历史',source_purge:'残留源文件',residual_task:'残留下载任务'}
export const reasons = {
 stability_window:'缺失情况正在观察，达到确认时间后再次核对。',
 confirmed_missing_dry_run:'缺失情况符合条件，当前仅预览，文件、任务与记录均未删除。',
 source_present:'源文件仍然有效，继续保留。',source_missing:'源文件已不存在，继续核对准确关联的记录。',
 destination_present:'已知入库文件仍在，保留入库文件及有效整理历史。',
 other_version_present:'还有有效版本，保留仍被使用的数据。',media_library_present:'媒体库仍存在此资源，保留相关数据。',
 downloader_task_present:'下载器中仍有任务，等待确认任务归属与状态。',transfer_task_active:'正在整理，继续保留。',
 presence_unknown:'文件状态暂时无法确认，保留并等待恢复。',storage_unavailable:'存储尚未就绪，保留文件与记录。',
 downloaders_incomplete:'下载器状态未完整取得，保留并等待重试。',media_lookup_incomplete:'媒体身份或范围尚未完整确认，保留复核。',
 destination_lookup_incomplete:'入库关联尚未完整确认，保留复核。',task_lookup_incomplete:'任务状态尚未完整确认，保留复核。',identity_unverified:'资源身份或归属尚不能准确对应，保留复核。',
 invalid_record:'记录路径或资源身份存在不确定信息，保留复核。',record_changed:'记录已经变化，等待重新核对。',
 observation_failed:'本项读取失败，保留并在后续核对中重试。',record_lookup_incomplete:'关联记录读取不完整，保留并等待重试。',
 record_absent:'这条记录已经不存在。',already_applied:'这条记录已经处理过。',records_updated:'准确关联的失效记录已清理。',
 source_purged:'确认失效的源文件、独占任务和对应记录已清理。',source_shared_by_task:'源文件被其他任务使用，保留文件与任务。',
 torrent_not_exclusive_single_file:'下载任务还有其他文件或无法证明独占，保留任务及共享文件。',torrent_file_incomplete:'文件尚未完整下载或大小不一致，继续保留。',
 task_inventory_incomplete:'下载任务清单读取不完整，保留并等待恢复。',file_inventory_incomplete:'任务文件清单读取不完整，保留并等待恢复。',
 source_signature_unverified:'源文件身份不能稳定确认，保留复核。',source_signature_unavailable:'暂时无法读取源文件身份，保留复核。',
 source_purge_evidence_changed:'执行期间文件或归属发生变化，已停止后续清理，等待复核。',downloader_remove_unconfirmed:'任务移除尚未全部确认，未继续删除文件。',
 source_unlink_unconfirmed:'源文件删除结果尚未确认，保留记录等待恢复处理。',source_purged_history_pending:'源文件已删除，剩余记录会继续核对处理。',
 source_purge_failed:'清理可能部分完成，保留后续处理入口等待恢复。',source_intent_store_failed:'恢复回执保存失败，已停止后续清理。',
 source_intent_revision_changed:'恢复回执与当前文件或记录不一致，保留复核。',cleanup_cancelled:'已请求停止，保留已完成结果和恢复位置。',source_changed:'源文件已变化，重新观察缺失状态。',
 receipt_capacity_wait:'关联恢复队列已达到上限，保留此记录，等待已有条目处理后继续。',
 association_snapshot_unavailable:'无法完整保存此文件的关联关系，保留文件记录等待复核。',
 new_or_changed_reference:'关联记录已新增或发生变化，保留并重新核对。',
 related_records_absent:'准确关联的历史已经不存在。',
 association_unverified:'尚未证明关联历史及文件归属完全一致，继续保留。',
 related_file_present:'源文件或入库文件仍正常存在，保留仍被使用的历史。',
 source_shared_or_signature_unavailable:'源文件还有其他硬链接，或文件身份暂时无法确认，继续保留。',
 task_absent:'对应下载任务已不存在，无需移除。',
 task_evidence_changed:'任务归属或运行状态发生变化，已停止该项操作。',
 residual_task_removed:'准确归属且已完成的残留下载任务已移除，未请求下载器删除目录。',
 related_evidence_changed:'关联文件、历史或任务状态发生变化，已保留等待重新核对。',
 missing_file_row_removed:'源路径确认缺失，已清理这条失效文件记录；有效入库文件与历史保留。',
 download_file_pruned:'源路径确认缺失，已清理这条失效文件记录。',
 related_history_removed:'已清理确认失效且无其他引用的关联历史。',
 task_moving:'下载任务正在搬移或分配文件，暂时保留。',task_checking:'下载任务正在校验，暂时保留。',
 task_queued_download:'任务仍在等待下载，继续保留。',task_downloading:'任务仍在下载，继续保留。',
 task_download_incomplete:'任务尚未完成或文件报错，继续保留。',task_state_unknown:'下载任务的运行状态无法确认，暂时保留。',
 source_shared_reference:'文件仍被其他记录引用，保留文件与关联数据。',conflicting_parent_histories:'关联下载历史存在真实冲突，保留等待复核。',
 unsafe_transfer_mapping:'整理映射未成功或无法精确确认，保留文件与历史。',transfer_mapping_changed:'整理映射发生变化，需要重新核对。',
 related_files_still_registered:'仍有同一任务的文件记录，继续保留关联历史。',receipt_signature_mismatch:'关联恢复回执的校验不一致，停止此项操作。',
 group_path_limit:'单任务关联路径超过本批安全上限，保留等待分批处理。',
 stale_transfer_removed:'已清理准确失效的整理记录，有效文件与其他记录保留。',
 transfer_records_absent:'对应整理记录已经不存在，无需处理。',
 transfer_evidence_incomplete:'整理记录关联证据不完整，保留等待复核。',
 transfer_evidence_changed:'整理记录关联证据已经变化，保留重新核对。',
 orphan_mapping_incomplete:'旧孤儿记录缺少完整且成功的文件映射，保留等待复核。',
 orphan_downloader_unverified:'旧整理记录的下载器归属无法唯一确认，保留等待复核。',
 destination_reappeared:'目标文件已经重新出现，停止清理并保留有效数据。',
}
export function errorText(code) {
 if (['batch_time_limit','batch_path_budget','batch_host_budget'].includes(code)) return '本批达到处理上限，已保留完成结果与继续位置，下轮继续核对。'
 const names={record_run_failed:'本次核对未能完成，请确认存储和下载器可用后重新核对。',
 receipt_store_failed:'恢复回执保存或回读失败，已停止后续操作。请恢复插件数据存储后重新核对。',
 candidate_read_failed:'文件记录读取不完整，未推进扫描位置。请恢复服务后重新核对。',
 invalid_record_state:'已保存的运行状态无法验证，已停止清理。请检查插件数据存储，保留现有数据后重试。',
 receipt_ledger_size_limit:'关联恢复队列达到容量上限，未继续清理。请检查保留条目的具体原因。',
 record_state_save_failed:'扫描状态保存失败，请恢复插件数据存储后重新核对。',
 source_intent_store_failed:'文件清理恢复回执保存失败，请恢复插件数据存储后重新核对。'}
 return names[code]||'本次核对未能完成。已完成的结果保留，恢复相关服务后可再次核对。'
}
export function reasonText(item) { return reasons[item.reason] || item.message || '保护条件尚未满足，已保留等待后续核对。' }
export function number(value) { return typeof value === 'number' && Number.isFinite(value) ? value.toLocaleString('zh-CN') : '—' }
export function date(value) {
 if (!value) return '尚无记录'
 if (typeof value === 'string' && !/^\d+(\.\d+)?$/.test(value)) return value
 return new Date(Number(value) * 1000).toLocaleString('zh-CN', { hour12:false })
}
