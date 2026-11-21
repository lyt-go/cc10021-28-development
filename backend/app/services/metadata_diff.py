"""元数据变更对比服务：检测和记录表结构变更，驱动血缘图谱更新。

domain_chain: 元数据变更对比 (metadata_diff)
  └─ 校验 → 血缘图谱更新 (lineage_graph)
  └─ 关联 → 质量事件 (dq_incident)
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.crawl_integrity import (
    CrawlIntegrityService,
    SCOPE_INTERRUPTED,
    SCOPE_DELETION,
    SCOPE_MISSING_EVIDENCE,
)
from app.store import store


class MetadataDiffService(BaseModuleService):
    MODULE = "metadata_diff"
    ENTITY = "变更记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["变更编号", "变更对象", "变更类型", "旧值", "新值", "变更批次", "影响评估", "变更时间"]
    REQUIRED_FIELDS = ["变更编号", "变更对象", "变更类型"]
    STATUS_ORDER = ["草稿", "待审核", "已确认", "已回滚"]
    ACTION_RULES = {"提交审核": "待审核", "确认变更": "已确认", "执行回滚": "已回滚"}
    NEGATIVE_ACTIONS = []

    def compare_schemas(self, old_columns: list[dict], new_columns: list[dict],
                         table_name: str, attestation_id: int | None = None) -> dict[str, Any]:
        """对比两次元数据采集的表结构差异，生成变更记录。

        解析新老字段列表，识别新增、删除、类型变更等差异，
        校验变更影响范围并写入变更记录，触发血缘图谱的更新。

        字段删除必须核对采集完整性凭证：仅当凭证为「已完成」空采集证据时，
        删除差异才进入正常待审核流程并触发血缘影响校验；无凭证或凭证来自
        采集中断时，删除差异一律挂起（旧字段保留），不产生下游删除影响。
        """
        old_map = {c.get("字段名称"): c for c in old_columns if c.get("字段名称")}
        new_map = {c.get("字段名称"): c for c in new_columns if c.get("字段名称")}
        old_names = set(old_map.keys())
        new_names = set(new_map.keys())

        additions = new_names - old_names
        deletions = old_names - new_names
        common = old_names & new_names

        changes = []
        # 识别新增字段
        for col_name in additions:
            changes.append({"字段": col_name, "变更类型": "新增",
                            "旧值": "N/A",
                            "新值": f"类型:{new_map[col_name].get('数据类型', 'unknown')}"})
        # 识别删除字段
        for col_name in deletions:
            changes.append({"字段": col_name, "变更类型": "删除",
                            "旧值": f"类型:{old_map[col_name].get('数据类型', 'unknown')}",
                            "新值": "N/A"})
        # 识别类型变更
        for col_name in common:
            old_type = old_map[col_name].get("数据类型", "")
            new_type = new_map[col_name].get("数据类型", "")
            if old_type != new_type:
                changes.append({"字段": col_name, "变更类型": "类型变更",
                                "旧值": old_type, "新值": new_type})

        batch_hash = hashlib.md5(f"{table_name}:{datetime.now(timezone.utc).isoformat()}".encode()).hexdigest()[:8]

        # 采集完整性凭证闸门：删除差异只有拿到本次采集的完整性证据才能确认
        integrity = CrawlIntegrityService()
        gate_ok = False
        gate_reason = "不涉及字段删除"
        trusted_attestation = None
        if deletions:
            if attestation_id is None:
                gate_reason = "缺少采集完整性凭证，已完成但没有记录的空态进入待凭证补录处置"
            else:
                gate_ok, gate_reason = integrity.is_trusted_empty_evidence(attestation_id, table_name)
                if gate_ok:
                    trusted_attestation = store.find(integrity.MODULE, attestation_id)
        suspended_deletions: list[str] = []
        confirmed_deletions: list[str] = []

        # 写入变更记录
        diff_rows = store.rows(self.MODULE)
        for change in changes:
            new_did = max((int(r.get("id", 0)) for r in diff_rows), default=0) + 1
            is_deletion = change["变更类型"] == "删除"
            if is_deletion and not gate_ok:
                suspended_deletions.append(change["字段"])
                entry_status = "待审核"
                entry_pending = True
                entry_abnormal = True
                attest_check = "挂起"
                review_scope = (
                    SCOPE_INTERRUPTED
                    if attestation_id is not None
                    else SCOPE_MISSING_EVIDENCE
                )
                hold_reason = gate_reason
            elif is_deletion:
                confirmed_deletions.append(change["字段"])
                entry_status = "待审核"
                entry_pending = True
                entry_abnormal = True
                attest_check = "通过"
                review_scope = SCOPE_DELETION
                hold_reason = ""
            else:
                entry_status = "已确认"
                entry_pending = False
                entry_abnormal = False
                attest_check = "不涉及"
                review_scope = ""
                hold_reason = ""
            diff_rows.append({
                "id": new_did,
                "变更编号": f"META-{new_did:04d}",
                "变更对象": f"{table_name}.{change['字段']}",
                "变更类型": change["变更类型"],
                "旧值": change["旧值"],
                "新值": change["新值"],
                "变更批次": batch_hash,
                "影响评估": self._assess_impact(change["变更类型"], table_name, change["字段"]),
                "变更时间": datetime.now(timezone.utc).isoformat(),
                "凭证核对": attest_check,
                "完整性凭证": trusted_attestation["凭证编号"] if (is_deletion and trusted_attestation) else None,
                "复核范围": review_scope,
                "挂起原因": hold_reason,
                "复核结论": None,
                "status": entry_status,
                "pending": entry_pending,
                "abnormal": entry_abnormal,
            })

        # 仅凭证核对通过的删除才触发血缘影响；挂起删除保留旧字段，不产生下游影响
        if confirmed_deletions:
            self._validate_lineage_impact(table_name, confirmed_deletions)
            if trusted_attestation is not None:
                integrity.write_review_conclusion(
                    int(trusted_attestation["id"]),
                    scope=SCOPE_DELETION,
                    conclusion=f"删除差异已凭凭证 {trusted_attestation['凭证编号']} 确认，进入血缘影响复核",
                    review_batch=batch_hash,
                )

        return {
            "表名称": table_name,
            "旧字段数": len(old_columns),
            "新字段数": len(new_columns),
            "变更总数": len(changes),
            "新增字段": list(additions),
            "删除字段": list(deletions),
            "类型变更": [c for c in changes if c["变更类型"] == "类型变更"],
            "变更批次": batch_hash,
            "凭证核对": "通过" if gate_ok else ("挂起" if deletions else "不涉及"),
            "删除确认": confirmed_deletions,
            "删除挂起": suspended_deletions,
            "挂起原因": gate_reason if suspended_deletions else "",
        }

    def review_crawl_emptiness(self) -> dict[str, Any]:
        """采集结论回写到变更复核清单。

        采集失败（中断）与已完成但没有记录的空态进入不同处置范围：
        - 采集中断：凭证挂起，目录保留原字段，不产生下游删除影响，等待重试；
        - 已完成空态：凭证可采信，旧字段按删除差异登记并触发血缘影响校验。
        已写过复核结论的凭证不重复登记。
        """
        integrity = CrawlIntegrityService()
        attestation_rows = store.rows(integrity.MODULE)
        interrupted_records: list[dict[str, Any]] = []
        confirmed_records: list[dict[str, Any]] = []
        skipped: list[str] = []

        for attestation in attestation_rows:
            if attestation.get("采集结果") != "空回包":
                continue
            if attestation.get("复核结论") is not None:
                skipped.append(attestation["凭证编号"])
                continue
            if attestation.get("完整性状态") == "采集中断":
                interrupted_records.append(self._record_interrupted_review(attestation))
            elif attestation.get("完整性状态") == "已完成":
                confirmed_records.extend(self._record_completed_empty_review(attestation))

        return {
            "本次登记中断批次": len(interrupted_records),
            "本次登记删除差异": len(confirmed_records),
            "已复核跳过": skipped,
            "采集中断处置": interrupted_records,
            "删除差异处置": confirmed_records,
        }

    def _record_interrupted_review(self, attestation: dict[str, Any]) -> dict[str, Any]:
        """中断批次：挂起删除，保留目录旧字段，结论回写凭证与复核清单。"""
        table_name = attestation["表名称"]
        batch_hash = self._review_batch_hash(attestation)
        diff_rows = store.rows(self.MODULE)
        new_did = max((int(r.get("id", 0)) for r in diff_rows), default=0) + 1
        old_snapshot = attestation.get("旧字段快照", [])
        conclusion = (
            f"采集{attestation.get('中断原因') or '中断'}，完整性证据缺失，"
            f"目录保留{len(old_snapshot)}个旧字段，挂起删除并等待重试"
        )
        entry = {
            "id": new_did,
            "变更编号": f"META-{new_did:04d}",
            "变更对象": table_name,
            "变更类型": "删除",
            "旧值": f"旧字段数:{len(old_snapshot)}",
            "新值": "空回包(采集未完成)",
            "变更批次": batch_hash,
            "影响评估": "暂不评估：采集中断，旧字段保留，未产生下游删除影响",
            "变更时间": datetime.now(timezone.utc).isoformat(),
            "凭证核对": "挂起",
            "完整性凭证": attestation["凭证编号"],
            "复核范围": SCOPE_INTERRUPTED,
            "挂起原因": attestation.get("中断原因") or "采集中断",
            "复核结论": conclusion,
            "status": "待审核",
            "pending": True,
            "abnormal": True,
        }
        diff_rows.append(entry)
        CrawlIntegrityService().write_review_conclusion(
            int(attestation["id"]),
            scope=SCOPE_INTERRUPTED,
            conclusion=conclusion,
            review_batch=batch_hash,
        )
        return dict(entry)

    def _record_completed_empty_review(self, attestation: dict[str, Any]) -> list[dict[str, Any]]:
        """已完成空态：凭完整性凭证确认删除差异，并触发下游血缘影响。"""
        table_name = attestation["表名称"]
        batch_hash = self._review_batch_hash(attestation)
        diff_rows = store.rows(self.MODULE)
        old_snapshot = list(attestation.get("旧字段快照", []))
        targets = old_snapshot or [f"{table_name}（整表撤表）"]
        entries: list[dict[str, Any]] = []
        for field_name in targets:
            new_did = max((int(r.get("id", 0)) for r in diff_rows), default=0) + 1
            entry = {
                "id": new_did,
                "变更编号": f"META-{new_did:04d}",
                "变更对象": f"{table_name}.{field_name}",
                "变更类型": "删除",
                "旧值": "目录登记字段",
                "新值": "N/A",
                "变更批次": batch_hash,
                "影响评估": self._assess_impact("删除", table_name, field_name),
                "变更时间": datetime.now(timezone.utc).isoformat(),
                "凭证核对": "通过",
                "完整性凭证": attestation["凭证编号"],
                "复核范围": SCOPE_DELETION,
                "挂起原因": "",
                "复核结论": (
                    f"凭证 {attestation['凭证编号']} 证明空结构采集已完成，"
                    "删除差异确认，转血缘完整性复核"
                ),
                "status": "待审核",
                "pending": True,
                "abnormal": True,
            }
            diff_rows.append(entry)
            entries.append(dict(entry))

        # 下游接到删除影响：仅已完成空态触发血缘校验
        self._validate_lineage_impact(table_name, targets)
        CrawlIntegrityService().write_review_conclusion(
            int(attestation["id"]),
            scope=SCOPE_DELETION,
            conclusion=(
                f"凭证 {attestation['凭证编号']} 证明空结构采集已完成，"
                f"确认{len(targets)}项删除差异并触发血缘影响校验"
            ),
            review_batch=batch_hash,
        )
        return entries

    def pending_review_list(self) -> dict[str, Any]:
        """变更复核清单的待核范围：按处置范围分组。"""
        diff_rows = store.rows(self.MODULE)
        pending_rows = [row for row in diff_rows if row.get("pending")]
        return {
            "待核总数": len(pending_rows),
            SCOPE_INTERRUPTED: [
                self._review_brief(row) for row in pending_rows
                if row.get("复核范围") == SCOPE_INTERRUPTED
            ],
            SCOPE_DELETION: [
                self._review_brief(row) for row in pending_rows
                if row.get("复核范围") == SCOPE_DELETION
            ],
            SCOPE_MISSING_EVIDENCE: [
                self._review_brief(row) for row in pending_rows
                if row.get("复核范围") == SCOPE_MISSING_EVIDENCE
            ],
        }

    def _review_brief(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "变更编号": row.get("变更编号"),
            "变更对象": row.get("变更对象"),
            "变更类型": row.get("变更类型"),
            "变更批次": row.get("变更批次"),
            "凭证核对": row.get("凭证核对"),
            "完整性凭证": row.get("完整性凭证"),
            "复核范围": row.get("复核范围"),
            "挂起原因": row.get("挂起原因"),
            "复核结论": row.get("复核结论"),
        }

    def _review_batch_hash(self, attestation: dict[str, Any]) -> str:
        raw = f"{attestation['凭证编号']}:{attestation['采集批号']}"
        return hashlib.md5(raw.encode()).hexdigest()[:8]

    def _assess_impact(self, change_type: str, table_name: str, field_name: str) -> str:
        """评估变更影响范围。"""
        if change_type == "删除":
            return f"高风险：可能影响下游依赖{table_name}的表，需校验血缘完整性"
        elif change_type == "类型变更":
            return f"中风险：可能影响ETL转换逻辑和数据写入"
        else:
            return "低风险：新增字段不影响已有逻辑"

    def _validate_lineage_impact(self, table_name: str, deleted_fields: list[str]) -> None:
        """校验血缘完整性：检测删除字段是否影响下游表依赖。"""
        lineage_rows = store.rows("lineage_graph")
        affected = [
            r for r in lineage_rows
            if r.get("上游表名") == table_name and r.get("status") == "已生效"
        ]
        for lr in affected:
            lr["abnormal"] = True
            lr["status"] = "待校验"

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "校验"
        snapshot["desc"] = "对比元数据schema差异并校验血缘影响"
        snapshot["artifact"] = "变更对比记录数据集"
        snapshot["artifact_desc"] = "含影响评估的变更对比记录数据集"
        return snapshot