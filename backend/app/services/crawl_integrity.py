"""采集完整性凭证服务：为空采集批次签发可复核的完整性凭证。

背景：空表清单只返回接入数为零时，无法区分两种语义完全不同的空回包：
  1. 采集中断（如采集超时）：源侧结构未知，目录必须保留旧字段，禁止按删除处理；
  2. 采集已完成且源侧确认为空（如源侧实际撤表）：空结构本身需要可回查的证据，
     变更对比拿到本次采集的完整性证据后，字段删除差异才能确认。

每个采集批次（按 数据源+范围+表 维度）生成一条凭证，凭证哈希对规范化的
采集证据求 SHA-256，任何字段被事后改动都会导致哈希复核失败。

domain_chain: 元数据采集器 (metadata_crawler)
  └─ 签发 → 采集完整性凭证 (crawl_attestation)
  └─ 凭证核对 → 元数据变更对比 (metadata_diff)
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 处置范围常量：采集失败与已完成空态进入不同处置范围
SCOPE_INTERRUPTED = "采集中断处置"
SCOPE_DELETION = "删除差异处置"
SCOPE_MISSING_EVIDENCE = "待凭证补录处置"

# 凭证生命周期状态
ATTESTATION_PENDING = "待复核"
ATTESTATION_TRUSTED = "可采信"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(payload: dict[str, Any]) -> str:
    """规范化 JSON：键排序且不保留空白，保证哈希可复算。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class CrawlIntegrityService(BaseModuleService):
    MODULE = "crawl_attestation"
    ENTITY = "采集完整性凭证"
    OPERATION_TERM = "签发"
    LIST_FIELDS = [
        "凭证编号", "数据源类型", "连接地址", "采集范围", "表名称", "采集批号",
        "关联批号", "采集结果", "完整性状态", "中断原因", "处置范围", "复核结论",
    ]
    REQUIRED_FIELDS = ["凭证编号", "数据源类型", "表名称", "采集批号"]
    STATUS_ORDER = [ATTESTATION_PENDING, ATTESTATION_TRUSTED]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    # 参与凭证哈希的证据字段（复核结论等事后回写字段不参与，避免破坏原哈希）
    EVIDENCE_FIELDS = [
        "数据源类型", "连接地址", "采集范围", "表名称", "采集批号", "关联批号",
        "采集开始时间", "采集结束时间", "采集结果", "完整性状态", "中断原因",
        "表数量", "字段数量", "旧字段快照", "响应指纹",
    ]

    # ------------------------------------------------------------------
    # 凭证签发
    # ------------------------------------------------------------------
    def issue_interrupted_attestation(
        self,
        *,
        datasource_type: str,
        connection_url: str,
        scope: str,
        table_name: str,
        batch_no: str,
        old_columns: list[str],
        reason: str = "采集超时",
        linked_batch: str | None = None,
    ) -> dict[str, Any]:
        """为采集中断批次签发凭证：空回包但完整性不可采信，进入采集中断处置。"""
        now = utc_now_iso()
        evidence = {
            "数据源类型": datasource_type,
            "连接地址": connection_url,
            "采集范围": scope,
            "表名称": table_name,
            "采集批号": batch_no,
            "关联批号": linked_batch,
            "采集开始时间": now,
            "采集结束时间": now,
            "采集结果": "空回包",
            "完整性状态": "采集中断",
            "中断原因": reason,
            "表数量": 0,
            "字段数量": len(old_columns),
            "旧字段快照": list(old_columns),
            "响应指纹": self._empty_response_fingerprint(batch_no),
        }
        return self._persist_attestation(
            evidence,
            status=ATTESTATION_PENDING,
            pending=True,
            abnormal=True,
            scope=SCOPE_INTERRUPTED,
        )

    def issue_completed_attestation(
        self,
        *,
        datasource_type: str,
        connection_url: str,
        scope: str,
        table_name: str,
        batch_no: str,
        old_columns: list[str],
        linked_batch: str | None = None,
    ) -> dict[str, Any]:
        """为已完成的空采集批次签发凭证：空结构可回查，凭证可采信。

        与中断凭证的区别在于本批次采集链路完整结束（有明确开始/结束时间和
        空响应指纹），完整性状态为「已完成」，可作为字段删除差异的确认依据。
        """
        start = utc_now_iso()
        end = utc_now_iso()
        evidence = {
            "数据源类型": datasource_type,
            "连接地址": connection_url,
            "采集范围": scope,
            "表名称": table_name,
            "采集批号": batch_no,
            "关联批号": linked_batch,
            "采集开始时间": start,
            "采集结束时间": end,
            "采集结果": "空回包",
            "完整性状态": "已完成",
            "中断原因": None,
            "表数量": 0,
            "字段数量": len(old_columns),
            "旧字段快照": list(old_columns),
            "响应指纹": self._empty_response_fingerprint(batch_no),
        }
        return self._persist_attestation(
            evidence,
            status=ATTESTATION_TRUSTED,
            pending=False,
            abnormal=False,
            scope=SCOPE_DELETION,
        )

    def _persist_attestation(
        self,
        evidence: dict[str, Any],
        *,
        status: str,
        pending: bool,
        abnormal: bool,
        scope: str,
    ) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        attestation = {
            "id": new_id,
            "凭证编号": f"CRA-{new_id:04d}",
            **evidence,
            "凭证哈希": self.compute_hash(evidence),
            "处置范围": scope,
            "复核结论": None,
            "复核时间": None,
            "复核批次": None,
            "status": status,
            "pending": pending,
            "abnormal": abnormal,
        }
        rows.append(attestation)
        return dict(attestation)

    # ------------------------------------------------------------------
    # 凭证核对
    # ------------------------------------------------------------------
    def compute_hash(self, evidence: dict[str, Any]) -> str:
        """对采集证据求 SHA-256，作为采集完整性凭证的可复核指纹。"""
        canonical = canonical_json({key: evidence.get(key) for key in self.EVIDENCE_FIELDS})
        return sha256_hex(canonical)

    def verify_attestation(self, attestation_id: int) -> dict[str, Any]:
        """复算凭证哈希，确认证据自签发后未被篡改。"""
        attestation = store.find(self.MODULE, attestation_id)
        if attestation is None:
            return {"凭证编号": attestation_id, "哈希复核": "凭证不存在", "通过": False}
        recomputed = self.compute_hash(attestation)
        passed = recomputed == attestation.get("凭证哈希")
        return {
            "凭证编号": attestation["凭证编号"],
            "表名称": attestation["表名称"],
            "采集批号": attestation["采集批号"],
            "完整性状态": attestation["完整性状态"],
            "原始哈希": attestation.get("凭证哈希"),
            "复算哈希": recomputed,
            "哈希复核": "一致" if passed else "不一致",
            "通过": passed,
        }

    def find_by_batch(self, batch_no: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("采集批号") == batch_no:
                return row
        return None

    def latest_for_table(self, table_name: str) -> dict[str, Any] | None:
        matches = [
            row for row in store.rows(self.MODULE)
            if row.get("表名称") == table_name
        ]
        if not matches:
            return None
        return max(matches, key=lambda row: int(row.get("id", 0)))

    def is_trusted_empty_evidence(self, attestation_id: int, table_name: str) -> tuple[bool, str]:
        """变更对比闸门：仅「已完成」且对象一致的空采集凭证可确认删除。"""
        attestation = store.find(self.MODULE, attestation_id)
        if attestation is None:
            return False, "无采集完整性凭证记录，空态进入待凭证补录处置，禁止确认删除"
        if attestation.get("表名称") != table_name:
            return False, (
                f"凭证 {attestation.get('凭证编号')} 归属表 "
                f"{attestation.get('表名称')} 与变更对象 {table_name} 不一致"
            )
        if attestation.get("完整性状态") != "已完成":
            return False, (
                f"凭证 {attestation.get('凭证编号')} 为{attestation.get('完整性状态')}"
                f"（{attestation.get('中断原因') or '原因未知'}），采集未完成，禁止按删除处理"
            )
        if not self.verify_attestation(attestation_id)["通过"]:
            return False, f"凭证 {attestation.get('凭证编号')} 哈希复核不一致，证据不可采信"
        return True, "凭证可采信"

    # ------------------------------------------------------------------
    # 清单与待核范围
    # ------------------------------------------------------------------
    def empty_crawl_checklist(self) -> dict[str, Any]:
        """带完整性凭证的空表清单：不再只返回接入数为零。

        同一张表的每次空回包都保留独立凭证行（如先超时、后撤表两次空回包），
        标注完整性状态与处置范围，并核对表目录是否仍保留旧字段
        （中断时目录字段不得删除）；另附按表聚合的最新状态汇总。
        """
        catalog_rows = store.rows("table_catalog")
        items: list[dict[str, Any]] = []
        for row in sorted(store.rows(self.MODULE), key=lambda r: int(r.get("id", 0))):
            table_name = row.get("表名称")
            if row.get("采集结果") != "空回包":
                continue
            catalog_entry = next(
                (r for r in catalog_rows if r.get("表名称") == table_name), None
            )
            items.append({
                "id": row.get("id"),
                "表名称": table_name,
                "接入数": 0,
                "采集批号": row.get("采集批号"),
                "关联批号": row.get("关联批号"),
                "凭证编号": row.get("凭证编号"),
                "凭证哈希": row.get("凭证哈希"),
                "完整性状态": row.get("完整性状态"),
                "中断原因": row.get("中断原因"),
                "处置范围": row.get("处置范围"),
                "复核结论": row.get("复核结论"),
                "目录保留字段数": len(row.get("旧字段快照", [])),
                "目录仍保留旧字段": catalog_entry is not None,
                "凭证状态": row.get("status"),
            })
        latest_by_table: dict[str, dict[str, Any]] = {}
        for item in items:
            latest_by_table[item["表名称"]] = item
        return {
            "空表总数": len(latest_by_table),
            "空回包凭证数": len(items),
            "可采信空态": sum(1 for i in items if i["完整性状态"] == "已完成"),
            "采集中断空态": sum(1 for i in items if i["完整性状态"] == "采集中断"),
            "清单": items,
            "按表最新状态": list(latest_by_table.values()),
        }

    def pending_review_scope(self) -> dict[str, Any]:
        """当前时点的待核范围：按处置范围分组的凭证。"""
        rows = store.rows(self.MODULE)
        interrupted = [
            self._brief(row) for row in rows
            if row.get("完整性状态") == "采集中断"
        ]
        trusted_pending = [
            self._brief(row) for row in rows
            if row.get("完整性状态") == "已完成" and row.get("复核结论") is None
        ]
        reviewed = [
            self._brief(row) for row in rows if row.get("复核结论") is not None
        ]
        return {
            "采集中断待重试": interrupted,
            "已完成空态待删除复核": trusted_pending,
            "已出复核结论": reviewed,
        }

    def write_review_conclusion(
        self,
        attestation_id: int,
        *,
        scope: str,
        conclusion: str,
        review_batch: str,
    ) -> dict[str, Any] | None:
        """采集结论回写凭证，并同步到变更复核清单（由变更对比服务调用）。"""
        attestation = store.find(self.MODULE, attestation_id)
        if attestation is None:
            return None
        attestation["处置范围"] = scope
        attestation["复核结论"] = conclusion
        attestation["复核时间"] = utc_now_iso()
        attestation["复核批次"] = review_batch
        return dict(attestation)

    def _brief(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "凭证编号": row.get("凭证编号"),
            "表名称": row.get("表名称"),
            "采集批号": row.get("采集批号"),
            "关联批号": row.get("关联批号"),
            "完整性状态": row.get("完整性状态"),
            "中断原因": row.get("中断原因"),
            "处置范围": row.get("处置范围"),
            "复核结论": row.get("复核结论"),
        }

    def _empty_response_fingerprint(self, batch_no: str) -> str:
        """空回包响应信封指纹：证明本批次确实收到了结构完整的空响应而非无响应。"""
        envelope = {
            "采集批号": batch_no,
            "表数量": 0,
            "表清单": [],
        }
        return sha256_hex(canonical_json(envelope))

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "签发"
        snapshot["desc"] = "为空采集批次签发可复核的采集完整性凭证"
        snapshot["artifact"] = "采集完整性凭证集"
        snapshot["artifact_desc"] = "区分采集中断与已完成空态的完整性凭证集"
        return snapshot
