"""元数据采集器服务：采集数据源schema并解析入库，驱动表目录注册和字段画像计算。

domain_chain: 采集器配置 (metadata_crawler)
  └─ 采集 → 表目录注册 (table_catalog)
  └─ 解析 → 字段画像计算 (column_profile)
"""

from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.crawl_integrity import CrawlIntegrityService
from app.store import store


class MetadataCrawlerService(BaseModuleService):
    MODULE = "metadata_crawler"
    ENTITY = "采集器配置"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["采集器编号", "数据源类型", "连接地址", "采集范围", "采集深度", "更新频率", "增量字段", "采集状态"]
    REQUIRED_FIELDS = ["采集器编号", "数据源类型", "连接地址"]
    STATUS_ORDER = ["草稿", "待校验", "采集中", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启动采集": "采集中", "停用采集": "已停用"}
    NEGATIVE_ACTIONS = ["停用采集"]

    def crawl_metadata_schema(self, datasource_type: str, connection_url: str, scope: str = "*") -> dict[str, Any]:
        """采集数据源元数据结构，返回解析后的schema字典。

        从连接地址采集表信息、字段定义和类型映射，将采集到的元数据结构解析为标准化的schema格式。
        采集结果包含表列表、每个表的列定义和约束信息，用于驱动下游表目录注册。
        """
        schema_hash = hashlib.sha256(f"{datasource_type}:{connection_url}:{scope}".encode()).hexdigest()[:12]
        tables = self._simulate_crawl_tables(datasource_type, scope)
        for tbl in tables:
            tbl["采集批号"] = schema_hash
            tbl["采集时间"] = datetime.now(timezone.utc).isoformat()
            # 采集表字段元数据
            tbl["字段列表"] = self._simulate_column_discovery(datasource_type, tbl["表名称"])
        return {
            "数据源类型": datasource_type,
            "采集时间": datetime.now(timezone.utc).isoformat(),
            "采集批号": schema_hash,
            "表数量": len(tables),
            "表清单": tables,
        }

    def _simulate_crawl_tables(self, ds_type: str, scope: str) -> list[dict[str, Any]]:
        """模拟从数据源采集表列表——解析表名、表类型和分区信息。"""
        base_tables = {
            "mysql": [
                ("ods_order_info", "BIGINT", 8560000),
                ("ods_user_profile", "VARCHAR", 12500000),
                ("dwd_trade_detail", "DECIMAL", 42000000),
                ("dws_daily_summary", "TIMESTAMP", 980000),
                ("dim_product_sku", "VARCHAR", 230000),
            ],
            "hive": [
                ("ads_sale_report", "DOUBLE", 6500000),
                ("dwd_log_event", "STRING", 180000000),
                ("ods_click_stream", "STRING", 320000000),
                ("dim_date_dim", "DATE", 3650),
                ("dws_user_tag", "MAP", 45000000),
            ],
            "postgresql": [
                ("inventory_snapshot", "INTEGER", 3200000),
                ("payment_record", "NUMERIC", 18900000),
                ("refund_audit", "TEXT", 450000),
            ],
        }
        tables = base_tables.get(ds_type, base_tables["mysql"])
        return [
            {
                "表名称": name,
                "表类型": col_type,
                "预估行数": rows,
                "分区字段": "dt" if ds_type == "hive" else None,
            }
            for name, col_type, rows in tables
        ]

    def _simulate_column_discovery(self, ds_type: str, table_name: str) -> list[dict[str, Any]]:
        """解析表字段元数据：字段名、数据类型、是否可为空、是否主键。"""
        base_cols = {
            "id": ("BIGINT", False, True),
            "name": ("VARCHAR(255)", True, False),
            "status": ("TINYINT", False, False),
            "amount": ("DECIMAL(18,2)", True, False),
            "created_at": ("TIMESTAMP", False, False),
            "updated_at": ("TIMESTAMP", True, False),
            "batch_id": ("VARCHAR(64)", True, False),
            "partition_dt": ("DATE", False, False),
        }
        col_names = list(base_cols.keys())[: min(len(base_cols), 5 + hash(table_name) % 4)]
        return [
            {
                "字段名称": col,
                "数据类型": base_cols[col][0],
                "可为空": base_cols[col][1],
                "是否主键": base_cols[col][2],
            }
            for col in col_names
        ]

    def ingest_metadata_batch(self, schema_result: dict[str, Any]) -> dict[str, Any]:
        """接入元数据批次到表目录存储，校验字段完整性后写入登记记录。

        对每一张采集到的表：解析元数据结构，校验字段完整性（检查主键是否存在），
        将表信息导入表目录，同时为每个字段接入画像计算流水线。
        """
        tables = schema_result.get("表清单", [])
        registered = 0
        failed = []
        catalog_rows = store.rows("table_catalog")

        for tbl in tables:
            # 校验字段完整性：检查主键
            cols = tbl.get("字段列表", [])
            has_pk = any(c.get("是否主键") for c in cols)
            if not has_pk:
                failed.append({"表名称": tbl["表名称"], "原因": "缺少主键约束"})
                continue

            # 将表信息导入表目录
            new_id = max((int(r.get("id", 0)) for r in catalog_rows), default=0) + 1
            catalog_entry = {
                "id": new_id,
                "表编号": f"TABL-{new_id:04d}",
                "表名称": tbl["表名称"],
                "所属库名": schema_result["数据源类型"],
                "表类型": tbl.get("表类型", "TABLE"),
                "字段数量": len(cols),
                "数据行数": str(tbl.get("预估行数", 0)),
                "存储大小": f"{tbl.get('预估行数', 0) * 256 // 1024} KB",
                "最后更新": schema_result["采集时间"],
                "status": "已注册",
                "pending": False,
                "abnormal": False,
            }
            catalog_rows.append(catalog_entry)
            registered += 1

        return {
            "接入批号": schema_result.get("采集批号"),
            "接入时间": datetime.now(timezone.utc).isoformat(),
            "总表数": len(tables),
            "成功注册": registered,
            "校验失败": len(failed),
            "失败明细": failed,
        }

    # ------------------------------------------------------------------
    # 空采集批次：超时中断 vs 可证明完成的空结果
    # ------------------------------------------------------------------
    def simulate_crawl_timeout(
        self,
        datasource_type: str,
        connection_url: str,
        scope: str,
        table_name: str,
        batch_no: str,
        timeout_seconds: int = 30,
    ) -> dict[str, Any]:
        """模拟采集超时：返回空回包但采集未完成，签发「采集中断」凭证。

        中断后的空回包不允许驱动字段删除，表目录保留原字段；
        凭证进入采集中断处置范围，等待重试拿到完整性证据。
        """
        old_columns = self._snapshot_catalog_columns(table_name)
        attestation = CrawlIntegrityService().issue_interrupted_attestation(
            datasource_type=datasource_type,
            connection_url=connection_url,
            scope=scope,
            table_name=table_name,
            batch_no=batch_no,
            old_columns=old_columns,
            reason=f"采集超时（{timeout_seconds}s 未返回）",
        )
        return {
            "采集批号": batch_no,
            "表名称": table_name,
            "采集结果": "空回包",
            "完整性状态": "采集中断",
            "中断原因": attestation["中断原因"],
            "凭证编号": attestation["凭证编号"],
            "凭证哈希": attestation["凭证哈希"],
            "处置范围": attestation["处置范围"],
            "目录保留字段数": len(old_columns),
            "说明": "采集中断，目录保留原字段，重试前不得按字段删除处理",
        }

    def submit_completed_empty_crawl(
        self,
        datasource_type: str,
        connection_url: str,
        scope: str,
        table_name: str,
        batch_no: str,
        linked_batch: str | None = None,
    ) -> dict[str, Any]:
        """提交可证明采集完成的空结果（源侧实际撤表场景）。

        空回包附带完整采集链路证据（开始/结束时间、空响应指纹），
        签发「已完成」凭证，可作为变更对比确认字段删除的依据；
        表目录此时仍保留原字段，等待变更复核确认后再处置。
        """
        old_columns = self._snapshot_catalog_columns(table_name)
        attestation = CrawlIntegrityService().issue_completed_attestation(
            datasource_type=datasource_type,
            connection_url=connection_url,
            scope=scope,
            table_name=table_name,
            batch_no=batch_no,
            old_columns=old_columns,
            linked_batch=linked_batch,
        )
        return {
            "采集批号": batch_no,
            "关联批号": linked_batch,
            "表名称": table_name,
            "采集结果": "空回包",
            "完整性状态": "已完成",
            "凭证编号": attestation["凭证编号"],
            "凭证哈希": attestation["凭证哈希"],
            "响应指纹": attestation["响应指纹"],
            "处置范围": attestation["处置范围"],
            "目录保留字段数": len(old_columns),
            "说明": "空结构采集完成且可回查，删除差异需经变更对比凭证核对确认",
        }

    def retry_interrupted_crawl(
        self,
        datasource_type: str,
        connection_url: str,
        scope: str,
        table_name: str,
        interrupted_batch_no: str,
        retry_batch_no: str,
        empty: bool = True,
    ) -> dict[str, Any]:
        """采集中断后的重试：关联原中断批次，证明空态来自完成的采集而非中断。

        重试不直接把旧字段当删除：只有重试批次拿到「已完成」完整性证据，
        后续变更对比才允许确认删除；关联批号保留可追溯链路。
        """
        integrity = CrawlIntegrityService()
        interrupted = integrity.find_by_batch(interrupted_batch_no)
        if interrupted is None:
            raise ValueError(f"中断批次 {interrupted_batch_no} 无凭证记录，无法关联重试")
        if interrupted.get("完整性状态") != "采集中断":
            raise ValueError(
                f"批次 {interrupted_batch_no} 状态为{interrupted.get('完整性状态')}，不属于采集中断重试范围"
            )
        if not empty:
            raise ValueError("当前重试仅支持空结构采集，非空重试应走正常采集入库链路")
        result = self.submit_completed_empty_crawl(
            datasource_type=datasource_type,
            connection_url=connection_url,
            scope=scope,
            table_name=table_name,
            batch_no=retry_batch_no,
            linked_batch=interrupted_batch_no,
        )
        result["重试说明"] = (
            f"重试批次 {retry_batch_no} 关联中断批次 {interrupted_batch_no}，"
            "中断批次保留采集中断处置结论，不参与删除确认"
        )
        return result

    def _snapshot_catalog_columns(self, table_name: str) -> list[str]:
        """采集前快照目录中该表的旧字段，供中断时证明目录保留了原字段。

        优先取字段画像中的真实字段名；表已登记但画像未生成时，
        退回目录登记的字段数量，用占位名保留字段数证据，避免旧字段静默丢失。
        """
        profile_rows = store.rows("column_profile")
        columns = sorted({
            row.get("字段名称")
            for row in profile_rows
            if row.get("关联表名") == table_name and row.get("字段名称")
        })
        if columns:
            return columns
        catalog_entry = next(
            (row for row in store.rows("table_catalog") if row.get("表名称") == table_name),
            None,
        )
        if catalog_entry is None:
            return []
        field_count = int(catalog_entry.get("字段数量") or 0)
        return [f"目录字段#{idx + 1}" for idx in range(field_count)]

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "采集"
        snapshot["desc"] = "采集数据源元数据结构并解析入库"
        snapshot["artifact"] = "表定义集合"
        snapshot["artifact_desc"] = "采集产出的表定义集合"
        return snapshot