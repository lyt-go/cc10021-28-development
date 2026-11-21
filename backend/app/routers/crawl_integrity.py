"""采集完整性凭证路由：超时模拟、空结果提交、重试、凭证核对与复核回写。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.schemas import (
    CrawlTimeoutPayload,
    EmptyCrawlPayload,
    RetryCrawlPayload,
)
from app.services.crawl_integrity import CrawlIntegrityService
from app.services.metadata_crawler import MetadataCrawlerService
from app.services.metadata_diff import MetadataDiffService
from app.services.table_catalog import TableCatalogService
from app.store import store

router = APIRouter(prefix="/api/crawl-integrity", tags=["采集完整性凭证"])


@router.post("/demo-seed")
def demo_seed(table_name: str, downstream_table: str, field_count: int = 4) -> dict[str, Any]:
    """演示/联调用：在运行进程内补齐目录登记与下游血缘，便于观察删除影响。"""
    catalog = TableCatalogService()
    catalog_rows = store.rows("table_catalog")
    if not any(r.get("表名称") == table_name for r in catalog_rows):
        columns = [
            {"字段名称": f"col_{idx}", "数据类型": "BIGINT" if idx == 1 else "VARCHAR(255)",
             "可为空": idx != 1, "是否主键": idx == 1}
            for idx in range(1, field_count + 1)
        ]
        catalog.register_table_with_columns(table_name, "hive", columns, row_count=96000)
    lineage_no = catalog.sync_lineage_entries(downstream_table, [table_name])
    return {"注册表": table_name, "新增血缘关系数": lineage_no}


@router.post("/simulate-timeout")
def simulate_timeout(payload: CrawlTimeoutPayload) -> dict[str, Any]:
    """先模拟超时：产生空回包但签发「采集中断」完整性凭证。"""
    return MetadataCrawlerService().simulate_crawl_timeout(
        datasource_type=payload.datasource_type,
        connection_url=payload.connection_url,
        scope=payload.scope,
        table_name=payload.table_name,
        batch_no=payload.batch_no,
        timeout_seconds=payload.timeout_seconds,
    )


@router.post("/submit-empty")
def submit_empty(payload: EmptyCrawlPayload) -> dict[str, Any]:
    """提交可证明采集完成的空结果，签发「已完成」完整性凭证。"""
    return MetadataCrawlerService().submit_completed_empty_crawl(
        datasource_type=payload.datasource_type,
        connection_url=payload.connection_url,
        scope=payload.scope,
        table_name=payload.table_name,
        batch_no=payload.batch_no,
        linked_batch=payload.linked_batch,
    )


@router.post("/retry")
def retry_after_interruption(payload: RetryCrawlPayload) -> dict[str, Any]:
    """采集中断后的重试：关联原中断批次，不直接把旧字段当删除。"""
    try:
        return MetadataCrawlerService().retry_interrupted_crawl(
            datasource_type=payload.datasource_type,
            connection_url=payload.connection_url,
            scope=payload.scope,
            table_name=payload.table_name,
            interrupted_batch_no=payload.interrupted_batch_no,
            retry_batch_no=payload.retry_batch_no,
            empty=payload.empty,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/attestations")
def list_attestations() -> dict[str, Any]:
    """空表清单：接入数为零的表及其采集完整性凭证。"""
    return CrawlIntegrityService().empty_crawl_checklist()


@router.get("/pending-scope")
def pending_scope() -> dict[str, Any]:
    """当前时点的待核范围（凭证视角）。"""
    return CrawlIntegrityService().pending_review_scope()


@router.post("/attestations/{attestation_id}/verify")
def verify_attestation(attestation_id: int) -> dict[str, Any]:
    """复算凭证哈希，复核采集完整性证据是否被篡改。"""
    result = CrawlIntegrityService().verify_attestation(attestation_id)
    if not result["通过"]:
        raise HTTPException(status_code=404 if result["哈希复核"] == "凭证不存在" else 409,
                            detail=result)
    return result


@router.post("/review")
def review_emptiness() -> dict[str, Any]:
    """采集结论回写变更复核清单，分流采集中断与已完成空态。"""
    return MetadataDiffService().review_crawl_emptiness()


@router.get("/review-pending")
def review_pending() -> dict[str, Any]:
    """变更复核清单当前待核范围（按处置范围分组）。"""
    return MetadataDiffService().pending_review_list()
