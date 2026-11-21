"""空采集批次校验链路：区分采集中断与可证明完成的空态。

场景（同一张分区表的两次空回包）：
  时点1：采集超时 → 空回包 + 「采集中断」凭证 → 删除挂起、目录保留旧字段；
  时点2：重试提交可证明完成的空结果（源侧撤表）→ 「已完成」凭证 →
         删除差异确认并产生下游血缘删除影响。

覆盖：凭证签发与哈希复核、两个时点的待核范围、变更对比凭证闸门、
采集结论回写变更复核清单、采集中断与已完成空态的处置分流。
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import HTTPException

from app.schemas import CrawlTimeoutPayload, EmptyCrawlPayload
from app.services.crawl_integrity import (
    CrawlIntegrityService,
    SCOPE_INTERRUPTED,
    SCOPE_DELETION,
    SCOPE_MISSING_EVIDENCE,
)
from app.routers.crawl_integrity import (
    simulate_timeout,
    submit_empty,
    pending_scope,
    review_emptiness,
    review_pending,
    list_attestations,
    verify_attestation,
)
from app.services.metadata_crawler import MetadataCrawlerService
from app.services.metadata_diff import MetadataDiffService
from app.services.table_catalog import TableCatalogService
from app.store import store


PARTITION_TABLE = "ods_partition_snapshot"
DS_TYPE = "hive"
CONN_URL = "hdfs://cluster/warehouse"
SCOPE = "dw.ods_partition_snapshot"
BATCH_TIMEOUT = "BATCH-TIMEOUT-01"
BATCH_RETRY = "BATCH-RETRY-02"

OLD_COLUMNS = [
    {"字段名称": "id", "数据类型": "BIGINT", "可为空": False, "是否主键": True},
    {"字段名称": "dt", "数据类型": "DATE", "可为空": False, "是否主键": False},
    {"字段名称": "amount", "数据类型": "DECIMAL(18,2)", "可为空": True, "是否主键": False},
    {"字段名称": "status", "数据类型": "TINYINT", "可为空": False, "是否主键": False},
]
OLD_FIELD_NAMES = [c["字段名称"] for c in OLD_COLUMNS]


@pytest.fixture
def isolated_scene():
    """每个用例独立的凭证/复核状态，并准备目录、画像与血缘底座。"""
    for module in ("crawl_attestation", "metadata_diff"):
        store.rows(module).clear()

    # 目录保留原字段：登记分区表并写入字段画像（旧字段快照来源）
    catalog = store.rows("table_catalog")
    if not any(r.get("表名称") == PARTITION_TABLE for r in catalog):
        TableCatalogService().register_table_with_columns(
            PARTITION_TABLE, DS_TYPE, OLD_COLUMNS, row_count=120000
        )
    # 下游血缘：PARTITION_TABLE 作为上游、dwd_partition_agg 作为下游
    lineage = store.rows("lineage_graph")
    for row in lineage:
        if row.get("上游表名") == PARTITION_TABLE:
            row["status"] = "已生效"
            row["abnormal"] = False
    if not any(r.get("上游表名") == PARTITION_TABLE for r in lineage):
        TableCatalogService().sync_lineage_entries(
            "dwd_partition_agg", [PARTITION_TABLE]
        )
    yield store


def _downstream_lineage_impact(table_name: str) -> list[dict]:
    return [
        r for r in store.rows("lineage_graph")
        if r.get("上游表名") == table_name
    ]


class TestTimepointOneInterruptedCrawl:
    """时点1：采集超时后的空回包。"""

    def test_timeout_issues_interrupted_attestation(self, isolated_scene):
        result = MetadataCrawlerService().simulate_crawl_timeout(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE, BATCH_TIMEOUT, timeout_seconds=30
        )
        assert result["采集结果"] == "空回包"
        assert result["完整性状态"] == "采集中断"
        assert result["处置范围"] == SCOPE_INTERRUPTED
        # 凭证可复核
        attestation = CrawlIntegrityService().find_by_batch(BATCH_TIMEOUT)
        assert attestation is not None
        assert attestation["凭证哈希"]
        assert attestation["中断原因"].startswith("采集超时")
        # 目录保留原字段
        assert result["目录保留字段数"] == len(OLD_FIELD_NAMES)
        assert sorted(attestation["旧字段快照"]) == sorted(OLD_FIELD_NAMES)

    def test_attestation_hash_reproducible_and_tamper_evident(self, isolated_scene):
        MetadataCrawlerService().simulate_crawl_timeout(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE, BATCH_TIMEOUT
        )
        integrity = CrawlIntegrityService()
        attestation = integrity.find_by_batch(BATCH_TIMEOUT)
        verified = integrity.verify_attestation(int(attestation["id"]))
        assert verified["通过"] is True
        assert verified["哈希复核"] == "一致"

        # 篡改证据后哈希复核必须失败
        attestation["表数量"] = 999
        tampered = integrity.verify_attestation(int(attestation["id"]))
        assert tampered["通过"] is False
        assert tampered["哈希复核"] == "不一致"
        attestation["表数量"] = 0

    def test_empty_checklist_distinguishes_interrupted_from_real_empty(self, isolated_scene):
        MetadataCrawlerService().simulate_crawl_timeout(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE, BATCH_TIMEOUT
        )
        checklist = CrawlIntegrityService().empty_crawl_checklist()
        assert checklist["空表总数"] == 1
        assert checklist["空回包凭证数"] == 1
        assert checklist["采集中断空态"] == 1
        assert checklist["可采信空态"] == 0
        item = checklist["清单"][0]
        assert item["接入数"] == 0
        assert item["完整性状态"] == "采集中断"
        assert item["目录仍保留旧字段"] is True

    def test_empty_checklist_keeps_both_timepoints_for_same_table(self, isolated_scene):
        crawler = MetadataCrawlerService()
        crawler.simulate_crawl_timeout(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE, BATCH_TIMEOUT
        )
        crawler.retry_interrupted_crawl(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE,
            interrupted_batch_no=BATCH_TIMEOUT, retry_batch_no=BATCH_RETRY,
        )
        checklist = CrawlIntegrityService().empty_crawl_checklist()
        # 同一张表两次空回包均保留可回查凭证
        assert checklist["空表总数"] == 1
        assert checklist["空回包凭证数"] == 2
        assert checklist["采集中断空态"] == 1
        assert checklist["可采信空态"] == 1
        statuses = {i["采集批号"]: i["完整性状态"] for i in checklist["清单"]}
        assert statuses == {BATCH_TIMEOUT: "采集中断", BATCH_RETRY: "已完成"}

    def test_pending_scope_at_timepoint_one(self, isolated_scene):
        MetadataCrawlerService().simulate_crawl_timeout(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE, BATCH_TIMEOUT
        )
        scope = CrawlIntegrityService().pending_review_scope()
        assert len(scope["采集中断待重试"]) == 1
        assert scope["采集中断待重试"][0]["采集批号"] == BATCH_TIMEOUT
        assert scope["已完成空态待删除复核"] == []

    def test_interrupted_crawl_cannot_confirm_deletion(self, isolated_scene):
        MetadataCrawlerService().simulate_crawl_timeout(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE, BATCH_TIMEOUT
        )
        attestation = CrawlIntegrityService().find_by_batch(BATCH_TIMEOUT)
        before = [dict(r) for r in _downstream_lineage_impact(PARTITION_TABLE)]

        diff = MetadataDiffService().compare_schemas(
            OLD_COLUMNS, [], PARTITION_TABLE, attestation_id=int(attestation["id"])
        )
        # 删除差异全部挂起，不产生下游删除影响
        assert diff["凭证核对"] == "挂起"
        assert set(diff["删除挂起"]) == set(OLD_FIELD_NAMES)
        assert diff["删除确认"] == []
        assert "采集中断" in diff["挂起原因"]
        after = _downstream_lineage_impact(PARTITION_TABLE)
        assert [r["status"] for r in after] == [r["status"] for r in before]

    def test_deletion_without_any_attestation_also_suspended(self, isolated_scene):
        diff = MetadataDiffService().compare_schemas(OLD_COLUMNS, [], PARTITION_TABLE)
        assert diff["凭证核对"] == "挂起"
        assert set(diff["删除挂起"]) == set(OLD_FIELD_NAMES)
        rows = [
            r for r in store.rows("metadata_diff")
            if r.get("复核范围") == SCOPE_MISSING_EVIDENCE
        ]
        assert len(rows) == len(OLD_FIELD_NAMES)

    def test_interrupted_review_writeback_keeps_old_fields(self, isolated_scene):
        MetadataCrawlerService().simulate_crawl_timeout(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE, BATCH_TIMEOUT
        )
        result = MetadataDiffService().review_crawl_emptiness()
        assert result["本次登记中断批次"] == 1
        assert result["本次登记删除差异"] == 0

        pending = MetadataDiffService().pending_review_list()
        assert len(pending[SCOPE_INTERRUPTED]) == 1
        assert pending[SCOPE_DELETION] == []
        review = pending[SCOPE_INTERRUPTED][0]
        assert review["凭证核对"] == "挂起"
        assert "保留" in review["复核结论"]

        # 凭证回写复核结论
        attestation = CrawlIntegrityService().find_by_batch(BATCH_TIMEOUT)
        assert attestation["复核结论"] is not None
        # 目录条目依旧保留
        assert any(r.get("表名称") == PARTITION_TABLE for r in store.rows("table_catalog"))
        # 中断不触发下游影响
        assert all(not r.get("abnormal") for r in _downstream_lineage_impact(PARTITION_TABLE))


class TestTimepointTwoCompletedEmptyCrawl:
    """时点2：重试提交可证明采集完成的空结果（源侧实际撤表）。"""

    def test_retry_links_interrupted_batch_and_issues_trusted_attestation(self, isolated_scene):
        crawler = MetadataCrawlerService()
        crawler.simulate_crawl_timeout(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE, BATCH_TIMEOUT
        )
        retry = crawler.retry_interrupted_crawl(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE,
            interrupted_batch_no=BATCH_TIMEOUT, retry_batch_no=BATCH_RETRY,
        )
        assert retry["完整性状态"] == "已完成"
        assert retry["关联批号"] == BATCH_TIMEOUT
        assert retry["处置范围"] == SCOPE_DELETION

        integrity = CrawlIntegrityService()
        trusted = integrity.find_by_batch(BATCH_RETRY)
        assert trusted["完整性状态"] == "已完成"
        # 中断批次原结论不被重试覆盖
        interrupted = integrity.find_by_batch(BATCH_TIMEOUT)
        assert interrupted["完整性状态"] == "采集中断"
        # 新凭证哈希可复核
        assert integrity.verify_attestation(int(trusted["id"]))["通过"] is True

    def test_retry_requires_existing_interrupted_batch(self, isolated_scene):
        with pytest.raises(ValueError):
            MetadataCrawlerService().retry_interrupted_crawl(
                DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE,
                interrupted_batch_no="NOT-EXIST", retry_batch_no=BATCH_RETRY,
            )

    def test_trusted_attestation_confirms_deletion_and_lineage_impact(self, isolated_scene):
        crawler = MetadataCrawlerService()
        crawler.simulate_crawl_timeout(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE, BATCH_TIMEOUT
        )
        retry = crawler.retry_interrupted_crawl(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE,
            interrupted_batch_no=BATCH_TIMEOUT, retry_batch_no=BATCH_RETRY,
        )
        trusted = CrawlIntegrityService().find_by_batch(BATCH_RETRY)
        before_status = [r["status"] for r in _downstream_lineage_impact(PARTITION_TABLE)]

        diff = MetadataDiffService().compare_schemas(
            OLD_COLUMNS, [], PARTITION_TABLE, attestation_id=int(trusted["id"])
        )
        assert diff["凭证核对"] == "通过"
        assert set(diff["删除确认"]) == set(OLD_FIELD_NAMES)
        assert diff["删除挂起"] == []

        # 下游接到删除影响：血缘关系进入待校验
        after = _downstream_lineage_impact(PARTITION_TABLE)
        assert any(r.get("status") == "待校验" and r.get("abnormal") for r in after)
        assert before_status != [r["status"] for r in after]

        # 凭证上的复核结论回写
        assert trusted["复核结论"] is not None
        assert retry["采集批号"] == BATCH_RETRY

    def test_pending_scope_at_timepoint_two(self, isolated_scene):
        crawler = MetadataCrawlerService()
        crawler.simulate_crawl_timeout(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE, BATCH_TIMEOUT
        )
        crawler.retry_interrupted_crawl(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE,
            interrupted_batch_no=BATCH_TIMEOUT, retry_batch_no=BATCH_RETRY,
        )
        scope = CrawlIntegrityService().pending_review_scope()
        assert len(scope["采集中断待重试"]) == 1
        assert len(scope["已完成空态待删除复核"]) == 1
        assert scope["已完成空态待删除复核"][0]["采集批号"] == BATCH_RETRY

    def test_review_writeback_routes_two_scopes_differently(self, isolated_scene):
        crawler = MetadataCrawlerService()
        crawler.simulate_crawl_timeout(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE, BATCH_TIMEOUT
        )
        crawler.retry_interrupted_crawl(
            DS_TYPE, CONN_URL, SCOPE, PARTITION_TABLE,
            interrupted_batch_no=BATCH_TIMEOUT, retry_batch_no=BATCH_RETRY,
        )
        review = MetadataDiffService().review_crawl_emptiness()
        assert review["本次登记中断批次"] == 1
        # 已完成空态按旧字段逐项登记删除差异
        assert review["本次登记删除差异"] == len(OLD_FIELD_NAMES)

        pending = MetadataDiffService().pending_review_list()
        assert len(pending[SCOPE_INTERRUPTED]) == 1
        assert len(pending[SCOPE_DELETION]) == len(OLD_FIELD_NAMES)

        # 复核结论回写到两类凭证
        integrity = CrawlIntegrityService()
        interrupted = integrity.find_by_batch(BATCH_TIMEOUT)
        trusted = integrity.find_by_batch(BATCH_RETRY)
        assert interrupted["处置范围"] == SCOPE_INTERRUPTED
        assert trusted["处置范围"] == SCOPE_DELETION
        assert interrupted["复核结论"] and trusted["复核结论"]

        # 再次执行不重复登记
        second = MetadataDiffService().review_crawl_emptiness()
        assert second["本次登记中断批次"] == 0
        assert second["本次登记删除差异"] == 0
        assert set(second["已复核跳过"]) == {interrupted["凭证编号"], trusted["凭证编号"]}

        # 仅已完成空态产生下游删除影响
        impacted = [r for r in _downstream_lineage_impact(PARTITION_TABLE) if r.get("abnormal")]
        assert impacted, "已完成空态的删除差异应触发下游血缘影响"


class TestCrawlIntegrityApi:
    """路由层（直接调用端点函数，避免引入额外测试依赖）：
    先模拟超时，再提交可证明空结果，检查两个时点。
    """

    def test_full_two_timepoint_flow_over_routes(self, isolated_scene):
        # 时点1：模拟超时
        timeout_result = simulate_timeout(CrawlTimeoutPayload(
            datasource_type=DS_TYPE,
            connection_url=CONN_URL,
            scope=SCOPE,
            table_name=PARTITION_TABLE,
            batch_no=BATCH_TIMEOUT,
            timeout_seconds=5,
        ))
        assert timeout_result["完整性状态"] == "采集中断"

        scope_one = pending_scope()
        assert len(scope_one["采集中断待重试"]) == 1
        assert scope_one["已完成空态待删除复核"] == []

        # 时点2：可证明完成的空结果（模拟重试成功，关联中断批次）
        empty_result = submit_empty(EmptyCrawlPayload(
            datasource_type=DS_TYPE,
            connection_url=CONN_URL,
            scope=SCOPE,
            table_name=PARTITION_TABLE,
            batch_no=BATCH_RETRY,
            linked_batch=BATCH_TIMEOUT,
        ))
        assert empty_result["完整性状态"] == "已完成"

        checklist = list_attestations()
        trusted_row = next(
            item for item in checklist["清单"] if item["采集批号"] == BATCH_RETRY
        )
        attestation_id = _attestation_id(trusted_row["凭证编号"])
        verify = verify_attestation(attestation_id)
        assert verify["哈希复核"] == "一致"
        assert verify["通过"] is True

        # 复核回写后两个处置范围并存
        review_emptiness()
        pending = review_pending()
        assert len(pending[SCOPE_INTERRUPTED]) == 1
        assert len(pending[SCOPE_DELETION]) == len(OLD_FIELD_NAMES)

    def test_verify_unknown_attestation_raises_404(self, isolated_scene):
        with pytest.raises(HTTPException) as exc:
            verify_attestation(9999)
        assert exc.value.status_code == 404


def _attestation_id(credential_no: str) -> int:
    row = next(
        r for r in store.rows("crawl_attestation") if r.get("凭证编号") == credential_no
    )
    return int(row["id"])
