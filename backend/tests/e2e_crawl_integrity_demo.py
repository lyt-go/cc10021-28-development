#!/usr/bin/env python3
"""端到端核验：同一分区表两次空回包（超时 -> 可证明空结果）的完整链路。

按任务要求依次检查：
  1. 先模拟超时（时点1），检查待核范围：仅采集中断处置、目录保留旧字段、下游无删除影响；
  2. 再提交可证明采集完成的空结果（时点2），检查待核范围：删除差异处置；
  3. 凭证哈希可复核、空表清单可区分两类空态；
  4. 复核回写后，只有时点2的已完成空态让下游血缘接到删除影响。
"""
from __future__ import annotations

import json
import os
import urllib.request

BASE = os.environ.get("E2E_BASE", "http://127.0.0.1:8000/api")
TBL = "e2e_partition_snapshot"
B1 = "E2E-BATCH-T1"
B2 = "E2E-BATCH-T2"


def call(method: str, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode())


def assert_true(cond: bool, msg: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + msg)
    if not cond:
        raise SystemExit(f"核验失败：{msg}")


def lineage_rows() -> list[dict]:
    return [
        r for r in call("GET", "/lineage_graph?size=200")["items"]
        if r.get("上游表名") == TBL
    ]


print("########## 准备：目录登记（4个旧字段）+ 下游血缘 ##########")
seed = call("POST", f"/crawl-integrity/demo-seed?table_name={TBL}&downstream_table=dwd_e2e_agg&field_count=4")
print(" ", seed)

# ============ 时点1：模拟超时 ============
print("\n########## 时点1：模拟采集超时（第一次空回包）##########")
t1 = call("POST", "/crawl-integrity/simulate-timeout", {
    "datasource_type": "hive", "connection_url": "hdfs://cluster/warehouse",
    "scope": f"dw.{TBL}", "table_name": TBL, "batch_no": B1, "timeout_seconds": 30,
})
print(" ", json.dumps(t1, ensure_ascii=False))
assert_true(t1["完整性状态"] == "采集中断", "时点1凭证为采集中断")
assert_true(t1["目录保留字段数"] == 4, "时点1目录保留4个旧字段")
assert_true(t1["处置范围"] == "采集中断处置", "时点1进入采集中断处置范围")

scope1 = call("GET", "/crawl-integrity/pending-scope")
assert_true(len(scope1["采集中断待重试"]) == 1, "时点1待核范围：1个中断批次待重试")
assert_true(scope1["已完成空态待删除复核"] == [], "时点1待核范围：无已完成空态")

lineage_t1 = lineage_rows()
assert_true(all(r["status"] == "已生效" and not r["abnormal"] for r in lineage_t1),
            "时点1下游血缘未接到任何删除影响")

review1 = call("POST", "/crawl-integrity/review")
assert_true(review1["本次登记中断批次"] == 1 and review1["本次登记删除差异"] == 0,
            "时点1复核回写：登记中断批次，不登记删除差异")
pending1 = call("GET", "/crawl-integrity/review-pending")
assert_true(len(pending1["采集中断处置"]) == 1 and pending1["删除差异处置"] == [],
            "时点1变更复核清单：中断处置1条、删除处置0条")
assert_true(lineage_rows()[0]["status"] == "已生效", "复核回写后下游血缘仍不受影响")

# ============ 时点2：可证明完成的空结果 ============
print("\n########## 时点2：重试提交可证明采集完成的空结果 ##########")
t2 = call("POST", "/crawl-integrity/retry", {
    "datasource_type": "hive", "connection_url": "hdfs://cluster/warehouse",
    "scope": f"dw.{TBL}", "table_name": TBL,
    "interrupted_batch_no": B1, "retry_batch_no": B2,
})
print(" ", json.dumps(t2, ensure_ascii=False))
assert_true(t2["完整性状态"] == "已完成", "时点2凭证为已完成")
assert_true(t2["关联批号"] == B1, "时点2重试批次关联中断批次")
assert_true(t2["处置范围"] == "删除差异处置", "时点2进入删除差异处置范围")

checklist = call("GET", "/crawl-integrity/attestations")
print("\n########## 空表清单（两类空态并存）##########")
for item in checklist["清单"]:
    print(" ", json.dumps(item, ensure_ascii=False))
assert_true(checklist["空表总数"] == 1, "空表清单按表聚合为1张表")
assert_true(checklist["空回包凭证数"] == 2, "空表清单保留两次空回包的独立凭证")
assert_true(checklist["采集中断空态"] == 1 and checklist["可采信空态"] == 1,
            "空表清单同时保留中断与已完成两个批次的凭证")

t2_row = next(i for i in checklist["清单"] if i["采集批号"] == B2)
verify = call("POST", f"/crawl-integrity/attestations/{t2_row['id']}/verify")
assert_true(verify["哈希复核"] == "一致" and verify["通过"], "时点2凭证哈希复核一致")

scope2 = call("GET", "/crawl-integrity/pending-scope")
assert_true(len(scope2["采集中断待重试"]) == 1, "时点2待核范围：中断批次仍在（不被重试覆盖）")
assert_true(len(scope2["已完成空态待删除复核"]) == 1, "时点2待核范围：1个已完成空态待删除复核")

review2 = call("POST", "/crawl-integrity/review")
assert_true(review2["本次登记中断批次"] == 0, "中断批次已复核，不重复登记")
assert_true(review2["本次登记删除差异"] == 4, "时点2按4个旧字段登记删除差异")
pending2 = call("GET", "/crawl-integrity/review-pending")
assert_true(len(pending2["采集中断处置"]) == 1, "复核清单：采集中断处置保留1条")
assert_true(len(pending2["删除差异处置"]) == 4, "复核清单：删除差异处置4条")
assert_true(all(r["凭证核对"] == "通过" for r in pending2["删除差异处置"]),
            "删除差异均凭已完成凭证核对通过")

print("\n########## 下游删除影响（仅时点2触发）##########")
for r in lineage_rows():
    print(" ", r["关系编号"], r["上游表名"], "->", r["下游表名"],
          "| status=", r["status"], "| abnormal=", r["abnormal"])
assert_true(any(r["status"] == "待校验" and r["abnormal"] for r in lineage_rows()),
            "时点2下游血缘接到删除影响并进入待校验")

print("\n全部核验通过 ✅")
