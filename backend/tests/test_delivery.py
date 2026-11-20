"""ETL 分区交付清单测试：结构兼容、幂等重试、失败定位与新旧结构核对。"""
from __future__ import annotations

import pytest

from app.delivery_registry import DeliveryError
from app.services import service_for
from app.store import Store, store as shared_store


POS_RULE = 1
POS_BATCH = "BATCH-20261118-POS"


@pytest.fixture
def store() -> Store:
    """每个测试使用独立 store 实例。

    各业务服务通过模块级单例 ``app.store.store`` 访问仓库，所以这里重置
    单例本身，确保服务的动作和测试读到的数据是同一份。
    """
    shared_store.reset()
    return shared_store


def _manifest_for_dwd(store: Store) -> dict:
    return store.delivery.create_manifest(POS_RULE, POS_BATCH)


def test_manifest_carries_batch_mapping_and_structure(store: Store) -> None:
    """交付清单必须同时写清源批次、映射快照和模型结构版本。"""
    manifest = _manifest_for_dwd(store)
    target = store.delivery.targets["dwd_sales_detail"]

    assert manifest["源批次"] == POS_BATCH
    assert manifest["规则快照"]["字段映射"] == "order_id=>order_id.amt=>sales_amt"
    assert manifest["规则版本"].startswith("rl-")
    assert manifest["模型结构版本"] == target["当前结构版本"]
    assert manifest["目标字段"] == ["order_id", "sales_amt", "dt"]
    assert manifest["一致性"]["规则与结构匹配"] is True
    assert {item["分区"] for item in manifest["分区"]} == {"2026-11-18", "2026-11-19"}


def test_first_failure_is_located_by_source_batch(store: Store) -> None:
    """分区分区失败后，可按源批次定位到首次失败记录。"""
    store.delivery.update_target_schema(
        "dwd_sales_detail",
        add_fields=[{"字段": "channel_code", "必填": True}],
    )
    manifest = store.delivery.create_manifest(POS_RULE, "BATCH-20261119-POS")
    assert manifest["状态"] == "结构不兼容"

    positioned = store.delivery.failures_by_source_batch("BATCH-20261119-POS")
    assert positioned["可定位"] is True
    assert positioned["失败分区数"] == 2
    first = positioned["失败分区"][0]["首次失败"]
    assert first["源批次"] == "BATCH-20261119-POS"
    assert "channel_code" in first["错误字段"]


def test_formal_delivery_is_idempotent(store: Store) -> None:
    """正式交付成功后重试不得重复发布。"""
    manifest = _manifest_for_dwd(store)
    first = store.delivery.run_manifest(manifest["id"])
    second = store.delivery.run_manifest(manifest["id"])

    assert first["发布条数"] == 2
    assert second["发布条数"] == 0
    assert sorted(second["幂等跳过"]) == ["2026-11-18", "2026-11-19"]
    assert second["已交付分区"] == ["2026-11-18", "2026-11-19"]
    rows = store.delivery.targets["dwd_sales_detail"]["分区"]["2026-11-18"]["行"]
    assert len(rows) == 3


def test_test_run_never_advances_pipeline(store: Store) -> None:
    """试运行不写目标库、不改变分区状态、不推进流水线。"""
    manifest = _manifest_for_dwd(store)
    result = store.delivery.run_manifest(manifest["id"], mode="试运行")

    assert result["正式"] is False
    assert result["发布条数"] == 0
    assert store.delivery.targets["dwd_sales_detail"]["分区"] == {}
    stored = store.delivery.get_manifest(manifest["id"])
    assert all(item["状态"] == "待交付" for item in stored["分区"])
    task = next(
        row for row in store.rows("deploy_pipeline")
        if row.get("交付清单编号") == manifest["清单编号"]
    )
    assert task["status"] == "待部署"
    assert task["已交付分区"] == []


def test_schema_growth_keeps_undelivered_partitions(store: Store) -> None:
    """目标结构不兼容时保留未交付分区与错误字段，不伪造交付。"""
    store.delivery.update_target_schema(
        "dwd_sales_detail",
        add_fields=[{"字段": "channel_code", "必填": True}],
        note="目标表新增渠道字段",
    )
    manifest = store.delivery.create_manifest(POS_RULE, "BATCH-20261119-POS")
    summary = store.delivery.run_manifest(manifest["id"])

    assert summary["发布条数"] == 0
    assert sorted(summary["未交付分区"]) == ["2026-11-20", "2026-11-21"]
    stored = store.delivery.get_manifest(manifest["id"])
    for item in stored["分区"]:
        assert item["状态"] == "结构不兼容"
        assert item["错误字段"] == ["channel_code"]
        assert item["首次失败"]["源批次"] == "BATCH-20261119-POS"


def test_old_partitions_stay_on_old_structure(store: Store) -> None:
    """结构升级后旧分区仍保留原结构版本，不混入新版本字段。"""
    manifest = _manifest_for_dwd(store)
    store.delivery.run_manifest(manifest["id"])

    store.delivery.update_target_schema(
        "dwd_sales_detail",
        add_fields=[{"字段": "channel_code"}],
        note="新增可选字段",
    )
    detail = store.delivery.target_detail("dwd_sales_detail")
    partitions = {item["分区"]: item for item in detail["分区明细"]}

    assert partitions["2026-11-18"]["结构版本"] == manifest["模型结构版本"]
    assert partitions["2026-11-18"]["字段"] == ["order_id", "sales_amt", "dt"]
    assert "channel_code" in detail["实际可用字段"]

    verify = store.delivery.verify_manifest(manifest["id"])
    assert verify["混版分区"] == []
    assert verify["流水线已交付分区"] == ["2026-11-18", "2026-11-19"]


def test_redelivery_overwrites_whole_partition(store: Store) -> None:
    """重新交付同一分区时整分区覆盖，不会残留旧版本字段。"""
    first = _manifest_for_dwd(store)
    store.delivery.run_manifest(first["id"])

    store.delivery.update_target_schema("dwd_sales_detail", add_fields=["channel_code"])
    rule = store.delivery.get_rule(POS_RULE)
    rule["字段映射"] = "order_id=>order_id.amt=>sales_amt.channel=>channel_code"
    second = store.delivery.create_manifest(POS_RULE, POS_BATCH)

    assert sorted(second["目标字段"]) == ["channel_code", "dt", "order_id", "sales_amt"]
    summary = store.delivery.run_manifest(second["id"])
    assert summary["发布条数"] == 2

    partition = store.delivery.targets["dwd_sales_detail"]["分区"]["2026-11-18"]
    assert partition["结构版本"] == second["模型结构版本"]
    for row in partition["行"]:
        assert set(row) == set(second["目标字段"])

    verify = store.delivery.verify_manifest(second["id"])
    assert verify["混版分区"] == []
    assert verify["通过"] is True


def test_recompute_confirmed_partitions(store: Store) -> None:
    """已确认分区可按清单快照复算，且复算不产生发布。"""
    manifest = _manifest_for_dwd(store)
    store.delivery.run_manifest(manifest["id"])

    result = store.delivery.recompute_manifest(manifest["id"])
    assert result["可复算分区数"] == 2
    assert result["一致分区数"] == 2
    assert all(item["可复算"] for item in result["分区"])


def test_compare_old_and_new_structure_results(store: Store) -> None:
    """同一源批次可分别核对旧作业与新结构结果。"""
    old = _manifest_for_dwd(store)
    store.delivery.run_manifest(old["id"])

    store.delivery.update_target_schema(
        "dwd_sales_detail",
        add_fields=[{"字段": "channel_code", "必填": True}],
    )
    new = store.delivery.create_manifest(POS_RULE, POS_BATCH)

    comparison = store.delivery.compare_manifests(old["id"], new["id"])
    assert comparison["差异分区数"] == 2
    for item in comparison["分区对比"]:
        assert item["仅新结构字段"] == ["channel_code"]
        assert item["旧状态"] == "已交付"
        assert item["新状态"] == "结构不兼容"
        assert item["共源记录数"] >= 1


def test_compare_rejects_mixed_source_batches(store: Store) -> None:
    """不同源批次的清单不能互相比较，避免批次差异混入字段核对。"""
    first = _manifest_for_dwd(store)
    second = store.delivery.create_manifest(POS_RULE, "BATCH-20261119-POS")
    with pytest.raises(DeliveryError):
        store.delivery.compare_manifests(first["id"], second["id"])


def test_pipeline_links_source_records(store: Store) -> None:
    """部署流水线推进后要能关联每个已交付分区的源记录。"""
    manifest = _manifest_for_dwd(store)
    service = service_for("deploy_pipeline")
    task = next(
        row for row in store.rows("deploy_pipeline")
        if row.get("交付清单编号") == manifest["清单编号"]
    )
    entry, message = service.run_action(task["id"], "执行部署")

    assert entry is not None, message
    assert entry["status"] == "已部署"
    assert entry["已交付分区"] == ["2026-11-18", "2026-11-19"]
    assert entry["未交付分区"] == []
    linked = {item["分区"]: item["源记录"] for item in entry["源记录关联"]}
    assert linked["2026-11-18"] == ["SRC-POS-0001", "SRC-POS-0002", "SRC-POS-0003"]
    assert all(item["源批次"] == POS_BATCH for item in entry["源记录关联"])

    verify = store.delivery.verify_manifest(manifest["id"])
    assert verify["通过"] is True


def test_pipeline_requires_manifest(store: Store) -> None:
    """没有交付清单时，部署任务拒绝用模拟结果完成正式交付。"""
    service = service_for("deploy_pipeline")
    entry, missing = service.create_entry({
        "任务名称": "无清单任务",
        "目标模型": "fact_without_manifest",
        "部署环境": "生产",
    })
    assert entry is not None and not missing
    assert entry["交付清单编号"] == ""

    result, message = service.run_action(entry["id"], "执行部署")
    assert result is not None
    assert result["status"] == "部署失败"
    assert "交付清单" in message


def test_pipeline_blocks_stale_rule_version(store: Store) -> None:
    """清单生成后规则又被改动时，正式部署必须拦下，避免规则和结构错配。"""
    manifest = _manifest_for_dwd(store)
    rule = store.delivery.get_rule(POS_RULE)
    rule["转换函数"] = "COALESCE.UPPER"

    service = service_for("deploy_pipeline")
    task = next(
        row for row in store.rows("deploy_pipeline")
        if row.get("交付清单编号") == manifest["清单编号"]
    )
    entry, message = service.run_action(task["id"], "执行部署")
    assert entry is not None
    assert entry["status"] == "部署失败"
    assert "规则版本" in message
    assert store.delivery.targets["dwd_sales_detail"]["分区"] == {}


def test_partition_unit_check_is_read_only(store: Store) -> None:
    """分区单元对账只读：给出可交付/被拦下分区，不改交付状态。"""
    store.delivery.update_target_schema(
        "dwd_sales_detail",
        add_fields=[{"字段": "channel_code", "必填": True}],
    )
    manifest = store.delivery.create_manifest(POS_RULE, "BATCH-20261119-POS")
    check = store.delivery.check_partition_units(manifest["id"])

    assert check["可交付分区"] == []
    assert sorted(check["被拦下分区"]) == ["2026-11-20", "2026-11-21"]
    assert check["发布条数"] == 0
    stored = store.delivery.get_manifest(manifest["id"])
    assert all(item["状态"] == "结构不兼容" for item in stored["分区"])
    assert store.delivery.targets["dwd_sales_detail"]["分区"] == {}


def test_source_batch_links_manifests_and_failures(store: Store) -> None:
    """源批次可以反查它产生的清单与失败分区，方便旧作业按批次定位。"""
    first = _manifest_for_dwd(store)
    store.delivery.run_manifest(first["id"])
    store.delivery.update_target_schema(
        "dwd_sales_detail",
        add_fields=[{"字段": "channel_code", "必填": True}],
    )
    second = store.delivery.create_manifest(POS_RULE, POS_BATCH)

    related = store.delivery.manifest_for_batch(POS_BATCH)
    assert [item["id"] for item in related] == [first["id"], second["id"]]
    failures = store.delivery.failures_by_source_batch(POS_BATCH)
    assert failures["失败分区数"] == 2
    assert all(item["首次失败"]["源批次"] == POS_BATCH for item in failures["失败分区"])


def test_row_level_required_field_is_positioned(store: Store) -> None:
    """必填字段在源记录中为空时不算结构不兼容，但失败仍可按源批次定位。"""
    store.delivery.update_target_schema(
        "dwd_sales_detail",
        add_fields=[{"字段": "channel_code", "必填": True}],
        required_fields=["order_id", "dt", "channel_code"],
    )
    rule = store.delivery.get_rule(POS_RULE)
    rule["字段映射"] = "order_id=>order_id.amt=>sales_amt.channel=>channel_code"
    manifest = store.delivery.create_manifest(POS_RULE, POS_BATCH)

    # 结构匹配（规则已覆盖 channel_code），但源记录没有 channel 值。
    assert manifest["一致性"]["规则与结构匹配"] is True
    assert all(item["状态"] == "待交付" for item in manifest["分区"])
    assert all(item["行级校验"] == ["channel_code"] for item in manifest["分区"])

    summary = store.delivery.run_manifest(manifest["id"])
    assert summary["发布条数"] == 0
    assert sorted(summary["未交付分区"]) == ["2026-11-18", "2026-11-19"]
    failures = store.delivery.failures_by_source_batch(POS_BATCH)
    assert failures["可定位"] is True
    assert failures["失败分区"][0]["首次失败"]["原因"] == "必填字段为空"
