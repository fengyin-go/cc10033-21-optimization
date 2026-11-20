"""ETL 分区交付清单接口：清单、源批次、目标表结构和核对动作。

这些是固定路径（``/api/delivery/...``），必须在资源路由 ``/api/{module}``
之前注册，否则会被动态路径当成模块 key 抢走。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.delivery_registry import DeliveryError
from app.store import store

router = APIRouter(prefix="/api/delivery", tags=["ETL分区交付"])


class SourceBatchPayload(BaseModel):
    源批次: str
    目标表: str
    源表: str = ""
    分区字段: str = "dt"
    记录: list[dict[str, Any]] = Field(default_factory=list)


class ManifestPayload(BaseModel):
    rule_id: int
    source_batch: str
    environment: str = "预发"
    schema_version: str | None = None
    note: str = ""


class RunPayload(BaseModel):
    mode: str = "正式"
    partitions: list[str] = Field(default_factory=list)
    operator: str = "部署流水线"


class ComparePayload(BaseModel):
    old_manifest_id: int
    new_manifest_id: int


class SchemaPayload(BaseModel):
    add_fields: list[Any] = Field(default_factory=list)
    remove_fields: list[str] = Field(default_factory=list)
    required_fields: list[str] | None = None
    note: str = ""


def _guard(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except DeliveryError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


@router.get("/manifests")
def list_manifests() -> dict[str, Any]:
    items = store.delivery.list_manifests()
    return {"items": items, "total": len(items)}


@router.post("/manifests")
def create_manifest(payload: ManifestPayload) -> dict[str, Any]:
    manifest = _guard(
        store.delivery.create_manifest,
        payload.rule_id, payload.source_batch,
        environment=payload.environment,
        schema_version=payload.schema_version,
        note=payload.note,
    )
    return {"ok": True, "message": f"交付清单 {manifest['清单编号']} 已生成", "manifest": manifest}


@router.get("/manifests/{manifest_id}")
def manifest_detail(manifest_id: int) -> dict[str, Any]:
    return _guard(store.delivery.manifest_detail, manifest_id)


@router.post("/manifests/{manifest_id}/run")
def run_manifest(manifest_id: int, payload: RunPayload) -> dict[str, Any]:
    summary = _guard(
        store.delivery.run_manifest,
        manifest_id,
        mode=payload.mode,
        partitions=payload.partitions or None,
        operator=payload.operator,
    )
    ok = not summary.get("未交付分区") and summary.get("正式", True)
    if summary.get("正式") is False:
        ok = True
        message = "试运行完成，未写入目标库，也未推进流水线"
    elif summary.get("未交付分区"):
        message = f"交付未完成，保留未交付分区：{'、'.join(summary['未交付分区'])}"
    else:
        message = f"分区交付完成，发布 {summary.get('发布条数', 0)} 条"
    return {"ok": ok, "message": message, "result": summary}


@router.get("/manifests/{manifest_id}/recompute")
def recompute_manifest(manifest_id: int) -> dict[str, Any]:
    return _guard(store.delivery.recompute_manifest, manifest_id)


@router.get("/manifests/{manifest_id}/verify")
def verify_manifest(manifest_id: int) -> dict[str, Any]:
    return _guard(store.delivery.verify_manifest, manifest_id)


@router.get("/manifests/{manifest_id}/check")
def check_partition_units(manifest_id: int) -> dict[str, Any]:
    """逐分区只读对账：给出可正式交付 / 被拦下的分区清单。"""
    return _guard(store.delivery.check_partition_units, manifest_id)


@router.get("/manifests/{manifest_id}/failures")
def manifest_failures(manifest_id: int, source_batch: str | None = Query(default=None)) -> dict[str, Any]:
    manifest = _guard(store.delivery.get_manifest, manifest_id)
    batch = source_batch or manifest["源批次"]
    return _guard(store.delivery.failures_by_source_batch, batch)


@router.get("/source-batches/{source_batch}/failures")
def source_batch_failures(source_batch: str) -> dict[str, Any]:
    """按源批次定位首次失败的分区，跨清单汇总。"""
    return _guard(store.delivery.failures_by_source_batch, source_batch)


@router.post("/compare")
def compare_manifests(payload: ComparePayload) -> dict[str, Any]:
    return _guard(
        store.delivery.compare_manifests,
        payload.old_manifest_id, payload.new_manifest_id,
    )


@router.get("/source-batches")
def list_source_batches() -> dict[str, Any]:
    items = store.delivery.list_source_batches()
    return {"items": items, "total": len(items)}


@router.post("/source-batches")
def register_source_batch(payload: SourceBatchPayload) -> dict[str, Any]:
    batch = _guard(store.delivery.register_source_batch, payload.model_dump())
    return {"ok": True, "message": f"源批次 {batch['源批次']} 已登记", "batch": batch}


@router.get("/source-batches/{source_batch}")
def source_batch_detail(source_batch: str) -> dict[str, Any]:
    batch = store.delivery.source_batches.get(source_batch)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"源批次 {source_batch} 未登记")
    detail = dict(batch)
    detail["关联清单"] = store.delivery.manifest_for_batch(source_batch)
    detail["失败分区"] = store.delivery.failures_by_source_batch(source_batch)["失败分区"]
    return detail


@router.get("/targets")
def list_targets() -> dict[str, Any]:
    items = store.delivery.list_targets()
    return {"items": items, "total": len(items)}


@router.get("/targets/{table}")
def target_detail(table: str) -> dict[str, Any]:
    return _guard(store.delivery.target_detail, table)


@router.post("/targets/{table}/schema")
def update_schema(table: str, payload: SchemaPayload) -> dict[str, Any]:
    detail = _guard(
        store.delivery.update_target_schema,
        table,
        add_fields=payload.add_fields or None,
        remove_fields=payload.remove_fields or None,
        required_fields=payload.required_fields,
        note=payload.note,
    )
    return {
        "ok": True,
        "message": f"目标表 {table} 结构已更新为 {detail['当前结构版本']}",
        "target": detail,
    }
