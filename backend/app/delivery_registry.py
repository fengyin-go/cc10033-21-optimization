"""ETL 分区交付登记簿。

这里承载“ETL 分区交付清单”的核心领域状态，而不是把它塞进某张 CRUD 表：

* 目标表结构版本（字段、必填、字段类型、结构哈希）
* 源批次与源记录（按分区归组）
* 交付清单（规则快照 + 源批次 + 固定结构版本）
* 分区交付结果（正式交付 / 试运行 / 幂等重试 / 结构不兼容 / 首次失败）
* 已交付到目标库的分区数据（整分区覆盖，绝不把不同版本字段混在一起）

该对象由 :class:`app.store.Store` 在初始化时创建，因此既能直接读写
``deploy_pipeline`` 任务，也能让各业务服务通过 ``store.delivery`` 访问。
"""
from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Iterable

TEST_MODES = {"试运行", "测试", "test", "dry_run", "dry-run"}
FORMAL_MODES = {"正式", "正式交付", "formal", "publish"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _digest(*parts: Any) -> str:
    raw = "\u0001".join(str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    separators = [",", "，", "、", "/", "|", "\n"]
    for sep in separators:
        text = text.replace(sep, ".")
    return [part.strip() for part in text.split(".") if part.strip()]


def _field_type(field: str) -> str:
    lower = field.lower()
    if lower.endswith("_amt") or "amount" in lower or "price" in lower:
        return "DECIMAL(18,2)"
    if lower.endswith(("_qty", "_cnt")) or lower.endswith("_count"):
        return "BIGINT"
    if lower in {"dt", "date", "biz_date"} or lower.endswith("_date"):
        return "DATE"
    return "VARCHAR(128)"


def _field_defs(fields: Iterable[str], required: Iterable[str]) -> list[dict[str, Any]]:
    required_set = set(required)
    return [
        {"字段": field, "类型": _field_type(field), "必填": field in required_set}
        for field in fields
    ]


def parse_field_mapping(mapping_text: str) -> list[tuple[str, str]]:
    """解析 ``src=>target.src2=>target2`` 形式的字段映射。"""
    pairs: list[tuple[str, str]] = []
    for chunk in str(mapping_text or "").replace("，", ".").replace(",", ".").split("."):
        if "=>" not in chunk:
            continue
        source, target = chunk.split("=>", 1)
        source, target = source.strip(), target.strip()
        if source and target:
            pairs.append((source, target))
    return pairs


class DeliveryError(ValueError):
    """交付相关的业务错误，路由层会转换成 400。"""


class DeliveryRegistry:
    """ETL 分区交付清单、目标表结构和目标库分区数据的统一登记簿。"""

    def __init__(self, store: Any) -> None:
        self._store = store
        self.targets: dict[str, dict[str, Any]] = {}
        self.source_batches: dict[str, dict[str, Any]] = dict(deepcopy(_SOURCE_BATCHES))
        self.manifests: dict[int, dict[str, Any]] = {}
        self.publications: dict[str, dict[str, Any]] = {}
        self._next_manifest_id = 1
        self._bootstrap_targets()
        self._bootstrap_pipeline_links()

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------
    @staticmethod
    def target_fields(rule: dict[str, Any]) -> list[str]:
        fields = [target for _, target in parse_field_mapping(str(rule.get("字段映射", "")))]
        partition_field = "dt"
        if partition_field not in fields:
            fields.append(partition_field)
        return fields

    @staticmethod
    def rule_version(rule: dict[str, Any]) -> str:
        return "rl-" + _digest(
            rule.get("id", ""),
            rule.get("规则名称", ""),
            rule.get("源表", ""),
            rule.get("目标表", ""),
            rule.get("字段映射", ""),
            rule.get("清洗逻辑", ""),
            rule.get("转换函数", ""),
            rule.get("写入模式", ""),
        )

    @staticmethod
    def schema_version(fields: Iterable[str], required: Iterable[str],
                       types: dict[str, str] | None = None) -> str:
        types = types or {}
        signature = "|".join(
            f"{field}:{types.get(field) or _field_type(field)}"
            for field in fields
        )
        return "sch-" + _digest(signature, ",".join(sorted(required)))

    def _bootstrap_targets(self) -> None:
        """按仓库中现有的 ETL 映射规则推导目标表初始结构。"""
        seen: set[str] = set()
        for rule in self._store.rows("etl_mapping"):
            target_table = str(rule.get("目标表", "")).strip()
            if not target_table or target_table in seen or rule.get("status") == "已废弃":
                continue
            seen.add(target_table)
            fields = self.target_fields(rule)
            mapped = [target for _, target in parse_field_mapping(str(rule.get("字段映射", "")))]
            required = [mapped[0], "dt"] if mapped else ["dt"]
            self._create_target(target_table, fields, required, note="初始目标表结构")

    def _bootstrap_pipeline_links(self) -> None:
        """给种子部署任务挂上可读的交付清单提示，便于从流水线反查。"""
        for row in self._store.rows("deploy_pipeline"):
            if "交付清单" not in row:
                row["交付清单编号"] = ""
                row["源批次"] = ""

    def _create_target(self, table: str, fields: list[str], required: list[str],
                       note: str = "") -> dict[str, Any]:
        fields = list(dict.fromkeys(fields))
        required = [field for field in dict.fromkeys(required) if field in fields]
        types = {field: _field_type(field) for field in fields}
        version = self.schema_version(fields, required, types)
        target = {
            "表名": table,
            "分区字段": "dt",
            "当前结构版本": version,
            "当前必填字段": required,
            "实际可用字段": fields,
            "字段定义": _field_defs(fields, required),
            "结构版本": [{
                "版本": version,
                "字段": fields,
                "必填字段": required,
                "字段定义": _field_defs(fields, required),
                "新增字段": [],
                "移除字段": [],
                "说明": note,
                "生效时间": _now(),
            }],
            "分区": {},
            "交付记录": [],
        }
        self.targets[table] = target
        return target

    def _rule_rows(self) -> list[dict[str, Any]]:
        return list(self._store.rows("etl_mapping"))

    def get_rule(self, rule_id: int) -> dict[str, Any]:
        for rule in self._rule_rows():
            if int(rule.get("id", 0)) == int(rule_id):
                return rule
        raise DeliveryError(f"ETL 映射规则 {rule_id} 不存在")

    def ensure_target_for_rule(self, rule: dict[str, Any]) -> dict[str, Any]:
        table = str(rule.get("目标表", "")).strip()
        if not table:
            raise DeliveryError("映射规则未配置目标表，无法生成分区交付清单")
        if table not in self.targets:
            fields = self.target_fields(rule)
            mapped = [target for _, target in parse_field_mapping(str(rule.get("字段映射", "")))]
            required = [mapped[0], "dt"] if mapped else ["dt"]
            self._create_target(table, fields, required, note="按 ETL 映射规则推导的目标表结构")
        return self.targets[table]

    @staticmethod
    def _schema_of(target: dict[str, Any], version: str | None = None) -> dict[str, Any]:
        """按版本号取目标表结构；不传版本时取当前结构。清单创建时用它固定字段。"""
        wanted = version or target["当前结构版本"]
        for schema in target["结构版本"]:
            if schema["版本"] == wanted:
                return schema
        raise DeliveryError(f"目标表 {target['表名']} 不存在结构版本 {wanted}")

    # ------------------------------------------------------------------
    # 源批次
    # ------------------------------------------------------------------
    def list_source_batches(self) -> list[dict[str, Any]]:
        result = []
        for batch in self.source_batches.values():
            partition_field = batch.get("分区字段", "dt")
            partitions: dict[str, int] = {}
            for record in batch["记录"]:
                partitions[str(record.get(partition_field, "未分区"))] = (
                    partitions.get(str(record.get(partition_field, "未分区")), 0) + 1
                )
            result.append({
                "源批次": batch["源批次"],
                "源表": batch.get("源表", ""),
                "目标表": batch.get("目标表", ""),
                "分区字段": partition_field,
                "记录数": len(batch["记录"]),
                "分区": partitions,
                "状态": batch.get("状态", "可交付"),
            })
        return sorted(result, key=lambda item: item["源批次"])

    def register_source_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_id = str(payload.get("源批次", "")).strip()
        target = str(payload.get("目标表", "")).strip()
        records = payload.get("记录")
        if not batch_id or not target or not isinstance(records, list) or not records:
            raise DeliveryError("源批次、目标表和记录均不能为空")
        if batch_id in self.source_batches:
            raise DeliveryError(f"源批次 {batch_id} 已登记，请勿重复提交")
        partition_field = str(payload.get("分区字段", "dt"))
        normalized: list[dict[str, Any]] = []
        for index, record in enumerate(records, start=1):
            if not isinstance(record, dict):
                raise DeliveryError("源记录必须是对象")
            item = {str(key): value for key, value in record.items()}
            item.setdefault("源记录ID", f"{batch_id}-{index:04d}")
            if partition_field not in item:
                raise DeliveryError(f"源记录 {item['源记录ID']} 缺少分区字段 {partition_field}")
            normalized.append(item)
        batch = {
            "源批次": batch_id,
            "源表": str(payload.get("源表", "")),
            "目标表": target,
            "分区字段": partition_field,
            "记录": normalized,
            "状态": "可交付",
            "登记时间": _now(),
        }
        self.source_batches[batch_id] = batch
        return batch

    # ------------------------------------------------------------------
    # 目标表结构
    # ------------------------------------------------------------------
    def list_targets(self) -> list[dict[str, Any]]:
        result = []
        for target in self.targets.values():
            partition_states: dict[str, int] = {}
            for partition in target["分区"].values():
                status = partition.get("状态", "已交付")
                partition_states[status] = partition_states.get(status, 0) + 1
            result.append({
                "表名": target["表名"],
                "当前结构版本": target["当前结构版本"],
                "实际可用字段": list(target["实际可用字段"]),
                "当前必填字段": list(target["当前必填字段"]),
                "结构版本数": len(target["结构版本"]),
                "已交付分区数": len(target["分区"]),
                "分区状态": partition_states,
            })
        return sorted(result, key=lambda item: item["表名"])

    def target_detail(self, table: str) -> dict[str, Any]:
        target = self.targets.get(table)
        if target is None:
            raise DeliveryError(f"目标表 {table} 不存在")
        detail = deepcopy(target)
        detail["分区明细"] = [
            {
                "分区": partition,
                "结构版本": data.get("结构版本", ""),
                "字段": list(data.get("字段", [])),
                "行数": data.get("行数", len(data.get("行", []))),
                "源批次": data.get("源批次", ""),
                "规则版本": data.get("规则版本", ""),
                "交付清单编号": data.get("交付清单编号", ""),
                "交付时间": data.get("交付时间", ""),
                "状态": data.get("状态", "已交付"),
            }
            for partition, data in sorted(target["分区"].items())
        ]
        return detail

    def update_target_schema(
        self,
        table: str,
        add_fields: Any = None,
        remove_fields: Any = None,
        required_fields: Any = None,
        note: str = "",
    ) -> dict[str, Any]:
        """升级目标表结构。

        新增字段只改表结构，不重写已交付分区；已交付分区的行仍带有
        自己的 ``结构版本`` 和字段清单，重新交付时会整分区覆盖，避免混版。
        """
        target = self.targets.get(table)
        if target is None:
            raise DeliveryError(f"目标表 {table} 不存在")

        additions = self._normalize_field_specs(add_fields)
        removals = _as_list(remove_fields)
        old_fields = list(target["实际可用字段"])
        old_required = list(target["当前必填字段"])

        fields = [field for field in old_fields if field not in removals]
        types = {item["字段"]: item["类型"] for item in target["字段定义"]}
        for field, field_type in additions.items():
            if field not in fields:
                fields.append(field)
            types[field] = field_type
        if required_fields is None:
            required = [field for field in old_required if field in fields]
            # 新增字段显式标了必填时自动进入必填清单
            for field, spec in additions.items():
                if spec.get("必填") and field not in required:
                    required.append(field)
        else:
            required = [field for field in _as_list(required_fields) if field in fields]
            missing_required = [field for field in _as_list(required_fields) if field not in fields]
            if missing_required:
                raise DeliveryError(f"必填字段不在目标结构中：{'、'.join(missing_required)}")

        if fields == old_fields and required == old_required:
            target["结构说明"] = note or target.get("结构说明", "")
            return self.target_detail(table)

        new_version = self.schema_version(fields, required, types)
        for schema in target["结构版本"]:
            if schema["版本"] == new_version:
                # 字段变化被抵消，仅同步必填标记
                schema["必填字段"] = required
                schema["字段定义"] = _field_defs(fields, required)
                target["当前必填字段"] = required
                return self.target_detail(table)

        added_names = [field for field in fields if field not in old_fields]
        removed_names = [field for field in old_fields if field not in fields]
        schema = {
            "版本": new_version,
            "字段": fields,
            "必填字段": required,
            "字段定义": _field_defs(fields, required),
            "新增字段": added_names,
            "移除字段": removed_names,
            "说明": note or "目标表结构变更",
            "生效时间": _now(),
        }
        target["结构版本"].append(schema)
        target["当前结构版本"] = new_version
        target["当前必填字段"] = required
        target["实际可用字段"] = fields
        target["字段定义"] = schema["字段定义"]
        self._store.channel.publish("etl.target_schema_changed", {
            "target_table": table,
            "schema_version": new_version,
            "added_fields": added_names,
            "removed_fields": removed_names,
            "required_fields": required,
            "note": note,
            "changed_at": schema["生效时间"],
        })
        return self.target_detail(table)

    @staticmethod
    def _normalize_field_specs(add_fields: Any) -> dict[str, dict[str, Any]]:
        if add_fields is None:
            return {}
        raw_items = add_fields if isinstance(add_fields, list) else _as_list(add_fields)
        result: dict[str, dict[str, Any]] = {}
        for item in raw_items:
            if isinstance(item, dict):
                field = str(item.get("字段", "")).strip()
                if not field:
                    continue
                result[field] = {
                    "类型": str(item.get("类型") or _field_type(field)),
                    "必填": bool(item.get("必填", False)),
                }
            else:
                field = str(item).strip()
                if field:
                    result[field] = {"类型": _field_type(field), "必填": False}
        return result

    # ------------------------------------------------------------------
    # 交付清单
    # ------------------------------------------------------------------
    def list_manifests(self) -> list[dict[str, Any]]:
        return [self._manifest_summary(item) for item in
                sorted(self.manifests.values(), key=lambda row: row["id"], reverse=True)]

    def _manifest_summary(self, manifest: dict[str, Any]) -> dict[str, Any]:
        partitions = manifest.get("分区", [])
        delivered = [item for item in partitions if item["状态"] == "已交付"]
        failed = [item for item in partitions if item["状态"] in {"交付失败", "结构不兼容"}]
        return {
            "id": manifest["id"],
            "清单编号": manifest["清单编号"],
            "目标表": manifest["目标表"],
            "规则ID": manifest["规则ID"],
            "规则名称": manifest["规则名称"],
            "规则版本": manifest["规则版本"],
            "源批次": manifest["源批次"],
            "模型结构版本": manifest["模型结构版本"],
            "部署环境": manifest["部署环境"],
            "状态": manifest["状态"],
            "分区总数": len(partitions),
            "已交付分区数": len(delivered),
            "失败分区数": len(failed),
            "试用运行次数": len(manifest.get("试运行记录", [])),
            "创建时间": manifest["创建时间"],
            "最近交付时间": manifest.get("最近交付时间", ""),
        }

    def create_manifest(
        self,
        rule_id: int,
        source_batch: str,
        environment: str = "预发",
        schema_version: str | None = None,
        note: str = "",
    ) -> dict[str, Any]:
        rule = self.get_rule(rule_id)
        target = self.ensure_target_for_rule(rule)
        batch = self.source_batches.get(source_batch)
        if batch is None:
            raise DeliveryError(f"源批次 {source_batch} 未登记，无法生成交付清单")
        if batch.get("目标表") and batch["目标表"] != target["表名"]:
            raise DeliveryError(
                f"源批次 {source_batch} 属于目标表 {batch['目标表']}，与规则目标表 {target['表名']} 不一致"
            )

        schema = self._schema_of(target, schema_version)
        fields = list(schema["字段"])
        required = list(schema["必填字段"])
        rule_snapshot = {
            "规则ID": rule.get("id"),
            "规则名称": rule.get("规则名称", ""),
            "规则版本": self.rule_version(rule),
            "源表": rule.get("源表", ""),
            "目标表": rule.get("目标表", ""),
            "字段映射": rule.get("字段映射", ""),
            "清洗逻辑": rule.get("清洗逻辑", ""),
            "转换函数": rule.get("转换函数", ""),
            "写入模式": rule.get("写入模式", ""),
        }
        manifest_id = self._next_manifest_id
        self._next_manifest_id += 1
        manifest_no = f"MDL-{datetime.now().strftime('%Y%m%d')}-{manifest_id:03d}"

        target_fields = target["实际可用字段"]
        mapping = parse_field_mapping(rule_snapshot["字段映射"])
        mapping_targets = [target_field for _, target_field in mapping]
        global_errors = self._compatibility_errors(
            fields=fields,
            required=required,
            target_fields=target_fields,
            mapping_targets=mapping_targets,
            partition_field=target["分区字段"],
            record_keys={key for record in batch["记录"] for key in record},
        )
        consistency = {
            "规则与结构匹配": not global_errors,
            "映射目标字段": mapping_targets,
            "结构字段": fields,
            "错误字段": global_errors,
        }

        skeleton = {"目标表": target["表名"], "目标字段": fields, "规则快照": rule_snapshot}
        partitions: list[dict[str, Any]] = []
        for partition, records in self._group_records(batch, target["分区字段"]).items():
            rows, _ = self._build_rows(skeleton, {"分区": partition}, records)
            row_errors = self._row_required_errors(rows, required)
            # 结构层面的不兼容（字段缺失/规则未覆盖）直接判为结构不兼容；
            # 源记录取值为空属于行级问题，清单仍可登记，正式交付时再按分区失败处理。
            errors = list(global_errors)
            source_ids = [str(record.get("源记录ID", "")) for record in records if record.get("源记录ID")]
            partition_entry: dict[str, Any] = {
                "分区": partition,
                "分区条件": f"{target['分区字段']}={partition}",
                "源批次": source_batch,
                "源记录": source_ids,
                "源记录数": len(records),
                "状态": "结构不兼容" if errors else "待交付",
                "转换规则版本": rule_snapshot["规则版本"],
                "结构版本": schema["版本"],
                "目标字段": fields,
                "错误字段": errors,
                "行级校验": row_errors,
                "重试次数": 0,
                "首次失败": None,
                "交付记录": [],
                "交付编号": "",
                "交付时间": "",
                "可复算": True,
            }
            if errors:
                partition_entry["首次失败"] = {
                    "源批次": source_batch,
                    "原因": "目标结构不兼容",
                    "错误字段": errors,
                    "时间": _now(),
                }
            elif row_errors:
                # 行级为空不算结构不兼容，但同样要让失败第一次就被记到源批次上。
                partition_entry["首次失败"] = {
                    "源批次": source_batch,
                    "原因": "必填字段为空",
                    "错误字段": row_errors,
                    "时间": _now(),
                }
                partition_entry["沿用建议"] = "需补齐映射来源"
                partition_entry["沿用依据"] = f"必填字段取值为空：{'、'.join(row_errors)}"
            else:
                reuse = self._assess_reuse(target, partition, rule_snapshot["规则版本"],
                                           schema["版本"], fields)
                partition_entry["沿用建议"] = reuse["建议"]
                partition_entry["沿用依据"] = reuse["依据"]
            if "沿用建议" not in partition_entry:
                partition_entry["沿用建议"] = "结构不兼容，暂不可沿用"
                partition_entry["沿用依据"] = f"错误字段：{'、'.join(errors)}"
            partitions.append(partition_entry)

        manifest = {
            "id": manifest_id,
            "清单编号": manifest_no,
            "目标表": target["表名"],
            "规则ID": rule.get("id"),
            "规则名称": rule.get("规则名称", ""),
            "规则版本": rule_snapshot["规则版本"],
            "规则快照": rule_snapshot,
            "源批次": source_batch,
            "部署环境": environment or "预发",
            "模型结构版本": schema["版本"],
            "目标字段": fields,
            "必填字段": required,
            "字段结构": schema["字段定义"],
            "一致性": consistency,
            "状态": "结构不兼容" if all(item["状态"] == "结构不兼容" for item in partitions) else "待交付",
            "分区": partitions,
            "构建记录": [],
            "试运行记录": [],
            "交付记录": [],
            "说明": note,
            "创建时间": _now(),
            "最近交付时间": "",
        }
        self.manifests[manifest_id] = manifest
        self._ensure_pipeline_task(manifest)
        self._store.channel.publish("etl.manifest_created", {
            "manifest_id": manifest_id,
            "manifest_no": manifest_no,
            "target_table": target["表名"],
            "source_batch": source_batch,
            "rule_version": rule_snapshot["规则版本"],
            "schema_version": schema["版本"],
            "partitions": [item["分区"] for item in partitions],
            "compatible": consistency["规则与结构匹配"],
        })
        return deepcopy(manifest)

    def _group_records(self, batch: dict[str, Any], partition_field: str) -> dict[str, list[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in batch["记录"]:
            value = str(record.get(partition_field, "未分区"))
            grouped.setdefault(value, []).append(record)
        return dict(sorted(grouped.items()))

    @staticmethod
    def _compatibility_errors(
        *,
        fields: list[str],
        required: list[str],
        target_fields: list[str],
        mapping_targets: list[str],
        partition_field: str,
        record_keys: set[str],
    ) -> list[str]:
        """清单级结构兼容检查：映射目标、必填字段、目标表现有字段三方对齐。

        注意这里 *不* 检查源记录里的值：源系统缺值属于行级问题，
        只有结构（字段是否被规则覆盖、目标库是否具备该字段）不匹配才算结构不兼容。
        """
        errors: list[str] = []
        for field in mapping_targets:
            if field not in fields:
                errors.append(field)
        for field in required:
            if field in mapping_targets:
                continue
            if field == partition_field or field in record_keys:
                continue
            if field not in errors:
                errors.append(field)
        for field in fields:
            if field not in target_fields and field not in errors:
                errors.append(field)
        return errors

    def _assess_reuse(self, target: dict[str, Any], partition: str, rule_version: str,
                      schema_version: str, fields: list[str]) -> dict[str, str]:
        existing = target["分区"].get(partition)
        if existing is None:
            return {"建议": "需新建", "依据": "目标库尚无该分区"}
        same_version = existing.get("结构版本") == schema_version
        same_rule = existing.get("规则版本") == rule_version
        same_fields = list(existing.get("字段", [])) == fields
        if same_version and same_rule and same_fields:
            return {"建议": "可沿用", "依据": "结构版本、规则版本和字段清单一致"}
        if same_fields:
            return {"建议": "可沿用", "依据": "字段清单一致，可复算校验后沿用"}
        return {
            "建议": "需重算",
            "依据": f"目标分区结构 {existing.get('结构版本')} / 字段 {existing.get('字段')} 与清单不一致",
        }

    def get_manifest(self, manifest_id: int) -> dict[str, Any]:
        manifest = self.manifests.get(int(manifest_id))
        if manifest is None:
            raise DeliveryError(f"交付清单 {manifest_id} 不存在")
        return manifest

    def manifest_detail(self, manifest_id: int) -> dict[str, Any]:
        manifest = deepcopy(self.get_manifest(manifest_id))
        task = self._pipeline_task(manifest["清单编号"])
        manifest["部署任务"] = deepcopy(task) if task else None
        manifest["目标表实际可用字段"] = list(
            self.targets.get(manifest["目标表"], {}).get("实际可用字段", [])
        )
        return manifest

    # ------------------------------------------------------------------
    # 正式交付 / 试运行
    # ------------------------------------------------------------------
    def run_manifest(
        self,
        manifest_id: int,
        mode: str = "正式",
        partitions: list[str] | None = None,
        operator: str = "部署流水线",
    ) -> dict[str, Any]:
        manifest = self.get_manifest(manifest_id)
        normalized_mode = str(mode or "正式").strip()
        test_run = normalized_mode in TEST_MODES
        if not test_run and normalized_mode not in FORMAL_MODES:
            raise DeliveryError(f"未知执行模式 {mode}，只允许正式或试运行")

        wanted = set(str(item) for item in (partitions or []))
        targets = [
            item for item in manifest["分区"]
            if not wanted or item["分区"] in wanted
        ]
        if not targets:
            raise DeliveryError("没有匹配的分区可执行")

        if test_run:
            return self._test_run(manifest, targets, operator)

        target_table = self.targets[manifest["目标表"]]
        current_schema = self._schema_of(target_table)
        actual_fields = list(target_table["实际可用字段"])
        global_errors: list[str] = []
        for field in manifest["目标字段"]:
            if field not in actual_fields and field not in global_errors:
                global_errors.append(field)
        for field in current_schema["必填字段"]:
            if field not in manifest["目标字段"] and field not in global_errors:
                global_errors.append(field)

        summary = {
            "清单编号": manifest["清单编号"],
            "模式": "正式",
            "目标表": manifest["目标表"],
            "源批次": manifest["源批次"],
            "结构版本": manifest["模型结构版本"],
            "规则版本": manifest["规则版本"],
            "已交付分区": [],
            "未交付分区": [],
            "幂等跳过": [],
            "结构不兼容分区": [],
            "发布条数": 0,
            "目标字段": list(manifest["目标字段"]),
            "执行时间": _now(),
        }

        for item in targets:
            # 幂等优先：这份清单已经成功发布过的分区，无论目标表结构后来如何演进，
            # 都直接命中已有交付，不再重复发布，也不被后来的结构变更改判为失败。
            publication_key = self._publication_key(manifest, item)
            existing_publication = self.publications.get(publication_key)
            existing_partition = target_table["分区"].get(item["分区"])
            already_written = (
                existing_publication is not None
                and existing_partition is not None
                and existing_partition.get("交付编号") == existing_publication["交付编号"]
            )
            if already_written:
                item["状态"] = "已交付"
                item["可复算"] = True
                item["确认"] = True
                item["错误字段"] = []
                item["交付记录"].append({
                    "交付编号": existing_publication["交付编号"],
                    "模式": "正式",
                    "结果": "幂等命中（未重复发布）",
                    "时间": _now(),
                    "字段": list(manifest["目标字段"]),
                })
                summary["幂等跳过"].append(item["分区"])
                summary["已交付分区"].append(item["分区"])
                continue

            errors = list(global_errors) + [
                error for error in item.get("错误字段", []) if error not in global_errors
            ]
            records = self._records_for_partition(manifest)
            source_records = records.get(item["分区"], [])
            if errors:
                self._mark_failure(manifest, item, errors, "目标结构不兼容")
                summary["结构不兼容分区"].append(item["分区"])
                summary["未交付分区"].append(item["分区"])
                continue

            rows, source_ids = self._build_rows(manifest, item, source_records)
            row_errors = self._row_required_errors(rows, manifest["必填字段"])
            if row_errors:
                self._mark_failure(manifest, item, row_errors, "必填字段为空")
                summary["结构不兼容分区"].append(item["分区"])
                summary["未交付分区"].append(item["分区"])
                continue

            delivery_no = f"DLV-{manifest['清单编号']}-{item['分区']}"
            if existing_partition is not None:
                history = existing_partition.setdefault("历史", [])
                history.append({key: value for key, value in existing_partition.items() if key != "历史"})
            target_table["分区"][item["分区"]] = {
                "结构版本": manifest["模型结构版本"],
                "字段": list(manifest["目标字段"]),
                "行": rows,
                "源记录": source_ids,
                "源批次": manifest["源批次"],
                "规则版本": manifest["规则版本"],
                "交付清单编号": manifest["清单编号"],
                "交付编号": delivery_no,
                "交付时间": _now(),
                "行数": len(rows),
                "状态": "已交付",
            }
            publication = {
                "交付编号": delivery_no,
                "幂等键": publication_key,
                "清单编号": manifest["清单编号"],
                "清单ID": manifest["id"],
                "目标表": manifest["目标表"],
                "分区": item["分区"],
                "源批次": manifest["源批次"],
                "规则版本": manifest["规则版本"],
                "结构版本": manifest["模型结构版本"],
                "字段": list(manifest["目标字段"]),
                "源记录": source_ids,
                "行数": len(rows),
                "模式": "正式",
                "状态": "成功",
                "交付时间": _now(),
            }
            self.publications[publication_key] = publication
            summary["发布条数"] += 1

            item["状态"] = "已交付"
            item["可复算"] = True
            item["确认"] = True
            item["交付编号"] = delivery_no
            item["交付时间"] = publication["交付时间"]
            item["交付记录"].append({
                "交付编号": delivery_no,
                "模式": "正式",
                "结果": "成功",
                "时间": publication["交付时间"],
                "字段": list(manifest["目标字段"]),
                "行数": len(rows),
            })
            summary["已交付分区"].append(item["分区"])
            self._store.channel.publish("etl.partition_delivered", {
                "manifest_no": manifest["清单编号"],
                "target_table": manifest["目标表"],
                "partition": item["分区"],
                "source_batch": manifest["源批次"],
                "source_records": source_ids,
                "rule_version": manifest["规则版本"],
                "schema_version": manifest["模型结构版本"],
                "delivery_no": delivery_no,
                "rows": len(rows),
            })

        self._finalize_manifest(manifest, summary)
        return summary

    def _test_run(self, manifest: dict[str, Any], targets: list[dict[str, Any]],
                  operator: str) -> dict[str, Any]:
        previews: list[dict[str, Any]] = []
        records = self._records_for_partition(manifest)
        for item in targets:
            rows, source_ids = self._build_rows(manifest, item, records.get(item["分区"], []))
            errors = list(item.get("错误字段", [])) + self._row_required_errors(
                rows, manifest["必填字段"]
            )
            previews.append({
                "分区": item["分区"],
                "源批次": manifest["源批次"],
                "源记录": source_ids,
                "行数": len(rows),
                "目标字段": list(manifest["目标字段"]),
                "错误字段": list(dict.fromkeys(errors)),
                "可直接交付": not errors,
                "样例": rows[:3],
            })
        run_at = _now()
        record = {
            "模式": "试运行",
            "执行人": operator,
            "时间": run_at,
            "清单编号": manifest["清单编号"],
            "源批次": manifest["源批次"],
            "结构版本": manifest["模型结构版本"],
            "规则版本": manifest["规则版本"],
            "分区结果": previews,
            "发布条数": 0,
            "说明": "试运行只用于核对，不写入目标库，也不改变分区交付状态",
        }
        manifest["试运行记录"].append(record)
        manifest["构建记录"].append({
            "构建编号": f"TRY-{manifest['清单编号']}-{len(manifest['构建记录']) + 1:02d}",
            "模式": "试运行",
            "时间": run_at,
            "执行人": operator,
            "结构版本": manifest["模型结构版本"],
            "规则版本": manifest["规则版本"],
            "分区数": len(previews),
            "发布条数": 0,
            "分区结果": [
                {"分区": item["分区"], "状态": "试运行通过" if item["可直接交付"] else "试运行发现错误",
                 "错误字段": item["错误字段"]}
                for item in previews
            ],
        })
        return {
            "清单编号": manifest["清单编号"],
            "模式": "试运行",
            "正式": False,
            "目标表": manifest["目标表"],
            "源批次": manifest["源批次"],
            "结构版本": manifest["模型结构版本"],
            "分区结果": previews,
            "发布条数": 0,
            "提示": "试运行结果不计入正式交付，部署流水线状态未推进",
            "执行时间": run_at,
        }

    def check_partition_units(self, manifest_id: int, operator: str = "部署流水线") -> dict[str, Any]:
        """部署流水线的分区单元对账：逐分区只读核对结构是否可用于正式交付。

        与试运行一样不写目标库、不改分区交付状态，区别在于它按分区明确给出
        “可正式交付 / 被拦下”，便于流水线只推进结构匹配的分区。
        """
        manifest = self.get_manifest(manifest_id)
        target = self.targets[manifest["目标表"]]
        actual_fields = list(target["实际可用字段"])
        records = self._records_for_partition(manifest)
        results: list[dict[str, Any]] = []
        compatible: list[str] = []
        blocked: list[str] = []
        global_errors = [
            field for field in manifest["目标字段"] if field not in actual_fields
        ]
        for item in manifest["分区"]:
            errors = list(dict.fromkeys(global_errors + list(item.get("错误字段", []))))
            if not errors:
                rows, _ = self._build_rows(manifest, item, records.get(item["分区"], []))
                errors = self._row_required_errors(rows, manifest["必填字段"])
            results.append({
                "分区": item["分区"],
                "源批次": item["源批次"],
                "源记录": list(item["源记录"]),
                "结构版本": manifest["模型结构版本"],
                "规则版本": manifest["规则版本"],
                "错误字段": errors,
                "可正式交付": not errors,
            })
            (blocked if errors else compatible).append(item["分区"])
        return {
            "清单编号": manifest["清单编号"],
            "模式": "分区单元对账",
            "执行人": operator,
            "目标表": manifest["目标表"],
            "源批次": manifest["源批次"],
            "结构版本": manifest["模型结构版本"],
            "规则版本": manifest["规则版本"],
            "分区结果": results,
            "可交付分区": compatible,
            "被拦下分区": blocked,
            "发布条数": 0,
            "时间": _now(),
            "说明": "只读对账，不写目标库",
        }

    @staticmethod
    def _row_required_errors(rows: list[dict[str, Any]], required: list[str]) -> list[str]:
        errors = []
        for field in required:
            if any(row.get(field) in (None, "") for row in rows):
                errors.append(field)
        return errors

    def _records_for_partition(self, manifest: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        batch = self.source_batches.get(manifest["源批次"])
        if batch is None:
            raise DeliveryError(f"源批次 {manifest['源批次']} 已不存在，无法复算")
        target = self.targets[manifest["目标表"]]
        return self._group_records(batch, target["分区字段"])

    def manifest_for_batch(self, source_batch: str) -> list[dict[str, Any]]:
        """按源批次列出所有关联的清单（旧作业和新结构各自核对时用）。"""
        return [
            self._manifest_summary(item)
            for item in sorted(self.manifests.values(), key=lambda row: row["id"])
            if item["源批次"] == source_batch
        ]

    def _build_rows(self, manifest: dict[str, Any], item: dict[str, Any],
                    records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
        target = self.targets[manifest["目标表"]]
        partition_field = target["分区字段"]
        mappings = parse_field_mapping(manifest["规则快照"]["字段映射"])
        by_target = {target_field: source_field for source_field, target_field in mappings}
        fields = list(manifest["目标字段"])
        rows: list[dict[str, Any]] = []
        source_ids: list[str] = []
        for record in records:
            row: dict[str, Any] = {}
            for field in fields:
                if field in by_target:
                    row[field] = record.get(by_target[field])
                elif field == partition_field:
                    row[field] = record.get(partition_field, item["分区"])
                elif field in record:
                    # 同名直取：用于目标表新增的普通字段
                    row[field] = record.get(field)
                else:
                    # 规则未覆盖的新字段：先按结构位置留空再统一校验
                    row[field] = None
            rows.append(row)
            source_ids.append(str(record.get("源记录ID", "")))
        return rows, source_ids

    @staticmethod
    def _publication_key(manifest: dict[str, Any], item: dict[str, Any]) -> str:
        return "pub-" + _digest(
            manifest["目标表"],
            item["分区"],
            manifest["源批次"],
            manifest["规则版本"],
            manifest["模型结构版本"],
            ",".join(manifest["目标字段"]),
        )

    def _mark_failure(self, manifest: dict[str, Any], item: dict[str, Any],
                      errors: list[str], reason: str) -> None:
        item["状态"] = "结构不兼容" if reason == "目标结构不兼容" else "交付失败"
        item["错误字段"] = list(dict.fromkeys(errors))
        item["重试次数"] = int(item.get("重试次数", 0)) + 1
        if not item.get("首次失败"):
            item["首次失败"] = {
                "源批次": manifest["源批次"],
                "原因": reason,
                "错误字段": list(item["错误字段"]),
                "时间": _now(),
            }
        item["交付记录"].append({
            "交付编号": "",
            "模式": "正式",
            "结果": f"失败：{reason}",
            "时间": _now(),
            "错误字段": list(item["错误字段"]),
        })

    def _finalize_manifest(self, manifest: dict[str, Any], summary: dict[str, Any]) -> None:
        partitions = manifest["分区"]
        delivered = [item for item in partitions if item["状态"] == "已交付"]
        failed = [item for item in partitions if item["状态"] in {"交付失败", "结构不兼容"}]
        pending = [item for item in partitions if item["状态"] == "待交付"]
        if delivered and not failed and not pending:
            status = "已交付"
        elif delivered and (failed or pending):
            status = "部分交付"
        elif failed and len(failed) == len(partitions):
            status = "结构不兼容"
        elif failed:
            status = "交付失败"
        else:
            status = "待交付"
        manifest["状态"] = status
        if delivered:
            manifest["最近交付时间"] = _now()

        formal_record = {
            "构建编号": f"BLD-{manifest['清单编号']}-{len(manifest['构建记录']) + 1:02d}",
            "模式": "正式",
            "时间": summary["执行时间"],
            "结构版本": manifest["模型结构版本"],
            "规则版本": manifest["规则版本"],
            "源批次": manifest["源批次"],
            "分区数": len(partitions),
            "已交付分区": list(summary["已交付分区"]),
            "未交付分区": list(summary["未交付分区"]),
            "幂等跳过": list(summary["幂等跳过"]),
            "结构不兼容分区": list(summary["结构不兼容分区"]),
            "发布条数": summary["发布条数"],
            "目标字段": list(manifest["目标字段"]),
        }
        manifest["构建记录"].append(formal_record)
        manifest["交付记录"].append(formal_record)
        self._update_pipeline_task(manifest, summary)
        self._store.channel.publish("etl.delivery_finished", {
            "manifest_no": manifest["清单编号"],
            "target_table": manifest["目标表"],
            "source_batch": manifest["源批次"],
            "status": manifest["状态"],
            "delivered": list(summary["已交付分区"]),
            "undelivered": list(summary["未交付分区"]),
            "published": summary["发布条数"],
        })

    # ------------------------------------------------------------------
    # 复算 / 失败定位 / 对比 / 核对
    # ------------------------------------------------------------------
    def recompute_manifest(self, manifest_id: int) -> dict[str, Any]:
        """对已确认（已交付）分区按清单快照重新计算，不产生任何发布。"""
        manifest = self.get_manifest(manifest_id)
        target = self.targets[manifest["目标表"]]
        records = self._records_for_partition(manifest)
        checked: list[dict[str, Any]] = []
        for item in manifest["分区"]:
            if item["状态"] != "已交付":
                continue
            rows, source_ids = self._build_rows(manifest, item, records.get(item["分区"], []))
            latest = target["分区"].get(item["分区"], {})
            target_rows = latest.get("行", [])
            expected = self._rows_digest(rows)
            actual = self._rows_digest(target_rows)
            checked.append({
                "分区": item["分区"],
                "可复算": True,
                "一致": expected == actual and latest.get("结构版本") == manifest["模型结构版本"],
                "结构版本": manifest["模型结构版本"],
                "目标结构版本": latest.get("结构版本", ""),
                "目标字段": list(latest.get("字段", [])),
                "源记录": source_ids,
                "计算行数": len(rows),
                "目标行数": len(target_rows),
                "计算摘要": expected,
                "目标摘要": actual,
            })
        return {
            "清单编号": manifest["清单编号"],
            "目标表": manifest["目标表"],
            "源批次": manifest["源批次"],
            "结构版本": manifest["模型结构版本"],
            "规则版本": manifest["规则版本"],
            "可复算分区数": len(checked),
            "一致分区数": sum(1 for item in checked if item["一致"]),
            "分区": checked,
            "提示": "复算只校验已确认分区，不重复发布、不改写目标库",
        }

    @staticmethod
    def _rows_digest(rows: list[dict[str, Any]]) -> str:
        normalized = [json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows]
        return _digest(*normalized) if normalized else _digest("")

    def failures_by_source_batch(self, source_batch: str) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        for manifest in self.manifests.values():
            for item in manifest["分区"]:
                first = item.get("首次失败")
                if not first:
                    continue
                if str(first.get("源批次", "")) != str(source_batch):
                    continue
                results.append({
                    "清单编号": manifest["清单编号"],
                    "清单ID": manifest["id"],
                    "目标表": manifest["目标表"],
                    "规则名称": manifest["规则名称"],
                    "源批次": source_batch,
                    "分区": item["分区"],
                    "当前状态": item["状态"],
                    "首次失败": first,
                    "错误字段": list(item.get("错误字段", [])),
                })
        return {
            "源批次": source_batch,
            "失败分区数": len(results),
            "失败分区": results,
            "可定位": bool(results),
        }

    def compare_manifests(self, old_manifest_id: int, new_manifest_id: int) -> dict[str, Any]:
        old = self.get_manifest(old_manifest_id)
        new = self.get_manifest(new_manifest_id)
        if old["源批次"] != new["源批次"]:
            raise DeliveryError("对比的两份清单必须使用同一源批次，避免批次差异混入字段对比")
        old_records = self._records_for_partition(old)
        new_records = self._records_for_partition(new)
        old_by_partition = {item["分区"]: item for item in old["分区"]}
        new_by_partition = {item["分区"]: item for item in new["分区"]}
        partition_names = sorted(set(old_by_partition) | set(new_by_partition))
        comparisons: list[dict[str, Any]] = []
        for partition in partition_names:
            old_item = old_by_partition.get(partition)
            new_item = new_by_partition.get(partition)
            old_rows, old_ids = self._rows_for_compare(old, old_item, old_records, partition)
            new_rows, new_ids = self._rows_for_compare(new, new_item, new_records, partition)
            old_by_id = {source_id: row for source_id, row in zip(old_ids, old_rows)}
            new_by_id = {source_id: row for source_id, row in zip(new_ids, new_rows)}
            only_old = sorted(set(old_by_id) - set(new_by_id))
            only_new = sorted(set(new_by_id) - set(old_by_id))
            value_changes: list[dict[str, Any]] = []
            for source_id in sorted(set(old_by_id) & set(new_by_id)):
                old_row, new_row = old_by_id[source_id], new_by_id[source_id]
                for field in sorted(set(old_row) | set(new_row)):
                    if old_row.get(field) != new_row.get(field):
                        value_changes.append({
                            "源记录ID": source_id,
                            "字段": field,
                            "旧值": old_row.get(field),
                            "新值": new_row.get(field),
                        })
            comparisons.append({
                "分区": partition,
                "旧状态": (old_item or {}).get("状态", "无此分区"),
                "新状态": (new_item or {}).get("状态", "无此分区"),
                "旧字段": list(old["目标字段"]),
                "新字段": list(new["目标字段"]),
                "仅旧结构字段": [field for field in old["目标字段"] if field not in new["目标字段"]],
                "仅新结构字段": [field for field in new["目标字段"] if field not in old["目标字段"]],
                "共源记录数": len(set(old_by_id) & set(new_by_id)),
                "仅旧源记录": only_old,
                "仅新源记录": only_new,
                "值差异": value_changes,
                "行数一致": len(old_rows) == len(new_rows),
            })
        return {
            "旧清单": old["清单编号"],
            "新清单": new["清单编号"],
            "源批次": old["源批次"],
            "旧结构版本": old["模型结构版本"],
            "新结构版本": new["模型结构版本"],
            "旧规则版本": old["规则版本"],
            "新规则版本": new["规则版本"],
            "分区对比": comparisons,
            "差异分区数": sum(
                1 for item in comparisons
                if item["值差异"] or item["仅旧结构字段"] or item["仅新结构字段"]
                or item["仅旧源记录"] or item["仅新源记录"]
            ),
            "提示": "两侧按源记录ID 对齐，可分别核对旧作业结果和新结构结果",
        }

    def _rows_for_compare(
        self,
        manifest: dict[str, Any],
        item: dict[str, Any] | None,
        records: dict[str, list[dict[str, Any]]],
        partition: str,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        if item is None:
            return [], []
        if item["状态"] == "已交付":
            target = self.targets[manifest["目标表"]]
            partition_data = target["分区"].get(partition, {})
            return (
                list(partition_data.get("行", [])),
                list(partition_data.get("源记录", [])),
            )
        return self._build_rows(manifest, item, records.get(partition, []))

    def verify_manifest(self, manifest_id: int) -> dict[str, Any]:
        manifest = self.get_manifest(manifest_id)
        target = self.targets[manifest["目标表"]]
        task = self._pipeline_task(manifest["清单编号"])
        delivered = [item["分区"] for item in manifest["分区"] if item["状态"] == "已交付"]
        formal_builds = [item for item in manifest["构建记录"] if item["模式"] == "正式"]
        latest_build = formal_builds[-1] if formal_builds else None
        pipeline_delivered = list(task.get("已交付分区", [])) if task else []
        actual_fields = list(target["实际可用字段"])
        missing_fields = [field for field in manifest["目标字段"] if field not in actual_fields]

        version_mismatch: list[dict[str, Any]] = []
        mixed_version: list[str] = []
        superseded: list[dict[str, str]] = []
        for partition in delivered:
            data = target["分区"].get(partition, {})
            owner = data.get("交付清单编号", "")
            if owner and owner != manifest["清单编号"]:
                # 同一分区被更新的清单整分区重写过：旧清单的交付记录仍可核对，
                # 但目标库当前内容归属新清单，不能算旧清单的版本不一致。
                superseded.append({"分区": partition, "当前归属清单": owner})
            elif data.get("结构版本") != manifest["模型结构版本"]:
                version_mismatch.append({
                    "分区": partition,
                    "目标结构版本": data.get("结构版本", ""),
                })
            expected = list(data.get("字段", manifest["目标字段"]))
            for row in data.get("行", []):
                if list(row.keys()) != expected:
                    mixed_version.append(partition)
                    break

        # 试运行不会生成交付编号：凡是标记为“已交付”的分区都必须有正式交付编号，
        # 否则说明有人把试运行结果当成了正式交付。
        delivered_without_delivery_no = [
            item["分区"] for item in manifest["分区"]
            if item["状态"] == "已交付" and not item.get("交付编号")
        ]

        checks = {
            "构建记录存在": latest_build is not None,
            "构建记录与清单一致": bool(
                latest_build
                and latest_build.get("结构版本") == manifest["模型结构版本"]
                and latest_build.get("规则版本") == manifest["规则版本"]
            ),
            "流水线已交付分区一致": sorted(pipeline_delivered) == sorted(delivered),
            "目标字段实际可用": not missing_fields,
            "分区结构版本一致": not version_mismatch,
            "分区字段未混版": not mixed_version,
            "试运行未计入正式交付": not delivered_without_delivery_no,
        }
        return {
            "清单编号": manifest["清单编号"],
            "目标表": manifest["目标表"],
            "源批次": manifest["源批次"],
            "结构版本": manifest["模型结构版本"],
            "规则版本": manifest["规则版本"],
            "构建记录": formal_builds,
            "流水线": task,
            "目标表实际可用字段": actual_fields,
            "清单目标字段": list(manifest["目标字段"]),
            "缺少字段": missing_fields,
            "已交付分区": delivered,
            "流水线已交付分区": pipeline_delivered,
            "版本不一致分区": version_mismatch,
            "已由新清单覆盖的分区": superseded,
            "混版分区": mixed_version,
            "检查项": checks,
            "通过": all(checks.values()),
            "提示": "核对构建记录、流水线已交付分区和目标数据库实际可用字段；试运行不视为正式交付",
        }

    # ------------------------------------------------------------------
    # 部署流水线联动
    # ------------------------------------------------------------------
    def _pipeline_task(self, manifest_no: str) -> dict[str, Any] | None:
        for row in self._store.rows("deploy_pipeline"):
            if row.get("交付清单编号") == manifest_no:
                return row
        return None

    def _ensure_pipeline_task(self, manifest: dict[str, Any]) -> dict[str, Any]:
        existing = self._pipeline_task(manifest["清单编号"])
        if existing:
            return existing
        rows = self._store.rows("deploy_pipeline")
        row = {
            "id": max((int(item.get("id", 0)) for item in rows), default=0) + 1,
            "status": "待部署",
            "pending": True,
            "abnormal": False,
            "任务名称": f"ETL分区交付 {manifest['清单编号']}",
            "目标模型": manifest["目标表"],
            "部署环境": manifest["部署环境"],
            "前置检查": "交付清单结构匹配/源批次可定位/目标字段可用",
            "回滚方案": "保留原分区，仅重试未交付分区",
            "部署状态": "等待分区交付",
            "执行日志": (
                f"交付清单 {manifest['清单编号']}，源批次 {manifest['源批次']}，"
                f"规则 {manifest['规则名称']}，待推进 {len(manifest['分区'])} 个分区"
            ),
            "交付清单编号": manifest["清单编号"],
            "交付清单ID": manifest["id"],
            "源批次": manifest["源批次"],
            "已交付分区": [],
            "未交付分区": [item["分区"] for item in manifest["分区"]],
            "源记录关联": [],
        }
        rows.append(row)
        return row

    def _update_pipeline_task(self, manifest: dict[str, Any], summary: dict[str, Any]) -> None:
        row = self._ensure_pipeline_task(manifest)
        delivered = [item["分区"] for item in manifest["分区"] if item["状态"] == "已交付"]
        undelivered = [item["分区"] for item in manifest["分区"] if item["状态"] != "已交付"]
        row["已交付分区"] = delivered
        row["未交付分区"] = undelivered
        row["源记录关联"] = [
            {"分区": item["分区"], "源批次": item["源批次"], "源记录": list(item["源记录"])}
            for item in manifest["分区"]
            if item["状态"] == "已交付"
        ]
        row["执行日志"] = (
            f"{manifest['清单编号']}：已交付 {len(delivered)}/{len(manifest['分区'])} 个分区；"
            f"本次发布 {summary['发布条数']} 条，幂等跳过 {len(summary['幂等跳过'])} 条；"
            f"未交付 {','.join(undelivered) if undelivered else '无'}"
        )
        if delivered and not undelivered:
            row["status"] = "已部署"
            row["部署状态"] = "成功"
            row["pending"] = False
            row["abnormal"] = False
        elif delivered:
            row["status"] = "部署失败"
            row["部署状态"] = "部分分区未交付"
            row["pending"] = True
            row["abnormal"] = True
        else:
            row["status"] = "部署失败"
            row["部署状态"] = "全部分区未交付"
            row["pending"] = True
            row["abnormal"] = True


# ----------------------------------------------------------------------
# 示例源批次：同一批数据可以在目标表新增字段前后分别继续部署
# ----------------------------------------------------------------------
_SOURCE_BATCHES: dict[str, dict[str, Any]] = {
    "BATCH-20261118-POS": {
        "源批次": "BATCH-20261118-POS",
        "源表": "pos_sales_raw",
        "目标表": "dwd_sales_detail",
        "分区字段": "dt",
        "状态": "可交付",
        "记录": [
            {"源记录ID": "SRC-POS-0001", "order_id": "SO-1001", "amt": "199.00",
             "store_id": "ST-01", "dt": "2026-11-18"},
            {"源记录ID": "SRC-POS-0002", "order_id": "SO-1002", "amt": "88.50",
             "store_id": "ST-02", "dt": "2026-11-18"},
            {"源记录ID": "SRC-POS-0003", "order_id": "SO-1003", "amt": "120.00",
             "store_id": "ST-01", "dt": "2026-11-18"},
            {"源记录ID": "SRC-POS-0004", "order_id": "SO-1004", "amt": "45.00",
             "store_id": "ST-03", "dt": "2026-11-19"},
            {"源记录ID": "SRC-POS-0005", "order_id": "SO-1005", "amt": "0.00",
             "store_id": "ST-02", "dt": "2026-11-19"},
        ],
    },
    "BATCH-20261119-POS": {
        "源批次": "BATCH-20261119-POS",
        "源表": "pos_sales_raw",
        "目标表": "dwd_sales_detail",
        "分区字段": "dt",
        "状态": "可交付",
        "记录": [
            {"源记录ID": "SRC-POS-0101", "order_id": "SO-1101", "amt": "59.90",
             "store_id": "ST-01", "dt": "2026-11-20"},
            {"源记录ID": "SRC-POS-0102", "order_id": "SO-1102", "amt": "260.00",
             "store_id": "ST-04", "dt": "2026-11-20"},
            {"源记录ID": "SRC-POS-0103", "order_id": "SO-1103", "amt": "18.00",
             "store_id": "ST-03", "dt": "2026-11-21"},
        ],
    },
    "BATCH-20261118-WMS": {
        "源批次": "BATCH-20261118-WMS",
        "源表": "warehouse_op_log",
        "目标表": "dwd_warehouse_change",
        "分区字段": "dt",
        "状态": "可交付",
        "记录": [
            {"源记录ID": "SRC-WMS-0001", "inv_id": "INV-001", "qty_change": "12",
             "dt": "2026-11-18"},
            {"源记录ID": "SRC-WMS-0002", "inv_id": "INV-002", "qty_change": "-3",
             "dt": "2026-11-18"},
        ],
    },
    "BATCH-20261118-CRM": {
        "源批次": "BATCH-20261118-CRM",
        "源表": "ext_customer",
        "目标表": "dim_customer",
        "分区字段": "dt",
        "状态": "可交付",
        "记录": [
            {"源记录ID": "SRC-CRM-0001", "cust_id": "C-1001", "level": "GOLD",
             "dt": "2026-11-18"},
            {"源记录ID": "SRC-CRM-0002", "cust_id": "C-1002", "level": "SILVER",
             "dt": "2026-11-18"},
            {"源记录ID": "SRC-CRM-0003", "cust_id": "C-1003", "level": "GOLD",
             "dt": "2026-11-19"},
        ],
    },
    "BATCH-20261118-SCM": {
        "源批次": "BATCH-20261118-SCM",
        "源表": "scm_supplier",
        "目标表": "dim_supplier",
        "分区字段": "dt",
        "状态": "可交付",
        "记录": [
            {"源记录ID": "SRC-SCM-0001", "supp_id": "S-2001", "grade": "A",
             "dt": "2026-11-18"},
            {"源记录ID": "SRC-SCM-0002", "supp_id": "S-2002", "grade": "B",
             "dt": "2026-11-18"},
        ],
    },
}
