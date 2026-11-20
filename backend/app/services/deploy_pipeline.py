"""部署流水线服务：按 ETL 分区交付清单推进数仓模型的分区交付。

改造后的职责边界：
- 部署任务不再自己“模拟 ETL 成功”，而是绑定一份 ETL 分区交付清单
  （由 ``etl_mapping`` + 源批次 + 目标表结构版本生成）。
- 执行部署 -> 按清单逐分区正式交付：结构不兼容的分区保留为未交付，
  并带出错误字段；已确认分区可复算；重复执行走幂等键，不重复发布。
- 试运行结果单独记录，绝不推进流水线状态，避免用测试结果伪造正式交付。
- 失败分区可通过源批次反查，便于旧作业和新结构各自核对。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

ENVIRONMENTS = {"开发", "预发", "生产", "灰度"}


class DeployPipelineService(BaseModuleService):
    MODULE = "deploy_pipeline"
    ENTITY = "部署任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务名称", "目标模型", "部署环境", "前置检查", "回滚方案", "部署状态", "执行日志"]
    REQUIRED_FIELDS = ["任务名称", "目标模型", "部署环境"]
    STATUS_ORDER = ["待部署", "部署中", "已部署", "部署失败", "已回滚"]
    ACTION_RULES = {"执行部署": "待部署", "触发回滚": "已部署"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """创建部署任务：检查部署环境，并提示需要哪份分区交付清单。"""
        env = str(values.get("部署环境", "")).strip()
        if env not in ENVIRONMENTS:
            env = "预发"
            values["部署环境"] = env

        target_model = str(values.get("目标模型", "")).strip()

        # 消费上游发布事件，自动构建前置检查列表
        prerequisites = _gather_prerequisites(target_model)
        if prerequisites:
            values["前置检查"] = "/".join(prerequisites)

        # 自动生成回滚方案
        if not values.get("回滚方案"):
            values["回滚方案"] = _generate_rollback_plan(target_model)

        # 关联该目标模型最近的一份交付清单（如果已生成）
        manifest = _latest_manifest_for(target_model)
        if manifest:
            values["交付清单编号"] = manifest["清单编号"]
            values["交付清单ID"] = manifest["id"]
            values["源批次"] = manifest["源批次"]
        else:
            values["交付清单编号"] = ""
            values["源批次"] = ""

        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing

        # super.create_entry 只保留 LIST_FIELDS，这里补回交付清单关联字段
        entry["交付清单编号"] = manifest["清单编号"] if manifest else ""
        entry["交付清单ID"] = manifest["id"] if manifest else None
        entry["源批次"] = manifest["源批次"] if manifest else ""
        entry["已交付分区"] = []
        entry["未交付分区"] = []
        entry["源记录关联"] = []

        store.channel.publish("deploy.task_created", {
            "task_name": entry["任务名称"],
            "target_model": target_model,
            "environment": env,
            "prerequisites": prerequisites,
            "rollback_plan": entry.get("回滚方案"),
            "manifest_no": entry.get("交付清单编号", ""),
            "entry_id": entry["id"],
        })

        store.artifacts.register(
            "deploy_pipeline", "deploy_task",
            f"deploy_{entry['id']}",
            {"task_name": entry["任务名称"], "target_model": target_model, "env": env},
            source_modules=["fact_design", "agg_summary", "etl_mapping", "deploy_pipeline"],
        )
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action not in self.ACTION_RULES:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        if action == "触发回滚":
            super().run_action(entry_id, action)  # 复用状态流转
            rollback_result = _execute_rollback(entry)
            entry["status"] = "已回滚"
            entry["部署状态"] = f"已回滚至{rollback_result.get('rollback_version', '上一版本')}"
            entry["执行日志"] = f"回滚原因: {rollback_result.get('reason', '手动触发')}"
            entry["pending"] = True
            entry["abnormal"] = True
            store.channel.publish("deploy.rollback", {
                "task_name": entry["任务名称"],
                "target_model": entry.get("目标模型", ""),
                "rollback_to": rollback_result.get("rollback_version"),
                "entry_id": entry_id,
            })
            return entry, f"{self.ENTITY}已{action}"

        return self._execute_delivery(entry)

    # ------------------------------------------------------------------
    # 执行部署：以 ETL 分区交付清单为准
    # ------------------------------------------------------------------
    def _execute_delivery(self, entry: dict[str, Any]) -> tuple[dict[str, Any], str]:
        manifest_id = entry.get("交付清单ID") or _manifest_id_of(entry.get("交付清单编号", ""))
        if manifest_id is None:
            # 任务登记时清单可能还没生成：执行时按目标表再找一次最近的清单。
            latest = _latest_manifest_for(entry.get("目标模型", ""))
            if latest is not None:
                manifest_id = latest["id"]
                entry["交付清单ID"] = latest["id"]
                entry["交付清单编号"] = latest["清单编号"]
                entry["源批次"] = latest["源批次"]
        if manifest_id is None:
            _mark_task_failed(
                entry,
                "未绑定 ETL 分区交付清单，拒绝以模拟结果执行正式部署",
                reason="缺少交付清单",
            )
            return entry, "部署失败: 未绑定 ETL 分区交付清单"

        manifest = store.delivery.get_manifest(int(manifest_id))
        entry["status"] = "部署中"
        entry["pending"] = True

        prerequisite = _run_preflight_checks(entry, manifest)
        if not prerequisite["all_passed"]:
            _mark_task_failed(entry, prerequisite["detail"], reason="前置检查失败")
            _record_retry(entry, prerequisite["detail"])
            return entry, f"部署失败: {prerequisite['detail']}"

        summary = store.delivery.run_manifest(int(manifest_id), mode="正式", operator=entry["任务名称"])
        delivered = list(summary["已交付分区"])
        undelivered = list(summary["未交付分区"])
        entry["已交付分区"] = delivered
        entry["未交付分区"] = undelivered
        entry["源记录关联"] = [
            {
                "分区": item["分区"],
                "源批次": item["源批次"],
                "源记录": list(item["源记录"]),
                "交付编号": item.get("交付编号", ""),
            }
            for item in manifest["分区"]
            if item["分区"] in delivered
        ]
        entry["交付清单编号"] = manifest["清单编号"]
        entry["源批次"] = manifest["源批次"]
        entry["交付结构版本"] = manifest["模型结构版本"]
        entry["交付规则版本"] = manifest["规则版本"]

        if undelivered:
            errors: dict[str, list[str]] = {
                item["分区"]: list(item.get("错误字段", []))
                for item in manifest["分区"]
                if item["分区"] in undelivered
            }
            entry["错误字段"] = errors
            detail = "；".join(
                f"{partition} 未交付({','.join(fields) or '未说明'})"
                for partition, fields in errors.items()
            )
            _mark_task_failed(
                entry,
                f"{manifest['清单编号']} 保留未交付分区：{detail}",
                reason="目标结构不兼容",
            )
            _record_retry(entry, detail)
            store.channel.publish("deploy.failed", {
                "task_name": entry["任务名称"],
                "target_model": entry.get("目标模型", ""),
                "manifest_no": manifest["清单编号"],
                "source_batch": manifest["源批次"],
                "reason": detail,
                "undelivered": undelivered,
                "entry_id": entry["id"],
            })
            return entry, f"部署失败: 保留 {len(undelivered)} 个未交付分区"

        # 全部交付成功
        entry["status"] = "已部署"
        entry["部署状态"] = "成功"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["错误字段"] = {}
        entry["执行日志"] = (
            f"{manifest['清单编号']}：{len(delivered)} 个分区交付完成，"
            f"本次发布 {summary['发布条数']} 条，幂等跳过 {len(summary['幂等跳过'])} 条，"
            f"结构版本 {manifest['模型结构版本']}"
        )
        store.channel.publish("deploy.published", {
            "task_name": entry["任务名称"],
            "target_model": entry.get("目标模型", ""),
            "environment": entry.get("部署环境", ""),
            "deployed_at": datetime.now(timezone.utc).isoformat(),
            "source_version": manifest["模型结构版本"],
            "manifest_no": manifest["清单编号"],
            "source_batch": manifest["源批次"],
            "partitions": delivered,
            "entry_id": entry["id"],
        })
        cp_id = store.recovery.create_checkpoint("deploy_pipeline", f"deploy_{entry['id']}", {
            "target_model": entry.get("目标模型", ""),
            "manifest_no": manifest["清单编号"],
            "source_batch": manifest["源批次"],
            "partitions": delivered,
            "deployed_at": datetime.now(timezone.utc).isoformat(),
        })
        store.recovery.mark_completed(cp_id)
        return entry, f"{self.ENTITY}已执行部署：{len(delivered)} 个分区"

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        env_stats: dict[str, dict[str, int]] = {}
        for row in store.rows(self.MODULE):
            env = row.get("部署环境", "未知")
            stat = env_stats.setdefault(env, {"total": 0, "deployed": 0, "failed": 0, "rollback": 0})
            stat["total"] += 1
            if row.get("status") == "已部署":
                stat["deployed"] += 1
            elif row.get("status") == "部署失败":
                stat["failed"] += 1
            elif row.get("status") == "已回滚":
                stat["rollback"] += 1
        base["environment_stats"] = env_stats
        base["manifests"] = len(store.delivery.list_manifests())
        base["delivered_partitions"] = sum(
            len(row.get("已交付分区", [])) for row in store.rows(self.MODULE)
        )
        base["undelivered_partitions"] = sum(
            len(row.get("未交付分区", [])) for row in store.rows(self.MODULE)
        )
        return base


def _latest_manifest_for(target_model: str) -> dict[str, Any] | None:
    """找到目标表最近一份交付清单，便于任务创建时自动关联。"""
    matches = [
        manifest for manifest in store.delivery.manifests.values()
        if manifest["目标表"] == target_model
    ]
    if not matches:
        return None
    return max(matches, key=lambda item: item["id"])


def _manifest_id_of(manifest_no: str) -> int | None:
    if not manifest_no:
        return None
    for manifest in store.delivery.manifests.values():
        if manifest["清单编号"] == manifest_no:
            return manifest["id"]
    return None


def _mark_task_failed(entry: dict[str, Any], detail: str, reason: str) -> None:
    entry["status"] = "部署失败"
    entry["部署状态"] = reason
    entry["执行日志"] = detail
    entry["pending"] = True
    entry["abnormal"] = True


def _record_retry(entry: dict[str, Any], reason: str) -> None:
    attempts = int(entry.get("_retry_attempt", 0)) + 1
    entry["_retry_attempt"] = attempts
    store.recovery.log_retry("deploy_pipeline", f"deploy_{entry['id']}", attempts, reason, False)


def _gather_prerequisites(target_model: str) -> list[str]:
    """基于目标模型收集前置依赖检查项。"""
    manifest = _latest_manifest_for(target_model)
    prereqs = [
        f"交付清单 {manifest['清单编号']} 已生成" if manifest else "待生成 ETL 分区交付清单"
    ]
    target = store.delivery.targets.get(target_model)
    prereqs.append(
        f"目标结构 {target['当前结构版本']}" if target else "目标表结构未登记"
    )

    if "dwd_" in target_model:
        prereqs.append("ETL映射已部署")
    elif "dim_" in target_model:
        prereqs.append("维度属性已确认")
        prereqs.append("SCD策略已启用")

    for fact in store.rows("fact_design"):
        if fact.get("事实名称", "") in target_model or target_model in fact.get("事实名称", ""):
            if fact.get("status") == "已发布":
                prereqs.append("源事实表已发布")
            break

    rule_status = {
        rule.get("status") for rule in store.rows("etl_mapping")
        if target_model == str(rule.get("目标表", ""))
    }
    if "已部署" in rule_status:
        prereqs.append("ETL映射已部署")
    return list(dict.fromkeys(prereqs))


def _generate_rollback_plan(target_model: str) -> str:
    """根据目标模型生成对应的回滚方案。"""
    if "dim_" in target_model:
        return "恢复上一版本快照"
    if "dwd_" in target_model:
        return "保留原分区，仅重试未交付分区"
    if "dws_" in target_model:
        return "删除异常分区按交付清单重建"
    return "回滚到部署前状态"


def _run_preflight_checks(entry: dict[str, Any], manifest: dict[str, Any]) -> dict[str, Any]:
    """执行部署前置检查：只依据真实登记的目标结构与规则，不做模拟通过。"""
    target_model = entry.get("目标模型", "")
    checks: dict[str, Any] = {"items": [], "all_passed": True}

    checks["items"].append({
        "check": "交付清单目标表一致",
        "passed": manifest["目标表"] == target_model,
    })
    actual_fields = list(
        store.delivery.targets.get(manifest["目标表"], {}).get("实际可用字段", [])
    )
    missing = [field for field in manifest["目标字段"] if field not in actual_fields]
    checks["items"].append({
        "check": f"目标字段可用({len(manifest['目标字段']) - len(missing)}/{len(manifest['目标字段'])})",
        "passed": not missing,
        "missing": missing,
    })

    rule = store.delivery.get_rule(manifest["规则ID"])
    current_rule_version = store.delivery.rule_version(rule)
    checks["items"].append({
        "check": "转换规则版本与清单一致",
        "passed": current_rule_version == manifest["规则版本"],
        "current": current_rule_version,
        "manifest": manifest["规则版本"],
    })

    checks["all_passed"] = all(item["passed"] for item in checks["items"])
    checks["detail"] = "；".join(
        f"{item['check']}: {'通过' if item['passed'] else '失败'}"
        for item in checks["items"]
    )
    return checks


def _execute_rollback(entry: dict[str, Any]) -> dict[str, Any]:
    """执行回滚操作：以目标库中该任务最近交付的分区版本作为回退点。"""
    return {
        "rollback_version": entry.get("交付结构版本") or "上一版本",
        "reason": "校验未通过，手动触发回滚",
        "rolled_back_at": datetime.now(timezone.utc).isoformat(),
    }
