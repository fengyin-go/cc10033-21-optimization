# 维度建模开发中心

面向数据仓库建设场景，提供维度建模、事实表设计、缓慢变化维和汇总层构建的建模平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/views/DeliveryManifestView.vue  ETL 分区交付清单工作台
│   ├── src/data/modules.ts              20个业务模块配置
│   ├── src/api/              统一请求封装（delivery.ts 为交付清单接口）
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/delivery_registry.py  ETL 分区交付清单/目标表结构/分区数据登记簿
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/routers/delivery.py  交付清单、源批次与目标表结构接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   ├── app/services/deploy_pipeline.py  按交付清单推进分区的部署流水线
│   ├── app/store.py          内存数据仓库与示例数据
│   └── tests/test_delivery.py  交付清单业务测试
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 维度定义 | `dim_definition` | 维度表 | 维度编码、维度名称、主键定义 |
| 维度属性 | `dim_attr` | 属性字段 | 属性名称、所属维度、数据类型 |
| 事实设计 | `fact_design` | 事实表 | 事实名称、业务过程、粒度声明 |
| 度量配置 | `measure_config` | 度量字段 | 度量名称、所属事实、度量类型 |
| SCD处理 | `scd_handler` | SCD策略 | 策略名称、目标维度、SCD类型 |
| 代理键 | `surrogate_key` | 键值定义 | 键值名称、目标维度、生成策略 |
| 层级构建 | `hierarchy_build` | 层级关系 | 层级名称、所属维度、父子字段 |
| 星型查询 | `star_query` | 查询定义 | 查询名称、事实表、关联维度 |
| 汇总构建 | `agg_summary` | 汇总表 | 汇总名称、源事实表、汇总维度 |
| 桥接关联 | `dim_bridge` | 桥接表 | 桥接名称、多对多维度、关联事实 |
| ETL映射 | `etl_mapping` | 映射规则 | 规则名称、源表、目标表 |
| 快照计划 | `snapshot_plan` | 快照策略 | 策略名称、目标表、快照周期 |
| 模型血缘 | `model_lineage` | 依赖关系 | 关系ID、上游模型、下游模型 |
| 一致性维度 | `conform_dim` | 标准维度 | 标准名称、维度主题、标准键值 |
| DataVault | `data_vault` | Vault模型 | 模型名称、Hub实体、Link关系 |
| 多维立方 | `cube_builder` | Cube定义 | Cube名称、源事实表、维度组合 |
| 变更日志 | `slow_change_log` | 维度变更 | 变更ID、目标维度、变更字段 |
| 日期维度 | `date_dim_gen` | 日期记录 | 日期键、完整日期、年份 |
| 模型校验 | `model_validate` | 校验规则 | 规则名称、目标模型、校验类型 |
| 部署流水线 | `deploy_pipeline` | 部署任务 | 任务名称、目标模型、部署环境 |

## ETL 分区交付清单

`/delivery` 页面（后端 `/api/delivery/...`）把「转换规则 + 源批次 + 目标字段结构」
固化成一份可核对的交付清单，部署流水线按清单推进各分区，而不是按模拟结果发布。

- **清单内容**：规则快照（含规则版本 `rl-xxx`）、源批次、模型结构版本 `sch-xxx`、
  必填字段、每个分区的源记录 ID、首次失败与错误字段、沿用建议。
- **正式交付**：每个分区整分区覆盖写入目标表，写入行保留自己的结构版本与字段清单；
  重试命中同一幂等键时直接跳过，不会重复发布。
- **试运行**：只做预演与核对，返回预览样例，不写目标库、不改变分区状态、
  不推进流水线（避免用测试结果伪造正式交付）。
- **结构不兼容**：目标表新增必填字段而规则尚未覆盖时，清单保留未交付分区与错误字段，
  部署任务标记失败；补齐映射后重新生成清单即可继续交付。
- **失败定位**：`GET /api/delivery/source-batches/{源批次}` 可反查该批次的清单和
  首次失败分区；失败记录永远带着源批次与错误字段。
- **复算与核对**：`/recompute` 按清单快照复算已确认分区（不发布）；
  `/verify` 核对构建记录、流水线已交付分区和目标库实际可用字段；
  `POST /api/delivery/compare` 用同一源批次分别核对旧作业结果与新结构结果。
- **结构演进**：`POST /api/delivery/targets/{表}/schema` 升级目标表结构，
  已交付分区仍保留原字段归属；旧清单的分区被新清单整分区覆盖后会标注
  「已由新清单覆盖」，不会被误判为混版。

常用接口：

```text
GET  /api/delivery/manifests                 交付清单列表
POST /api/delivery/manifests                 生成清单（rule_id + source_batch）
POST /api/delivery/manifests/{id}/run        执行：mode=正式|试运行
GET  /api/delivery/manifests/{id}/recompute  已确认分区复算
GET  /api/delivery/manifests/{id}/verify     构建记录/流水线/目标字段核对
GET  /api/delivery/manifests/{id}/check      逐分区只读对账
GET  /api/delivery/source-batches            源批次及其分区统计
GET  /api/delivery/source-batches/{批次}      源批次 → 关联清单 + 失败分区
GET  /api/delivery/targets                   目标表结构与实际可用字段
POST /api/delivery/targets/{表}/schema       新增/移除字段、更正必填字段
POST /api/delivery/compare                   新旧清单结果对比
```

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
