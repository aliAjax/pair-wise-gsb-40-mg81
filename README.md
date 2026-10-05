# 海上搜救协调系统

标准库实现的独立协调原型，使用 SQLite 保存事件、搜救资源、搜索区域、线索、离线批次和时间线。

## 运行

要求 Python 3.11+（在当前 Python 3.9 环境也可运行）。

```bash
python3 app.py --init --seed
python3 app.py
```

默认地址为 `http://127.0.0.1:8206`，数据库默认为 `maritime_sar.db`。`--db`、`--host`、`--port` 可覆盖默认值。

## 主要接口

写操作使用 JSON，并需要 `X-User` 与 `X-Role` 请求头。角色包括 `coordinator`、`operator`、`field`、`analyst`、`viewer`。

- `GET /health`、`GET /api/state`、`GET /api/export?incident_id=`
- `POST /api/incidents`：创建遇险事件并识别重复报警
- `POST /api/incidents/sea-state`：更新海况（乐观版本校验，自动失效重算未出发任务）
- `POST /api/incidents/plan`：按区域优先级、能力、航程与海况批量预占
- `POST /api/assets`：登记资源
- `POST /api/assets/update`：更新资源位置/能力/航程/海况上限（触发未出发任务重算）
- `POST /api/areas`：创建搜索区域
- `POST /api/areas/update`：更新区域版本/优先级/位置（触发未出发任务重算）
- `POST /api/assignments`：按当时能力、航程和区域优先级预占，冻结三方版本依据
- `POST /api/assignments/dispatch`：预占任务出发，出发后原依据不再被覆盖
- `POST /api/candidates/accept`：采纳保留下来的候选预占
- `POST /api/clues`、`POST /api/clues/verify`
- `POST /api/assets/withdraw`：撤回资源；未出发预占失效重算，已出发任务保留
- `POST /api/incidents/transfer`、`POST /api/incidents/close`
- `POST /api/offline/batch`：完整批次先落库再应用，按事件号幂等合并，失败凭批次号恢复
- `GET /api/incidents/{id}/timeline`

## 协调链语义

- **预占依据快照**：每次预占生成一条 `assignments` 记录（页面“协调链”可见），冻结事件海况版本、资源能力/位置/航程版本、区域版本与优先级、实际航程核算。区域状态为 `assigned`（未出发，链阶段 `reserved`）或 `active`（已出发，链阶段 `active`）。
- **先到者生效**：两个值班员同时修改同一资源时，事务串行化 + 资源/区域唯一索引保证先到者占用；后到者不报错丢弃，而是保留为 `candidate`，409 响应与页面都会列出冲突（`version_stale`、`asset_busy`、`area_taken`、`capability_missing`、`sea_state_exceeded`、`range_exceeded` 等），资源释放后可经 `/api/candidates/accept` 生效。
- **变更联动**：海况、资源、区域任一版本变化，事件下未出发（`reserved`）预占失效（保留为 `invalidated` 历史）、资源释放，并按区域优先级重新预占；已出发（`active`）任务完全不动，时间线仍指向出发时的依据。
- **离线回连**：批次先以 `staged` 状态保存完整载荷，再整批应用；应用过程意外失败标 `failed`，重新提交同一 `client_batch_id` 即从完整批次恢复（即使重传载荷不完整也以首次完整载荷为准）。事件按 `client_event_id` 去重，重复提交不重复占用；离线预占意图由协调员直接生效，现场角色提交则进候选待确认。
- **同一版本依据**：页面、时间线（basis 列）和 `/api/export` 展示同一条链的版本依据。旧库启动时自动把“区域已挂资源但无链记录”的历史数据回填为带 `backfilled` 标记的依据，时间线相关条目同步回填。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整协调流程、重复报警、错误位置、资源并发占用、候选保留与采纳、海况/资源/区域变更失效重算、已出发依据保留、按优先级预占、离线幂等与失败恢复、旧数据回填、版本依据一致性和权限拒绝。

## 局限

身份依赖调用方传入的用户和角色头；坐标使用球面距离近似；不会自动计算漂移概率区；文件附件、气象服务、真实通信链路和地理围栏未包含在内。
