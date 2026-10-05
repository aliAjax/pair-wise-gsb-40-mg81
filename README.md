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

- `GET /health`、`GET /api/state`、`GET /api/export`（与页面、时间线同一版本依据的导出快照）
- `POST /api/incidents`：创建遇险事件并识别重复报警
- `POST /api/incidents/sea_state`：更新海况，未出发任务失效重算，已出发任务保留原依据
- `POST /api/assets`：登记资源
- `POST /api/assets/update`：更新资源位置/能力/状态，触发未出发任务重算
- `POST /api/areas`：创建搜索区域
- `POST /api/areas/update`：更新区域（版本变化触发重算）
- `POST /api/assignments`：按当时能力、海况和航程预占资源；冲突时 409 返回 `conflicts` 清单并保留候选
- `POST /api/assignments/depart`：任务出发，依据快照此后不再被覆盖
- `POST /api/assignments/candidates/retry`、`POST /api/assignments/candidates/dismiss`：处理冲突候选
- `POST /api/clues`、`POST /api/clues/verify`
- `POST /api/assets/withdraw`：撤回资源并释放任务
- `POST /api/incidents/transfer`、`POST /api/incidents/close`
- `POST /api/offline/batch`：按事件号幂等合并离线记录（线索/时间线/派遣）；批次原始负载落库，写入失败后用相同批次号重试即可从完整批次恢复，重复提交不重复占用
- `GET /api/incidents/{id}/timeline`

## 协调链与版本依据

每次分配生成派遣任务（`planned` → `departed`），并保存下达时刻的依据快照（资源版本与位置、航程、适用海况、事件海况、区域版本与优先级）。海况、资源状态或区域版本变化时，未出发任务自动失效并按区域优先级重算；已出发任务保留原依据。两个值班员同时修改同一资源时先到者生效，后到者的请求保留为候选并列出冲突。旧库打开时自动迁移：缺失的版本列回填为 1，历史占用补建派遣任务依据。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整协调流程、重复报警、错误位置、资源并发占用（先到者生效、后到者保留候选）、海况/资源/区域变化触发的失效重算、已出发任务依据保留、离线幂等与批次恢复、旧数据回填和权限拒绝。

## 局限

身份依赖调用方传入的用户和角色头；坐标使用球面距离近似；不会自动计算漂移概率区；文件附件、气象服务、真实通信链路和地理围栏未包含在内。
