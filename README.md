# AgriSync — 作业数据跨地域同步、出境审计与田块作业序列完整性服务

跨国农机运营集团的作业数据平台。播种机、收割机、施肥机在各区域上传作业轨迹、地块面积、油耗与设备告警；
**区域节点本地确认**，复制编排器按**数据类别 × 驻留政策**生成跨区副本，聚合服务**在区域内**计算可出境指标，
出境审计服务在**交付前**重新核验授权与范围，容灾登记册记录每个区域的**主备角色**和**最后已确认复制位置**。

此外，区域内提供**田块作业序列完整性数据产品**：把标准化作业事实、田块边界/作物季节归属、农业操作规则与
**事件发生时间**关联，用有限状态机（FSM）与带时间约束的部分序图两条路线并行推导每块田的已确认阶段、允许的
替代/重复操作、缺失前置证据、顺序冲突与待核范围；总部只获得获准粒度的**阶段覆盖计数**，不取得任何田块级轨迹。

技术栈：Go 1.26 + PostgreSQL 18。后端服务，提供 OpenAPI 3.0.3 文档与内置（离线打包）Swagger UI。

---

## 1. 设计要点

### 1.1 区域隔离是物理的

控制面库 `agrisync_control` 与四个区域库 `agrisync_region_{de_bb,br_mt,us_ia,fr_nor}` 是**独立数据库**。
区域写入只触碰本区域库，控制面库永远不存受限原始轨迹（唯一例外是政策显式允许出境的区域）。因此
"集团只能取得经批准的聚合结果" 不是靠查询过滤，而是靠数据库边界。

### 1.2 本地确认、政策过滤后的异步复制

一次上传在**一个本地事务**里完成：作业记录 + 本地日志（gapless 序列）+ 事务性 outbox + 回执。

- outbox 按类别拆事件：`operation_summary`、`raw_trajectory`、`device_alarm`、`aggregate_metric`；
- 复制按 `residency_policies(region, category, outbound_allowed)` 逐事件过滤，受限轨迹**连控制面都进不去**；
- 副本先落 `unconfirmed`，接收方确认后才变 `confirmed` 并对集团可见，同时推进 DR 登记册的
  `last_confirmed_seq` 与复制游标 `confirmed_seq`；
- 复制失败/链路断开时游标不动，重试从同一位置继续。所有副本写入都是 **upsert**（主键 + `(region, device, segment)` 唯一索引），
  所以"至少一次"重投不会让面积重复累加。

### 1.3 重复上传按"设备序号 + 区域来源"去重

- 片段级：`unique (device_serial, segment_id)`，作用域是接收区域（区域来源隐含在库边界内，跨区副本显式带 `source_region`）；
- 批次级：`(batch_id, 内容 SHA-256)` 相同则返回原始回执（`replayed_batch=true`），内容不同则 `409`；
- 断网补传时重传已接收片段 → `duplicates` 计数，面积/油耗不变。

### 1.4 断网期间农场继续记账，集团统计显式滞后

- 链路关闭（`POST /replication/links/{region}` 或故障注入）不影响本区域写入；
- 聚合报告的 `coverage` 明确列出 `included_regions`、`excluded_regions`、`stale_regions`（延迟区域）、
  每区域 `data_cutoff` / `lag_seconds`、以及所使用的 `replication_seq`（复制版本）；
- 未确认区域不计入合计，只作为"被排除且滞后"出现。

### 1.5 故障切换只从"已确认位置"继续

`POST /dr/{region}/failover`：

1. 提升备节点，`epoch+1`；
2. 删除该区域所有 `unconfirmed` 副本（旧主未确认的缓存**不得**被当作已发布数据）；
3. 复制游标回绕到 DR 登记册的 `last_confirmed_seq`，状态 `pending_reconcile`；
4. 本地日志/本地记账**不截断**——新主节点继续接受现场写入；
5. 续传从 `last_confirmed_seq + 1` 开始，因 upsert 幂等，不会重复计入。

### 1.6 审批撤回后交付必须重新核验

`POST /aggregates/{run}/deliver` 在**发送前**重新读取当前审批（状态/版本/有效期/区域范围/指标范围）与当前驻留政策，
并对**将要发送的确切字节**做结构扫描（禁止 `lat/lon/points/trajectory/...`）。任一检查失败：

- 交付状态 `blocked`，`artifact=null`、`artifact_sha256=""`（**不返回任何聚合内容**）；
- 写入 `delivery.blocked` 审计（`decision=deny`，含失败检查项与原因）；
- 同一聚合运行不能二次交付（`aggregate_run` 已达 `delivered` 时返回 `409`）。

### 1.7 审计链可证明"某份跨境报表从未包含受限原始轨迹"

- 出境文件按**原始字节**存 `text`（不是 `jsonb`，避免键重排/数字归一化破坏哈希），交付时记录 SHA-256；
- `GET /deliveries/{id}/verify` 重新计算哈希、扫描受限字段、校验对应审计条目与整条哈希链；
- 审计账本为哈希链（`prev_hash` + 规范化 payload），落库前对 `detail` 做 JSON 归一化、时间截断到微秒，
  保证"写入时哈希 == 读回后重算哈希"；任何越权改库都会被 `GET /audit/verify` 定位到具体行号。

### 1.8 田块作业序列完整性：描述数据证据，不发出农业操作指令

该产品回答的是"**现有证据能否解释这一田块本季的作业序列**"，而不是"这块田接下来该做什么"。输出是对证据完备性与
时序一致性的判定，不构成任何播种、施肥、收获的作业指令。

- **只按事件发生时间判断**：事实同时带 `event_at`（作业发生）与 `received_at`（区域收到）。设备离线补传的事件
  `event_at` 早、`received_at` 晚，引擎一律按 `event_at` 排序；迟到事件可以**新增解释**，但不会因接收顺序把已确认
  操作判为倒序，也不会否定既有确认（`late_arrival` 仅作标记）。
- **按当时有效的归属判断**：田块边界与作物季节都是时间版本化的，推导只使用事件时刻有效的归属；不会拿当前作物
  计划回填、改写当时的证据。
- **两条设计路线并行、固定同一版本组合**：每个审查运行同时运行
  - 有限状态机 `fsm`：按作物/季节声明的状态与迁移推进，规则直观、便于逐步解释；补种是 `alternative`、重复施肥是
    `duplicate`，无合法迁移即顺序冲突；
  - 带时间约束的部分序图 `partial_order`：以"操作 + 前置条件 + 时间约束"表达多施肥批次与并行准备，证据缺失时维护
    多个可行阶段路径（`allowed_paths` 给出全部可行拓扑序），复核成本更高但表达力更强。
  一次运行把**语义事实版本、归属版本、两套规则版本（含规则内容 SHA-256）**冻结为一个 `version_bundle`。
- **冲突/待核保持，不为完整率选路径**：同一时段互斥作物（`mutex_crop_same_window`）、田块边界重叠
  （`boundary_overlap`）、作业语义待核（`operation_semantics_pending`）、规则版本不适用于该品种
  （`rule_version_not_applicable`）都会让该季保持 `conflict` 或 `pending_review`，系统不会任选一条路径把它"洗白"。
  已被显式取代（`superseded_by` 非空）的边界是更正记录，不与新边界算重叠。
- **更正生成新运行，旧汇总仍可复演**：规则或归属更正后新建审查运行；事实/规则/归属均版本化且运行结果整份冻结，
  旧 `run_id` 回查永远返回当时的结论与版本组合。
- **总部只获得获准粒度**：区域内保留田块级事实、路径、冲突证据与待核范围；发布到控制面的只有
  `(区域, 引擎, 阶段) -> 田块季数量` 的计数，不含任何 `field_id`/`fact_id`。集团汇总只读取 `confirmed` 发布，
  无发布的区域显式列为 `excluded` 并说明原因，不计零、不回填。

---

## 2. 快速启动

### 2.1 Docker Compose（一键）

```bash
docker compose up --build
# API:        http://localhost:8080
# Swagger UI: http://localhost:8080/docs
# OpenAPI:    http://localhost:8080/openapi.json
```

Compose 起 `postgres:18-alpine`（`ccdev/ccdev`，映射 5432）与 API 容器；API 启动时自动建库、建表、写入区域/政策/DR 登记册。
镜像地址可通过 `POSTGRES_IMAGE`、`GO_IMAGE`、`RUNTIME_IMAGE` 覆盖以适配镜像代理：

```bash
GO_IMAGE=<mirror>/golang:1.26-bookworm RUNTIME_IMAGE=<mirror>/distroless-static:nonroot docker compose up --build
```

### 2.2 本地开发（容器内已预装运行时）

```bash
# 依赖：PostgreSQL 18 已在 127.0.0.1:5432（库/用户/密码均为 ccdev）
/opt/postgresql-18/bin/pg_isready -h 127.0.0.1 -p 5432
export PATH=/opt/go/bin:$PATH
go build ./...
go run ./cmd/server            # 默认 :8080

# 可选：后台每 5s 自动复制（默认关闭，保证回放脚本确定性）
REPLICATION_INTERVAL=5s go run ./cmd/server
```

无 Docker 时同样可用 `make run`。数据库账号仅用于隔离开发/测试，不作为生产配置。

### 2.3 一键验证

```bash
make verify            # go build + vet + 单元测试 + 起临时服务 + 两套验收回放（同步/容灾/审计 + 序列完整性）
# 或对已运行实例分别回放：
./scripts/replay.sh http://localhost:8080
./scripts/sequence_replay.sh http://localhost:8080
```

两个回放脚本都输出逐条 `PASS/FAIL`，失败时退出码非 0。`scripts/demo.sh` 是给人读的分阶段演示（不做断言）。

回放脚本有少量断言需要直接查库（去重后的面积、副本行数、越权改库），默认使用
`/opt/postgresql-18/bin/psql` 连接 `127.0.0.1:5432`（容器内预装）；在其他环境运行时可覆盖：
`PSQL=/usr/bin/psql PGHOST=... PGPORT=... PGUSER=... PGPASSWORD=... ./scripts/replay.sh http://localhost:8080`。
在 Docker Compose 场景下把 `PGHOST` 指向映射出来的 5432 即可（`docker compose up` 已映射）。

---

## 3. 测试账号（Bearer Token）

除 `/healthz`、`/openapi.json`、`/docs`、`/swagger-ui/*` 外，所有接口都需要 `Authorization: Bearer <token>`。

| 角色 | Token | 权限 |
| --- | --- | --- |
| 平台管理员 `admin` | `demo-admin-token` | 链接开关、批量复制、主备切换、重置演示数据 |
| 集团分析 `group_analyst` | `demo-analyst-token` | 提交出境申请、计算聚合、交付；**无**区域原始记录读取通道 |
| 数据管理员 `approver` | `demo-approver-token` | 审批 / 驳回 / 撤回；**不能**自行提交申请或交付 |
| 审计员 `auditor` | `demo-auditor-token` | 只读：审计、校验 |
| 区域操作 `region_operator` | `demo-operator-de-bb` | 仅 DE-BB 的写入与本地查询 |
| 区域操作 | `demo-operator-br-mt` | 仅 BR-MT |
| 区域操作 | `demo-operator-us-ia` | 仅 US-IA |
| 区域操作 | `demo-operator-fr-nor` | 仅 FR-NOR |

Swagger UI 中点击 **Authorize** 填入上述任一 token 即可调用。

---

## 4. 样例数据（`samples/`）

| 文件 | 用途 |
| --- | --- |
| `regions/de-bb.json` 等 4 个 | 每区域首批 4 个作业片段（含轨迹与告警） |
| `offline/br-mt-offline-backfill.json` | BR-MT 断网期间继续作业的 3 个片段（本地提交，链路恢复后补传） |
| `offline/br-mt-retry-overlap.json` | 断网补传时**重叠重传**：3 个已接收片段 + 1 个新片段（验证去重与面积不重复） |
| `dr/us-ia-post-switch.json` | 主备切换后在新主节点继续上传 |
| `approvals/wide.json` | 四区域 × 全指标的出境申请 |
| `approvals/narrow.json` | 三区域（DE-BB/BR-MT/US-IA）× 3 指标的窄范围申请 |
| `sequence/br-mt-*.json` | 序列完整性样例：7 个田块季，覆盖补种、两次施肥、收获早于播种、离线迟到、边界修订、边界重叠、作物冲突、规则版本不适用 |

重新生成：`python3 scripts/gen_samples.py` 与 `python3 scripts/gen_sequence_samples.py`。

---

## 5. 验收回放（`scripts/replay.sh`）

脚本对运行中的服务做**可断言**的回放，覆盖题目要求的四类验收场景与审计链：

| 场景 | 断言示例 |
| --- | --- |
| 区域断网 | 断网后本地仍 `accepted=3`、日志 `entries=head=7` 且 `gapless=true`、`pending_outbox=7`；集团聚合 `excluded_regions/stale_regions = BR-MT`，合计只含其余三区 |
| 复制重试 | 重叠重传 `duplicates=3`；失败注入时游标 `confirmed_seq` 不动、`retry_count` 递增；重试成功后集团副本面积/行数 **等于** 区域自身面积/行数 |
| 主备切换 | 注入确认失败→存在 4 行 `unconfirmed`；failover 丢弃这 4 行、`resumed_from_seq == DR last_confirmed_seq`、新主 `epoch=2`、本地日志仍 `gapless`、续传后面积无重复；另注入一行崩溃写入者的孤儿副本，切换后同样被清除且计入报告 |
| 审批撤回 | 撤回后交付 `status=blocked`、`artifact=null`、无 `artifact_sha256`、原因为 `approval_status`；窄范围申请越权指标在聚合阶段 `403` |
| 出境审计 | 存储字节重算哈希一致、`restricted_fields` 为空、审计条目与整链校验通过；**越权改库后** `audit/verify` 返回 `valid=false` 且 `broken_at` 指向被改行 |
| 权限边界 | 无/错 token 401；区域操作员跨区 403；分析员自批 403；操作员触发全局复制/切换 403；集团分析读取区域原始记录 403；审计员可读区域记录用于审计 |

`scripts/sequence_replay.sh` 针对序列完整性产品做独立回放（66 项断言）：

| 场景 | 断言示例 |
| --- | --- |
| 补种 + 两次施肥 | FSM 与部分序都判 `ok`；第二次播种 `alternative`、第二次施肥 `duplicate`；部分序保留 2 条可行阶段路径 |
| 收获早于可证实播种 | 两引擎均 `conflict`/`order_conflict`；后到的播种仍 `confirmed`，不被倒序否定 |
| 离线迟到作业 | 迟到补传带 `late_arrival=true` 但仍 `confirmed`；事件按 `event_at` 而非接收顺序排列，序列判 `ok` |
| 缺失前置与待核范围 | 缺收获证据的田块 `pending_review`，`missing_prerequisites` 指明阶段、`pending_ranges` 给出待核时间窗 |
| 边界修订 | 已被 `superseded_by` 取代的旧边界不算重叠；补证据后新建运行转 `ok`，旧运行仍复演原 `pending_review` |
| 边界重叠 / 作物冲突 | `attribution_status=conflict`，证据按两引擎各留一条；互斥作物证据同时点名 maize 与 soybean |
| 规则版本不适用 | 品种专属规则已 `retired` 且无替代时 `rule_applicability=not_applicable`，两引擎均待核，不回退泛化规则 |
| 出境粒度 | 发布与汇总中 0 个田块标识；集团分析读区域详情 403；无发布区域显式 `excluded` 而非计零 |

一次完整运行的实际结果（`make verify`，2026-11-20 在本容器内执行，两套回放均通过）：

```
# 跨区同步/容灾/出境审计回放
checks passed: 105, failed: 0
ALL CHECKS PASSED
# 田块作业序列完整性回放
checks passed: 66, failed: 0
ALL SEQUENCE CHECKS PASSED
```

---

## 6. API 一览

完整定义见 `GET /openapi.json`（OpenAPI 3.0.3，45 路径，Swagger 分组 `Sequence` 为序列产品），交互文档见 `/docs`。

| 分组 | 接口 |
| --- | --- |
| 区域 | `POST /regions/{region}/ingest`、`GET /regions/{region}/status`、`GET /regions/{region}/operations`、`GET /regions/{region}/journal/verify`、`GET /regions` |
| 序列（区域内） | `GET/POST /regions/{region}/sequence/facts`、`POST .../facts/import`、`GET/POST .../sequence/seasons`、`.../sequence/boundaries`、`.../sequence/calendars`、`.../sequence/rules`、`GET/POST .../sequence/reviews`、`GET .../sequence/reviews/{id}`、`POST .../sequence/reviews/{id}/publish` |
| 序列（集团侧） | `GET /sequence/coverage/publications`、`GET/POST /sequence/coverage/rollups`（仅阶段计数，无田块数据） |
| 复制 | `POST /replication/run`、`POST /replication/batch`、`GET /replication/cursors`、`GET /replication/runs`、`GET/POST /replication/links[/{region}]`、`GET /replication/policies`、`GET /replication/replicas` |
| 出境申请 | `GET/POST /outbound/requests`、`GET /outbound/requests/{id}`、`POST .../decide`、`POST .../revoke` |
| 聚合 | `GET/POST /aggregates`、`GET /aggregates/{id}` |
| 交付 | `POST /aggregates/{id}/deliver`、`GET /deliveries/{id}`、`GET /deliveries/{id}/verify` |
| 容灾 | `GET /dr`、`GET /dr/{region}`、`POST /dr/{region}/failover`、`POST /dr/{region}/failback` |
| 审计 | `GET /audit`、`GET /audit/verify`、`GET /audit/{subject}` |
| 演示 | `POST /demo/reset`（`DEMO_MODE=false` 时禁用）、`GET /demo/summary` |

复制故障注入（仅用于验收演练）：`{"fault":"link_down"|"copy_fail"|"confirm_fail"}`。

---

## 7. 手工走一遍（curl）

```bash
A='Authorization: Bearer demo-admin-token'
curl -s -XPOST localhost:8080/demo/reset -H "$A"
curl -s -XPOST localhost:8080/regions/BR-MT/ingest \
  -H 'Authorization: Bearer demo-operator-br-mt' -H 'Content-Type: application/json' \
  --data-binary @samples/regions/br-mt.json
curl -s -XPOST localhost:8080/replication/batch -H "$A" -H 'Content-Type: application/json' -d '{}'
curl -s localhost:8080/replication/replicas -H "$A"        # 只有 US-IA 有 raw_trajectories
curl -s localhost:8080/replication/policies -H "$A"        # 每个区域每类别的出境政策
```

出境链路（申请 → 审批 → 聚合 → 交付 → 验证）：

```bash
AN='Authorization: Bearer demo-analyst-token'; AP='Authorization: Bearer demo-approver-token'
APR=$(curl -s -XPOST localhost:8080/outbound/requests -H "$AN" -H 'Content-Type: application/json' \
  --data-binary @samples/approvals/wide.json | jq -r .id)
curl -s -XPOST localhost:8080/outbound/requests/$APR/decide -H "$AP" -H 'Content-Type: application/json' \
  -d '{"decision":"approved"}' | jq -c '{status,version}'
RUN=$(curl -s -XPOST localhost:8080/aggregates -H "$AN" -H 'Content-Type: application/json' \
  -d "{\"approval_id\":\"$APR\",\"period_from\":\"2026-11-01T00:00:00Z\",\"period_to\":\"2026-11-30T00:00:00Z\",\"metric_keys\":[\"operation_count\",\"completion_rate\",\"area_ha\"]}" | jq -r .id)
DLV=$(curl -s -XPOST localhost:8080/aggregates/$RUN/deliver -H "$AN" | tee /tmp/d.json | jq -r .id)
jq -c '{status,included:.artifact.included_regions,excluded:.artifact.excluded_regions,sha:.artifact_sha256}' /tmp/d.json
curl -s localhost:8080/deliveries/$DLV/verify -H 'Authorization: Bearer demo-auditor-token' | jq -c .
curl -s localhost:8080/audit/verify -H 'Authorization: Bearer demo-auditor-token' | jq -c .
```

田块作业序列完整性（区域内录入 → 双引擎审查 → 仅计数出境 → 集团汇总）：

```bash
OP='Authorization: Bearer demo-operator-br-mt'
AN='Authorization: Bearer demo-analyst-token'
for f in seasons boundaries calendars rules facts; do
  curl -s -XPOST localhost:8080/regions/BR-MT/sequence/$f -H "$OP" \
    -H 'Content-Type: application/json' --data-binary @samples/sequence/br-mt-$f.json
done
# 也可把已上传的作业直接标准化为事实（幂等）
curl -s -XPOST localhost:8080/regions/BR-MT/sequence/facts/import -H "$OP"
# 双引擎审查：返回每块田的阶段、允许路径、缺失前置、冲突与待核范围
RID=$(curl -s -XPOST localhost:8080/regions/BR-MT/sequence/reviews -H "$OP" \
  -H 'Content-Type: application/json' \
  -d '{"period_from":"2026-11-01T00:00:00Z","period_to":"2026-11-30T00:00:00Z"}' | jq -r .run.run_id)
curl -s localhost:8080/regions/BR-MT/sequence/reviews/$RID -H "$OP" | jq -c '.fields[]|{season_id,fsm_status,po_status}'
# 区域只发布获准粒度的阶段计数；集团侧拿不到任何田块标识
curl -s -XPOST localhost:8080/regions/BR-MT/sequence/reviews/$RID/publish -H "$OP"
curl -s -XPOST localhost:8080/sequence/coverage/rollups -H "$AN" -H 'Content-Type: application/json' \
  -d '{"engine":"fsm","period_from":"2026-11-01T00:00:00Z","period_to":"2026-12-31T00:00:00Z","scope_regions":["BR-MT","DE-BB"]}'
```

---

## 8. 配置项

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | 监听地址 |
| `PGHOST` / `PGPORT` / `PGUSER` / `PGPASSWORD` / `PGSSLMODE` | `127.0.0.1` / `5432` / `ccdev` / `ccdev` / `disable` | PostgreSQL 连接 |
| `PG_ADMIN_URL` | 空 | 建库用连接串（默认用 `PG_ADMIN_DB=postgres` 推导） |
| `CONTROL_DB` | `agrisync_control` | 控制面库名 |
| `REGION_DB_PREFIX` | `agrisync_region_` | 区域库前缀（后接小写化区域码） |
| `REGIONS` | `DE-BB,BR-MT,US-IA,FR-NOR` | 启用的区域 |
| `REPLICATION_BATCH` | `500` | 单次复制最大事件数 |
| `REPLICATION_INTERVAL` | 空（关闭） | 后台复制间隔，如 `5s`；关闭时复制完全由 API 驱动 |
| `DELIVERY_CHANNEL` | `secure_store` | 交付通道标签（写入交付记录与审计） |
| `DEMO_MODE` | `true` | `false` 时禁用 `/demo/reset` |
| `TOKEN_*` | 见第 3 节 | 各角色令牌，生产环境必须覆盖 |

---

## 9. 目录结构

```
cmd/server/            服务入口（含 -healthcheck 子命令，供 distroless 健康检查）
internal/api/          HTTP 路由、鉴权、OpenAPI 文档、后台复制器（sequence.go 为序列产品接口）
internal/service/      区域写入/去重、复制、聚合、审批与交付、容灾、审计链；
                       sequence_*.go 为序列事实/归属/规则注册、FSM 与部分序双引擎、审查运行与覆盖发布
internal/model/        通用模型与序列产品模型（sequence.go）
internal/db/           PostgreSQL 连接池（每库一个）、schema、种子、重置
internal/config/       区域/令牌/连接串配置
web/swagger-ui/        内置 Swagger UI 资产（离线可用，Apache-2.0，见 LICENSE/NOTICE）
samples/               多区域、断网补传、重试重叠、切换后续传、不同授权范围、序列六类场景（sequence/）
scripts/               gen_samples.py、gen_sequence_samples.py、replay.sh、sequence_replay.sh、demo.sh、verify.sh
docs/architecture.md   数据流、失败模式与序列产品说明
```

## 10. 已知边界

- 交付通道 `secure_store` 是本地归档语义（文件存于 `deliveries.artifact`），不包含真实外部传输；接入真实接收方时在
  `Deliver` 的 `sent` 分支替换即可，审计与复核逻辑不变。
- 驻留政策目前通过种子数据维护（只读 API 暴露），政策变更是治理动作，不在演示界面内修改。
- DR 是登记册 + 本地主备角色指针的模拟：单进程内切换，不包含真实流复制；但"只从已确认位置续传"的约束是真实执行的。
- 后台复制默认关闭以确保回放确定性；开启后与回放脚本并行会有竞态（脚本会自行重新复制并断言，通常仍能通过，但不建议）。
