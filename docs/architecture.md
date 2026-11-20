# 架构与失败模式

## 1. 数据流

```
              ┌─────────────────────────── 区域 (每个区域独立 PostgreSQL 数据库) ───────────────────────────┐
 农机/网关 ──▶ │ POST /regions/{r}/ingest                                                                   │
              │   ┌─ 单事务 ─────────────────────────────────────────────────────────────────────────────┐ │
              │   │ operation_events (唯一: device_serial, segment_id)  ← 本地权威记录                    │ │
              │   │ journal_entries   (seq 连续, 无空洞)                     ← 本地记录连续性证据         │ │
              │   │ outbox_events     (seq 连续; 每操作拆 summary/trajectory/alarm 事件)                    │ │
              │   │ ingest_receipts   (batch_id + 内容 SHA-256)                                           │ │
              │   └───────────────────────────────────────────────────────────────────────────────────────┘ │
              │ 聚合时在区域内计算 ─▶ metric_snapshots + outbox(aggregate_metric)                          │
              └───────────────────────────────────────┬────────────────────────────────────────────────────┘
                                                      │ 复制编排器 (政策过滤 + 逐事件)
                                                      ▼
              ┌────────────────────────── 控制面库 agrisync_control ──────────────────────────────────────┐
              │ residency_policies  ← 逐类别出境判定                                                       │
              │ replica_op_summaries / replica_trajectories / replica_alarms / replica_metrics            │
              │   status = unconfirmed → confirmed                                                       │
              │ replication_cursors (last_seq, confirmed_seq)   replication_runs (含 fault/错误)          │
              │ dr_registry (primary/standby/active, epoch, last_confirmed_seq)                          │
              │ outbound_requests (审批, version)   aggregate_runs / aggregate_run_regions (coverage)     │
              │ deliveries (artifact 原始字节 + sha256)   audit_events (哈希链)                           │
              └──────────────────────────────────────────────────────────────────────────────────────────┘
```

关键不变式：

| 不变式 | 实现 |
| --- | --- |
| 受限原始轨迹不离境 | 区域库独存原始点；复制逐事件按 `residency_policies` 过滤；交付前再扫描实际字节 |
| 集团可见 = 已确认 | 副本 `unconfirmed` 不参与聚合；聚合只读 `status='confirmed'` 的 `replica_metrics` |
| 去重不重复计面积 | 主键 `(source_region, entity_id)` + 唯一索引 `(source_region, device_serial, segment_id)`，全部 upsert |
| 本地记录连续 | `journal_entries` 序列在写事务内按行锁分配，`/journal/verify` 用 `generate_series` 找空洞 |
| 切换只从已确认位置续传 | failover 删除 `unconfirmed`、把游标回绕到 `dr_registry.last_confirmed_seq` |
| 撤回即阻断 | deliver 事务外重新读取审批与政策；`artifact` 仅在全部检查通过时生成 |
| 报表可自证 | artifact 按字节存 `text` + SHA-256；`/deliveries/{id}/verify` 复算并校验审计条目；`/audit/verify` 校验整链 |

## 2. 失败模式与行为

### 2.1 区域断网（链路故障）

- 写入路径完全不触碰控制面 → 现场仍可连续记账，`journal_head == journal_entries`。
- outbox 事件堆积（`pending_outbox` 可见），但本地记录、去重、回执全部正常。
- 集团聚合：该区域 `replica_metrics` 无本窗口确认快照 → `excluded_regions` + `stale_regions`，合计不含该区；
  `data_cutoff` 取所有纳入区域的最早确认截止点。
- 链路恢复后 `POST /replication/run` 从 `last_seq` 继续，一次扫完积压；重叠重传按片段去重。

### 2.2 复制重试 / 至少一次投递

- `copy_fail`：apply 事务回滚，游标不动，`retry_count++`，`replication_runs` 记录失败原因。
- `confirm_fail`：数据已写但 ack 丢失 → 行存在且保持 `unconfirmed`（对集团不可见），游标/DR 不前进。
- 崩溃后重投：相同 `(region, entity_id)` upsert；聚合只读确认快照，不会二次累加面积。

### 2.3 主备切换

- 只能从 `last_confirmed_seq` 续传；`unconfirmed` 行被删除（旧主未确认缓存绝不视为已发布）。
- 本地日志与本地记账不截断，新主继续接受写入，`epoch` 递增并写入 `dr_events` 与审计。
- 续传后再次确认，`replica_*` 回到与区域库一致的行数/面积。

### 2.4 审批在聚合运行中被撤回

- 聚合运行时记录 `approval_version`；交付时重新读取审批：状态、版本、有效期、区域范围、指标范围。
- 任一不一致 → `blocked`；`artifact=null`，不写任何出境内容；审计为 `delivery.blocked`（`deny`）。
- 指标超出审批范围在聚合阶段即 `403`；区域超出审批范围在聚合阶段被范围裁剪，交付阶段再校验一次。

### 2.5 审计链

- 链式哈希：`hash = sha256(canonical{seq, occurred_at(μs), actor, role, action, subject, region, decision, detail, prev_hash})`。
- `detail` 入库前 JSON 归一化（等价于 jsonb 往返后的形态），避免写读差异。
- 任何行被改写：`prev_hash` 与上一条不符或内容哈希不符 → `/audit/verify` 返回 `valid=false`、`broken_at=<seq>`。

## 3. 权限模型

| 角色 | 可做 | 不可做 |
| --- | --- | --- |
| `region_operator`（绑定单一区域） | 本区域上传、本区域状态/作业/日志校验、本区域复制、序列事实/归属/规则录入与审查、发布阶段计数 | 跨区读写、全局复制、切换、聚合、审批、集团汇总 |
| `group_analyst` | 提交出境申请、聚合、交付、读取已发布阶段计数与构建集团汇总 | 审批/撤回、切换、重置、读取区域原始记录与田块级审查详情 |
| `approver` | 审批、驳回、撤回 | 提交申请、交付、聚合 |
| `auditor` | 读审计、校验、区域只读核查 | 任何写操作 |
| `admin` | 链路开关、全局复制、主备切换、演示重置 | 仍受交付前置检查约束（不能绕过审计） |

自批被禁止：`decide` 需要 `approver` 角色，`group_analyst` 调用返回 `403`。

## 4. 田块作业序列完整性产品

### 4.1 数据流（全部推导留在区域库内）

```
 标准化作业事实 sequence_facts (event_at, received_at)
 田块季节归属   field_seasons / field_boundary_revisions / crop_calendars（时间版本化）
 农业规则       agronomy_rule_sets（fsm / partial_order，版本化，含规则内容哈希）
                          │  创建审查运行（一次事务，只读上述输入的冻结快照）
                          ▼
        ┌────────────── 区域库（不出境）──────────────┐
        │ FSM 引擎 runFSM        部分序引擎 runPartialOrder
        │ sequence_review_fields / _events / _issues / _engines
        │ 已确认阶段 · 允许路径 · 缺失前置 · 顺序冲突 · 待核范围
        └───────────────────────┬───────────────────────┘
                                 │ publish：仅 (区域,引擎,阶段)->计数
                                 ▼
        控制面 sequence_coverage_publications(confirmed) → sequence_coverage_rollups
        （无 field_id / fact_id / 证据；集团可读的最细粒度）
```

### 4.2 两条推导路线

| 维度 | 有限状态机 `fsm` | 部分序图 `partial_order` |
| --- | --- | --- |
| 规则表达 | 状态 + 迁移；`repeatable`/`alternative` 标记重复与替代 | 操作节点（max_count、min_gap、optional）+ requires/mutex 约束 |
| 补种 | 第二次播种命中 `alternative` 迁移，解释为 `alternative` | 计数内的第二次同名操作解释为 `alternative` |
| 多次施肥 | 施肥态上的 `repeatable` 自环，解释为 `duplicate` | 可选操作、不限次数、最小间隔，解释为 `duplicate` |
| 收获早于可证实播种 | 当前状态无合法迁移 → `order_conflict` | 前置 `seeded` 未满足；最终化时前置仍缺或后补均判冲突 |
| 证据缺失 | 必需阶段无事实 → `missing_prerequisites` + 待核范围 | 维护多个可行阶段（`allowed_paths` 枚举全部拓扑序），缺失前置显式列出 |
| 复核成本 | 单一线性状态，解释直接 | 多可行阶段并存，查询/复核成本更高 |

### 4.3 关键不变式

| 不变式 | 实现 |
| --- | --- |
| 按事件时间而非接收顺序 | 事实按 `(event_at, received_at, fact_id)` 排序；离线补传仅打 `late_arrival` 标记，不产生倒序判定 |
| 按当时有效归属 | 边界/作物声明带时间窗，按事实 `event_at` 取当时有效版本，不用当前计划回填 |
| 迟到不否定已确认 | 新事实只追加解释；冲突事实保留 `conflict`，已确认事实不被改写 |
| 冲突/待核不被洗白 | 互斥作物、边界重叠、语义待核、规则不适用都固定状态；完整率统计只数已确认阶段 |
| 固定版本组合 | 每次运行冻结 `fact_version + attribution_version + 两套规则（含 SHA-256）` 为 `version_bundle` |
| 更正可复演 | 更正只产生新运行；旧运行行不更新，`run_id` 回查恒得原结论 |
| 出境仅计数粒度 | 发布表/汇总表无田块标识；区域详情接口对 `group_analyst` 返回 403；发布受 `aggregate_metric` 驻留政策约束 |
| 无发布不计零 | 集团汇总对缺少 confirmed 发布的区域显式 `excluded` 并给原因 |

### 4.4 判定的是数据证据，不是作业指令

所有输出描述"证据是否足以解释该季作业序列"。`allowed_paths` 是规则允许的解释空间，`missing_prerequisites` 与
`pending_ranges` 是证据缺口，`conflict` 是证据相互矛盾；它们不指示农机应当执行何种操作，也不替代农艺决策。
