# Trading Platform

一个面向高并发、强一致和持久化交易场景的分片式交易平台。项目采用 **分片单写状态机 + WAL 组提交 + 快照恢复 + 分片级复制** 的架构，通用数据库仅承担异步查询、对账和归档，不位于核心交易提交链路。

> 当前阶段：M4 在线分片迁移与弹性扩容已实现。每个分片是一个成员集可动态变更的 openraft 复制组；迁移只冻结目标分片，完成 learner 追赶、精确位点校验、成员变更、旧主 fence、目标主选举和路由 epoch 原子发布。Go 控制面提供迁移编排与审计 API，Rust 节点暴露本地管理接口；传输层仍为进程内通道，跨机器网络化是明确的后续边界。

| 里程碑 | 状态 | 验收证据 |
| --- | --- | --- |
| M1 单机交易内核、WAL 与快照 | 已完成 | 见 [M1 已实现](#快速开始m1-单机-wal快照) 与 [本机 M1 基线](#本机-m1-基线) |
| M2 多逻辑分片与无状态路由 | 已完成 | 见 [M2 多逻辑分片](#m2-多逻辑分片) |
| M3 分片级复制与自动故障切换 | 已完成（2026-11-03） | 见 [M3 章节](#m3-分片级复制与自动故障切换) 与 [M3 验收结论](#m3-验收结论) |
| M4 在线分片迁移与弹性扩容 | 已完成（2026-11-03） | 见 [M4 章节](#m4-在线分片迁移与弹性扩容)、[M4 验收结论](#m4-验收结论) 与 [M4 发布说明](docs/releases/M4.md) |

## 1. 项目目标

- 核心交易在分片内强一致，余额、持仓等状态只有一个写入 owner。
- 成功响应之前完成符合配置要求的持久化，避免已确认交易因进程崩溃而丢失。
- 通过逻辑分片水平扩展，而不是对同一分片增加多个写节点。
- 通过快照和 WAL 确定性重放实现快速、可验证的故障恢复。
- 通过分片级复制、主备切换和在线迁移实现高可用与弹性扩容。
- 以持久化后的端到端吞吐和 P99/P999 延迟验收，而不是只测内存写入速度。

### 非目标

- 不在第一阶段实现完整交易所、复杂撮合算法或通用分布式数据库。
- 不用 Redis、Kafka 或 PostgreSQL 的同步写入伪装成交易核心的高性能。
- 不承诺脱离交易大小、热点分布、持久化等级和硬件条件的固定 TPS。
- 不允许跨分片异步消息在缺少业务中间状态时替代原子交易。

## 快速开始：M1 单机 WAL/快照

### 已实现

- 44 字节固定宽度二进制交易命令，支持开户、充值、提现和原子转账。
- 单写执行线程、有界 MPSC 队列、阻塞背压和非阻塞过载拒绝。
- 数据目录独占文件锁，同一目录不允许同时打开两个写入实例。
- 批量顺序 WAL 以追加模式写入，每批一次 `sync_data`；首次创建 WAL 时同步父目录。
- WAL 序号、长度和 CRC32 校验；半条尾部恢复到最近有效位置，完整记录损坏则拒绝启动。
- `request_id` 持久化幂等，重试返回首次序号和结果，不重复修改余额。
- 快照临时文件刷盘、原子替换和父目录同步；快照 CRC 覆盖最后序号、长度和状态。
- 快照成功后安全清空已覆盖 WAL；启动时加载快照并重放后续日志。
- 独立多线程压测工具，输出端到端吞吐、P50/P99/P999、错误数，并逐账户校验余额、序号和幂等结果。

### 构建与验证

```bash
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo build --release --workspace
```

运行 10 万笔持久化转账，并在结束后生成快照、截断 WAL、重新打开验证恢复：

```bash
cargo run --release -p trading-loadgen -- \
  --ops 100000 \
  --concurrency 32 \
  --accounts 1024 \
  --batch 64 \
  --checkpoint
```

仅用于对照的非刷盘模式：

```bash
cargo run --release -p trading-loadgen -- \
  --ops 100000 \
  --concurrency 32 \
  --unsafe-buffered \
  --checkpoint
```

`--unsafe-buffered` 不保证崩溃后保留已响应交易，禁止作为严格持久化结果汇报。

### M2 多逻辑分片

使用 `--shards` 指定逻辑分片数。路由表携带单调递增的版本，账户按固定路由槽定位分片；M2 只允许提升版本，M4 的在线迁移只改变分片的物理 voter 集并提升 epoch，逻辑槽位映射保持不变。原子转账要求两个账户位于同一分片，跨分片转账在进入执行队列前拒绝。

```bash
cargo run --release -p trading-loadgen -- \
  --ops 100000 \
  --concurrency 32 \
  --accounts 1024 \
  --shards 4 \
  --batch 64 \
  --checkpoint
```

数据分别保存在 `shard-N/partition.wal` 和 `shard-N/snapshot.bin`。压测输出每分片吞吐占比、队列深度、批次延迟、P50/P99/P999 和独立序号，并逐分片校验余额、幂等结果及 checkpoint 恢复。过期的显式路由会收到当前路由作为重定向，客户端使用原 `request_id` 重试即可保持幂等。

追加 `--postgres-url postgres://user:password@host/database` 可启用异步已提交日志投影。各分片写线程在 WAL 提交和状态执行后，按分片序号发送 `(shard_id, route_version, sequence)` 事件；PostgreSQL 投影在独立线程中批量事务写入 `trading_committed_events`，并以该三元组主键和 `ON CONFLICT DO NOTHING` 保证重复消费不产生重复行。

### M3 分片级复制与自动故障切换

使用 `--replicas N`（奇数，至少 3）把每个分片组成 Raft 复制组。WAL 就是 Raft 日志：一条命令一条日志项，raft index 与命令序号一一对应；多数副本持久化后才向客户端确认提交。`--failover` 在压测中段杀死每个分片的 leader，测量选举与追赶并校验不丢不重；`--migrate` 在压测中段在线搬迁分片 0，见 [M4 章节](#m4-在线分片迁移与弹性扩容)。

```bash
cargo run --release -p trading-loadgen -- \
  --replicas 3 \
  --ops 20000 \
  --concurrency 8 \
  --accounts 64 \
  --unsafe-buffered

# 故障演练：压测中段杀 leader，测量选举与副本追赶
cargo run --release -p trading-loadgen -- \
  --replicas 3 \
  --ops 6000 \
  --concurrency 8 \
  --accounts 64 \
  --unsafe-buffered \
  --failover
```

已实现：

- 基于 openraft 0.9（选型见 [ADR 0001](docs/adr/0001-raft-implementation-choice.md)）的分片复制组：bootstrap、多数派提交、日志复制与快照压实。
- leader fencing：客户端只接受 quorum-ack 未过期的 leader；旧主恢复后以 follower 身份追赶日志，不能继续写入。
- 客户端重定向与幂等重试：`NoLeader`/`ForwardToLeader` 时复用同一 `request_id` 重试，切换期间不重复扣账。
- 副本运维工具：crash/restart 副本、快照加日志追赶、目录文件锁拒绝双开。
- 进程内传输带定向断链注入，用于网络分区演练。

故障注入测试（`replication/tests/m3_acceptance.rs`）覆盖：主崩溃后已提交日志不丢失、分区中的 leader 无法提交、重复请求跨切换只应用一次、落后副本经快照和日志追平、同目录双开被拒绝。

2026-11-03，本机 macOS Intel x86_64，release 构建，3 副本、1 分片、8 并发、64 账户、快照阈值 1000：

| 模式 | 吞吐量 | P50 | P99 | P999 | 错误/重试 |
| --- | ---: | ---: | ---: | ---: | ---: |
| UnsafeBuffered 3 副本（2 万笔） | 13,586 ops/s | 316us | 3.4ms | 25.9ms | 0/0 |
| Durable 3 副本（2 千笔） | 43 ops/s | 179.1ms | 203.9ms | 259.4ms | 0/0 |

- 复制模式下快照、日志压实和冲突截断遵循 `--unsafe-buffered`/Durable 语义；此前这些路径无条件 fsync（macOS 目录 fsync 单次约 60-100ms，且在状态机 worker 内阻塞应用），修复前基线仅 296 ops/s、P99=376ms。
- Durable 复制的瓶颈是本机磁盘：quorum 提交路径串行执行 leader 与 follower 各一次 fsync（对照：同机单写引擎每批一次 fsync，P50=19.1ms）。该数字只代表本机环境。
- 故障演练（UnsafeBuffered，6 千笔）：election_ms=1,578、catchup_ms=105、RPO=0-committed、客户端 0 错误 0 重试；切换停写窗口由内部重试吸收（P999=1.63s）。

### M3 验收结论

| 验收标准 | 结论 | 证据 |
| --- | --- | --- |
| 主节点故障后已提交日志不丢失，旧主恢复后不能继续写入 | 通过 | `leader_crash_failover_preserves_committed_writes`：崩主后继续提交，存活副本余额等于崩溃前已提交总额；旧主重启后以 follower 追平且 term 与集群收敛 |
| 网络分区、重复请求和切换期间重试不会导致重复扣账 | 通过 | `partitioned_leader_cannot_commit_and_retries_apply_once`：隔离 leader 只得到 `NoLeader`，愈合后同 `request_id` 重放返回原结果，三副本余额仅扣一次 |
| 新副本可通过快照和日志完成追赶 | 通过 | `lagging_replica_catches_up_after_snapshot_purge`：副本停机期间日志被快照压实（阈值 30），重启后经快照加日志追平并继续跟随 |
| 记录并评估故障检测时间、切换停写时间和恢复目标 | 通过 | election_ms=1,578、catchup_ms=105、RPO=0-committed、客户端 0 错误 0 重试 |

故障注入测试共 5 项全部通过（`cargo test -p trading-replication`），另有 `commit_replicates_and_duplicate_requests_apply_once` 与 `replica_directory_lock_fences_double_open` 覆盖复制可见性、重复请求去重和目录双开围栏。

### M4 在线分片迁移与弹性扩容

完整变更、使用方式、验收证据、性能基线与已知边界见 [M4 发布说明](docs/releases/M4.md)。

在线迁移只改变分片的**物理 voter 集**，不改变逻辑的账户→分片映射。每个分片仍是独立 Raft 组，迁移按集群串行编排，其他分片持续服务。

已实现：

- 动态副本集合：运行期创建、停止、退役副本；目标副本以 learner 加入，经快照和 WAL 追赶到 `catchup_lag` 阈值以内。
- 完整迁移序列：创建目标副本 → learner 加入 → 持续追赶 → 冻结该分片提交 → 精确位点校验（lag=0）→ 成员变更（ReplaceAllVoters）→ 提交后同步成员日志 → 退役旧主 → 选举新主 → 原子发布路由 epoch。
- 分片级停写：每分片一个 `RwLock<()>` 提交闸门。普通提交取读锁；迁移冻结取写锁，先排空在途提交再进入临界区，因此停写是有界等待而不是请求失败。
- 迁移模式：`scale-out`、`relocate-leader`、`scale-in`、`replace`。
- 旧主 fence：旧主在闸门内停止并写入 `retired-{id}` 标记；重启后不会被发现为 voter，也不能成为 leader。目标副本有落后日志时不会被偏好选举绕过。
- 路由收敛与重定向：客户端缓存 `Route`；epoch 不匹配时返回 `StaleRoute { current }`，刷新后用同一 `request_id` 重试，状态机幂等去重。
- 失败语义：成员变更提交前失败 → `RolledBack`，丢弃目标副本，旧 voter 集与 epoch 保持不变；变更已提交但后续步骤失败 → `Degraded`，绝不回滚已提交的 voter 集，仍发布 epoch 并保留现场。
- 审计：迁移报告包含 voters/leader/epoch 前后值、追赶位点、`positions_before_promote`、分阶段耗时和逐事件时间线；Go 控制面将每次尝试追加到 `audit.jsonl`，含幂等键、节点 HTTP 状态和完整节点报告。

节点与管理 API：

```bash
cargo run --release -p trading-node -- \
  --data-dir data/node --shards 4 --replicas 3 \
  --unsafe-buffered --admin-addr 127.0.0.1:8081
```

`trading-node` 只暴露运维管理接口，不是客户端交易网关：`GET /admin/health`、`GET /admin/routes`、`GET /admin/shards`、`GET /admin/shards/{id}`、`POST /admin/shards/{id}/migrate`、`POST /admin/shutdown`。迁移返回 `200 completed`、`409 rolled-back`、`503 degraded`；可用 `"fault":"crash-target-before-learner"` 注入回滚演练。

Go 控制面（策略校验、幂等键、审计持久化）：

```bash
cd control-plane
go run ./cmd/controlplane \
  --addr 127.0.0.1:8080 \
  --node-url http://127.0.0.1:8081 \
  --state-dir control-plane-state

curl -s -X POST http://127.0.0.1:8080/v1/shards/0/migrations \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: shard-0-to-node-4' \
  -d '{"mode":"relocate-leader","target_node":4}'

curl -s http://127.0.0.1:8080/v1/shards
curl -s http://127.0.0.1:8080/v1/migrations
```

控制面接口：`GET /healthz`、`GET /v1/node`、`GET /v1/shards`、`GET /v1/routes`、`POST /v1/shards/{shard}/migrations`、`GET /v1/migrations`、`GET /v1/migrations/{id}`。迁移结果映射为 `201 completed`、`409 rolled-back`、`503 degraded`、`502` 传输失败；相同 `Idempotency-Key` 重放返回原记录（`200` + `replayed:true`），且同一分片存在在途迁移时拒绝新计划。

压测中段在线搬迁分片 0（先扩到 4 voter，再退役旧主缩回 3 voter）：

```bash
cargo run --release -p trading-loadgen -- \
  --replicas 3 --shards 4 --ops 100000 --concurrency 16 \
  --accounts 256 --unsafe-buffered --migrate
```

2026-11-03，本机 macOS Intel x86_64，release 构建，3 副本、4 分片、16 并发、256 账户、10 万笔转账、UnsafeBuffered：

| 指标 | 结果 |
| --- | --- |
| 吞吐与正确性 | 15,982 ops/s；100,000 笔全部成功，0 错误、0 重试失败 |
| 全量延迟 | P50 286.1us / P99 20.3ms / P999 67.3ms |
| 迁移总耗时 | 1,579ms（追赶 335ms、冻结 40ms、成员切换 25ms、选举 1,090ms） |
| 被迁移分片停写 | 冻结临界区 40ms；闸门释放后的选举约 1.09s，分片 0 观察 max 1.19s，由客户端重试吸收 |
| 其他分片影响 | 迁移窗口内 P99 18.5–20.3ms、max 19.8–20.3ms，无停写 |
| 路由收敛 | epoch 1 → 2，64 次 stale-route 重定向后收敛；`route.version` 持久化 |
| 数据一致性 | 各分片每个副本余额 640,000,000,000、账户数 64、applied 位点一致；旧主 1 退役后不在 voter 集 |

说明：`freeze_ms` 只覆盖成员变更与旧主退役的临界区；停写的主要部分来自旧主退役后的 leader lease 等待，必须等约一个 election timeout 才允许新主当选，这是当前设计的有意安全边界。

### M4 验收结论

| 验收标准 | 结论 | 证据 |
| --- | --- | --- |
| 迁移期间不出现双主、已提交交易丢失或重复执行 | 通过 | `online_migration_freezes_only_target_shard_and_fences_old_primary` 全程采样每个分片最多一个 leader，旧主退役后不能当选；10 万笔演练 0 错误，逐副本余额守恒且位点一致；迁移后的重试复用 `request_id`，幂等结果不重复 |
| 只有被迁移分片出现受控的短暂停写，其他分片持续服务 | 通过 | 同一测试在迁移期间对另一分片持续提交并断言全部成功；演练中其他分片迁移窗口 P99 约 20ms、max 约 20.3ms，无停写 |
| 新旧路由最终收敛，旧主被可靠 fence | 通过 | 路由 epoch 与持久化 `route.version` 一致；`migrated_group_reopens_with_new_membership_and_epoch` 重启后恢复新 voter 集与 epoch；重启退役副本不进入 voter 集也不能成为 leader |
| 在持续压测下记录迁移对 P99/P999 和恢复时间的影响 | 通过 | 上文基线与 `migration_window`/`migration_pause` 输出：整体 P99 20.3ms、P999 67.3ms；目标分片 max 1.19s，其他分片 P99 约 20ms |

M4 测试共 4 项（`crates/replication/tests/m4_acceptance.rs`）全部通过：在线迁移与 fence、失败回滚与重试、扩缩容保持多数派、迁移后重启恢复。节点管理 API 集成测试与 Go 控制面单元测试覆盖迁移状态映射、幂等重放、在途冲突和审计落盘。

### 本机 M1 基线

2026-11-01，本机 macOS Intel x86_64，release 构建，10 万笔转账、32 并发、1024 个账户、最大批次 64。每笔命令 44 字节，对应 WAL 记录 68 字节；结果不包含账户初始化耗时。

| 模式 | 吞吐量 | 耗时 | P50 | P99 | P999 | 错误 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Durable | 1,343 ops/s | 74.455s | 20.4ms | 56.4ms | 107.5ms | 0 |
| UnsafeBuffered 对照 | 50,282 ops/s | 1.989s | 479.5us | 3.316ms | 7.461ms | 0 |

两组测试均通过逐账户余额校验、最终序号校验、幂等结果数量校验、快照生成和重新恢复验证。该结果只代表上述本机环境；durable 模式的主要瓶颈是刷盘延迟，不能将对照模式数字宣传为持久化 TPS。

### 当前边界

- 当前是嵌入式交易内核、复制节点进程和压测工具，尚未提供面向客户端的网络交易网关；`trading-node` 只暴露运维 admin API。
- 当前 checkpoint 在单写执行器停止、取得 `Partition` 所有权后执行；在线快照将在后续实现。
- 幂等结果完整保存在内存和快照中，尚未实现保留窗口和淘汰策略。
- 已测试半条日志与 CRC 损坏，尚未进行真实断电、磁盘固件和带断电保护硬件验证。
- 复制组传输层为进程内通道：副本崩溃、分区、切换与迁移都在单进程内注入和验证；跨节点网络化、真实网络分区与 TLS 仍是后续边界。
- 副本数据目录使用 fs2 文件锁防止跨进程双开；同进程内由门面层防护，跨机器部署需要控制面协调。
- `--replicas` 复制模式暂未接入 PostgreSQL 投影（单写模式已支持），投影与复制组的位点协调在后续里程碑处理。
- UnsafeBuffered 下快照、日志压实和截断不做 fsync，与该模式“崩溃可能丢失最近写入”的契约一致；checkpoint 例外，它总是全量刷盘。
- 迁移编排在单进程内验证：每分片 voter 是组内副本 id，不是跨机器地址；跨机器副本网络化、TLS/鉴权和多节点容量调度属于后续。
- 迁移中逻辑槽位映射保持不变，只改变物理 voter 集并提升 epoch；改变账户→分片映射的热点槽位搬迁尚未实现。
- 迁移按集群串行化；同一集群的并行多分片迁移、全局迁移队列与限流尚未实现。
- 退役副本保留数据目录供 fence 演练人工重启，不自动清理。
- Go 控制面是单实例内存索引加 JSONL 审计，无分布式协调、TLS 或鉴权。

## 2. 总体架构

```mermaid
flowchart LR
    C[Client] --> G[Stateless Gateway]
    G --> R[Versioned Shard Router]
    R --> A[Partition Group A]
    R --> B[Partition Group B]
    R --> N[Partition Group N]
    A --> AW[WAL and Snapshot A]
    B --> BW[WAL and Snapshot B]
    N --> NW[WAL and Snapshot N]
    A --> Q[Async Committed-Log Projection]
    B --> Q
    N --> Q
    Q --> D[(PostgreSQL Query Model)]
    CP[Go Control Plane] -. route and migration .-> R
    CP -. shard operations .-> A
    CP -. shard operations .-> B
    CP -. shard operations .-> N
```

### 数据面

数据面负责所有延迟敏感操作：

1. 接入、协议解析、无状态校验和幂等检查。
2. 根据版本化路由表定位分片主节点。
3. 分片内定序，不维护集群全局总序。
4. WAL 顺序追加、组提交和分片级副本复制。
5. 单写线程确定性修改内存状态。
6. 达到提交条件并执行完成后返回结果。

### 控制面

控制面负责非热路径操作：

- 节点、分片和路由表管理。
- 路由 epoch、leader fence 和分片迁移编排。
- 副本状态、快照、日志位点和容量监控。
- 运维 API、审计和故障处理工具。

控制面不应成为每笔交易必须同步访问的依赖。

### 查询与事件投影

- 下游只消费已经提交的日志。
- 事件使用 `(partition_id, epoch, sequence)` 定位，保证分片内有序和幂等消费。
- PostgreSQL 保存异步查询模型，不作为核心余额的写入真相源。
- 通知、搜索、报表和归档可以最终一致；核心账务不能因此降级为最终一致。

## 3. 核心设计原则

1. **分片内强一致**：每个分片同一时刻只有一个写主节点，状态由单写线程独占。
2. **日志先持久化**：严格模式下，WAL 未达到提交条件不得返回成功。
3. **组提交**：将多笔交易合并为顺序写批次，避免每笔交易单独刷盘。
4. **确定性状态机**：相同快照和日志序列必须得到完全相同的状态与结果。
5. **快照加重放**：通过最近快照和后续 WAL 恢复，截断已安全覆盖的旧日志。
6. **分片内定序**：集群不依赖全局连续序号，避免重新引入单点串行瓶颈。
7. **优先本地交易**：绝大多数交易必须在单分片内原子完成，跨分片操作进入明确的慢路径。
8. **分片级复制**：副本用于高可用和读扩展，不用于同一分片的多主写入。
9. **异步下游**：消息、查询库和归档只消费已提交日志，不阻塞核心提交。
10. **幂等与背压**：所有入口可安全重试，所有队列有界，过载时快速拒绝。
11. **epoch 与 fencing**：主备切换和分片迁移必须阻止旧主继续提交，防止脑裂。
12. **金额精确表示**：使用整数最小货币单位或严格定点数，禁止浮点金额。

## 4. 单分片交易提交流程

```text
request
  -> stateless validation and request_id deduplication
  -> versioned shard routing
  -> partition leader sequencing
  -> WAL append and replica persistence
  -> quorum/commit condition reached
  -> deterministic in-memory state transition
  -> response with request_id and committed sequence
```

关键约束：

- 时间、随机数和外部报价等不确定输入必须进入交易命令或日志，不能在重放时重新获取。
- WAL 记录需要包含分片、epoch、序号、长度、payload 和校验信息，以识别半条写入及损坏。
- 已提交日志才可发布到下游；消费者按请求 ID 或日志位点幂等。
- 如果仅本地刷盘，主要解决进程崩溃和本机持久化；如果要求主机故障下 RPO=0，必须等待同步副本达到提交条件。

## 5. 水平扩容方案

### 逻辑分片

- 初始建立足够多的逻辑分片，一台物理节点可运行多个分片。
- 路由表保存分片到主备节点的映射，并携带单调递增的 epoch。
- 新增节点时只迁移部分逻辑分片，不全量重新散列所有数据。
- 总吞吐近似为各分片吞吐之和，再扣除热点倾斜、跨分片交易和复制开销。

### 跨分片交易

优先通过数据建模，将必须共同满足强一致约束的状态放入同一分片。确实无法避免时：

- 不允许中间状态的原子交易：使用带持久化协调者的原子提交协议，接受更高延迟和恢复复杂度。
- 允许处理中、冻结或待入账状态：使用资金预留、业务工作流、Outbox 和补偿。

跨分片交易比例是核心容量指标。如果它成为主流量，增加节点可能不会带来线性收益。

### 在线分片迁移

1. 在目标节点创建新副本。
2. 复制最近快照并持续同步 WAL。
3. 目标副本基本追平后，短暂停写或 fence 旧主。
4. 同步最后日志并确认双方提交位点。
5. 提升目标节点为主，增加 epoch，原子更新路由表。
6. 通过重定向和幂等重试收敛旧请求。
7. 验证新主稳定后下线旧副本。

迁移只影响目标分片，不应停止整个集群。

## 6. 四个里程碑

### M1：单机交易内核、WAL 与快照

**状态：已完成。**

**目标**：先实现一个正确、可恢复、可压测的单分片交易内核。

主要交付：

- Rust 工作空间和固定二进制请求/日志协议。
- 单写状态机、整数金额、账户操作和 `request_id` 幂等。
- 有界输入队列与背压。
- WAL 顺序追加、CRC、组提交和可配置刷盘策略。
- 快照、日志重放和安全日志截断。
- 独立压测程序和基础指标。

验收标准：

- 相同快照和 WAL 重放后的状态、结果及幂等记录一致。
- 进程崩溃和半条日志注入后能够恢复到明确的一致位点。
- 在声明的持久化模式下，已响应交易不会因进程崩溃而丢失。
- 给出固定硬件、日志大小和批次条件下的端到端 TPS、P99/P999 和恢复时间基线。

### M2：多逻辑分片与无状态路由

**状态：已完成。**

**目标**：将单节点内核扩展为可分片部署的数据面。

主要交付：

- 逻辑分片模型和版本化路由表。
- 每分片独立定序、WAL、快照、队列和执行线程。
- 单分片原子交易主路径。
- 分片级容量、队列深度、热点和延迟指标。
- 异步已提交日志投影到 PostgreSQL 的最小实现。

验收标准：

- 均匀负载下增加分片后吞吐接近预期增长。
- 不同分片互不共享可变状态，不需要全局写锁或全局序号。
- 路由错误、过期和重定向不会导致重复执行。
- 查询投影可重复消费且不产生重复结果。

### M3：分片级复制与自动故障切换

**状态：已完成（2026-11-03）。** 实现与验收证据见 [M3 章节](#m3-分片级复制与自动故障切换)；Raft 选型见 [ADR 0001](docs/adr/0001-raft-implementation-choice.md)。

**目标**：让每个分片成为可容错的小型复制状态机。

主要交付：

- 基于成熟 Raft 实现的分片复制组，不自研共识算法。
- 日志持久化、多数派提交、快照复制和副本追赶。
- leader lease/epoch、fencing 和旧主拒绝写。
- 客户端重定向、重试与持久化幂等。
- 主备切换和副本重建工具。

验收标准：

- 主节点故障后，已提交日志不丢失，旧主恢复后不能继续写入。
- 网络分区、重复请求和切换期间重试不会导致重复扣账。
- 新副本可通过快照和日志完成追赶。
- 记录并评估故障检测时间、切换停写时间和恢复目标。

### M4：在线分片迁移与弹性扩容

**状态：已完成（2026-11-03）。** 实现与验收证据见 [M4 章节](#m4-在线分片迁移与弹性扩容)；代码位于 `crates/replication`（迁移编排）、`crates/node`（节点管理 API）与 `control-plane/`（Go 控制面）。

**目标**：新增节点后，可以在不全集群停写的情况下重新分配分片。

主要交付：

- Go 控制面和分片迁移编排 API。
- 目标副本创建、快照传输、WAL 追赶和位点校验。
- 短暂 fence、epoch 提升、路由原子切换和旧主下线。
- 路由版本收敛、重定向、审计和迁移失败恢复。
- 扩容、缩容和热点分片搬迁演练。

验收标准：

- 迁移期间不出现双主、已提交交易丢失或重复执行。
- 只有被迁移分片出现受控的短暂停写，其他分片持续服务。
- 新旧路由最终收敛，旧主被可靠 fence。
- 在持续压测下记录迁移对 P99/P999 和恢复时间的影响。

## 7. 推荐技术栈

| 模块 | 推荐选择 | 说明 |
| --- | --- | --- |
| 交易核心、状态机 | Rust | 无 GC，内存布局和线程模型可控 |
| 网络接入 | Rust + Tokio | 处理并发连接；核心状态机不依赖异步任务调度 |
| 分片执行 | 固定 OS 线程 + 有界 SPSC/MPSC 队列 | 分片单写，避免共享可变状态和锁竞争 |
| WAL | 批量顺序写、`sync_data`；后续评估预分配与 `writev` | 先建立可靠基线，再根据 profile 评估 `io_uring` |
| 日志格式 | 固定小端二进制 + CRC32 | 避免 JSON 热路径开销，支持损坏检测 |
| 分片复制 | openraft 0.9（Rust） | 经 [ADR 0001](docs/adr/0001-raft-implementation-choice.md) 选定，已用于 M3 分片复制组；禁止自研共识 |
| 控制面 | Go + HTTP/JSON（当前标准库实现） | 路由、节点、迁移和运维工具；接口稳定后可评估 gRPC |
| 查询投影 | PostgreSQL | 异步查询、对账和归档，不在提交主链路 |
| 消息系统 | 初期不强制；需要时引入 Kafka | 仅传输已提交事件，不是核心提交依赖 |
| 可观测性 | Prometheus + Grafana + OpenTelemetry | 覆盖吞吐、延迟、日志、副本和迁移指标 |
| 测试 | Rust 属性测试、故障注入、独立压测端 | 验证确定性、恢复、幂等和分布式故障 |
| 本地环境 | Docker Compose | 只编排控制面、查询和监控；核心性能测试尽量贴近裸机 |

### 暂不做的事情

- 不在第一版引入 Kubernetes、服务网格或大量微服务。
- 不在未建立普通系统调用基线前使用 DPDK、RDMA 或复杂 NUMA 优化。
- 不在核心热路径使用 JSON、同步 RPC、分布式锁或远程配置读取。
- 不为了组件齐全而过早引入 Redis 和 Kafka。

## 8. 建议的代码结构

以下结构随里程碑逐步创建，不需要一次性生成空壳：

```text
trading-platform/
├── crates/
│   ├── protocol/       # 请求、响应和 WAL 二进制格式
│   ├── storage/        # WAL、快照、校验与恢复
│   ├── partition/      # 单写状态机、队列、路由和分片执行
│   ├── projection/     # 已提交日志的异步 PostgreSQL 投影
│   ├── replication/    # 分片复制、主备切换与在线迁移（M3/M4）
│   ├── node/           # 复制节点进程与本地管理 API（M4）
│   ├── gateway/        # 网络接入、路由和背压
│   └── loadgen/        # 独立压测、故障与迁移演练
├── control-plane/      # Go 控制面：迁移编排、策略校验与审计（M4）
├── deploy/             # 本地依赖和监控环境
├── docs/
│   └── adr/            # 关键架构决策记录
└── tests/              # 故障注入、一致性和迁移测试
```

## 9. 必须持续观测的指标

- 持久化后响应的端到端 TPS，以及 P50/P99/P999 延迟。
- 组提交批次大小、WAL 字节数、刷盘和副本确认延迟。
- 各分片吞吐、队列深度、拒绝率和热点倾斜。
- 单分片交易与跨分片交易比例。
- 已提交位点、已执行位点、副本落后量和快照进度。
- 主备切换停写时间、崩溃恢复时间和分片迁移影响。

压测必须报告请求和日志大小、账户分布、热点比例、持久化等级、副本数量和硬件条件，并避免 coordinated omission 导致延迟结果被低估。

## 10. 实施顺序

严格按以下顺序推进：

```text
M1 单机 WAL/快照       已完成
  -> M2 多分片路由      已完成
  -> M3 Raft 主备切换   已完成
  -> M4 在线分片迁移    已完成
```

每个里程碑先满足正确性和故障注入验收，再进行性能优化。性能数字只有在交易语义、持久化等级和故障行为明确之后才有意义。
