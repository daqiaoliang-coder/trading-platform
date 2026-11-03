# ADR 0001:M3 分片复制选用 openraft

- 状态:已接受(2026-11-03)
- 决策人:trading-platform 维护者

## 决策

每个逻辑分片是一个独立的 openraft 复制组,版本锁定 0.9 系列(当前 0.9.25),启用 `single-term-leader` 特性。共识、多数派提交、领导者租约/epoch fencing、快照传输与副本追赶全部由 openraft 提供,不自研共识算法(遵守 README §7 技术约束)。

关键映射:

| 领域概念 | 实现方式 |
| --- | --- |
| Raft 日志 | WAL v2(`storage::Wal` 记录含 term;single-term-leader 下 `CommittedLeaderId` 仅含 term,恰好与记录格式一一对应) |
| raft index 与命令序号 | 一个命令一条日志项,index 即 `sequence`,保持既有连续序号语义 |
| 状态机 | `partition::State` 原样复用,`request_id` 幂等即 raft 层重试安全性的来源 |
| 快照 | `storage::Snapshot` v2(含 last_term)+ 成员信息编码为信封,本地持久化与网络传输共用同一载荷 |
| 网络 | 进程内有界通道 + 定向断链注入(分区测试用);TCP/gRPC 属 M4 网关 |
| 提交路径 | 客户端 → leader `client_write`,多数派 fsync 确认后才返回;`ForwardToLeader` 作为重定向信号,由门面与压测端配合重试 |

## 备选方案

- **raft-rs(TiKV)**:成熟、同步、无运行时依赖;但需要自行实现就绪循环、提案-响应匹配、租约 fencing、快照握手等全部胶水,出错面大,且构建对 protoc 有隐性风险。
- **自研共识**:被项目约束明确禁止。

## 权衡与后果

- 引入 tokio 运行时:状态机保持同步单写(apply 由 openraft 内部串行化),提交路径由门面 `block_on` 桥接,不改变既有执行线程模型。
- openraft 迭代快:workspace 锁定 `0.9`,升级需重跑复制测试套件后再接受。
- 快照载荷在内存 `Cursor<Vec<u8>>` 中传输,M3 不做流式文件传输(压测规模下可接受)。
- 复制模式耐久性与单机模式一致:WAL fsync(`Durable`)后再向多数派确认;`UnsafeBuffered` 仅供基准模式。
- 既有 `Partition`/`Engine` 单机路径保持不变;复制模式不接入投影(M3 边界,见 README)。
