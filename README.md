# 本地多节点链上数据采集与索引组件

一套可**本地启动**的多节点链上数据采集与索引组件。用三个内置的模拟 RPC 节点提供区块与日志数据，
重点解决：节点择优与故障隔离、并发采集下的幂等一致索引、链头回退/重组回滚，以及失败原因的可解释、可复现。

## 1. 运行与构建

```bash
mvn compile                  # 编译
mvn test                     # 运行全部 19 个单测（含节点选择/回退/并发幂等场景）
mvn spring-boot:run          # 启动服务（端口随机，见启动日志）
# 或打包后指定端口与状态文件运行：
mvn package -DskipTests
java -jar target/onchain-rpc-indexer-java-1.0-SNAPSHOT.jar \
     --server.port=8080 --indexer.state-file=data/index-state.json
```

仅依赖 spring-boot-starter-web / starter-test，存储为单个 JSON 快照文件，无需外部数据库。

## 2. 组件结构

| 包/类 | 职责 |
| --- | --- |
| `rpc.SimulatedRpcNode` | 可本地启动的模拟节点：确定性链 + 延迟/抖动/下线/定点故障/畸形报文/解码失败/内容篡改/整链替换 |
| `node.DefaultNodeSelector` | 探测、滑动窗口统计、择优、连续失败隔离、冷却后恢复探测 |
| `indexer.ChainIndexer` | 采集编排：故障切换、链尖对齐、分叉回滚、重放、幂等写入、拒绝留痕、位点保护 |
| `indexer.BlockValidator` | 区块内容校验，归一化三类拒绝原因 |
| `indexer.HeightMutex` | 按高度分段锁，同高度并发串行、异高度并行 |
| `storage.IndexStore` | JSON 快照持久化：区块/日志、链头位点、固化位点、拒绝留痕、事件账本（原子落盘） |
| `web.IndexerController` | 本地验证 REST 接口 |

## 3. 节点择优与隔离恢复规则（可查询、可解释）

每个节点维护最近 `indexer.window-size`（默认 10）次调用结果，统计**成功率**与**平均延迟**。

- **择优评分（分数越低越优）**
  - 成功率 ≥ 0.5 视为健康抖动，不产生成功率惩罚；成功率 < 0.5 按 `(1-成功率)×1000` 重罚；
  - 同健康等级内延迟越低越优（延迟以毫秒/1000 缩放到成功率量纲）；
  - 零样本节点给温和先验分 0.5：优于已证实不健康者，但劣于“已证实又快又稳”的节点，避免冷节点压制有良好记录的节点；
  - 分数相同按节点 id 稳定排序。
- **短暂抖动不误判**
  - 只有**连续失败**达到 `indexer.failure-threshold`（默认 3）才隔离，零星失败穿插成功不隔离；
  - 另有成功率熔断兜底：样本 ≥3 时成功率 < 0.5，或**唯一一次观测就是传输类失败**时隔离；
  - 畸形/解码失败属于**数据问题**而非节点不可达，不参与传输熔断（节点某一高度数据坏不代表整节点下线）。
- **隔离与恢复**
  - 隔离后选择阶段完全跳过该节点，并记录隔离时间；
  - 冷却 `indexer.recover-cooldown`（默认 2s）到期后，下一次选择会对其发起一次**真实探测**
    （`getHeadHeight`，超时 `indexer.probe-timeout` 默认 500ms）：
    - 探测成功：解除隔离、重置观测窗口（故障周期结束），重新参与择优；
    - 探测失败：继续隔离并**重新计时冷却**。
  - 采集过程中节点恢复成功（返回 OK）也会立即解除隔离。
- **可解释**：`GET /api/nodes` 返回每节点 `isolated / consecutiveFailures / successRate /
  avgLatencyMillis / recoverAfterMillis / lastError`；选择时日志打印完整排序依据，形如
  `node-a[score=.. okRate=.. avgMs=.. cf=.. n=..] | node-b[ISOLATED cf=.. cooldownRemainMs=.. lastError=..]`。

## 4. 并发写入的一致性保证

- **分段互斥 + 全局写锁**：同高度的并发采集经 `HeightMutex` 串行化（不同高度不互斥）；
  整段推进在 `ChainIndexer` 全局写锁内完成，杜绝数据竞争。
- **按高度幂等去重**：区块以高度为键，`(高度, 哈希)` 相同直接幂等返回，重复投递/并发请求
  不产生重复记录；同高度不同哈希会被拒绝（必须先走重组回滚），不允许覆盖。
- **单次写入原子落盘**：每次状态变更后通过“临时文件 + 原子 move”覆盖快照，
  单次采集的区块、日志、位点、留痕、事件要么整体可见要么不可见，**无半写**。
- **连续高度、无空洞**：请求位点超前于已索引头会被明确拒绝，禁止跳过高度。

## 5. 畸形/解码失败数据：不静默丢弃、原因可区分

报文先经“序列化 → 解码”流程，再做内容校验。拒绝原因归一化为三类并写入**拒绝留痕**（`GET /api/status`）：

| RejectReason | 触发情形 |
| --- | --- |
| `MALFORMED_BLOCK` | 报文截断/字段缺失、空哈希/父哈希、日志字段缺失、高度不符等结构畸形 |
| `DECODE_ERROR` | 数值字段不可解码、哈希非 `0x`+64 位十六进制等格式问题 |
| `CONTENT_MISMATCH` | 哈希与按内容重算结果不一致（内容篡改）、父哈希接不上、日志索引有洞/重复 |

所有健康节点在某高度都返回畸形数据时，该高度被拒绝留痕并**停止推进**，不会跨过坏数据污染后续索引。

## 6. 链头回退与重组（Reorg）语义

每次推进开始先做**链尖对齐**，逐高度采集中还会做**父哈希对齐**：

1. 本地链头高于采信节点链头（链头回退），或链尖哈希与节点不一致（分叉/节点切换）→ 触发对齐；
2. 自链头向下与采信节点逐高度比对哈希，找到**共同祖先**；
3. 将共同祖先之后的已索引区块**按降序回滚**，并在新链上从分叉点**重放**；
4. 回滚（`ROLLBACK` / `ROLLBACK(align)`）、重放（`INDEX`）、拒绝（`REJECT`）都写入**事件账本**，
   与区块、位点同快照持久化——**重启后回滚与重放结论完整可复现**，再次推进为空操作。

**固化位点（finalized）保护**：

- 固化位点单调递增，固化高度及以下数据**永不回滚**；
- 若分叉共同祖先低于固化位点，拒绝采信该分叉，维持原链并记录 `CONTENT_MISMATCH` 留痕；
- **请求携带的位点若 ≤ 已固化位点，直接拒绝（HTTP 409 `STALE_CHECKPOINT`），而不是从中间继续。**

## 7. REST 接口（本地验证）

| 方法与路径 | 说明 |
| --- | --- |
| `GET  /api/status` | 链头/固化位点、当前选中节点、全部节点健康依据、拒绝留痕、事件账本 |
| `GET  /api/nodes` | 节点择优/隔离依据 |
| `GET  /api/blocks` | 已索引区块（去重后唯一记录） |
| `POST /api/indexer/advance?fromHeight=0` | 从位点向链头推进（回滚/重放/拒绝结论都在返回体） |
| `POST /api/indexer/finalize?height=5` | 推进固化位点 |
| `POST /api/sim/nodes/{id}/down` · `/up` | 节点下线/恢复 |
| `POST /api/sim/nodes/{id}/reorg?forkHeight=5&newHeadHeight=6` | 从某高度起替换为分叉链 |
| `POST /api/sim/nodes/{id}/block/{h}/rpc-fault?code=TIMEOUT` | 定点注入归一化 RPC 错误 |
| `POST /api/sim/nodes/{id}/block/{h}/malformed` | 定点返回截断（畸形）报文 |
| `POST /api/sim/nodes/{id}/block/{h}/undecodable` | 定点返回不可解码字段 |
| `POST /api/sim/nodes/{id}/block/{h}/hash-mismatch` | 定点内容篡改（哈希不符） |
| `POST /api/sim/nodes/{id}/faults/clear` | 清除故障并恢复初始链 |

## 8. 本地验证步骤（端到端）

```bash
java -jar target/onchain-rpc-indexer-java-1.0-SNAPSHOT.jar --server.port=8080 \
     --indexer.state-file=/tmp/ix/state.json

# 1) 全量采集
curl -X POST 'localhost:8080/api/indexer/advance?fromHeight=0'
curl localhost:8080/api/blocks        # 8 个区块（高度 0..7）

# 2) 节点切换：下线 node-a，再推进（已采部分幂等），观察自动切换 node-b
curl -X POST localhost:8080/api/sim/nodes/node-a/down
curl -X POST 'localhost:8080/api/indexer/advance?fromHeight=0'
curl localhost:8080/api/nodes         # node-a 被隔离并显示依据

# 3) 链头回退/重组：从高度5分叉为更短的新链（头=6），再推进应回滚 7,6,5 并重放
curl -X POST 'localhost:8080/api/sim/nodes/node-a/reorg?forkHeight=5&newHeadHeight=6'
curl -X POST 'localhost:8080/api/sim/nodes/node-b/reorg?forkHeight=5&newHeadHeight=6'
curl -X POST 'localhost:8080/api/indexer/advance?fromHeight=0'

# 4) 畸形/解码失败拒绝（未采高度定点注入）
curl -X POST 'localhost:8080/api/sim/nodes/node-a/block/6/rpc-fault?code=MALFORMED_RESPONSE'
curl -X POST 'localhost:8080/api/sim/nodes/node-b/block/6/rpc-fault?code=MALFORMED_RESPONSE'
curl -X POST 'localhost:8080/api/sim/nodes/node-c/block/6/rpc-fault?code=DECODE_ERROR'
curl -X POST 'localhost:8080/api/indexer/advance?fromHeight=0'   # 0..5 成功，6 被三类原因留痕并停止

# 5) 固化位点保护
curl -X POST 'localhost:8080/api/indexer/finalize?height=5'
curl -i -X POST 'localhost:8080/api/indexer/advance?fromHeight=3'  # HTTP 409 STALE_CHECKPOINT
```

## 9. 测试覆盖

- `NodeSelectorTest`：延迟择优、短暂抖动不隔离、连续失败隔离与切换、冷却后探测恢复、
  探测失败重置冷却、排除已尝试节点。
- `IdempotentIndexingTest`：全量采集、串行/并发重复投递无重复、空库并发首采恰好一条/高度、
  畸形与解码失败的可区分留痕、内容篡改拒绝、过期位点拒绝、空洞拒绝。
- `ReorgIndexerTest`：链头回退回滚重放、单节点分叉经父哈希检出回滚、回滚/重放结论重启复现、
  固化数据不可回滚、采集中节点切换保持父子链一致。

单测日志中会打印：**节点选择依据**（score/成功率/延迟/连续失败/冷却）、**采集位点**
（requestedFrom/effectiveFrom/indexedHead/finalized）与**归一化后的错误码与拒绝原因**。
