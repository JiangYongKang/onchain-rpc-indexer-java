# onchain-rpc-indexer-java

本地多节点链上数据采集与索引组件。通过可本地启动的模拟节点（`MockChainNode`）提供区块与日志数据，
在节点故障、链头回退与重复采集场景下保证索引结果一致且可解释。

## 组件结构

```
node/       ChainNode 抽象 + MockChainNode（本地模拟节点：可配置延迟、抖动失败、宕机、重组、畸形数据注入）
selection/  NodeSelector：节点探测、择优、故障隔离与恢复，选择依据可查询
store/      JournaledIndexStore：原子写入、幂等、重组回滚、固化位点、追加日志持久化
ingest/     IngestionService：选节点 -> 取块 -> 校验 -> 原子写入 -> 重组对齐
api/        IndexController：REST 查询与触发入口
```

## 节点择优与隔离恢复规则

- **择优打分**：按 `(是否半开, 平均延迟 EMA, 历史失败率)` 升序打分，最优者被选中。
  每次选择的完整依据（各节点隔离状态、剩余冷却时间、延迟、失败率）通过
  `SelectionReport.rationale` 返回，并可由 `GET /api/nodes` 查询。
- **抖动不误判**：只有**连续**失败达到阈值（默认 3 次）才隔离；任何一次成功都会清零连续失败计数，
  因此短暂抖动（失败-成功交替）不会触发隔离。
- **隔离与恢复**：被隔离节点在冷却期（默认 5s）内不参与选择；冷却期满后进入**半开**状态，
  允许一次探测调用——探测成功则解除隔离、清零计数；探测失败则重新隔离并重新计时。
- **单次采集内的切换**：一次采集中某节点调用失败后，该节点在本次采集的剩余尝试中被排除，
  立即切换到次优节点，失败记录仍计入统计（可能影响后续隔离判定）。

## 重组回滚语义

- 写入按**连续追加**校验：新区块高度必须是 `本地链头 + 1` 且父哈希与本地链头哈希一致。
- **父哈希不匹配**（采信节点切换或链头回退）：回滚本地链头一层，由采集方从回滚位点的下一高度
  向同一节点重新取块对齐，逐层迭代直到找到分叉点，然后继续向前采集到本次目标高度。
- **同高度哈希冲突**：回滚该高度及以上的全部数据后再按上一条对齐。
- **固化位点**：`finalizeUpTo(h)` 之后的位点不可变更。请求位点 `<= 固化位点` 且与已存数据不一致时
  返回 `BELOW_FINALIZED_CURSOR` 明确拒绝（ identical 重投按幂等忽略）；重组回滚触及固化位点时同样拒绝，
  固化数据不会被破坏。
- **重启可复现**：每次变更（apply / rollback / finalize）先写入追加日志
  （`indexer.journal.path`，默认 `./data/index-journal.jsonl`）再更新内存，重启后按日志回放，
  链头、各高度哈希与固化位点完全复现。

## 并发写入的一致性保证

- 存储层所有写操作串行化（`synchronized`），单次采集的写入是原子的：要么整个区块（含日志）写入，
  要么整体拒绝，不存在半写状态。
- 同一高度的并发请求或重复投递：第一个请求追加成功，其余命中同高度同哈希校验，
  返回 `DUPLICATE_IGNORED` 幂等忽略，不会产生重复记录。
- 校验失败即拒绝，拒绝原因可区分：

  | 原因 | 含义 |
  |---|---|
  | `MALFORMED_BLOCK` | 区块/日志字段缺失或格式非法（如哈希非 `0x` 前缀） |
  | `DECODE_FAILURE` | 字段存在但无法解码（如日志 data 非十六进制） |
  | `BELOW_FINALIZED_CURSOR` | 位点低于或触及固化位点，或重组深度超过固化位点 |
  | `INVALID_HEIGHT` | 高度为负或不连续（存在空洞） |

## 本地验证方法

### 运行测试

```bash
mvn test
```

覆盖场景：

- `NodeSelectorTest`：抖动不误判、连续失败隔离、冷却后半开恢复、低延迟择优与依据可解释；
- `IngestionConsistencyTest`：16 线程并发采集同一高度仅一条写入、重复投递幂等、
  畸形/解码失败原因可区分、低于固化位点明确拒绝；
- `ReorgRecoveryTest`：节点切换 + 重组回滚对齐、重启后回放复现、链头回退对齐、
  重组不得跨越固化位点。

单测日志中会打印节点选择依据（`节点选择依据: [...]`）、采集位点
（`采集位点: height=.. node=.. 本地链头=.. 固化位点=..`）与归一化错误
（`写入被拒绝: .. 归一化原因=..`）。

### 启动本地服务并冒烟验证

```bash
mvn spring-boot:run
# 启动后注册 node-a(5ms) / node-b(20ms) / node-c(50ms) 三个模拟节点（同一条模拟链）

curl -X POST 'http://localhost:<port>/api/collect?height=0'   # 采集指定高度
curl -X POST 'http://localhost:<port>/api/collect/latest'     # 采集链头
curl 'http://localhost:<port>/api/nodes'                      # 节点状态与隔离依据
curl 'http://localhost:<port>/api/index/state'                # 链头 / 固化位点 / 已索引高度
curl -X POST 'http://localhost:<port>/api/index/finalize?height=2'
curl -X POST 'http://localhost:<port>/api/collect?height=1'   # -> BELOW_FINALIZED_CURSOR
```

重启进程后再次查询 `/api/index/state`，可验证索引状态从日志中完整复现。
