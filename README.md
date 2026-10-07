# onchain-rpc-indexer-java

本地多节点链上数据采集与索引组件。通过可本地启动的模拟节点（`MockChainNode`）提供区块与日志数据，
在节点故障、链头回退与重复采集场景下保证索引结果一致且可解释。

## 组件结构

```
node/       ChainNode 抽象 + MockChainNode（本地模拟节点：可配置延迟、抖动失败、宕机、重组、畸形数据注入）
selection/  NodeSelector：节点探测、择优、故障隔离与恢复，选择依据可查询
store/      JournaledIndexStore：原子写入、幂等、重组回滚、固化位点、追加日志持久化
ingest/     IngestionService：单高度采集（选节点 -> 取块 -> 校验 -> 原子写入 -> 重组对齐）
follow/     BlockFollower：自动追块（整段补齐 / 后台按节奏追链头），进度与失败分类可查询
api/        IndexController：REST 查询与触发入口
```

## 自动追块

### 触发方式

- **整段补齐**：`POST /api/follow/range?from=0&to=500` 一次性把 `[from, to]` 连续补齐。
  同步返回 `FollowRangeResult`：要么 `success=true` 且本地链头对齐到 `to`，
  要么 `success=false` 并带可区分的 `failureReason` 与每个节点的不可信记录 `nodeErrors`。
- **后台自动追**：开关 `indexer.follow.enabled`（默认 `false`），或运行时
  `POST /api/follow/auto?enabled=true`。开启后按 `indexer.follow.interval-ms`
  （默认 5000ms）的节奏每轮向链头追，单轮最多推进 `indexer.follow.max-batch-per-round`
  （默认 500）个高度，防止单轮过长。

### 追块语义

- 追块起点永远取 `本地链头 + 1`：已索引的高度计入 `skipped` 直接跳过，**不缺块、不重采、
  不写一半**（存储层原子写入 + 幂等去重兜底）。
- 同一时刻只允许一个追块任务：重复触发（或自动轮与手动段追冲突）时后到的触发直接返回
  `started=false`，只算一次；与手动单高度采集并发时靠存储层幂等保证不产生重复记录。
- **重启续追**：追块进度就是索引日志里的链头，进程重启回放日志后从上次位置继续，
  已索引的不重复处理，未追的不跳过。
- 追块中遇到链头回退/重组：复用存储层重组回滚语义，回滚到分叉点后从同一链视图继续对齐。

### 追块期间的节点容错与恢复

- 某高度上节点**取不到块、返回畸形数据或解不出来**：记为该节点一次失败并立即换其他健康节点
  继续该高度，不把坏块当成功、也不整段放弃；每个节点最近的不可信原因记入结果的
  `nodeErrors` 并打印日志。
- 只有**所有可用节点都给不出可信数据**时才停止，失败原因可区分：

  | 原因 | 含义 |
  |---|---|
  | `NO_AVAILABLE_NODE` | 节点全部隔离或调用失败，无节点可用 |
  | `NO_TRUSTWORTHY_DATA` | 节点都在，但该高度上返回的数据都不可信 |
  | `BELOW_FINALIZED_CURSOR` | 追块触及固化位点边界被拒绝 |
  | `INVALID_HEIGHT` | 请求区间非法 |
  | `REORG_LIMIT_EXCEEDED` | 重组对齐迭代超过上限 |

- 节点的隔离/恢复规则不变（连续失败达阈值隔离 -> 冷却 -> 半开探测 -> 恢复），
  追块与手动采集共用同一套 `NodeSelector` 统计：被隔离节点冷却期满后进入半开，
  探测成功即真正重新参与追块。`GET /api/nodes` 中每个节点可见：
  `state`（`AVAILABLE` / `ISOLATED` / `HALF_OPEN`）、`remainingCooldownMs`
  （还要等多久才能恢复探测）、`lastError`（最近一次为什么出问题）及累计成败计数。

### 追块进度查询

`GET /api/follow/status` 返回：

- `autoEnabled` / `running`：自动追块开关、当前是否有任务在跑；
- `currentHeight` / `targetHeight` / `lag`：本地链头、最近观察到的链头目标、落后量；
- `lastRoundWritten` / `lastRoundSkipped` / `lastRoundFailed`：最近一轮写入、重复跳过、失败块数；
- `failureReasons`：最近一轮失败原因分类计数；`lastError`：最近一次失败详情；
- `totalWritten` / `totalSkipped` / `totalFailed`：累计统计。

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

## 与既有语义的兼容范围

本版只新增追块层与节点状态视图字段，既有行为不变：

- **单高度采集**（`POST /api/collect`、`/api/collect/latest`）：语义完全不变，
  与追块并发时由存储层幂等保证同一高度只落一条记录；
- **固化位点**：追块触及固化位点时按 `BELOW_FINALIZED_CURSOR` 停止，固化数据不会被改动；
- **链头回退/重组回滚**：追块复用同一套回滚对齐逻辑，重组不得跨越固化位点的约束同样生效；
- **拒绝原因枚举**（`MALFORMED_BLOCK` / `DECODE_FAILURE` / `BELOW_FINALIZED_CURSOR` /
  `INVALID_HEIGHT`）保持不变，追块层仅在其上映射出段级失败分类。

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
  重组不得跨越固化位点；
- `BlockFollowerTest`：整段追块补齐对齐、节点坏数据自动切换且整段追完、
  隔离节点冷却恢复后重新参与、重复触发 + 并发手动采集不产生重复记录、
  自动追块按节奏限量推进且重启后续追不重采、全部节点不可信时给出可区分失败结论。

单测日志中会打印追块位点（`追块位点: height=.. 候选节点=.. 依据=[..]`）、节点选择依据
（`节点选择依据: [...]`）、节点切换原因（`节点 x 在高度 h 返回不可信数据(..)，切换节点`）、
可区分失败结论（`追块失败: height=.. 原因=NO_TRUSTWORTHY_DATA .. 节点记录={..}`）
与归一化错误（`写入被拒绝: .. 归一化原因=..`）。

### 启动本地服务并冒烟验证

```bash
mvn spring-boot:run
# 启动后注册 node-a(5ms) / node-b(20ms) / node-c(50ms) 三个模拟节点（同一条模拟链）

curl -X POST 'http://localhost:<port>/api/collect?height=0'   # 采集指定高度
curl -X POST 'http://localhost:<port>/api/collect/latest'     # 采集链头
curl -X POST 'http://localhost:<port>/api/follow/range?from=0&to=500'  # 一次补齐一段高度
curl -X POST 'http://localhost:<port>/api/follow/auto?enabled=true'    # 开启后台自动追块
curl 'http://localhost:<port>/api/follow/status'             # 追块进度与失败分类
curl 'http://localhost:<port>/api/nodes'                      # 节点状态/隔离原因/恢复等待时间
curl 'http://localhost:<port>/api/index/state'                # 链头 / 固化位点 / 已索引高度
curl -X POST 'http://localhost:<port>/api/index/finalize?height=2'
curl -X POST 'http://localhost:<port>/api/collect?height=1'   # -> BELOW_FINALIZED_CURSOR
```

自动追块的节奏可用配置调整（`application.properties` 或环境变量）：

```properties
indexer.follow.enabled=true          # 启动即开启后台自动追块
indexer.follow.interval-ms=5000      # 每轮间隔
indexer.follow.max-batch-per-round=500  # 单轮最多推进的高度数
```

重启进程后再次查询 `/api/index/state`，可验证索引状态从日志中完整复现。
