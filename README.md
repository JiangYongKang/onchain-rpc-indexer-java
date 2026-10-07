# onchain-rpc-indexer-java

本地多节点链上数据采集与索引组件。通过可本地启动的模拟节点（`MockChainNode`）提供区块与日志数据，
在节点故障、链头回退与重复采集场景下保证索引结果一致且可解释。

## 组件结构

```
node/       ChainNode 抽象 + MockChainNode（本地模拟节点：可配置延迟、抖动失败、宕机、重组、畸形数据注入）
selection/  NodeSelector：节点探测、择优、故障隔离与恢复，选择依据可查询
store/      JournaledIndexStore：原子写入、幂等、重组回滚、固化位点、追加日志持久化
ingest/     IngestionService：选节点 -> 取块 -> 校验 -> 原子写入 -> 重组对齐
catchup/    CatchupService + CatchupScheduler：一段高度补齐与后台自动追块，进度与失败分类可查询
api/        IndexController：REST 查询与触发入口
```

## 节点择优与隔离恢复规则

- **择优打分**：按 `(是否半开, 平均延迟 EMA, 历史失败率)` 升序打分，最优者被选中。
  每次选择的完整依据（各节点隔离状态、剩余冷却时间、延迟、失败率）通过
  `SelectionReport.rationale` 返回，并可由 `GET /api/nodes` 查询。
- **抖动不误判**：只有**连续**失败达到阈值（默认 3 次）才隔离；任何一次成功都会清零连续失败计数，
  因此短暂抖动（失败-成功交替）不会触发隔离。
- **隔离与恢复**：被隔离节点在冷却期（默认 5s）内不参与选择；冷却期满后进入**半开**状态，
  下一次选择时**半开节点优先获得一次真实探测**——探测成功则解除隔离、清零计数并重新参与采集；
  探测失败则重新隔离并重新计时。节点当前是 `AVAILABLE` / `ISOLATED` / `HALF_OPEN`、
  距恢复探测还剩多少毫秒（`recoveryRemainingMs`）、最近一次失败原因（`lastError`），
  都可由 `GET /api/nodes` 查询。
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

## 自动追块

在单高度采集之上提供"一次补齐一段高度"与"后台按节奏追链头"两种用法。

### 触发方式与节奏配置

| 入口 | 说明 |
|---|---|
| `POST /api/catchup?from=&to=` | 一次性追一段高度 `[from, to]`（含两端），同步返回结果 |
| `POST /api/catchup/head?batchSize=` | 手动触发一轮向链头追块（单轮最多 `batchSize` 个高度） |
| `POST /api/catchup/auto?enabled=` | 运行时开关后台自动追块 |
| `POST /api/catchup/auto/interval?ms=` | 运行时调整追块节奏（下一轮生效） |
| `GET /api/catchup/status` | 追块进度与最近一轮统计 |

后台自动追块由 `CatchupScheduler` 驱动，配置项（`application.properties`）：

```properties
indexer.catchup.auto-enabled=false   # 是否开启后台自动追块
indexer.catchup.interval-ms=5000     # 追块节奏：每轮间隔
indexer.catchup.batch-size=500       # 单轮最多追的高度数
```

每轮自动追块：向择优节点询问链头 -> 从 `本地链头 + 1` 追到 `min(链头, 起点 + batchSize - 1)`。
已对齐时轮次空跑成功，不重复写入。

### 追块语义

- **对齐或明确失败**：一段追块结束后，本地链头要么与目标对齐（`success=true`），
  要么返回可区分的失败结论（`success=false` + `failureReasons` 分类计数 + 可解释 `error`），
  不存在"写了一半却报成功"。
- **不缺块、不重复**：起点取 `max(请求起点, 本地链头 + 1)`——已索引的高度计入
  `duplicates` 幂等跳过、不发重复写入；未追的高度逐个采集、不跳过。
  存储层保证连续追加，中间不会出现空洞。
- **重复触发只算一次**：同一时刻只允许一轮追块在执行；追块进行中再次触发返回
  "已有追块任务进行中，本次触发幂等忽略"。与手动采集（`/api/collect`）并发时，
  由存储层同高度同哈希幂等保证只算一次（`DUPLICATE_IGNORED`）。
- **重启续追**：索引状态由追加日志持久化，进程重启后链头自动回放恢复，
  下一轮追块从 `本地链头 + 1` 继续——已索引的不重复处理，未追的不跳过。

### 追块期间的节点容错与恢复

- 追块使用容错模式采集：某节点**取不到块、返回畸形数据或数据解不出来**时，
  该节点被记录失败（计入隔离判定），自动切换到次优健康节点继续当前高度，
  该高度不会被当成已成功。
- 只有**所有健康节点都给不出可信数据**时整段才停止，失败记录包含高度、
  分类原因（`MALFORMED_BLOCK` / `DECODE_FAILURE` / `NO_HEALTHY_NODE` / `NODE_CALL_FAILURE`）
  与详情，已写入的高度保持完整，可从失败高度再次触发续追。
- 被隔离节点冷却期满后进入半开并**优先获得一次真实探测**，探测成功即恢复参与，
  不会一直挂着用不上。

### 追块进度查询

`GET /api/catchup/status` 返回：

| 字段 | 含义 |
|---|---|
| `autoEnabled` / `intervalMs` | 自动追块开关与当前节奏 |
| `running` | 当前是否有一轮追块在执行 |
| `currentHeight` / `targetHeight` | 本轮正在追的高度 / 目标高度（未在追为 -1） |
| `localHead` / `lag` | 本地链头 / 离最近已知链头还差多少 |
| `lastAppended` / `lastDuplicates` / `lastFailed` | 最近一轮写入 / 幂等跳过 / 失败块数 |
| `lastFailureReasons` | 最近一轮失败原因分类计数 |

### 与原有语义的兼容范围

- 单高度采集（`/api/collect`、`/api/collect/latest`）行为不变：数据类拒绝仍直接归一化返回、
  不切换节点；节点容错切换只在追块路径（`CatchupService`）启用。
- 固化位点语义不变：追块起点低于固化位点的部分按已索引跳过；
  重组回滚触及固化位点仍明确拒绝。
- 链头回退/重组回滚语义不变：追块逐高度复用单高度采集的重组对齐逻辑，
  分叉时同样回滚到分叉点以下再对齐。
- 节点状态查询（`/api/nodes`）新增 `state` 与 `recoveryRemainingMs` 字段，
  原有字段含义不变。

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
- `CatchupServiceTest`：一次追块补齐一千个高度并对齐、个别节点坏数据时整段仍追完、
  被隔离节点冷却期满后真实恢复参与、重复触发 + 并发手动采集不产生重复记录、
  重启后从上次位置续追、全部节点不可信时整段停止且失败原因可区分、
  批量上限与进度可见性。

单测日志中会打印节点选择依据（`节点选择依据: [...]`）、采集位点
（`采集位点: height=.. node=.. 本地链头=.. 固化位点=..`）、追块位点
（`自动追块一轮: 区间=[.., ..] 远端链头=.. 本地链头=..`、`追块一轮完成: .. 写入=.. 幂等跳过=.. 失败=..`）、
可区分失败原因（`追块整段停止: height=.. 失败分类=..`）与归一化错误
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

# 自动追块
curl -X POST 'http://localhost:<port>/api/catchup?from=0&to=50'   # 一次补齐一段高度
curl -X POST 'http://localhost:<port>/api/catchup/head'           # 手动追一轮到链头
curl -X POST 'http://localhost:<port>/api/catchup/auto?enabled=true'
curl -X POST 'http://localhost:<port>/api/catchup/auto/interval?ms=2000'
curl 'http://localhost:<port>/api/catchup/status'                 # 进度与最近一轮统计
```

重启进程后再次查询 `/api/index/state`，可验证索引状态从日志中完整复现；
再触发 `/api/catchup/head` 可验证从上次追到的位置继续、已索引高度幂等跳过。
