package com.github.highcumontoa.onchainrpcindexerjava.indexer;

import com.github.highcumontoa.onchainrpcindexerjava.model.AdvanceResult;
import com.github.highcumontoa.onchainrpcindexerjava.model.Block;
import com.github.highcumontoa.onchainrpcindexerjava.model.RejectReason;
import com.github.highcumontoa.onchainrpcindexerjava.model.RejectedFetch;
import com.github.highcumontoa.onchainrpcindexerjava.model.RpcErrorCode;
import com.github.highcumontoa.onchainrpcindexerjava.node.NodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.ChainRpcClient;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.RpcException;
import com.github.highcumontoa.onchainrpcindexerjava.storage.IndexStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 链上数据采集与索引编排。
 *
 * <h3>关键语义</h3>
 * <ul>
 *     <li><b>幂等并发</b>：同高度采集经 {@link HeightMutex} 分段串行 + 存储层按高度去重，
 *         重复投递不产生重复记录；整段推进由全局写锁保护，写入原子落盘，无半写/数据竞争。</li>
 *     <li><b>择优与故障切换</b>：每次选择依据可解释；调用失败归一化记录，连续失败由选择器隔离，
 *         采集时自动切换到次优节点重试。</li>
 *     <li><b>畸形数据不静默丢弃</b>：以可区分的 {@link RejectReason} 写入拒绝留痕并停止推进，
 *         避免错误数据跨过污染后续索引。</li>
 *     <li><b>回退/重组</b>：采信节点切换或链头回退导致哈希分叉时，回滚至共同祖先（不低于固化位点），
 *         随后在新链上重放；回滚与重放结论记入事件账本并随快照持久化，重启可复现。</li>
 *     <li><b>固化位点保护</b>：请求位点低于已固化位点直接抛 {@link StaleCheckpointException}。</li>
 * </ul>
 */
public class ChainIndexer {

    private static final Logger log = LoggerFactory.getLogger(ChainIndexer.class);

    private record FetchOutcome(Block block, String nodeId, RpcErrorCode code,
                                String errorDetail, long latencyMillis,
                                List<RejectedFetch> wireRejections) {
        static FetchOutcome ok(Block block, String nodeId, long latencyMillis) {
            return new FetchOutcome(block, nodeId, RpcErrorCode.OK, "", latencyMillis, List.of());
        }

        static FetchOutcome fail(RpcErrorCode code, String detail,
                                 List<RejectedFetch> wireRejections) {
            return new FetchOutcome(null, null, code, detail, 0L, wireRejections);
        }

        /** 报文层问题：数据已返回但结构畸形/不可解码，应留痕拒绝，而非静默丢弃。 */
        boolean isDataRejection() {
            return code == RpcErrorCode.MALFORMED_RESPONSE || code == RpcErrorCode.DECODE_ERROR;
        }
    }

    /** 归一化 RPC 错误码到数据拒绝原因。 */
    private static RejectReason toRejectReason(RpcErrorCode code) {
        return switch (code) {
            case DECODE_ERROR -> RejectReason.DECODE_ERROR;
            case CONTENT_MISMATCH -> RejectReason.CONTENT_MISMATCH;
            default -> RejectReason.MALFORMED_BLOCK;
        };
    }

    private final List<ChainRpcClient> clients;
    private final NodeSelector selector;
    private final IndexStore store;
    private final BlockValidator validator;
    private final HeightMutex heightMutex;

    /** 整段推进的全局写锁：保证写入原子性与跨高度一致性。 */
    private final Object writeLock = new Object();

    public ChainIndexer(List<ChainRpcClient> clients, NodeSelector selector, IndexStore store) {
        this.clients = clients;
        this.selector = selector;
        this.store = store;
        this.validator = new BlockValidator();
        this.heightMutex = new HeightMutex();
    }

    /**
     * 从指定位点开始向链头推进。
     *
     * @param fromHeight 请求位点（必须 &gt;= 已固化位点；低于当前已索引头时从已索引头继续，不重复写）
     */
    public AdvanceResult advance(long fromHeight) {
        List<Long> indexed = new ArrayList<>();
        List<Long> rolledBack = new ArrayList<>();
        List<RejectedFetch> rejected = new ArrayList<>();
        String nodeBefore = selector.select().orElse("<none>");

        synchronized (writeLock) {
            // 推进前先与采信节点链尖对齐：本地链头高于远端（链头回退）或链尖哈希分叉时执行回滚。
            // 对齐属于系统维护动作，不接受请求位点约束（固化点之前的数据仍不会被回滚）。
            RemoteHead tip = remoteHeadWithFailover();
            if (tip.nodeId != null) {
                alignChainTip(tip, rolledBack);
            } else {
                log.warn("链尖对齐跳过：无法获取链头 {}", tip.errorDetail);
            }

            // 对齐后再做位点保护：请求位点若低于已固化位点必须明确拒绝，而不是从中间继续。
            long finalized = store.finalizedHeight();
            if (fromHeight <= finalized) {
                log.warn("拒绝采集请求：请求位点 {} 低于已固化位点 {}", fromHeight, finalized);
                throw new StaleCheckpointException(fromHeight, finalized);
            }

            long currentHead = store.headHeight();
            if (fromHeight > currentHead + 1) {
                throw new IllegalArgumentException("请求位点 " + fromHeight
                        + " 超出连续索引范围（当前已索引头=" + currentHead
                        + "），不允许跳过高度制造空洞；下一合法位点=" + (currentHead + 1));
            }
            long start = Math.max(fromHeight, currentHead + 1);

            log.info("采集开始 requestFrom={} effectiveFrom={} indexedHead={} finalized={} selectedNode={}",
                    fromHeight, start, currentHead, finalized, nodeBefore);

            long height = start;
            boolean completed = false;
            String stopReason = null;

            outer:
            while (true) {
                // 每个高度单独串行化：与其它并发的同高度请求互斥。
                synchronized (heightMutex.lockFor(height)) {
                    // 先确认链头（含故障切换）；链头不可得则本轮安全停止，不产生半截数据。
                    RemoteHead head = remoteHeadWithFailover();
                    if (head.nodeId == null) {
                        stopReason = "无法获取链头：" + head.errorDetail;
                        log.warn("采集在高度 {} 停止：{}", height, stopReason);
                        break outer;
                    }
                    if (height > head.headHeight) {
                        completed = true;
                        stopReason = "已到达链头 " + head.headHeight;
                        log.info("采集推进到达链头 head={} node={}", head.headHeight, head.nodeId);
                        break outer;
                    }

                    Block existing = store.getBlock(height).orElse(null);
                    if (existing != null) {
                        // 重复投递：已存在且哈希一致即为幂等命中，直接跳过。
                        log.info("采集位点 {} 命中已索引区块(幂等跳过) hash={}", height, existing.hash());
                        height++;
                        continue;
                    }

                    FetchOutcome outcome = fetchWithFailover(height);
                    if (outcome.block() == null) {
                        // 报文畸形/解码失败：所有节点的异常数据都留痕，绝不静默丢弃。
                        if (!outcome.wireRejections().isEmpty()) {
                            for (RejectedFetch wire : outcome.wireRejections()) {
                                rejected.add(wire);
                                store.recordRejection(wire);
                                store.appendEvent("REJECT(wire) height=" + height
                                        + " node=" + wire.nodeId() + " reason=" + wire.reason()
                                        + " detail=" + wire.detail());
                            }
                            stopReason = "所有节点在高度 " + height + " 均返回畸形/不可解码数据，已拒绝并停止";
                        } else {
                            stopReason = "所有健康节点调用失败: " + outcome.code() + " " + outcome.errorDetail();
                        }
                        log.warn("采集在高度 {} 停止：{}", height, stopReason);
                        break outer;
                    }

                    Block block = outcome.block();
                    Block previous = store.getBlock(height - 1).orElse(null);

                    // 与已索引父块哈希对齐：分叉（节点切换/链头回退）触发回滚。
                    if (previous != null && !block.parentHash().equals(previous.hash())) {
                        log.info("检测到链分叉 at height={}：parentHash={} 与已索引父块 hash={} 不一致，触发回滚",
                                height, block.parentHash(), previous.hash());
                        long rollbackTo = findCommonAncestor(outcome.nodeId(), store.headHeight());
                        if (rollbackTo < store.finalizedHeight()) {
                            // 共同祖先低于固化位点：分叉冲突到了不可回滚区域，拒绝采信并停止。
                            stopReason = "分叉共同祖先 " + rollbackTo + " 低于固化位点 "
                                    + store.finalizedHeight() + "，拒绝覆盖已固化数据";
                            RejectedFetch conflict = new RejectedFetch(height, outcome.nodeId(),
                                    RejectReason.CONTENT_MISMATCH, stopReason, System.currentTimeMillis());
                            rejected.add(conflict);
                            store.recordRejection(conflict);
                            log.warn("{}", stopReason);
                            break outer;
                        }
                        List<Long> removed = store.removeBlocksAbove(rollbackTo);
                        rolledBack.addAll(removed);
                        store.appendEvent("ROLLBACK node=" + outcome.nodeId()
                                + " fromHeight=" + (rollbackTo + 1) + " removed=" + removed);
                        log.info("已回滚至共同祖先 keepHeight={} removedHeights={}", rollbackTo, removed);
                        height = rollbackTo + 1;
                        continue;
                    }

                    // 内容校验：畸形/解码失败/内容不一致 → 留痕、拒绝、停止。
                    Optional<BlockValidator.Rejected> invalid =
                            validator.validateWithDetail(block, previous, height);
                    if (invalid.isPresent()) {
                        RejectReason reason = invalid.get().reason();
                        String detail = invalid.get().detail();
                        // 内容篡改同样计入该节点的失败统计，支撑连续失败隔离。
                        if (reason == RejectReason.CONTENT_MISMATCH) {
                            selector.recordOutcome(outcome.nodeId(), RpcErrorCode.CONTENT_MISMATCH,
                                    outcome.latencyMillis());
                        }
                        RejectedFetch record = new RejectedFetch(height, outcome.nodeId(),
                                reason, detail, System.currentTimeMillis());
                        rejected.add(record);
                        store.recordRejection(record);
                        store.appendEvent("REJECT height=" + height + " node=" + outcome.nodeId()
                                + " reason=" + reason + " detail=" + detail);
                        log.warn("区块数据被拒绝 height={} node={} reason={} detail={}",
                                height, outcome.nodeId(), reason, detail);
                        stopReason = "数据被拒绝: " + reason;
                        break outer;
                    }

                    boolean inserted = store.putBlock(block);
                    if (inserted) {
                        indexed.add(height);
                        store.appendEvent("INDEX height=" + height + " node=" + outcome.nodeId()
                                + " hash=" + block.hash());
                        log.info("已索引 height={} node={} hash={} logs={}",
                                height, outcome.nodeId(), block.hash(), block.logs().size());
                    } else {
                        log.info("采集位点 {} 幂等命中（存储层去重）", height);
                    }
                    height++;
                }
            }

            String nodeAfter = selector.select().orElse("<none>");
            AdvanceResult result = new AdvanceResult(
                    fromHeight, start, List.copyOf(indexed), List.copyOf(rolledBack),
                    List.copyOf(rejected), nodeBefore, nodeAfter, completed,
                    stopReason == null ? "未推进" : stopReason);
            store.appendEvent("ADVANCE requestFrom=" + fromHeight + " effectiveFrom=" + start
                    + " indexed=" + indexed + " rolledBack=" + rolledBack
                    + " rejected=" + rejected.size() + " completed=" + completed
                    + " stopReason=" + result.stopReason());
            log.info("采集结束 {}", result);
            return result;
        }
    }

    /** 提升固化位点（单调递增，重启后仍生效）。 */
    public void setFinalized(long finalizedHeight) {
        synchronized (writeLock) {
            store.updateFinalized(finalizedHeight);
            store.appendEvent("FINALIZED height=" + finalizedHeight);
            log.info("固化位点推进至 {}", finalizedHeight);
        }
    }

    // ------------------------------------------------------------------
    // 内部
    // ------------------------------------------------------------------

    /**
     * 按择优顺序逐节点拉取区块：
     * <ul>
     *     <li>传输类错误（超时/不可用）归一化记录并切换下一节点；</li>
     *     <li>报文类错误（畸形/解码失败）为数据问题：仍尝试其余节点，
     *         同时生成可区分原因的拒绝留痕；全部节点都失败后由调用方停止推进。</li>
     * </ul>
     */
    private FetchOutcome fetchWithFailover(long height) {
        RpcErrorCode lastCode = null;
        String lastDetail = "";
        List<String> attempted = new ArrayList<>();
        List<RejectedFetch> wireRejections = new ArrayList<>();
        while (true) {
            Optional<String> chosen = selector.select(attempted);
            if (chosen.isEmpty()) {
                if (attempted.isEmpty()) {
                    return FetchOutcome.fail(RpcErrorCode.UNAVAILABLE,
                            "无任何健康节点可选", wireRejections);
                }
                return FetchOutcome.fail(lastCode,
                        "已尝试节点 " + attempted + "；最后错误: " + lastDetail, wireRejections);
            }
            String nodeId = chosen.get();
            attempted.add(nodeId);
            ChainRpcClient client = findClient(nodeId);
            long startNs = System.nanoTime();
            try {
                Block block = client.getBlockByHeight(height);
                long latency = (System.nanoTime() - startNs) / 1_000_000L;
                selector.recordOutcome(nodeId, RpcErrorCode.OK, latency);
                log.info("拉取区块成功 height={} node={} latencyMs={}", height, nodeId, latency);
                return FetchOutcome.ok(block, nodeId, latency);
            } catch (RpcException e) {
                long latency = (System.nanoTime() - startNs) / 1_000_000L;
                selector.recordOutcome(nodeId, e.code(), latency);
                lastCode = e.code();
                lastDetail = e.getMessage();
                if (e.code() == RpcErrorCode.MALFORMED_RESPONSE || e.code() == RpcErrorCode.DECODE_ERROR) {
                    wireRejections.add(new RejectedFetch(height, nodeId,
                            toRejectReason(e.code()), e.getMessage(), System.currentTimeMillis()));
                    log.warn("节点返回畸形/不可解码数据，记录拒绝并切换节点 height={} node={} code={}",
                            height, nodeId, e.code());
                } else {
                    log.warn("拉取区块失败，尝试切换节点 height={} node={} code={} latencyMs={} detail={}",
                            height, nodeId, e.code(), latency, e.getMessage());
                }
            }
        }
    }

    /**
     * 链尖对齐：本地领先于远端链头（回退）或链尖哈希与远端不同（重组）时，
     * 回滚至共同祖先（不低于固化位点），为随后的重放做准备。
     */
    private void alignChainTip(RemoteHead tip, List<Long> rolledBack) {
        long localHead = store.headHeight();
        boolean needCheck = localHead > tip.headHeight();
        if (!needCheck && localHead >= 0 && localHead <= tip.headHeight()) {
            try {
                Block remoteTipBlock = findClient(tip.nodeId()).getBlockByHeight(localHead);
                needCheck = !remoteTipBlock.hash().equals(store.getBlock(localHead).orElseThrow().hash());
            } catch (RpcException e) {
                selector.recordOutcome(tip.nodeId(), e.code(), 0L);
                log.warn("链尖对齐探测失败，跳过本轮对齐 node={} code={}", tip.nodeId(), e.code());
                return;
            }
        }
        if (!needCheck) {
            return;
        }
        log.info("链尖需要对齐 localHead={} remoteHead={} node={}",
                localHead, tip.headHeight(), tip.nodeId());
        long ancestor = findCommonAncestor(tip.nodeId(), Math.max(localHead, tip.headHeight()));
        if (ancestor < store.finalizedHeight()) {
            // 不允许回滚固化数据；保留现状并留痕，等待更高链或人工介入。
            String detail = "链尖对齐失败：共同祖先 " + ancestor + " 低于固化位点 " + store.finalizedHeight();
            store.recordRejection(new RejectedFetch(Math.max(localHead, tip.headHeight()),
                    tip.nodeId(), RejectReason.CONTENT_MISMATCH, detail, System.currentTimeMillis()));
            store.appendEvent("ALIGN_ABORT " + detail);
            log.warn("{}", detail);
            return;
        }
        if (ancestor == localHead) {
            return;
        }
        List<Long> removed = store.removeBlocksAbove(ancestor);
        rolledBack.addAll(removed);
        store.appendEvent("ROLLBACK(align) node=" + tip.nodeId()
                + " keepHeight=" + ancestor + " removed=" + removed);
        log.info("链尖对齐完成，已回滚 keepHeight={} removedHeights={}", ancestor, removed);
    }

    /**
     * 沿采信节点链与本地链比对，自本地链头向下寻找共同祖先高度。
     */
    private long findCommonAncestor(String nodeId, long localHead) {
        ChainRpcClient client = findClient(nodeId);
        long cursor = localHead;
        while (cursor >= 0) {
            long h = cursor;
            Optional<Block> localBlock = store.getBlock(h);
            if (localBlock.isEmpty()) {
                cursor--;
                continue;
            }
            try {
                Block remoteBlock = client.getBlockByHeight(h);
                if (remoteBlock.hash().equals(localBlock.get().hash())) {
                    return h;
                }
            } catch (RpcException e) {
                selector.recordOutcome(nodeId, e.code(), 0L);
                if (e.code() == RpcErrorCode.NOT_FOUND) {
                    // 采信节点链更短（链头回退），继续向更低高度比对。
                    cursor--;
                    continue;
                }
                log.warn("寻找共同祖先时节点失败 node={} height={} code={}", nodeId, h, e.code());
                return store.finalizedHeight() - 1;
            }
            cursor--;
        }
        return -1L;
    }

    private record RemoteHead(String nodeId, long headHeight, String errorDetail) {
        static RemoteHead unavailable(String detail) {
            return new RemoteHead(null, -1L, detail);
        }
    }

    /** 获取链头，按择优顺序逐个尝试并归一化记录失败，全部失败时返回不可用结论。 */
    private RemoteHead remoteHeadWithFailover() {
        RpcErrorCode lastCode = RpcErrorCode.UNAVAILABLE;
        String lastDetail = "无健康节点";
        List<String> attempted = new ArrayList<>();
        while (true) {
            Optional<String> chosen = selector.select(attempted);
            if (chosen.isEmpty()) {
                return RemoteHead.unavailable(lastCode + " " + lastDetail);
            }
            String nodeId = chosen.get();
            ChainRpcClient client = findClient(nodeId);
            long startNs = System.nanoTime();
            try {
                long head = client.getHeadHeight();
                selector.recordOutcome(nodeId, RpcErrorCode.OK, (System.nanoTime() - startNs) / 1_000_000L);
                return new RemoteHead(nodeId, head, "");
            } catch (RpcException e) {
                selector.recordOutcome(nodeId, e.code(), (System.nanoTime() - startNs) / 1_000_000L);
                lastCode = e.code();
                lastDetail = nodeId + ":" + e.getMessage();
                log.warn("获取链头失败，切换节点 node={} code={}", nodeId, e.code());
            }
        }
    }

    private ChainRpcClient findClient(String nodeId) {
        for (ChainRpcClient client : clients) {
            if (client.id().equals(nodeId)) {
                return client;
            }
        }
        return null;
    }
}
