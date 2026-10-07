package com.github.highcumontoa.onchainrpcindexerjava.follow;

import com.github.highcumontoa.onchainrpcindexerjava.model.BlockData;
import com.github.highcumontoa.onchainrpcindexerjava.node.NodeCallException;
import com.github.highcumontoa.onchainrpcindexerjava.selection.NodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.store.ApplyResult;
import com.github.highcumontoa.onchainrpcindexerjava.store.IndexStore;
import com.github.highcumontoa.onchainrpcindexerjava.store.RejectionException;
import com.github.highcumontoa.onchainrpcindexerjava.store.RejectionReason;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 自动追块器：
 * - followRange(from, to)：一次性把一段高度连续补齐，结束要么链头对齐目标，要么给出可区分失败结论；
 * - tick()：按可配置节奏（indexer.follow.interval-ms）在后台向链头追，可用开关启停；
 * - 同一时刻只允许一个追块任务，重复触发直接忽略（只算一次）；与手动采集并发时靠存储层幂等去重；
 * - 追块起点永远取 store.headHeight()+1：重启后从日志回放的链头继续，已索引的不重采、不缺块；
 * - 单个高度上某节点数据不可信（取不到/畸形/解不出）时自动换其他健康节点，
 *   全部可用节点都给不出可信数据才停止并记录可区分原因。
 */
@Component
public class BlockFollower {

    private static final Logger log = LoggerFactory.getLogger(BlockFollower.class);
    /** 单轮迭代上限（含重组回退重采），防止异常分叉导致死循环。 */
    private static final int MAX_ITERATIONS_PER_ROUND = 10_000;

    private final NodeSelector selector;
    private final IndexStore store;
    private final FollowerProperties properties;

    /** 同一时刻只允许一个追块任务。 */
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile boolean autoEnabled;
    private volatile long targetHeight = -1;
    private volatile String lastError;

    private volatile long lastRoundWritten;
    private volatile long lastRoundSkipped;
    private volatile long lastRoundFailed;
    private volatile Map<String, Long> failureReasons = Map.of();
    private volatile long lastRoundAtEpochMs;
    private long totalWritten;
    private long totalSkipped;
    private long totalFailed;

    public BlockFollower(NodeSelector selector, IndexStore store, FollowerProperties properties) {
        this.selector = selector;
        this.store = store;
        this.properties = properties;
        this.autoEnabled = properties.isEnabled();
    }

    /** 一次性把 [from, to] 这段高度补齐。已有追块在跑时本次触发被忽略。 */
    public FollowRangeResult followRange(long from, long to) {
        if (from < 0 || to < from) {
            return new FollowRangeResult(false, false, from, to, store.headHeight(), 0, 0,
                    FollowFailureReason.INVALID_HEIGHT, "非法区间: [" + from + ", " + to + "]", Map.of());
        }
        if (!running.compareAndSet(false, true)) {
            log.info("追块触发被忽略: 已有任务在执行 from={} to={}", from, to);
            return FollowRangeResult.notStarted(from, to);
        }
        try {
            return doFollow(from, to);
        } finally {
            running.set(false);
        }
    }

    /** 自动追块调度入口：节奏由 indexer.follow.interval-ms 控制。 */
    @Scheduled(fixedDelayString = "${indexer.follow.interval-ms:5000}",
            initialDelayString = "${indexer.follow.interval-ms:5000}")
    public void scheduledTick() {
        tick();
    }

    /** 自动追块一轮：询问链头 -> 从本地链头下一高度追到目标（单轮限量）。 */
    public void tick() {
        if (!autoEnabled) {
            return;
        }
        if (!running.compareAndSet(false, true)) {
            log.info("自动追块本轮跳过: 已有任务在执行");
            return;
        }
        try {
            NodeSelector.Selection selection = selector.select();
            if (selection == null) {
                recordFailureRound(FollowFailureReason.NO_AVAILABLE_NODE, "无可用节点（全部隔离）");
                return;
            }
            long head;
            try {
                long t0 = System.nanoTime();
                head = selection.node().latestHeight();
                selector.recordSuccess(selection.node().id(), (System.nanoTime() - t0) / 1_000_000);
            } catch (NodeCallException e) {
                selector.recordFailure(selection.node().id(), e.getMessage());
                recordFailureRound(FollowFailureReason.NO_AVAILABLE_NODE,
                        "获取链头失败: " + e.getMessage());
                return;
            }
            targetHeight = head;
            long from = store.headHeight() + 1;
            long to = Math.min(head, from + properties.getMaxBatchPerRound() - 1);
            if (to < from) {
                log.info("自动追块: 已对齐链头 head={} target={}", store.headHeight(), head);
                recordRound(0, 0, 0, Map.of(), null);
                return;
            }
            log.info("自动追块一轮: from={} to={} target={} 本地链头={}", from, to, head, store.headHeight());
            doFollow(from, to);
        } finally {
            running.set(false);
        }
    }

    /** 运行时开关自动追块。 */
    public void setAutoEnabled(boolean enabled) {
        this.autoEnabled = enabled;
        log.info("自动追块开关: {}", enabled ? "开启" : "关闭");
    }

    public boolean isAutoEnabled() {
        return autoEnabled;
    }

    /** 当前追块状态视图。 */
    public FollowStatus status() {
        long head = store.headHeight();
        long target = targetHeight;
        return new FollowStatus(
                autoEnabled,
                running.get(),
                head,
                store.finalizedHeight(),
                target,
                target >= 0 ? Math.max(0, target - head) : -1,
                lastRoundWritten,
                lastRoundSkipped,
                lastRoundFailed,
                failureReasons,
                totalWritten,
                totalSkipped,
                totalFailed,
                lastError,
                lastRoundAtEpochMs);
    }

    // ---- 核心：连续推进 [from, to]，逐高度容错 ----

    private FollowRangeResult doFollow(long from, long to) {
        long written = 0;
        long skipped = 0;
        Map<String, List<String>> nodeErrors = new LinkedHashMap<>();
        // 起点对齐到本地链头下一高度：已索引的计入跳过，绝不重采、不缺块
        long h = Math.max(from, store.headHeight() + 1);
        skipped += Math.max(0, h - from);
        log.info("追块开始: from={} to={} 有效起点={} 本地链头={} 固化位点={}",
                from, to, h, store.headHeight(), store.finalizedHeight());

        int iterations = 0;
        while (h <= to) {
            if (++iterations > MAX_ITERATIONS_PER_ROUND) {
                return failRound(from, to, written, skipped, h, nodeErrors,
                        FollowFailureReason.REORG_LIMIT_EXCEEDED, "重组迭代超过上限");
            }
            Set<String> tried = new HashSet<>();
            boolean sawUntrustedData = false;
            while (true) {
                NodeSelector.Selection selection = selector.selectExcluding(tried);
                if (selection == null) {
                    FollowFailureReason reason = sawUntrustedData
                            ? FollowFailureReason.NO_TRUSTWORTHY_DATA
                            : FollowFailureReason.NO_AVAILABLE_NODE;
                    return failRound(from, to, written, skipped, h, nodeErrors, reason,
                            "高度 " + h + " 上所有可用节点都给不出可信数据");
                }
                String nodeId = selection.node().id();
                log.info("追块位点: height={} 候选节点={} 依据={}", h, nodeId, selection.report().rationale());
                BlockData block;
                long latencyMs;
                try {
                    long t0 = System.nanoTime();
                    block = selection.node().getBlock(h);
                    latencyMs = (System.nanoTime() - t0) / 1_000_000;
                } catch (NodeCallException e) {
                    selector.recordFailure(nodeId, e.getMessage());
                    tried.add(nodeId);
                    addNodeError(nodeErrors, nodeId, "调用失败: " + e.getMessage());
                    log.warn("节点 {} 在高度 {} 调用失败，切换节点: {}", nodeId, h, e.getMessage());
                    continue;
                }
                if (block == null) {
                    tried.add(nodeId);
                    sawUntrustedData = true;
                    addNodeError(nodeErrors, nodeId, "无高度 " + h + " 的区块");
                    log.warn("节点 {} 无高度 {} 的区块，切换节点", nodeId, h);
                    continue;
                }
                ApplyResult result;
                try {
                    result = store.apply(block);
                } catch (RejectionException e) {
                    if (e.reason() == RejectionReason.MALFORMED_BLOCK
                            || e.reason() == RejectionReason.DECODE_FAILURE) {
                        // 数据不可信：记节点失败并换节点，不当成成功也不整段放弃
                        selector.recordFailure(nodeId, "不可信数据: " + e.reason());
                        tried.add(nodeId);
                        sawUntrustedData = true;
                        addNodeError(nodeErrors, nodeId, e.reason() + ": " + e.getMessage());
                        log.warn("节点 {} 在高度 {} 返回不可信数据({})，切换节点", nodeId, h, e.reason());
                        continue;
                    }
                    // 固化位点/高度类拒绝：换节点无意义，整段停止并给出可区分结论
                    return failRound(from, to, written, skipped, h, nodeErrors,
                            mapReason(e.reason()), e.getMessage());
                }
                // 数据被存储接受（含幂等忽略与重组对齐）才记节点成功
                selector.recordSuccess(nodeId, latencyMs);
                switch (result.outcome()) {
                    case APPENDED -> {
                        written++;
                        h = store.headHeight() + 1;
                    }
                    case DUPLICATE_IGNORED -> {
                        skipped++;
                        log.info("追块跳过重复: height={} hash={}", result.height(), result.hash());
                        h = store.headHeight() + 1;
                    }
                    case REORG_ROLLED_BACK -> {
                        log.info("追块中重组回滚: rolledBackTo={} 从高度 {} 继续",
                                result.rolledBackTo(), store.headHeight() + 1);
                        h = store.headHeight() + 1;
                    }
                }
                break;
            }
        }
        FollowRangeResult ok = new FollowRangeResult(true, true, from, to, store.headHeight(),
                written, skipped, null, null, Map.copyOf(nodeErrors));
        recordRound(written, skipped, 0, Map.of(), null);
        log.info("追块完成: [{}..{}] 写入={} 跳过={} 链头={}", from, to, written, skipped, store.headHeight());
        return ok;
    }

    private FollowRangeResult failRound(long from, long to, long written, long skipped, long height,
                                        Map<String, List<String>> nodeErrors,
                                        FollowFailureReason reason, String error) {
        log.warn("追块失败: height={} 原因={} 详情={} 节点记录={}", height, reason, error, nodeErrors);
        recordRound(written, skipped, 1, Map.of(reason.name(), 1L), reason + ": " + error);
        return new FollowRangeResult(true, false, from, to, store.headHeight(), written, skipped,
                reason, error, Map.copyOf(nodeErrors));
    }

    private void recordFailureRound(FollowFailureReason reason, String error) {
        log.warn("自动追块本轮失败: 原因={} 详情={}", reason, error);
        recordRound(0, 0, 1, Map.of(reason.name(), 1L), reason + ": " + error);
    }

    private synchronized void recordRound(long written, long skipped, long failed,
                                          Map<String, Long> reasons, String error) {
        lastRoundWritten = written;
        lastRoundSkipped = skipped;
        lastRoundFailed = failed;
        failureReasons = reasons;
        lastRoundAtEpochMs = System.currentTimeMillis();
        totalWritten += written;
        totalSkipped += skipped;
        totalFailed += failed;
        lastError = error;
    }

    private static FollowFailureReason mapReason(RejectionReason reason) {
        return switch (reason) {
            case BELOW_FINALIZED_CURSOR -> FollowFailureReason.BELOW_FINALIZED_CURSOR;
            default -> FollowFailureReason.INVALID_HEIGHT;
        };
    }

    private static void addNodeError(Map<String, List<String>> nodeErrors, String nodeId, String error) {
        nodeErrors.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(error);
    }
}
