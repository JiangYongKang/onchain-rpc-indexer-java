package com.github.highcumontoa.onchainrpcindexerjava.catchup;

import com.github.highcumontoa.onchainrpcindexerjava.ingest.IngestResult;
import com.github.highcumontoa.onchainrpcindexerjava.ingest.IngestionService;
import com.github.highcumontoa.onchainrpcindexerjava.node.NodeCallException;
import com.github.highcumontoa.onchainrpcindexerjava.selection.NodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.store.ApplyOutcome;
import com.github.highcumontoa.onchainrpcindexerjava.store.IndexStore;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 追块服务：一次性把一段高度补齐，或被调度器按节奏触发向链头追。
 * - 同一时刻只允许一轮追块：重复触发幂等忽略（可区分），不重复写入；
 * - 与手动采集并发时由存储层幂等保证只算一次；
 * - 起点取 max(请求起点, 本地链头+1)：已索引的高度不重复处理，未追的不跳过；
 * - 某高度所有健康节点都给不出可信数据时整段停止，并留下可解释的失败记录。
 */
@Service
public class CatchupService {

    private static final Logger log = LoggerFactory.getLogger(CatchupService.class);

    private final IngestionService ingestion;
    private final NodeSelector selector;
    private final IndexStore store;

    /** 追块互斥：同一时刻只允许一轮。 */
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile long currentHeight = -1;
    private volatile long targetHeight = -1;
    /** 最近一次观察到的远端链头（用于计算差距）。 */
    private volatile long lastKnownChainHead = -1;
    /** 最近一轮追块的结果（含统计），无则为 null。 */
    private volatile CatchupResult lastRound;

    public CatchupService(IngestionService ingestion, NodeSelector selector, IndexStore store) {
        this.ingestion = ingestion;
        this.selector = selector;
        this.store = store;
    }

    /** 追一段高度 [fromHeight, toHeight]（含两端）。 */
    public CatchupResult catchUpRange(long fromHeight, long toHeight) {
        if (fromHeight < 0 || toHeight < fromHeight) {
            return new CatchupResult(false, fromHeight, toHeight, store.headHeight(),
                    0, 0, 0, Map.of(), "非法追块区间: [" + fromHeight + ", " + toHeight + "]");
        }
        if (!running.compareAndSet(false, true)) {
            log.info("追块触发被幂等忽略：已有追块任务进行中 height={}->{}", currentHeight, targetHeight);
            return new CatchupResult(false, fromHeight, toHeight, store.headHeight(),
                    0, 0, 0, Map.of(), "已有追块任务进行中，本次触发幂等忽略");
        }
        try {
            return doCatchUp(fromHeight, toHeight);
        } finally {
            currentHeight = -1;
            targetHeight = -1;
            running.set(false);
        }
    }

    /** 自动追块一轮：从本地链头下一高度追到远端链头（受批量上限约束）。 */
    public CatchupResult catchUpToHead(int batchSize) {
        NodeSelector.Selection selection = selector.select();
        if (selection == null) {
            CatchupResult r = new CatchupResult(false, store.headHeight() + 1, -1, store.headHeight(),
                    0, 0, 0, Map.of("NO_HEALTHY_NODE", 1L), "无可用节点，无法获取链头");
            lastRound = r;
            log.warn("自动追块失败: {}", r.error());
            return r;
        }
        long chainHead;
        try {
            long t0 = System.nanoTime();
            chainHead = selection.node().latestHeight();
            selector.recordSuccess(selection.node().id(), (System.nanoTime() - t0) / 1_000_000);
        } catch (NodeCallException e) {
            selector.recordFailure(selection.node().id(), e.getMessage());
            CatchupResult r = new CatchupResult(false, store.headHeight() + 1, -1, store.headHeight(),
                    0, 0, 0, Map.of("NODE_CALL_FAILURE", 1L), "获取链头失败: " + e.getMessage());
            lastRound = r;
            log.warn("自动追块失败: {}", r.error());
            return r;
        }
        lastKnownChainHead = chainHead;
        long from = store.headHeight() + 1;
        if (from > chainHead) {
            CatchupResult r = new CatchupResult(true, from, chainHead, store.headHeight(),
                    0, 0, 0, Map.of(), null);
            lastRound = r;
            log.info("自动追块：本地链头 {} 已对齐远端链头 {}，无需追块", store.headHeight(), chainHead);
            return r;
        }
        long to = Math.min(chainHead, from + Math.max(1, batchSize) - 1);
        log.info("自动追块一轮: 区间=[{}, {}] 远端链头={} 本地链头={}", from, to, chainHead, store.headHeight());
        return catchUpRange(from, to);
    }

    /** 当前追块进度视图。 */
    public CatchupStatus status() {
        CatchupResult last = lastRound;
        long head = store.headHeight();
        long lag = lastKnownChainHead >= 0 ? Math.max(0, lastKnownChainHead - head) : -1;
        return new CatchupStatus(
                false, // autoEnabled 由调度器填充
                0,
                running.get(),
                currentHeight,
                targetHeight,
                head,
                lag,
                last == null ? 0 : last.appended(),
                last == null ? 0 : last.duplicates(),
                last == null ? 0 : last.failed(),
                last == null ? Map.of() : last.failureReasons());
    }

    /** 带 auto 信息的进度视图（供查询入口使用）。 */
    public CatchupStatus status(boolean autoEnabled, long intervalMs) {
        CatchupStatus base = status();
        return new CatchupStatus(autoEnabled, intervalMs, base.running(), base.currentHeight(),
                base.targetHeight(), base.localHead(), base.lag(), base.lastAppended(),
                base.lastDuplicates(), base.lastFailed(), base.lastFailureReasons());
    }

    // ---- 内部 ----

    private CatchupResult doCatchUp(long fromHeight, long toHeight) {
        targetHeight = toHeight;
        // 已索引的高度不重复处理：直接从本地链头下一高度开始真正采集
        long start = Math.max(fromHeight, store.headHeight() + 1);
        long duplicates = start - fromHeight;
        long appended = 0;
        long failed = 0;
        Map<String, Long> failureReasons = new TreeMap<>();
        if (duplicates > 0) {
            log.info("追块位点: 区间=[{}, {}] 已索引 [{}..{}] 幂等跳过，从 {} 开始采集",
                    fromHeight, toHeight, fromHeight, start - 1, start);
        }
        for (long h = start; h <= toHeight; h++) {
            currentHeight = h;
            IngestResult r = ingestion.collectOnceWithNodeFailover(h);
            if (r.success()) {
                if (ApplyOutcome.DUPLICATE_IGNORED.name().equals(r.outcome())) {
                    duplicates++;
                } else {
                    appended++;
                }
                continue;
            }
            // 所有健康节点都给不出可信数据：整段停止，留下可解释记录
            failed++;
            String category = classify(r);
            failureReasons.merge(category, 1L, Long::sum);
            CatchupResult result = new CatchupResult(false, fromHeight, toHeight, store.headHeight(),
                    appended, duplicates, failed, copyOf(failureReasons),
                    "高度 " + h + " 追块失败(" + category + "): " + r.error());
            lastRound = result;
            log.warn("追块整段停止: height={} 失败分类={} 原因={} 已写入={} 跳过={}",
                    h, category, r.error(), appended, duplicates);
            return result;
        }
        boolean aligned = store.headHeight() >= toHeight;
        CatchupResult result = new CatchupResult(aligned, fromHeight, toHeight, store.headHeight(),
                appended, duplicates, failed, copyOf(failureReasons),
                aligned ? null : "追块结束但本地链头 " + store.headHeight() + " 未对齐目标 " + toHeight);
        lastRound = result;
        log.info("追块一轮完成: 区间=[{}, {}] 写入={} 幂等跳过={} 失败={} 本地链头={} 对齐={}",
                fromHeight, toHeight, appended, duplicates, failed, store.headHeight(), aligned);
        return result;
    }

    /** 失败原因分类：数据类拒绝用归一化原因，节点问题单独分类，均可区分。 */
    private static String classify(IngestResult r) {
        if (r.rejectionReason() != null) {
            return r.rejectionReason().name();
        }
        if (r.error() != null && r.error().contains("无可用节点")) {
            return "NO_HEALTHY_NODE";
        }
        return "NODE_CALL_FAILURE";
    }

    private static Map<String, Long> copyOf(Map<String, Long> map) {
        return Map.copyOf(new LinkedHashMap<>(map));
    }
}
