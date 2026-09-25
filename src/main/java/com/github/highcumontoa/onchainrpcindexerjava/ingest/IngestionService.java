package com.github.highcumontoa.onchainrpcindexerjava.ingest;

import com.github.highcumontoa.onchainrpcindexerjava.model.BlockData;
import com.github.highcumontoa.onchainrpcindexerjava.node.NodeCallException;
import com.github.highcumontoa.onchainrpcindexerjava.selection.NodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.store.ApplyResult;
import com.github.highcumontoa.onchainrpcindexerjava.store.ApplyOutcome;
import com.github.highcumontoa.onchainrpcindexerjava.store.IndexStore;
import com.github.highcumontoa.onchainrpcindexerjava.store.RejectionException;
import com.github.highcumontoa.onchainrpcindexerjava.store.RejectionReason;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 采集服务：择优选节点 -> 取块 -> 校验并原子写入。
 * 节点失败时记录并切换；数据类拒绝（畸形/解码失败/低于固化位点）不切换节点，直接归一化返回。
 */
@Service
public class IngestionService {

    private static final Logger log = LoggerFactory.getLogger(IngestionService.class);
    /** 单次采集内重组迭代上限，防止异常分叉导致死循环。 */
    private static final int MAX_REORG_ITERATIONS = 64;

    private final NodeSelector selector;
    private final IndexStore store;

    public IngestionService(NodeSelector selector, IndexStore store) {
        this.selector = selector;
        this.store = store;
    }

    /** 采集指定高度。 */
    public IngestResult collectOnce(long height) {
        if (height < 0) {
            return IngestResult.rejected(height, RejectionReason.INVALID_HEIGHT, "高度为负: " + height);
        }
        if (height <= store.finalizedHeight()) {
            log.info("采集被拒绝: height={} 原因={} 固化位点={}", height,
                    RejectionReason.BELOW_FINALIZED_CURSOR, store.finalizedHeight());
            return IngestResult.rejected(height, RejectionReason.BELOW_FINALIZED_CURSOR,
                    "高度 " + height + " 低于或触及固化位点 " + store.finalizedHeight());
        }

        int maxAttempts = Math.max(1, selector.status().size());
        Set<String> failedNodes = new HashSet<>();
        String lastError = null;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            NodeSelector.Selection selection = selector.selectExcluding(failedNodes);
            if (selection == null) {
                log.warn("采集失败: 无可用节点 height={}", height);
                return IngestResult.failed(height, "无可用节点（全部隔离或本次均已失败）");
            }
            String nodeId = selection.node().id();
            log.info("节点选择依据: {}", selection.report().rationale());
            try {
                long t0 = System.nanoTime();
                BlockData block = selection.node().getBlock(height);
                selector.recordSuccess(nodeId, (System.nanoTime() - t0) / 1_000_000);
                if (block == null) {
                    lastError = "节点 " + nodeId + " 无高度 " + height + " 的区块";
                    log.warn("采集失败: {}", lastError);
                    continue;
                }
                return applyWithReorg(block, nodeId);
            } catch (NodeCallException e) {
                selector.recordFailure(nodeId, e.getMessage());
                failedNodes.add(nodeId);
                lastError = "节点 " + nodeId + " 调用失败: " + e.getMessage();
                log.warn("节点失败，尝试切换: {}", lastError);
            }
        }
        return IngestResult.failed(height, lastError == null ? "采集失败" : lastError);
    }

    /** 采集当前链头（先向择优节点询问链头高度）。 */
    public IngestResult collectLatest() {
        NodeSelector.Selection selection = selector.select();
        if (selection == null) {
            return IngestResult.failed(null, "无可用节点（全部隔离）");
        }
        try {
            long t0 = System.nanoTime();
            long head = selection.node().latestHeight();
            selector.recordSuccess(selection.node().id(), (System.nanoTime() - t0) / 1_000_000);
            return collectOnce(head);
        } catch (NodeCallException e) {
            selector.recordFailure(selection.node().id(), e.getMessage());
            return IngestResult.failed(null, "获取链头失败: " + e.getMessage());
        }
    }

    /**
     * 写入并处理重组：先回滚到分叉点以下对齐，再继续向前采集直到目标高度，
     * 保证单次采集结束后本地链头到达请求高度或明确失败。
     */
    private IngestResult applyWithReorg(BlockData block, String nodeId) {
        final long target = block.height();
        long cursor = target;
        BlockData current = block;
        boolean reorgHappened = false;
        for (int i = 0; i < MAX_REORG_ITERATIONS; i++) {
            log.info("采集位点: height={} node={} 本地链头={} 固化位点={}",
                    cursor, nodeId, store.headHeight(), store.finalizedHeight());
            ApplyResult result;
            try {
                result = store.apply(current);
            } catch (RejectionException e) {
                log.info("写入被拒绝: height={} 归一化原因={} 详情={}", cursor, e.reason(), e.getMessage());
                return IngestResult.rejected(cursor, e.reason(), e.getMessage());
            }
            switch (result.outcome()) {
                case APPENDED, DUPLICATE_IGNORED -> {
                    if (result.outcome() == ApplyOutcome.DUPLICATE_IGNORED) {
                        log.info("重复投递幂等忽略: height={} hash={}", cursor, current.hash());
                    }
                    if (cursor >= target) {
                        String outcome = reorgHappened
                                ? ApplyOutcome.REORG_ROLLED_BACK.name()
                                : (cursor == target && result.outcome() == ApplyOutcome.DUPLICATE_IGNORED
                                        ? ApplyOutcome.DUPLICATE_IGNORED.name()
                                        : ApplyOutcome.APPENDED.name());
                        return IngestResult.ok(target, nodeId, outcome);
                    }
                    cursor++;
                    current = fetchSameNode(nodeId, cursor);
                    if (current == null) {
                        return IngestResult.failed(cursor, "重组对齐后前推时节点 " + nodeId + " 无高度 " + cursor);
                    }
                }
                case REORG_ROLLED_BACK -> {
                    reorgHappened = true;
                    if (result.appended() && cursor >= target) {
                        return IngestResult.ok(target, nodeId, ApplyOutcome.REORG_ROLLED_BACK.name());
                    }
                    long next = result.appended() ? cursor + 1 : result.rolledBackTo() + 1;
                    log.info("重组回滚: rolledBackTo={} 从高度 {} 重新采集对齐", result.rolledBackTo(), next);
                    cursor = next;
                    current = fetchSameNode(nodeId, cursor);
                    if (current == null) {
                        return IngestResult.failed(cursor, "重组对齐时节点 " + nodeId + " 无高度 " + cursor);
                    }
                }
            }
        }
        return IngestResult.failed(cursor, "重组迭代超过上限 " + MAX_REORG_ITERATIONS);
    }

    /** 重组对齐期间从原节点取块，保证来自同一条链视图。 */
    private BlockData fetchSameNode(String nodeId, long height) {
        var node = selector.node(nodeId);
        if (node.isEmpty()) {
            return null;
        }
        try {
            return node.get().getBlock(height);
        } catch (NodeCallException e) {
            selector.recordFailure(nodeId, e.getMessage());
            return null;
        }
    }
}
