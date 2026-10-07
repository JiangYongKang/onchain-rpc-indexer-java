package com.github.highcumontoa.onchainrpcindexerjava.node;

import com.github.highcumontoa.onchainrpcindexerjava.model.BlockData;
import com.github.highcumontoa.onchainrpcindexerjava.model.LogEntry;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 本地模拟节点：确定性生成区块与日志，可配置延迟、抖动失败、
 * 宕机、链头回退与重组（fork），以及注入畸形/不可解码数据。
 */
public class MockChainNode implements ChainNode {

    private final String id;
    /** 链种子：同一批节点共享同一种子表示同一条链。 */
    private final String chainSeed;
    private volatile long head;
    private volatile long latencyMs;
    private volatile boolean down;
    /** 剩余多少次调用强制失败（模拟短暂抖动）。 */
    private final AtomicInteger failNextCalls = new AtomicInteger();
    /** 高度 -> fork 代际，用于重组后生成不同的哈希。 */
    private final Map<Long, Integer> forkSegments = new ConcurrentHashMap<>();
    private final AtomicInteger forkCounter = new AtomicInteger();
    /** 注入畸形数据的高度。 */
    private final Set<Long> malformedHeights = new CopyOnWriteArraySet<>();
    /** 注入解码失败数据的高度。 */
    private final Set<Long> undecodableHeights = new CopyOnWriteArraySet<>();

    public MockChainNode(String id, String chainSeed, long initialHead) {
        this.id = id;
        this.chainSeed = chainSeed;
        this.head = initialHead;
        this.forkSegments.put(0L, 0);
    }

    @Override
    public String id() {
        return id;
    }

    // ---- 行为配置 ----

    public void setLatencyMs(long latencyMs) {
        this.latencyMs = latencyMs;
    }

    public void setDown(boolean down) {
        this.down = down;
    }

    /** 接下来 n 次调用直接失败（模拟短暂抖动）。 */
    public void failNextCalls(int n) {
        failNextCalls.addAndGet(n);
    }

    public void advanceTo(long newHead) {
        this.head = newHead;
    }

    /** 从 forkFromHeight 起重组：该高度及之后哈希改变，链头回退到 newHead。 */
    public void reorg(long forkFromHeight, long newHead) {
        int gen = forkCounter.incrementAndGet();
        forkSegments.put(forkFromHeight, gen);
        this.head = newHead;
    }

    public void injectMalformedAt(long height) {
        malformedHeights.add(height);
    }

    public void injectUndecodableAt(long height) {
        undecodableHeights.add(height);
    }

    /** 清除全部注入的畸形/解码失败故障（模拟节点修复）。 */
    public void clearInjectedFaults() {
        malformedHeights.clear();
        undecodableHeights.clear();
    }

    // ---- ChainNode ----

    @Override
    public long latestHeight() {
        simulateCall();
        return head;
    }

    @Override
    public BlockData getBlock(long height) {
        simulateCall();
        if (height < 0 || height > head) {
            return null;
        }
        if (malformedHeights.contains(height)) {
            // 畸形：hash 缺失
            return new BlockData(height, null, hashOf(height - 1), logsOf(height));
        }
        if (undecodableHeights.contains(height)) {
            // 解码失败：日志 data 非十六进制
            return new BlockData(height, hashOf(height), hashOf(height - 1),
                    List.of(new LogEntry("0xcontract-" + chainSeed, List.of("0xtopic-" + height), "not-hex-data")));
        }
        return new BlockData(height, hashOf(height), hashOf(height - 1), logsOf(height));
    }

    private void simulateCall() {
        if (down) {
            throw new NodeCallException("node " + id + " is down");
        }
        int remaining = failNextCalls.get();
        if (remaining > 0 && failNextCalls.compareAndSet(remaining, remaining - 1)) {
            throw new NodeCallException("node " + id + " transient failure");
        }
        if (latencyMs > 0) {
            try {
                Thread.sleep(latencyMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new NodeCallException("interrupted", e);
            }
        }
    }

    private String hashOf(long height) {
        if (height < 0) {
            return "0xgenesis-parent";
        }
        int gen = 0;
        for (Map.Entry<Long, Integer> e : forkSegments.entrySet()) {
            if (e.getKey() <= height && e.getValue() >= gen) {
                gen = e.getValue();
            }
        }
        return "0x" + chainSeed + "-g" + gen + "-" + height;
    }

    private List<LogEntry> logsOf(long height) {
        return List.of(new LogEntry("0xcontract-" + chainSeed, List.of("0xtopic-" + height), "0x" + Long.toHexString(height)));
    }
}
