package com.github.highcumontoa.onchainrpcindexerjava.selection;

import com.github.highcumontoa.onchainrpcindexerjava.node.ChainNode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import org.springframework.stereotype.Component;

/**
 * 默认节点选择器：
 * - 连续失败达到阈值才隔离（短暂抖动不误判），成功即清零连续失败计数；
 * - 隔离冷却期结束后进入半开状态，允许一次探测，成功则恢复；
 * - 择优按 (平均延迟, 失败率) 打分，选择依据全部可查询、可解释。
 */
@Component
public class DefaultNodeSelector implements NodeSelector {

    static final int DEFAULT_ISOLATE_THRESHOLD = 3;
    static final long DEFAULT_RECOVERY_COOLDOWN_MS = 5_000;

    private final int isolateThreshold;
    private final long recoveryCooldownMs;
    private final LongSupplier clock;

    private final Map<String, ChainNode> nodes = new ConcurrentHashMap<>();
    private final Map<String, Stats> stats = new ConcurrentHashMap<>();

    private static final class Stats {
        int consecutiveFailures;
        long totalSuccesses;
        long totalFailures;
        double avgLatencyMs = -1;
        String lastError;
        boolean isolated;
        long isolatedAt;
        /** 半开：冷却结束，允许一次探测。 */
        boolean halfOpen;
    }

    public DefaultNodeSelector() {
        this(DEFAULT_ISOLATE_THRESHOLD, DEFAULT_RECOVERY_COOLDOWN_MS, System::currentTimeMillis);
    }

    public DefaultNodeSelector(int isolateThreshold, long recoveryCooldownMs, LongSupplier clock) {
        this.isolateThreshold = isolateThreshold;
        this.recoveryCooldownMs = recoveryCooldownMs;
        this.clock = clock;
    }

    @Override
    public void register(ChainNode node) {
        nodes.put(node.id(), node);
        stats.put(node.id(), new Stats());
    }

    @Override
    public Optional<ChainNode> node(String nodeId) {
        return Optional.ofNullable(nodes.get(nodeId));
    }

    @Override
    public synchronized Selection select() {
        return selectExcluding(Set.of());
    }

    @Override
    public synchronized Selection selectExcluding(Set<String> excludedNodeIds) {
        List<String> rationale = new ArrayList<>();
        long now = clock.getAsLong();
        List<Map.Entry<String, Stats>> candidates = new ArrayList<>();
        for (Map.Entry<String, Stats> e : stats.entrySet()) {
            if (excludedNodeIds.contains(e.getKey())) {
                rationale.add("节点 " + e.getKey() + " 在本次采集中已失败，暂时跳过");
                continue;
            }
            Stats s = e.getValue();
            if (s.isolated) {
                if (now - s.isolatedAt >= recoveryCooldownMs) {
                    s.halfOpen = true;
                    rationale.add("节点 " + e.getKey() + " 隔离冷却期(" + recoveryCooldownMs
                            + "ms)已满，进入半开状态等待探测");
                    candidates.add(e);
                } else {
                    rationale.add("节点 " + e.getKey() + " 已隔离(连续失败" + s.consecutiveFailures
                            + "次>=" + isolateThreshold + ")，剩余冷却 "
                            + (recoveryCooldownMs - (now - s.isolatedAt)) + "ms，跳过");
                }
            } else {
                candidates.add(e);
            }
        }
        if (candidates.isEmpty()) {
            rationale.add("无可用节点：全部节点均被隔离且未到恢复探测时间");
            return null;
        }
        // 半开节点优先探测：冷却期满后给它一次真实调用机会，探测成功即恢复，
        // 保证被隔离节点满足恢复条件后确实重新参与，而不是一直挂着用不上。
        List<Map.Entry<String, Stats>> halfOpen = candidates.stream()
                .filter(e -> e.getValue().halfOpen)
                .toList();
        if (!halfOpen.isEmpty()) {
            Map.Entry<String, Stats> probe = halfOpen.stream()
                    .min(Comparator.comparingDouble(e -> e.getValue().avgLatencyMs < 0
                            ? 0 : e.getValue().avgLatencyMs))
                    .orElseThrow();
            rationale.add("选中节点 " + probe.getKey() + "：半开探测优先，验证其是否已恢复");
            return new Selection(nodes.get(probe.getKey()),
                    new SelectionReport(probe.getKey(), rationale));
        }
        // 打分：按平均延迟升序、失败率升序。
        Map.Entry<String, Stats> best = candidates.stream()
                .min(Comparator
                        .<Map.Entry<String, Stats>>comparingDouble(e ->
                                e.getValue().avgLatencyMs < 0 ? 0 : e.getValue().avgLatencyMs)
                        .thenComparingDouble(this::failureRate))
                .orElseThrow();
        Stats s = best.getValue();
        rationale.add("选中节点 " + best.getKey() + "："
                + (s.halfOpen ? "半开探测； " : "")
                + "平均延迟=" + fmt(s.avgLatencyMs) + "ms"
                + "，失败率=" + fmt(failureRate(best))
                + "，连续失败=" + s.consecutiveFailures);
        return new Selection(nodes.get(best.getKey()), new SelectionReport(best.getKey(), rationale));
    }

    @Override
    public void recordSuccess(String nodeId, long latencyMs) {
        Stats s = stats.get(nodeId);
        if (s == null) {
            return;
        }
        synchronized (s) {
            s.totalSuccesses++;
            s.consecutiveFailures = 0;
            s.avgLatencyMs = s.avgLatencyMs < 0 ? latencyMs : s.avgLatencyMs * 0.7 + latencyMs * 0.3;
            if (s.isolated || s.halfOpen) {
                s.isolated = false;
                s.halfOpen = false;
                s.lastError = null;
            }
        }
    }

    @Override
    public void recordFailure(String nodeId, String error) {
        Stats s = stats.get(nodeId);
        if (s == null) {
            return;
        }
        synchronized (s) {
            s.totalFailures++;
            s.consecutiveFailures++;
            s.lastError = error;
            s.halfOpen = false;
            if (s.consecutiveFailures >= isolateThreshold && !s.isolated) {
                s.isolated = true;
                s.isolatedAt = clock.getAsLong();
            }
        }
    }

    @Override
    public List<NodeStatusView> status() {
        long now = clock.getAsLong();
        List<NodeStatusView> views = new ArrayList<>();
        for (Map.Entry<String, Stats> e : stats.entrySet()) {
            Stats s = e.getValue();
            synchronized (s) {
                String state;
                long remainingMs = 0;
                if (!s.isolated) {
                    state = "AVAILABLE";
                } else if (now - s.isolatedAt >= recoveryCooldownMs) {
                    state = "HALF_OPEN";
                } else {
                    state = "ISOLATED";
                    remainingMs = recoveryCooldownMs - (now - s.isolatedAt);
                }
                views.add(new NodeStatusView(e.getKey(), s.isolated, state, remainingMs,
                        s.consecutiveFailures, s.totalSuccesses, s.totalFailures,
                        s.avgLatencyMs, s.lastError));
            }
        }
        views.sort(Comparator.comparing(NodeStatusView::nodeId));
        return views;
    }

    private double failureRate(Map.Entry<String, Stats> e) {
        Stats s = e.getValue();
        long total = s.totalSuccesses + s.totalFailures;
        return total == 0 ? 0 : (double) s.totalFailures / total;
    }

    private static String fmt(double v) {
        return v < 0 ? "未知" : String.format("%.1f", v);
    }
}
