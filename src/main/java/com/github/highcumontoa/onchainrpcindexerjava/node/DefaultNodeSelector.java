package com.github.highcumontoa.onchainrpcindexerjava.node;

import com.github.highcumontoa.onchainrpcindexerjava.model.RpcErrorCode;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.ChainRpcClient;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.RpcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

/**
 * 默认节点选择器：探测、择优、故障隔离与恢复。
 *
 * <h3>规则（全部可通过 {@link #health()} / {@link #explain()} 查询解释）</h3>
 * <ul>
 *     <li>每个节点维护最近 {@code windowSize} 次调用结果，统计成功率与平均延迟；</li>
 *     <li><b>短暂抖动不误判</b>：只有连续失败次数达到 {@code failureThreshold} 才隔离，
 *         滑动窗口内的零星失败不触发隔离；</li>
 *     <li><b>隔离</b>：节点进入隔离态并记录时间，选择阶段完全跳过；</li>
 *     <li><b>恢复</b>：隔离超过 {@code recoverCooldown} 后，下一次选择对其发起一次真实探测，
 *         探测成功立即重新参与，探测失败重置冷却计时、继续隔离；</li>
 *     <li><b>择优</b>：健康节点按 score 排序（成功率越高、延迟越低越优），分数相同按节点 id 稳定排序。</li>
 * </ul>
 */
public class DefaultNodeSelector implements NodeSelector {

    private static final Logger log = LoggerFactory.getLogger(DefaultNodeSelector.class);

    private record Outcome(RpcErrorCode code, long latencyMillis) {
        boolean ok() {
            return code == RpcErrorCode.OK;
        }
    }

    private static final class Stat {
        final ChainRpcClient client;
        final Deque<Outcome> window = new ArrayDeque<>();
        long consecutiveFailures;
        boolean isolated;
        long isolatedAtMillis;
        String lastError = "";
        long probes;
        long probeFailures;

        Stat(ChainRpcClient client) {
            this.client = client;
        }
    }

    private final List<Stat> stats;
    private final int windowSize;
    private final int failureThreshold;
    private final long recoverCooldownMillis;
    private final Duration probeTimeout;

    public DefaultNodeSelector(List<ChainRpcClient> clients,
                               int windowSize,
                               int failureThreshold,
                               Duration recoverCooldown,
                               Duration probeTimeout) {
        this.stats = new ArrayList<>(clients.size());
        for (ChainRpcClient client : clients) {
            this.stats.add(new Stat(client));
        }
        this.windowSize = windowSize;
        this.failureThreshold = failureThreshold;
        this.recoverCooldownMillis = recoverCooldown.toMillis();
        this.probeTimeout = probeTimeout;
    }

    @Override
    public synchronized Optional<String> select() {
        return select(List.of());
    }

    @Override
    public synchronized Optional<String> select(Collection<String> excludeIds) {
        long now = System.currentTimeMillis();
        for (Stat stat : stats) {
            if (stat.isolated && now - stat.isolatedAtMillis >= recoverCooldownMillis) {
                boolean healthy = probe(stat);
                if (healthy) {
                    log.info("节点恢复探测成功，重新参与选择 nodeId={} probes={}", stat.client.id(), stat.probes);
                } else {
                    log.info("节点恢复探测失败，继续隔离 nodeId={} cooldownMs={} reason={}",
                            stat.client.id(), recoverCooldownMillis, stat.lastError);
                }
            }
        }

        Optional<String> chosen = stats.stream()
                .filter(s -> !s.isolated)
                .filter(s -> !excludeIds.contains(s.client.id()))
                .min(Comparator.comparingDouble(this::score).thenComparing(s -> s.client.id()))
                .map(s -> s.client.id());

        log.info("节点选择依据 {}", explain());
        return chosen;
    }

    @Override
    public synchronized void recordOutcome(String nodeId, RpcErrorCode code, long latencyMillis) {
        Stat stat = find(nodeId);
        if (stat == null) {
            return;
        }
        stat.window.addLast(new Outcome(code, latencyMillis));
        while (stat.window.size() > windowSize) {
            stat.window.removeFirst();
        }
        if (code == RpcErrorCode.OK) {
            if (stat.isolated) {
                log.info("节点在调用中恢复成功，立即解除隔离 nodeId={}", nodeId);
            } else if (stat.consecutiveFailures > 0) {
                log.info("节点调用恢复正常 nodeId={} 清零连续失败计数(原={})",
                        nodeId, stat.consecutiveFailures);
            }
            stat.consecutiveFailures = 0;
            stat.isolated = false;
            stat.isolatedAtMillis = 0L;
            stat.lastError = "";
        } else {
            stat.consecutiveFailures++;
            stat.lastError = code.name();
            boolean reachedConsecutive = stat.consecutiveFailures >= failureThreshold;
            double rate = successRate(stat);
            int n = stat.window.size();
            // 熔断：样本量 >=3 时成功率低于半数；或仅有的一次观测就是“传输类”失败
            // （畸形/解码失败属于数据问题，不应据此判定节点不可达）。
            boolean connectivityFault = code == RpcErrorCode.TIMEOUT || code == RpcErrorCode.UNAVAILABLE;
            boolean brokeSuccessRate = (n == 1 && rate == 0.0 && connectivityFault)
                    || (n >= 3 && rate < 0.5);
            if (!stat.isolated && (reachedConsecutive || brokeSuccessRate)) {
                stat.isolated = true;
                stat.isolatedAtMillis = System.currentTimeMillis();
                log.warn("节点触发隔离 nodeId={} 依据={} consecutiveFailures={}/{} successRate={} lastError={}",
                        nodeId,
                        reachedConsecutive ? "连续失败阈值" : "成功率熔断(<0.5)",
                        stat.consecutiveFailures, failureThreshold,
                        String.format("%.2f", successRate(stat)), code);
            } else {
                log.info("节点调用失败(暂不隔离) nodeId={} code={} consecutiveFailures={}/{} successRate={}",
                        nodeId, code, stat.consecutiveFailures, failureThreshold,
                        String.format("%.2f", successRate(stat)));
            }
        }
    }

    @Override
    public synchronized List<NodeHealthView> health() {
        long now = System.currentTimeMillis();
        List<NodeHealthView> views = new ArrayList<>(stats.size());
        for (Stat stat : stats) {
            double success = successRate(stat);
            double avg = avgLatency(stat);
            long recoverAfter = stat.isolated
                    ? Math.max(0L, stat.isolatedAtMillis + recoverCooldownMillis - now)
                    : 0L;
            views.add(new NodeHealthView(
                    stat.client.id(),
                    !stat.isolated,
                    stat.isolated,
                    stat.consecutiveFailures,
                    Math.round(avg * 100.0) / 100.0,
                    Math.round(success * 1000.0) / 1000.0,
                    stat.isolatedAtMillis,
                    recoverAfter,
                    stat.lastError
            ));
        }
        return List.copyOf(views);
    }

    /** 人类可读的择优/隔离解释（亦打印到单测日志）。 */
    public synchronized String explain() {
        long now = System.currentTimeMillis();
        List<String> parts = new ArrayList<>(stats.size());
        for (Stat stat : stats) {
            if (stat.isolated) {
                long remain = Math.max(0L, stat.isolatedAtMillis + recoverCooldownMillis - now);
                parts.add(String.format("%s[ISOLATED cf=%d cooldownRemainMs=%d lastError=%s]",
                        stat.client.id(), stat.consecutiveFailures, remain, stat.lastError));
            } else {
                parts.add(String.format("%s[score=%.2f okRate=%.2f avgMs=%.1f cf=%d n=%d]",
                        stat.client.id(), score(stat), successRate(stat), avgLatency(stat),
                        stat.consecutiveFailures, stat.window.size()));
            }
        }
        return String.join(" | ", parts);
    }

    // ------------------------------------------------------------------

    private boolean probe(Stat stat) {
        stat.probes++;
        long start = System.nanoTime();
        java.util.concurrent.FutureTask<Boolean> task = new java.util.concurrent.FutureTask<>(() -> {
            stat.client.getHeadHeight();
            return Boolean.TRUE;
        });
        Thread worker = new Thread(task, "node-probe-" + stat.client.id());
        worker.setDaemon(true);
        worker.start();
        RpcErrorCode code;
        String detail;
        try {
            task.get(probeTimeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            code = RpcErrorCode.OK;
            detail = "探测成功";
        } catch (java.util.concurrent.TimeoutException e) {
            task.cancel(true);
            code = RpcErrorCode.TIMEOUT;
            detail = "探测超时(>" + probeTimeout.toMillis() + "ms)";
        } catch (Exception e) {
            Throwable cause = e.getCause() instanceof RpcException re ? re : e.getCause();
            if (cause instanceof RpcException re) {
                code = re.code();
                detail = "探测失败:" + re.getMessage();
            } else {
                code = RpcErrorCode.UNAVAILABLE;
                detail = "探测失败:" + (cause == null ? e.getMessage() : cause.getMessage());
            }
        }
        long latency = Duration.ofNanos(System.nanoTime() - start).toMillis();
        if (code == RpcErrorCode.OK) {
            // 恢复探测成功意味着故障周期结束：开启新的观测窗口，旧失败样本不再拖累择优。
            stat.window.clear();
            stat.window.addLast(new Outcome(code, latency));
            stat.consecutiveFailures = 0;
            stat.isolated = false;
            stat.lastError = "";
            return true;
        }
        stat.probeFailures++;
        stat.consecutiveFailures++;
        stat.isolated = true;
        stat.isolatedAtMillis = System.currentTimeMillis();
        stat.lastError = code.name();
        log.info("节点探测失败 nodeId={} code={} detail={}", stat.client.id(), code, detail);
        return false;
    }

    /**
     * 分数越低越优：
     * <ul>
     *     <li>成功率 &ge; 0.5 视为健康抖动，不产生成功率惩罚（零星/间歇抖动不影响择优，
     *         持续故障由“连续失败隔离/成功率熔断”独立兜底）；</li>
     *     <li>成功率 &lt; 0.5 后按比例重罚（1000 倍权重）；</li>
     *     <li>延迟缩放到成功率量纲（毫秒/1000），因此延迟差异只在同健康等级内区分；</li>
     *     <li>零样本节点给予温和先验分 0.5：优于已证实不健康者，
     *         但劣于“已证实又快又稳”的节点（不会让冷节点永久压制有良好记录的节点）。</li>
     * </ul>
     */
    private double score(Stat stat) {
        if (stat.window.isEmpty()) {
            return 0.5;
        }
        double rate = successRate(stat);
        double avg = avgLatency(stat);
        double penalty = rate >= 0.5 ? 0.0 : (1.0 - rate) * 1000.0;
        return penalty + avg / 1000.0;
    }

    private static double successRate(Stat stat) {
        if (stat.window.isEmpty()) {
            return 1.0;
        }
        long ok = stat.window.stream().filter(Outcome::ok).count();
        return (double) ok / stat.window.size();
    }

    private static double avgLatency(Stat stat) {
        return stat.window.stream().mapToLong(o -> o.latencyMillis).average().orElse(0.0);
    }

    private Stat find(String nodeId) {
        for (Stat stat : stats) {
            if (stat.client.id().equals(nodeId)) {
                return stat;
            }
        }
        return null;
    }
}
