package com.github.highcumontoa.onchainrpcindexerjava.tests;

import com.github.highcumontoa.onchainrpcindexerjava.model.RpcErrorCode;
import com.github.highcumontoa.onchainrpcindexerjava.node.DefaultNodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.node.NodeHealthView;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.ChainRpcClient;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.SimulatedRpcNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 节点探测、择优、短暂抖动、故障隔离与恢复、采集期故障切换。
 */
class NodeSelectorTest {

    private static final Logger log = LoggerFactory.getLogger(NodeSelectorTest.class);

    private TestHarness harness(Path dir) {
        // 默认阈值=3 连续失败才隔离，冷却 150ms，探测超时 200ms
        return TestHarness.create(dir.resolve("state.json"), 3,
                Duration.ofMillis(150), Duration.ofMillis(200));
    }

    private TestHarness harness(Path dir, int threshold) {
        return TestHarness.create(dir.resolve("state.json"), threshold,
                Duration.ofMillis(150), Duration.ofMillis(200));
    }

    @Test
    void prefersLowestLatencyHealthyNode(@TempDir Path dir) {
        TestHarness h = harness(dir);
        // 各做几次成功调用，形成延迟样本
        for (int i = 0; i < 5; i++) {
            h.clients.forEach(c -> {
                long start = System.nanoTime();
                c.getHeadHeight();
                h.selector.recordOutcome(c.id(), RpcErrorCode.OK,
                        (System.nanoTime() - start) / 1_000_000L);
            });
        }
        String chosen = h.selector.select().orElseThrow();
        log.info("[择优] 选择依据={}", ((DefaultNodeSelector) h.selector).explain());
        assertEquals("node-a", chosen, "低延迟高成功率的 node-a 应被选中");
    }

    @Test
    void briefJitterDoesNotIsolate(@TempDir Path dir) {
        TestHarness h = harness(dir);
        // 4 个样本中 2 次失败（成功率恰好 0.5，不低于熔断线），且连续失败未达阈值：
        // 属于短暂抖动，不得隔离。
        h.selector.recordOutcome("node-a", RpcErrorCode.TIMEOUT, 5);
        h.selector.recordOutcome("node-a", RpcErrorCode.OK, 3);
        h.selector.recordOutcome("node-a", RpcErrorCode.UNAVAILABLE, 4);
        h.selector.recordOutcome("node-a", RpcErrorCode.OK, 3);

        NodeHealthView view = healthOf(h, "node-a");
        log.info("[抖动不隔离] node-a 健康状态 isolated={} consecutive={} successRate={} view={}",
                view.isolated(), view.consecutiveFailures(), view.successRate(), view);
        assertFalse(view.isolated(), "短暂抖动（成功率=0.5、连续失败<阈值）不应触发隔离");
        assertTrue(h.selector.select().orElseThrow().equals("node-a"),
                "未被隔离的最优节点仍应可被选中");
    }

    @Test
    void consecutiveFailuresIsolateAndSelectionFallsBack(@TempDir Path dir) {
        TestHarness h = harness(dir, 2);
        h.nodeA.setDown(true);
        // 一次推进：node-a 链头成功但区块拉取连续失败 2 次，达阈值后隔离并切换 node-b
        h.indexer.advance(0);
        NodeHealthView a = healthOf(h, "node-a");
        log.info("[隔离] node-a isolated={} consecutiveFailures={} lastError={} 选择依据={}",
                a.isolated(), a.consecutiveFailures(), a.lastError(),
                ((DefaultNodeSelector) h.selector).explain());
        assertTrue(a.isolated(), "node-a 连续失败后应被隔离");
        assertEquals("node-b", h.selector.select().orElseThrow(),
                "隔离后应择优切换到 node-b");
        // 数据通过 node-b 正常采到，无缺失
        assertEquals(7, h.store.headHeight(), "故障切换后仍应完整采集到链头");
    }

    @Test
    void isolatedNodeRecoversAfterCooldownAndSuccessfulProbe(@TempDir Path dir)
            throws InterruptedException {
        TestHarness h = harness(dir);
        h.nodeA.setDown(true);
        // 通过真实调用累积失败直至隔离
        for (int i = 0; i < 3; i++) {
            try {
                h.nodeA.getHeadHeight();
            } catch (RuntimeException ignored) {
                // 预期失败
            }
            h.selector.recordOutcome("node-a", RpcErrorCode.UNAVAILABLE, 1);
        }
        assertTrue(healthOf(h, "node-a").isolated(), "前置：node-a 已隔离");
        log.info("[恢复] 节点已隔离，冷却中 node-a={}", healthOf(h, "node-a"));

        // 冷却未到时 node-a 仍被跳过
        assertEquals("node-b", h.selector.select().orElseThrow());

        // 节点恢复上线并等待冷却到期
        h.nodeA.setDown(false);
        Thread.sleep(180);

        // 节点恢复上线；选择时的恢复探测（真实 getHeadHeight）成功后应解除隔离。
        String chosen = h.selector.select().orElseThrow();
        NodeHealthView after = healthOf(h, "node-a");
        log.info("[恢复] 冷却后探测 selected={} node-a={}", chosen, after);
        assertFalse(after.isolated(), "恢复探测成功后应解除隔离");
        // 恢复后窗口被重置，仅含探测成功样本，低延迟节点重新成为首选
        assertEquals("node-a", chosen, "恢复后的低延迟节点应重新被择优");
    }

    @Test
    void failedRecoveryProbeKeepsIsolationAndResetsCooldown(@TempDir Path dir)
            throws InterruptedException {
        TestHarness h = harness(dir);
        for (int i = 0; i < 3; i++) {
            h.selector.recordOutcome("node-a", RpcErrorCode.UNAVAILABLE, 1);
        }
        h.nodeA.setDown(true);
        Thread.sleep(180);
        // 冷却到期但节点仍故障：探测失败，继续隔离
        String chosen = h.selector.select().orElseThrow();
        NodeHealthView view = healthOf(h, "node-a");
        log.info("[恢复失败] selected={} node-a 仍隔离 isolated={} recoverAfterMs={}",
                chosen, view.isolated(), view.recoverAfterMillis());
        assertTrue(view.isolated(), "探测失败必须继续隔离并重置冷却");
        assertTrue(view.recoverAfterMillis() > 0, "冷却应被重新计时");
        assertEquals("node-b", chosen);
    }

    @Test
    void selectExcludesAlreadyTriedNodes() {
        DefaultNodeSelector selector = new DefaultNodeSelector(
                List.of(new SimulatedRpcNode("x", 1L)), 5, 3,
                Duration.ofSeconds(10), Duration.ofSeconds(1));
        assertTrue(selector.select(List.of("x")).isEmpty(),
                "唯一节点被排除时应返回空，避免重复选择失败节点");
    }

    private static NodeHealthView healthOf(TestHarness h, String nodeId) {
        Optional<NodeHealthView> view = h.selector.health().stream()
                .filter(v -> v.nodeId().equals(nodeId)).findFirst();
        return view.orElseThrow();
    }
}
