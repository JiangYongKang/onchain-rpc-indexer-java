package com.github.highcumontoa.onchainrpcindexerjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.onchainrpcindexerjava.node.MockChainNode;
import com.github.highcumontoa.onchainrpcindexerjava.selection.DefaultNodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.selection.NodeStatusView;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** 节点择优、故障隔离与恢复。 */
class NodeSelectorTest {

    private final AtomicLong clock = new AtomicLong(0);

    private DefaultNodeSelector newSelector() {
        return new DefaultNodeSelector(3, 5_000, clock::get);
    }

    @Test
    void transientJitterDoesNotIsolate() {
        DefaultNodeSelector selector = newSelector();
        selector.register(new MockChainNode("a", "seed", 10));
        // 短暂抖动：失败-成功交替，连续失败计数被清零，不得隔离
        for (int i = 0; i < 5; i++) {
            selector.recordFailure("a", "timeout");
            selector.recordFailure("a", "timeout");
            selector.recordSuccess("a", 10);
        }
        NodeStatusView view = selector.status().get(0);
        assertFalse(view.isolated(), "短暂抖动不得触发隔离");
        assertEquals(0, view.consecutiveFailures());
        assertNotNull(selector.select(), "抖动后节点仍应可被选中");
    }

    @Test
    void consecutiveFailuresIsolateAndRecoverAfterCooldown() {
        DefaultNodeSelector selector = newSelector();
        selector.register(new MockChainNode("a", "seed", 10));
        selector.recordFailure("a", "e1");
        selector.recordFailure("a", "e2");
        assertFalse(selector.status().get(0).isolated(), "未达阈值不得隔离");
        selector.recordFailure("a", "e3");
        assertTrue(selector.status().get(0).isolated(), "连续失败达阈值应隔离");
        assertNull(selector.select(), "唯一节点被隔离后无可用节点");

        // 冷却期未满：仍被隔离
        clock.addAndGet(4_999);
        assertNull(selector.select());
        // 冷却期满：进入半开，允许一次探测
        clock.addAndGet(1);
        var selection = selector.select();
        assertNotNull(selection, "冷却期满应允许半开探测");
        assertTrue(selection.report().rationale().stream().anyMatch(r -> r.contains("半开")),
                "选择依据应包含半开说明");

        // 探测失败：重新隔离
        selector.recordFailure("a", "still bad");
        assertTrue(selector.status().get(0).isolated());
        // 冷却后再探测成功：恢复
        clock.addAndGet(5_000);
        assertNotNull(selector.select());
        selector.recordSuccess("a", 8);
        NodeStatusView view = selector.status().get(0);
        assertFalse(view.isolated(), "探测成功应解除隔离");
        assertEquals(0, view.consecutiveFailures());
    }

    @Test
    void prefersLowerLatencyAndExplains() {
        DefaultNodeSelector selector = newSelector();
        selector.register(new MockChainNode("fast", "seed", 10));
        selector.register(new MockChainNode("slow", "seed", 10));
        selector.recordSuccess("fast", 5);
        selector.recordSuccess("slow", 50);
        var selection = selector.select();
        assertNotNull(selection);
        assertEquals("fast", selection.node().id());
        assertFalse(selection.report().rationale().isEmpty(), "选择依据必须可解释");
        assertTrue(selection.report().rationale().stream().anyMatch(r -> r.contains("平均延迟")));
    }

    @Test
    void isolatedNodeIsSkippedAndOthersServe() {
        DefaultNodeSelector selector = newSelector();
        selector.register(new MockChainNode("a", "seed", 10));
        selector.register(new MockChainNode("b", "seed", 10));
        selector.recordSuccess("a", 1);
        selector.recordSuccess("b", 100);
        // a 虽然更快，但连续失败被隔离后应选 b
        selector.recordFailure("a", "x");
        selector.recordFailure("a", "x");
        selector.recordFailure("a", "x");
        var selection = selector.select();
        assertNotNull(selection);
        assertEquals("b", selection.node().id());
        assertTrue(selection.report().rationale().stream().anyMatch(r -> r.contains("已隔离")));
    }
}
