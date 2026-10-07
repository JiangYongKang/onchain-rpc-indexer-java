package com.github.highcumontoa.onchainrpcindexerjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.onchainrpcindexerjava.follow.BlockFollower;
import com.github.highcumontoa.onchainrpcindexerjava.follow.FollowFailureReason;
import com.github.highcumontoa.onchainrpcindexerjava.follow.FollowRangeResult;
import com.github.highcumontoa.onchainrpcindexerjava.follow.FollowStatus;
import com.github.highcumontoa.onchainrpcindexerjava.follow.FollowerProperties;
import com.github.highcumontoa.onchainrpcindexerjava.ingest.IngestionService;
import com.github.highcumontoa.onchainrpcindexerjava.node.MockChainNode;
import com.github.highcumontoa.onchainrpcindexerjava.selection.DefaultNodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.selection.NodeState;
import com.github.highcumontoa.onchainrpcindexerjava.store.JournaledIndexStore;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 自动追块：整段补齐、节点容错切换、隔离恢复、重复触发与并发去重、重启续追。 */
class BlockFollowerTest {

    @TempDir
    Path tempDir;

    private final AtomicLong clock = new AtomicLong(0);

    private record Rig(MockChainNode a, MockChainNode b, DefaultNodeSelector selector,
                       JournaledIndexStore store, IngestionService ingestion,
                       FollowerProperties props, BlockFollower follower) {
    }

    private Rig newRig(Path journal) {
        MockChainNode a = new MockChainNode("a", "seed", 1_000);
        a.setLatencyMs(1);
        MockChainNode b = new MockChainNode("b", "seed", 1_000);
        b.setLatencyMs(50);
        DefaultNodeSelector selector = new DefaultNodeSelector(3, 5_000, clock::get);
        selector.register(a);
        selector.register(b);
        JournaledIndexStore store = new JournaledIndexStore(journal);
        FollowerProperties props = new FollowerProperties();
        return new Rig(a, b, selector, store, new IngestionService(selector, store),
                props, new BlockFollower(selector, store, props));
    }

    private static List<Long> range(long from, long to) {
        List<Long> heights = new ArrayList<>();
        for (long h = from; h <= to; h++) {
            heights.add(h);
        }
        return heights;
    }

    @Test
    void followRangeFillsWholeRangeAndAlignsToTarget() {
        Rig rig = newRig(tempDir.resolve("range.jsonl"));

        FollowRangeResult result = rig.follower().followRange(0, 500);

        assertTrue(result.started());
        assertTrue(result.success(), "整段追块应成功: " + result.error());
        assertEquals(500, result.finalHead(), "本地链头应对齐目标高度");
        assertEquals(501, result.written());
        assertEquals(0, result.skipped());
        assertEquals(range(0, 500), rig.store().indexedHeights(), "中间不得缺块、不得重复");

        FollowStatus status = rig.follower().status();
        assertEquals(500, status.currentHeight());
        assertEquals(501, status.lastRoundWritten());
        assertEquals(0, status.lastRoundFailed());
        assertFalse(status.running());
    }

    @Test
    void untrustedNodeDataFailsOverAndRangeStillCompletes() {
        Rig rig = newRig(tempDir.resolve("failover.jsonl"));
        // 更快的 a 在 10/11/12 高度分别返回畸形、解不出、畸形数据
        rig.a().injectMalformedAt(10);
        rig.a().injectUndecodableAt(11);
        rig.a().injectMalformedAt(12);

        FollowRangeResult result = rig.follower().followRange(0, 100);

        assertTrue(result.success(), "个别节点坏数据不应卡住整段: " + result.error());
        assertEquals(100, result.finalHead());
        assertEquals(range(0, 100), rig.store().indexedHeights(), "坏数据高度必须由健康节点补齐");
        // a 的不可信记录可解释、可查询
        assertTrue(result.nodeErrors().containsKey("a"), "应记录 a 节点的不可信原因");
        assertEquals(3, rig.selector().status().stream()
                .filter(v -> v.nodeId().equals("a"))
                .mapToInt(v -> (int) v.totalFailures()).sum(), "a 的三次坏数据都应计入失败");
        // 连续失败达阈值后 a 被隔离，后续高度由 b 服务
        assertTrue(rig.selector().status().stream()
                .anyMatch(v -> v.nodeId().equals("a") && v.state() == NodeState.ISOLATED));
        // 被拒绝的坏块没有留下半写数据：每个高度哈希都来自可信链
        for (long h = 10; h <= 12; h++) {
            assertTrue(rig.store().blockAt(h).orElseThrow().hash().startsWith("0xseed-"),
                    "高度 " + h + " 应写入健康节点的数据");
        }
    }

    @Test
    void isolatedNodeRejoinsAfterRecoveryCooldown() {
        Rig rig = newRig(tempDir.resolve("recover.jsonl"));
        // 让 a 连续失败被隔离
        rig.a().injectMalformedAt(5);
        rig.a().injectMalformedAt(6);
        rig.a().injectMalformedAt(7);
        FollowRangeResult first = rig.follower().followRange(0, 20);
        assertTrue(first.success());
        assertEquals(NodeState.ISOLATED, rig.selector().status().stream()
                .filter(v -> v.nodeId().equals("a")).findFirst().orElseThrow().state());

        // b 也宕机：此时无可用节点，追块必须给出可区分失败而不是误用隔离节点
        rig.b().setDown(true);
        FollowRangeResult stuck = rig.follower().followRange(21, 30);
        assertFalse(stuck.success());
        assertEquals(FollowFailureReason.NO_AVAILABLE_NODE, stuck.failureReason());
        assertEquals(20, rig.store().headHeight(), "失败不得写入半个块");

        // 冷却期未满：a 仍处于隔离，状态视图能看到剩余等待时间与最近失败原因
        clock.addAndGet(4_999);
        var view = rig.selector().status().stream()
                .filter(v -> v.nodeId().equals("a")).findFirst().orElseThrow();
        assertEquals(NodeState.ISOLATED, view.state());
        assertTrue(view.remainingCooldownMs() > 0, "应能看到还要等多久");
        assertNotNull(view.lastError(), "应能看到最近一次为什么出问题");

        // 冷却期满：a 进入半开等待探测；a 修复后重新参与追块
        clock.addAndGet(1);
        assertEquals(NodeState.HALF_OPEN, rig.selector().status().stream()
                .filter(v -> v.nodeId().equals("a")).findFirst().orElseThrow().state());
        FollowRangeResult resumed = rig.follower().followRange(21, 40);
        assertTrue(resumed.success(), "a 恢复后应能接着追完: " + resumed.error());
        assertEquals(range(0, 40), rig.store().indexedHeights());
        var recovered = rig.selector().status().stream()
                .filter(v -> v.nodeId().equals("a")).findFirst().orElseThrow();
        assertEquals(NodeState.AVAILABLE, recovered.state(), "探测成功后 a 应真正恢复可用");
        assertTrue(recovered.totalSuccesses() > 0);
    }

    @Test
    void duplicateTriggerAndConcurrentManualCollectProduceNoDuplicates() throws Exception {
        Rig rig = newRig(tempDir.resolve("dup-follow.jsonl"));
        // 先手动采集 0..4
        for (long h = 0; h <= 4; h++) {
            assertTrue(rig.ingestion().collectOnce(h).success());
        }
        rig.a().setLatencyMs(2);
        rig.b().setLatencyMs(5);

        // 后台追整段；追块期间重复触发 + 并发手动采集
        var future = java.util.concurrent.Executors.newSingleThreadExecutor()
                .submit(() -> rig.follower().followRange(0, 300));
        // 等后台追块确实开跑
        while (rig.store().headHeight() < 6) {
            Thread.sleep(1);
        }
        FollowRangeResult dup = rig.follower().followRange(0, 300);
        assertFalse(dup.started(), "同一段被重复触发时只能算一次");
        // 并发手动采集已索引高度：幂等忽略
        var manual = rig.ingestion().collectOnce(2);
        assertTrue(manual.success());
        assertEquals("DUPLICATE_IGNORED", manual.outcome());

        FollowRangeResult result = future.get();
        assertTrue(result.success(), "并发下整段仍应追完: " + result.error());
        assertEquals(300, rig.store().headHeight());
        assertEquals(range(0, 300), rig.store().indexedHeights(),
                "重复触发与并发手动采集下不得产生重复记录");
        assertEquals(296, result.written(), "0..4 已索引，只应真实写入 5..300");
        assertTrue(result.skipped() >= 5, "已索引高度应计入跳过");
    }

    @Test
    void autoTickFollowsChainHeadInBatchesAndResumesAfterRestart() {
        Path journal = tempDir.resolve("auto.jsonl");
        Rig rig = newRig(journal);
        rig.props().setMaxBatchPerRound(500);
        rig.follower().setAutoEnabled(true);

        rig.follower().tick();
        assertEquals(499, rig.store().headHeight(), "单轮最多推进 maxBatchPerRound 个高度");
        rig.follower().tick();
        rig.follower().tick();
        assertEquals(1_000, rig.store().headHeight(), "按节奏多轮后应对齐链头");
        FollowStatus aligned = rig.follower().status();
        assertEquals(1_000, aligned.targetHeight());
        assertEquals(0, aligned.lag(), "追上链头后落后量应为 0");
        assertTrue(aligned.autoEnabled());

        // 模拟重启：同一日志重建存储与追块器，应从上次位置续追而不是重采
        JournaledIndexStore restarted = new JournaledIndexStore(journal);
        assertEquals(1_000, restarted.headHeight());
        FollowerProperties props2 = new FollowerProperties();
        props2.setEnabled(true);
        BlockFollower follower2 = new BlockFollower(rig.selector(), restarted, props2);
        rig.a().advanceTo(1_200);
        rig.b().advanceTo(1_200);
        follower2.tick();
        assertEquals(1_200, restarted.headHeight(), "重启后应接着上次位置继续追");
        assertEquals(200, follower2.status().lastRoundWritten(), "已索引的不得重复处理");
        assertEquals(range(0, 1_200), restarted.indexedHeights(), "不得缺块也不得重复");
    }

    @Test
    void allNodesUntrustedStopsWithDistinguishableReason() {
        Rig rig = newRig(tempDir.resolve("untrusted.jsonl"));
        // a 和 b 在高度 7 都给不出可信数据
        rig.a().injectMalformedAt(7);
        rig.b().injectUndecodableAt(7);

        FollowRangeResult result = rig.follower().followRange(0, 20);

        assertFalse(result.success(), "所有节点都不可信时必须停下来");
        assertEquals(FollowFailureReason.NO_TRUSTWORTHY_DATA, result.failureReason(),
                "失败原因必须可与节点不可用区分");
        assertEquals(6, rig.store().headHeight(), "失败高度不得写入，前面已追的保留");
        assertEquals(range(0, 6), rig.store().indexedHeights());
        assertTrue(result.nodeErrors().containsKey("a") && result.nodeErrors().containsKey("b"),
                "应留下每个节点为什么不可信的记录");
        FollowStatus status = rig.follower().status();
        assertEquals(1, status.lastRoundFailed());
        assertTrue(status.failureReasons().containsKey("NO_TRUSTWORTHY_DATA"),
                "状态视图应能看到失败原因分类");
        assertNotNull(status.lastError());
    }
}
