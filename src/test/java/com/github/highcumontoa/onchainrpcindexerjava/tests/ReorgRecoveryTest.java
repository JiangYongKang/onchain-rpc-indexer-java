package com.github.highcumontoa.onchainrpcindexerjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.onchainrpcindexerjava.ingest.IngestResult;
import com.github.highcumontoa.onchainrpcindexerjava.ingest.IngestionService;
import com.github.highcumontoa.onchainrpcindexerjava.node.MockChainNode;
import com.github.highcumontoa.onchainrpcindexerjava.selection.DefaultNodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.store.JournaledIndexStore;
import com.github.highcumontoa.onchainrpcindexerjava.store.RejectionReason;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 节点切换、链头回退、重组回滚与重启复现。 */
class ReorgRecoveryTest {

    @TempDir
    Path tempDir;

    private record Rig(MockChainNode a, MockChainNode b, DefaultNodeSelector selector,
                       JournaledIndexStore store, IngestionService ingestion) {
    }

    private Rig newRig(Path journal) {
        MockChainNode a = new MockChainNode("a", "seed", 1_000);
        a.setLatencyMs(1);
        MockChainNode b = new MockChainNode("b", "seed", 1_000);
        b.setLatencyMs(50);
        DefaultNodeSelector selector = new DefaultNodeSelector();
        selector.register(a);
        selector.register(b);
        JournaledIndexStore store = new JournaledIndexStore(journal);
        return new Rig(a, b, selector, store, new IngestionService(selector, store));
    }

    private void collectRange(IngestionService ingestion, long from, long to) {
        for (long h = from; h <= to; h++) {
            IngestResult r = ingestion.collectOnce(h);
            assertTrue(r.success(), "采集高度 " + h + " 应成功: " + r.error());
        }
    }

    @Test
    void nodeSwitchWithReorgRollsBackAndAligns() {
        Rig rig = newRig(tempDir.resolve("reorg.jsonl"));
        // 从更快的 a 节点采集 0..5
        collectRange(rig.ingestion(), 0, 5);
        assertEquals("a", rig.ingestion().collectOnce(6).nodeId(), "延迟低的 a 应被优先选中");
        assertTrue(rig.store().blockAt(5).orElseThrow().hash().contains("-g0-"));

        // a 宕机；b 从高度 4 起分叉（链头回退语义：b 的视图与本地不一致）
        rig.a().setDown(true);
        rig.b().reorg(4, 1_000);

        IngestResult r7 = rig.ingestion().collectOnce(7);
        assertTrue(r7.success(), "切换节点后应对齐成功: " + r7.error());
        assertEquals("b", r7.nodeId(), "a 宕机后应切换到 b");

        // 0..3 保持旧链，4..7 已回滚并对齐到 b 的新链
        for (long h = 0; h <= 3; h++) {
            assertTrue(rig.store().blockAt(h).orElseThrow().hash().contains("-g0-"),
                    "分叉点之前应保持原链: height=" + h);
        }
        for (long h = 4; h <= 7; h++) {
            assertTrue(rig.store().blockAt(h).orElseThrow().hash().contains("-g1-"),
                    "分叉点之后应对齐到新链: height=" + h);
        }
        assertEquals(7, rig.store().headHeight());
        // a 的失败被记录且可查询
        assertTrue(rig.selector().status().stream()
                .anyMatch(v -> v.nodeId().equals("a") && v.totalFailures() > 0));
    }

    @Test
    void rollbackConclusionReproducibleAfterRestart() {
        Path journal = tempDir.resolve("restart.jsonl");
        Rig rig = newRig(journal);
        collectRange(rig.ingestion(), 0, 5);
        rig.a().setDown(true);
        rig.b().reorg(4, 1_000);
        collectRange(rig.ingestion(), 6, 8);
        rig.store().finalizeUpTo(2);

        // 模拟重启：用同一日志文件重建存储，结论必须可复现
        JournaledIndexStore restarted = new JournaledIndexStore(journal);
        assertEquals(rig.store().headHeight(), restarted.headHeight());
        assertEquals(rig.store().finalizedHeight(), restarted.finalizedHeight());
        assertEquals(rig.store().indexedHeights(), restarted.indexedHeights());
        for (long h = 0; h <= restarted.headHeight(); h++) {
            assertEquals(rig.store().blockAt(h).orElseThrow().hash(),
                    restarted.blockAt(h).orElseThrow().hash(),
                    "重启后高度 " + h + " 的哈希必须一致");
        }
        // 重启后固化位点仍然生效
        DefaultNodeSelector selector2 = new DefaultNodeSelector();
        selector2.register(rig.b());
        IngestionService ingestion2 = new IngestionService(selector2, restarted);
        IngestResult r = ingestion2.collectOnce(1);
        assertFalse(r.success());
        assertEquals(RejectionReason.BELOW_FINALIZED_CURSOR, r.rejectionReason());
    }

    @Test
    void headRetreatIsAlignedNotContinuedFromMiddle() {
        Rig rig = newRig(tempDir.resolve("retreat.jsonl"));
        collectRange(rig.ingestion(), 0, 8);

        // 采信节点切换且链头回退：b 的链在高度 6 分叉且链头回退到 6
        rig.a().setDown(true);
        rig.b().reorg(6, 6);

        // 重复采集已索引高度：哈希不同 -> 回滚到分叉点并对齐到 b 的链头
        IngestResult r = rig.ingestion().collectOnce(7);
        // b 的链头只有 6，高度 7 不存在 -> 本次采集失败但不得破坏已有数据
        assertFalse(r.success());
        IngestResult r6 = rig.ingestion().collectOnce(6);
        assertTrue(r6.success(), "链头回退后应对齐到新链头: " + r6.error());
        assertEquals(6, rig.store().headHeight());
        assertTrue(rig.store().blockAt(6).orElseThrow().hash().contains("-g1-"));
        assertTrue(rig.store().blockAt(5).orElseThrow().hash().contains("-g0-"));
    }

    @Test
    void reorgCannotCrossFinalizedBoundary() {
        Rig rig = newRig(tempDir.resolve("finalized-reorg.jsonl"));
        collectRange(rig.ingestion(), 0, 6);
        rig.store().finalizeUpTo(5);

        // b 从高度 3 分叉（分叉点低于固化位点 5）
        rig.a().setDown(true);
        rig.b().reorg(3, 1_000);

        IngestResult r = rig.ingestion().collectOnce(7);
        assertFalse(r.success(), "重组深度超过固化位点必须被拒绝");
        assertEquals(RejectionReason.BELOW_FINALIZED_CURSOR, r.rejectionReason());
        // 固化位点及以下的数据未被破坏（未固化的高度 6 允许被回滚）
        assertEquals(5, rig.store().headHeight());
        assertTrue(rig.store().blockAt(5).orElseThrow().hash().contains("-g0-"));
    }
}
