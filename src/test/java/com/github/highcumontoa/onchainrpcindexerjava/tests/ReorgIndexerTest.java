package com.github.highcumontoa.onchainrpcindexerjava.tests;

import com.github.highcumontoa.onchainrpcindexerjava.config.SimulationChains;
import com.github.highcumontoa.onchainrpcindexerjava.indexer.ChainIndexer;
import com.github.highcumontoa.onchainrpcindexerjava.model.AdvanceResult;
import com.github.highcumontoa.onchainrpcindexerjava.model.Block;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.SimulatedRpcNode;
import com.github.highcumontoa.onchainrpcindexerjava.storage.IndexStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 采信节点切换、链头回退与重组场景下的回滚、重放与重启复现。
 */
class ReorgIndexerTest {

    private static final Logger log = LoggerFactory.getLogger(ReorgIndexerTest.class);

    @Test
    void chainHeadRollbackRollsBackToShorterChainAndReplaysNewTip(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("s.json"));
        h.indexer.advance(0);
        assertEquals(7, h.store.headHeight());
        String oldHash5 = h.store.getBlock(5).orElseThrow().hash();

        // 全部节点链头回退：共同链保留到高度 4，高度 5 起为更短的新链（头=6）
        List<SimulatedRpcNode.BlockSpec> shorter = SimulationChains.reorgChain(5, 6);
        for (var node : List.of(h.nodeA, h.nodeB, h.nodeC)) {
            node.installChain(shorter);
        }

        AdvanceResult result = h.indexer.advance(0);
        log.info("[链头回退] rolledBack={} indexed={} stopReason={}",
                result.rolledBackHeights(), result.indexedHeights(), result.stopReason());

        assertEquals(List.of(7L, 6L, 5L), result.rolledBackHeights(), "应按降序回滚 5,6,7");
        assertEquals(6, h.store.headHeight(), "对齐后链头应为新链头 6");
        String newHash5 = h.store.getBlock(5).orElseThrow().hash();
        assertFalse(oldHash5.equals(newHash5), "高度 5 必须在新链上重放为不同哈希");
        // 事件账本中应有可复现的回滚结论
        assertTrue(h.store.events().stream().anyMatch(e -> e.startsWith("ROLLBACK(align)")),
                "回滚结论必须写入事件账本");
        log.info("[链头回退] 重放后高度5哈希 old={} new={}", oldHash5, newHash5);
    }

    @Test
    void reorgOnOneNodeIsDetectedViaParentHashAndRollsBack(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("s.json"));
        h.indexer.advance(0);
        String oldHash4 = h.store.getBlock(4).orElseThrow().hash();

        // 仅 node-a 从高度 4 起分叉，且其链头更高：节点切换/采信到 node-a 时检测分叉
        h.nodeA.installChain(SimulationChains.reorgChain(4, 9));
        // 让 node-b、node-c 暂时不可用，迫使采信 node-a 的分叉链
        h.nodeB.setDown(true);
        h.nodeC.setDown(true);

        AdvanceResult result = h.indexer.advance(0);
        log.info("[单节点重组] rolledBack={} indexed={} completed={} stop={}",
                result.rolledBackHeights(), result.indexedHeights(),
                result.completed(), result.stopReason());

        assertTrue(result.rolledBackHeights().containsAll(List.of(7L, 6L, 5L, 4L)),
                "分叉点之后的所有高度必须回滚: " + result.rolledBackHeights());
        assertEquals(9, h.store.headHeight(), "应在分叉链上重放到新链头 9");
        assertFalse(oldHash4.equals(h.store.getBlock(4).orElseThrow().hash()),
                "分叉点高度必须重放");
    }

    @Test
    void rollbackAndReplayConclusionsSurviveRestart(@TempDir Path dir) {
        Path stateFile = dir.resolve("restart-state.json");
        TestHarness first = TestHarness.create(stateFile);
        first.indexer.advance(0);
        first.nodeA.installChain(SimulationChains.reorgChain(5, 6));
        first.nodeB.installChain(SimulationChains.reorgChain(5, 6));
        first.nodeC.installChain(SimulationChains.reorgChain(5, 6));
        AdvanceResult reorgResult = first.indexer.advance(0);
        log.info("[重启前] 回滚={} 重放={} 头={}", reorgResult.rolledBackHeights(),
                reorgResult.indexedHeights(), first.store.headHeight());

        List<Block> blocksBeforeRestart = first.store.blocks();
        List<String> eventsBeforeRestart = first.store.events();

        // “重启”：基于同一快照文件重新构造存储与索引器、重新装相同的链
        TestHarness second = TestHarness.create(stateFile);
        second.nodeA.installChain(SimulationChains.reorgChain(5, 6));
        second.nodeB.installChain(SimulationChains.reorgChain(5, 6));
        second.nodeC.installChain(SimulationChains.reorgChain(5, 6));

        assertEquals(6, second.store.headHeight(), "重启后链头位点必须复现");
        assertEquals(blocksBeforeRestart.size(), second.store.blocks().size(),
                "重启后区块数量必须一致");
        for (int i = 0; i < blocksBeforeRestart.size(); i++) {
            Block before = blocksBeforeRestart.get(i);
            Block after = second.store.getBlock(before.height()).orElseThrow();
            assertEquals(before.hash(), after.hash(),
                    "重启后高度 " + before.height() + " 的哈希必须可复现");
        }
        assertEquals(eventsBeforeRestart, second.store.events(),
                "回滚与重放的事件账本必须随快照完整复现");

        // 重启后再次推进不得产生重复/变更（幂等且稳定）
        AdvanceResult afterRestart = second.indexer.advance(0);
        assertTrue(afterRestart.indexedHeights().isEmpty()
                        && afterRestart.rolledBackHeights().isEmpty(),
                "重启后对一致链重复推进必须是空操作");
        log.info("[重启后] 状态稳定 blocks={} 事件可复现", second.store.blocks().size());
    }

    @Test
    void finalizedDataCanNeverBeRolledBack(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("s.json"));
        h.indexer.advance(0);
        h.indexer.setFinalized(5);

        // 分叉点落在固化点之前：共同祖先 3 < finalized 5，必须拒绝覆盖。
        h.nodeA.installChain(SimulationChains.reorgChain(4, 9));
        h.nodeB.installChain(SimulationChains.reorgChain(4, 9));
        h.nodeC.installChain(SimulationChains.reorgChain(4, 9));

        // 从固化点之后发起请求（合法位点），专门验证“分叉不得回滚固化数据”。
        AdvanceResult result = h.indexer.advance(6);
        log.info("[固化保护] rolledBack={} rejected={} stop={}",
                result.rolledBackHeights(), result.rejectedFetches(), result.stopReason());
        assertTrue(result.rolledBackHeights().isEmpty(), "已固化数据绝不能被回滚");
        assertEquals(7, h.store.headHeight(), "拒绝分叉后必须维持原链状态");
        assertTrue(result.rejectedFetches().stream().findFirst().isPresent(),
                "冲突必须留下 CONTENT_MISMATCH 拒绝记录");
        assertEquals("CONTENT_MISMATCH",
                result.rejectedFetches().get(0).reason().name());

        // 固化位点自身不可回退
        assertThrows(IllegalArgumentException.class, () -> h.indexer.setFinalized(3),
                "固化位点只能单调递增");
    }

    @Test
    void nodeSwitchDuringAdvanceKeepsChainConsistent(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("s.json"));
        // node-a 仅在高度 3 定点超时；择优切换 node-b 继续，链必须连续一致
        h.nodeA.forceFault(3, com.github.highcumontoa.onchainrpcindexerjava.model.RpcErrorCode.TIMEOUT);
        AdvanceResult result = h.indexer.advance(0);
        log.info("[采集中切换] indexed={} completed={}", result.indexedHeights(), result.completed());
        assertTrue(result.completed(), "单高度故障切换后仍应完整采到链头");
        assertEquals(7, h.store.headHeight());
        // 父子链完整
        for (long height = 1; height <= 7; height++) {
            Block block = h.store.getBlock(height).orElseThrow();
            Block parent = h.store.getBlock(height - 1).orElseThrow();
            assertEquals(parent.hash(), block.parentHash(),
                    "高度 " + height + " 的父哈希必须正确衔接");
        }
        // 故障点恢复后重复推进仍幂等
        h.nodeA.clearForcedFaults();
        assertTrue(h.indexer.advance(0).indexedHeights().isEmpty());
    }
}
