package com.github.highcumontoa.onchainrpcindexerjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.onchainrpcindexerjava.ingest.IngestResult;
import com.github.highcumontoa.onchainrpcindexerjava.ingest.IngestionService;
import com.github.highcumontoa.onchainrpcindexerjava.node.MockChainNode;
import com.github.highcumontoa.onchainrpcindexerjava.selection.DefaultNodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.store.ApplyOutcome;
import com.github.highcumontoa.onchainrpcindexerjava.store.JournaledIndexStore;
import com.github.highcumontoa.onchainrpcindexerjava.store.RejectionReason;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 并发采集下的幂等性、原子性与可区分拒绝原因。 */
class IngestionConsistencyTest {

    @TempDir
    Path tempDir;

    private record Rig(MockChainNode node, JournaledIndexStore store, IngestionService ingestion) {
    }

    private Rig newRig(String journalName) {
        MockChainNode node = new MockChainNode("n1", "seed", 1_000);
        DefaultNodeSelector selector = new DefaultNodeSelector();
        selector.register(node);
        JournaledIndexStore store = new JournaledIndexStore(tempDir.resolve(journalName));
        return new Rig(node, store, new IngestionService(selector, store));
    }

    private void collectRange(IngestionService ingestion, long from, long to) {
        for (long h = from; h <= to; h++) {
            IngestResult r = ingestion.collectOnce(h);
            assertTrue(r.success(), "采集高度 " + h + " 应成功: " + r.error());
        }
    }

    @Test
    void concurrentSameHeightProducesNoDuplicates() throws Exception {
        Rig rig = newRig("concurrent.jsonl");
        collectRange(rig.ingestion(), 0, 4);

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Callable<IngestResult>> tasks = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            tasks.add(() -> rig.ingestion().collectOnce(5));
        }
        List<Future<IngestResult>> futures = pool.invokeAll(tasks);
        pool.shutdown();

        int appended = 0;
        int duplicates = 0;
        for (Future<IngestResult> f : futures) {
            IngestResult r = f.get();
            assertTrue(r.success(), "并发采集不应失败: " + r.error());
            if (ApplyOutcome.APPENDED.name().equals(r.outcome())) {
                appended++;
            } else if (ApplyOutcome.DUPLICATE_IGNORED.name().equals(r.outcome())) {
                duplicates++;
            }
        }
        assertEquals(1, appended, "同一高度并发采集只能有一个真实写入");
        assertEquals(threads - 1, duplicates, "其余并发请求应幂等忽略");
        assertEquals(5, rig.store().headHeight());
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L), rig.store().indexedHeights(),
                "不得出现重复或半写记录");
    }

    @Test
    void duplicateDeliveryIsIdempotent() {
        Rig rig = newRig("dup.jsonl");
        collectRange(rig.ingestion(), 0, 3);
        IngestResult again = rig.ingestion().collectOnce(3);
        assertTrue(again.success());
        assertEquals(ApplyOutcome.DUPLICATE_IGNORED.name(), again.outcome());
        assertEquals(List.of(0L, 1L, 2L, 3L), rig.store().indexedHeights());
    }

    @Test
    void malformedBlockRejectedWithDistinctReason() {
        Rig rig = newRig("malformed.jsonl");
        rig.node().injectMalformedAt(2);
        collectRange(rig.ingestion(), 0, 1);
        IngestResult r = rig.ingestion().collectOnce(2);
        assertFalse(r.success());
        assertEquals(RejectionReason.MALFORMED_BLOCK, r.rejectionReason(), "畸形数据必须给出可区分原因");
        assertEquals(1, rig.store().headHeight(), "被拒绝的区块不得留下半写数据");
    }

    @Test
    void undecodableBlockRejectedWithDistinctReason() {
        Rig rig = newRig("undecodable.jsonl");
        rig.node().injectUndecodableAt(2);
        collectRange(rig.ingestion(), 0, 1);
        IngestResult r = rig.ingestion().collectOnce(2);
        assertFalse(r.success());
        assertEquals(RejectionReason.DECODE_FAILURE, r.rejectionReason(),
                "解码失败必须与畸形数据可区分");
        assertEquals(1, rig.store().headHeight());
    }

    @Test
    void belowFinalizedCursorIsRejected() {
        Rig rig = newRig("finalized.jsonl");
        collectRange(rig.ingestion(), 0, 5);
        rig.store().finalizeUpTo(4);

        IngestResult r = rig.ingestion().collectOnce(3);
        assertFalse(r.success());
        assertEquals(RejectionReason.BELOW_FINALIZED_CURSOR, r.rejectionReason(),
                "低于固化位点的位点必须被明确拒绝");
        // 固化位点上的数据不得被改动
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L), rig.store().indexedHeights());
    }
}
