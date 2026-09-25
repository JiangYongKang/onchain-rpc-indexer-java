package com.github.highcumontoa.onchainrpcindexerjava.tests;

import com.github.highcumontoa.onchainrpcindexerjava.indexer.StaleCheckpointException;
import com.github.highcumontoa.onchainrpcindexerjava.model.AdvanceResult;
import com.github.highcumontoa.onchainrpcindexerjava.model.Block;
import com.github.highcumontoa.onchainrpcindexerjava.model.RejectReason;
import com.github.highcumontoa.onchainrpcindexerjava.model.RejectedFetch;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.SimulatedRpcNode.BlockSpec;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.SimulatedRpcNode.WireFault;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发采集幂等性、写入原子性、畸形数据拒绝与固化位点保护。
 */
class IdempotentIndexingTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotentIndexingTest.class);

    @Test
    void fullAdvanceIndexesEveryHeightOnce(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("s.json"));
        AdvanceResult result = h.indexer.advance(0);
        log.info("[全量采集] {}", result);
        assertTrue(result.completed(), "应采集到链头");
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L), result.indexedHeights());
        assertEquals(7, h.store.headHeight());
        // 高度连续无空洞
        List<Long> heights = h.store.blocks().stream().map(Block::height).toList();
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L), heights);
    }

    @Test
    void repeatedAndConcurrentAdvanceProducesNoDuplicates(@TempDir Path dir) throws Exception {
        TestHarness h = TestHarness.create(dir.resolve("s.json"));

        // 串行重复投递：第二次不应新增任何记录
        AdvanceResult first = h.indexer.advance(0);
        AdvanceResult second = h.indexer.advance(0);
        log.info("[重复投递] first.indexed={} second.indexed={}",
                first.indexedHeights().size(), second.indexedHeights().size());
        assertTrue(first.indexedHeights().size() == 8 && second.indexedHeights().isEmpty(),
                "重复投递必须幂等：第二次无新增");

        // 并发重复投递：多线程同时推进，不允许重复记录/异常/数据竞争
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<AdvanceResult>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                start.await();
                return h.indexer.advance(0);
            }));
        }
        ready.await();
        start.countDown();
        for (Future<AdvanceResult> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        List<Block> blocks = h.store.blocks();
        Set<Long> uniqueHeights = blocks.stream().map(Block::height).collect(Collectors.toSet());
        log.info("[并发幂等] blocks={} uniqueHeights={} 事件账本条数={}",
                blocks.size(), uniqueHeights.size(), h.store.events().size());
        assertEquals(8, blocks.size(), "并发采集后每个高度只能有一条记录");
        assertEquals(8, uniqueHeights.size(), "不得出现重复高度");
    }

    @Test
    void concurrentAdvanceFromEmptyProducesExactlyOneRecordPerHeight(@TempDir Path dir)
            throws Exception {
        TestHarness h = TestHarness.create(dir.resolve("s.json"));
        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        Set<Throwable> errors = Collections.newSetFromMap(new ConcurrentHashMap<>());
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                try {
                    ready.countDown();
                    start.await();
                    h.indexer.advance(0);
                } catch (Throwable t) {
                    errors.add(t);
                }
            }));
        }
        ready.await();
        start.countDown();
        for (Future<?> future : futures) {
            future.get(15, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertTrue(errors.isEmpty(), "并发采集不应抛出异常: " + errors);
        assertEquals(8, h.store.blocks().size(), "空库并发首采也必须恰好每高度一条");
        log.info("[空库并发] blocks={} 无异常，写入一致", h.store.blocks().size());
    }

    @Test
    void malformedResponseIsRejectedWithDistinctReasonAndNotSilentlyDropped(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("s.json"));
        // node-a 与 node-b 在高度 4 返回截断报文（畸形），node-c 在高度 4 不可解码。
        installFaultAtHeight4(h, WireFault.TRUNCATED, WireFault.TRUNCATED, WireFault.BAD_NUMBER);

        AdvanceResult result = h.indexer.advance(0);
        log.info("[畸形拒绝] 推进结论 completed={} stopReason={} rejected={}",
                result.completed(), result.stopReason(), result.rejectedFetches());

        // 高度 0..3 正常写入，高度 4 被拒后停止，无半写越过 4
        assertEquals(3, h.store.headHeight(), "畸形数据必须阻断推进，不得半写到后续高度");
        List<RejectedFetch> rejections = h.store.rejections();
        assertEquals(3, rejections.size(), "三个节点的畸形/解码失败都必须留痕，不得静默丢弃");
        Set<RejectReason> reasons = rejections.stream().map(RejectedFetch::reason)
                .collect(Collectors.toSet());
        assertTrue(reasons.contains(RejectReason.MALFORMED_BLOCK), "截断报文应归为 MALFORMED_BLOCK");
        assertTrue(reasons.contains(RejectReason.DECODE_ERROR), "不可解码字段应归为 DECODE_ERROR");
        for (RejectedFetch rejection : rejections) {
            assertEquals(4, rejection.height());
            log.info("[拒绝留痕] node={} reason={} detail={}",
                    rejection.nodeId(), rejection.reason(), rejection.detail());
        }
    }

    @Test
    void hashMismatchIsRejectedAsContentMismatch(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("s.json"));
        List<BlockSpec> tampered = new ArrayList<>(
                com.github.highcumontoa.onchainrpcindexerjava.config.SimulationChains.initialChain());
        tampered.set(5, tampered.get(5).withHashMismatch());
        for (var node : List.of(h.nodeA, h.nodeB, h.nodeC)) {
            node.installChain(tampered);
        }
        AdvanceResult result = h.indexer.advance(0);
        log.info("[内容篡改] rejected={}", result.rejectedFetches());
        assertEquals(4, h.store.headHeight(), "内容篡改高度必须阻断");
        assertTrue(h.store.rejections().stream()
                        .allMatch(r -> r.reason() == RejectReason.CONTENT_MISMATCH),
                "哈希与内容不一致必须归一化为 CONTENT_MISMATCH");
    }

    @Test
    void requestBelowFinalizedIsRejectedNotResumed(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("s.json"));
        h.indexer.advance(0);
        h.indexer.setFinalized(5);
        log.info("[固化位点] finalized={}", h.store.finalizedHeight());

        StaleCheckpointException ex = assertThrows(StaleCheckpointException.class,
                () -> h.indexer.advance(3),
                "位点低于已固化位点必须明确拒绝");
        log.warn("[过期位点] 拒绝请求 requested={} finalized={} message={}",
                ex.requestedHeight(), ex.finalizedHeight(), ex.getMessage());
        assertEquals(3, ex.requestedHeight());
        assertEquals(5, ex.finalizedHeight());
    }

    @Test
    void gapRequestIsRejected(@TempDir Path dir) {
        TestHarness h = TestHarness.create(dir.resolve("s.json"));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> h.indexer.advance(5), "空库从中间高度开始必须拒绝以避免空洞");
        log.warn("[空洞拒绝] {}", ex.getMessage());
    }

    private void installFaultAtHeight4(TestHarness h, WireFault a, WireFault b, WireFault c) {
        installFault(h.nodeA, 4, a);
        installFault(h.nodeB, 4, b);
        installFault(h.nodeC, 4, c);
    }

    private void installFault(com.github.highcumontoa.onchainrpcindexerjava.rpc.SimulatedRpcNode node,
                              long height, WireFault fault) {
        List<BlockSpec> specs = new ArrayList<>(
                com.github.highcumontoa.onchainrpcindexerjava.config.SimulationChains.initialChain());
        specs.set((int) height, specs.get((int) height).withWireFault(fault));
        node.installChain(specs);
    }
}
