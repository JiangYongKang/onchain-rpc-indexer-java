package com.github.highcumontoa.onchainrpcindexerjava.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.highcumontoa.onchainrpcindexerjava.catchup.CatchupResult;
import com.github.highcumontoa.onchainrpcindexerjava.catchup.CatchupService;
import com.github.highcumontoa.onchainrpcindexerjava.catchup.CatchupStatus;
import com.github.highcumontoa.onchainrpcindexerjava.ingest.IngestResult;
import com.github.highcumontoa.onchainrpcindexerjava.ingest.IngestionService;
import com.github.highcumontoa.onchainrpcindexerjava.node.MockChainNode;
import com.github.highcumontoa.onchainrpcindexerjava.selection.DefaultNodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.selection.NodeStatusView;
import com.github.highcumontoa.onchainrpcindexerjava.store.JournaledIndexStore;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 自动追块：整段补齐、节点容错、隔离恢复、幂等与重启续追。 */
class CatchupServiceTest {

    @TempDir
    Path tempDir;

    private record Rig(List<MockChainNode> nodes, DefaultNodeSelector selector,
                       JournaledIndexStore store, IngestionService ingestion, CatchupService catchup) {
    }

    private Rig newRig(String journalName, long chainHead) {
        return newRig(journalName, chainHead, new AtomicLong(0));
    }

    private Rig newRig(String journalName, long chainHead, AtomicLong clock) {
        MockChainNode a = new MockChainNode("node-a", "seed", chainHead);
        MockChainNode b = new MockChainNode("node-b", "seed", chainHead);
        MockChainNode c = new MockChainNode("node-c", "seed", chainHead);
        DefaultNodeSelector selector = new DefaultNodeSelector(3, 1_000, clock::get);
        // 预设延迟画像：a 最优，b 次之，c 最慢（不打真实 sleep，确定性打分）
        selector.register(a);
        selector.register(b);
        selector.register(c);
        selector.recordSuccess("node-a", 1);
        selector.recordSuccess("node-b", 10);
        selector.recordSuccess("node-c", 50);
        JournaledIndexStore store = new JournaledIndexStore(tempDir.resolve(journalName));
        IngestionService ingestion = new IngestionService(selector, store);
        return new Rig(List.of(a, b, c), selector, store, ingestion,
                new CatchupService(ingestion, selector, store));
    }

    private static List<Long> range(long from, long to) {
        return LongStream.rangeClosed(from, to).boxed().toList();
    }

    @Test
    void catchUpRangeFillsWholeSegmentAndAligns() {
        Rig rig = newRig("range.jsonl", 1_000);
        CatchupResult r = rig.catchup().catchUpRange(0, 999);
        assertTrue(r.success(), "整段追块应成功: " + r.error());
        assertEquals(1_000, r.appended(), "一千个高度应全部真实写入");
        assertEquals(0, r.failed());
        assertEquals(999, r.headAfter(), "本地链头应与目标对齐");
        assertEquals(range(0, 999), rig.store().indexedHeights(), "中间不得缺块");
    }

    @Test
    void badDataFromOneNodeStillCompletesSegment() {
        Rig rig = newRig("baddata.jsonl", 100);
        // 最优节点 node-a 在高度 5 返回畸形数据
        rig.nodes().get(0).injectMalformedAt(5);

        CatchupResult r = rig.catchup().catchUpRange(0, 9);
        assertTrue(r.success(), "个别节点坏数据不应卡住整段: " + r.error());
        assertEquals(10, r.appended());
        assertEquals(range(0, 9), rig.store().indexedHeights());
        assertTrue(rig.store().blockAt(5).isPresent(), "高度 5 应由健康节点补齐");
        NodeStatusView a = rig.selector().status().stream()
                .filter(v -> v.nodeId().equals("node-a")).findFirst().orElseThrow();
        assertTrue(a.totalFailures() >= 1, "坏数据节点必须被记录失败");
        assertNotNull(a.lastError(), "最近一次失败原因应可查询");
    }

    @Test
    void isolatedNodeRejoinsAfterRecovery() {
        AtomicLong clock = new AtomicLong(0);
        Rig rig = newRig("recover.jsonl", 100, clock);
        MockChainNode a = rig.nodes().get(0);
        // node-a 连续在 3 个高度返回畸形数据 -> 连续失败达阈值被隔离
        a.injectMalformedAt(3);
        a.injectMalformedAt(4);
        a.injectMalformedAt(5);

        CatchupResult r1 = rig.catchup().catchUpRange(0, 9);
        assertTrue(r1.success(), "node-a 被隔离后应由健康节点完成整段: " + r1.error());
        NodeStatusView aView = viewOf(rig, "node-a");
        assertTrue(aView.isolated(), "连续返回不可信数据应被隔离");
        assertEquals("ISOLATED", aView.state());
        assertTrue(aView.recoveryRemainingMs() > 0, "隔离中应能看到剩余恢复等待时间");

        // 冷却期未满：仍被隔离，不参与
        clock.addAndGet(999);
        assertEquals("ISOLATED", viewOf(rig, "node-a").state());
        // 冷却期满且节点已修复：半开探测成功后恢复并重新参与
        clock.addAndGet(1);
        assertEquals("HALF_OPEN", viewOf(rig, "node-a").state(), "冷却期满应进入等待恢复确认状态");
        a.clearInjectedFaults();
        long successesBefore = viewOf(rig, "node-a").totalSuccesses();

        CatchupResult r2 = rig.catchup().catchUpRange(10, 19);
        assertTrue(r2.success(), "恢复后整段仍应追完: " + r2.error());
        NodeStatusView aAfter = viewOf(rig, "node-a");
        assertFalse(aAfter.isolated(), "探测成功应解除隔离");
        assertEquals("AVAILABLE", aAfter.state());
        assertTrue(aAfter.totalSuccesses() > successesBefore,
                "恢复后的节点必须真实重新参与采集");
    }

    @Test
    void duplicateTriggerAndConcurrentManualCollectProduceNoDuplicates() throws Exception {
        Rig rig = newRig("dup.jsonl", 100);
        ExecutorService pool = Executors.newFixedThreadPool(3);
        // 同一段被重复触发 + 手动采集同时跑
        List<Callable<Object>> tasks = new ArrayList<>();
        tasks.add(() -> rig.catchup().catchUpRange(0, 19));
        tasks.add(() -> rig.catchup().catchUpRange(0, 19));
        tasks.add(() -> {
            for (long h = 0; h <= 19; h++) {
                rig.ingestion().collectOnce(h);
            }
            return null;
        });
        List<Future<Object>> futures = pool.invokeAll(tasks);
        pool.shutdown();

        long totalAppended = 0;
        int ignored = 0;
        for (Future<Object> f : futures) {
            Object o = f.get();
            if (o instanceof CatchupResult r) {
                if (!r.success() && r.error() != null && r.error().contains("幂等忽略")) {
                    ignored++;
                } else {
                    assertTrue(r.success(), "追块不应出现意外失败: " + r.error());
                    totalAppended += r.appended();
                }
            }
        }
        assertTrue(ignored >= 1 || totalAppended <= 20,
                "重复触发只能算一次：要么被幂等忽略，要么全部命中幂等跳过");
        assertEquals(range(0, 19), rig.store().indexedHeights(),
                "并发与重复触发下不得产生重复记录或缺块");
        assertEquals(19, rig.store().headHeight());
    }

    @Test
    void restartResumesFromLastPosition() {
        Rig rig = newRig("restart.jsonl", 100);
        CatchupResult r1 = rig.catchup().catchUpRange(0, 9);
        assertTrue(r1.success());

        // 模拟进程重启：同一日志路径重建存储与追块服务
        JournaledIndexStore store2 = new JournaledIndexStore(tempDir.resolve("restart.jsonl"));
        assertEquals(9, store2.headHeight(), "重启后链头应从日志回放恢复");
        IngestionService ingestion2 = new IngestionService(rig.selector(), store2);
        CatchupService catchup2 = new CatchupService(ingestion2, rig.selector(), store2);

        CatchupResult r2 = catchup2.catchUpRange(0, 19);
        assertTrue(r2.success(), "重启后应能接着往下追: " + r2.error());
        assertEquals(10, r2.appended(), "只有未追的高度应真实写入");
        assertEquals(10, r2.duplicates(), "已索引的高度应幂等跳过、不重复处理");
        assertEquals(range(0, 19), store2.indexedHeights(), "不得缺块也不得重复");
    }

    @Test
    void allNodesUntrustworthyStopsWithDistinctReason() {
        Rig rig = newRig("allbad.jsonl", 100);
        // 全部节点在高度 4 都返回不可解码数据
        rig.nodes().forEach(n -> n.injectUndecodableAt(4));

        CatchupResult r = rig.catchup().catchUpRange(0, 9);
        assertFalse(r.success(), "所有节点都给不出可信数据时必须停下来");
        assertEquals(4, r.appended(), "失败高度之前的块应已写入");
        assertEquals(1, r.failed());
        assertEquals(1L, r.failureReasons().get("DECODE_FAILURE"), "失败原因必须可区分");
        assertTrue(r.error().contains("DECODE_FAILURE"), "失败记录应能解释原因: " + r.error());
        assertEquals(3, rig.store().headHeight(), "失败高度不得被当成成功写入");
        assertEquals(range(0, 3), rig.store().indexedHeights());
    }

    @Test
    void catchUpToHeadRespectsBatchAndExposesProgress() {
        Rig rig = newRig("auto.jsonl", 15);
        // 批量上限 5：一轮只追 5 个高度
        CatchupResult r1 = rig.catchup().catchUpToHead(5);
        assertTrue(r1.success());
        assertEquals(5, r1.appended());
        assertEquals(4, rig.store().headHeight());

        CatchupStatus mid = rig.catchup().status(true, 1_000);
        assertTrue(mid.autoEnabled());
        assertEquals(1_000, mid.intervalMs());
        assertFalse(mid.running());
        assertEquals(4, mid.localHead());
        assertEquals(11, mid.lag(), "离链头差距应可见");
        assertEquals(5, mid.lastAppended(), "最近一轮写入数应可见");

        // 继续追到链头对齐
        rig.catchup().catchUpToHead(500);
        CatchupStatus done = rig.catchup().status(true, 1_000);
        assertEquals(15, done.localHead());
        assertEquals(0, done.lag(), "对齐后差距应归零");
        assertEquals(range(0, 15), rig.store().indexedHeights());

        // 已对齐时再触发一轮：空跑成功，不重复写入
        CatchupResult idle = rig.catchup().catchUpToHead(500);
        assertTrue(idle.success());
        assertEquals(0, idle.appended());
    }

    private static NodeStatusView viewOf(Rig rig, String nodeId) {
        return rig.selector().status().stream()
                .filter(v -> v.nodeId().equals(nodeId)).findFirst().orElseThrow();
    }
}
