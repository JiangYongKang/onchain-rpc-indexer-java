package com.github.highcumontoa.onchainrpcindexerjava.api;

import com.github.highcumontoa.onchainrpcindexerjava.catchup.CatchupResult;
import com.github.highcumontoa.onchainrpcindexerjava.catchup.CatchupScheduler;
import com.github.highcumontoa.onchainrpcindexerjava.catchup.CatchupService;
import com.github.highcumontoa.onchainrpcindexerjava.catchup.CatchupStatus;
import com.github.highcumontoa.onchainrpcindexerjava.ingest.IngestResult;
import com.github.highcumontoa.onchainrpcindexerjava.ingest.IngestionService;
import com.github.highcumontoa.onchainrpcindexerjava.selection.NodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.selection.NodeStatusView;
import com.github.highcumontoa.onchainrpcindexerjava.store.JournaledIndexStore;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 查询节点状态、索引状态并触发采集的 REST 入口。 */
@RestController
@RequestMapping("/api")
public class IndexController {

    private final NodeSelector selector;
    private final IngestionService ingestion;
    private final JournaledIndexStore store;
    private final CatchupService catchup;
    private final CatchupScheduler scheduler;

    public IndexController(NodeSelector selector, IngestionService ingestion, JournaledIndexStore store,
                           CatchupService catchup, CatchupScheduler scheduler) {
        this.selector = selector;
        this.ingestion = ingestion;
        this.store = store;
        this.catchup = catchup;
        this.scheduler = scheduler;
    }

    /** 节点状态与隔离/恢复依据。 */
    @GetMapping("/nodes")
    public List<NodeStatusView> nodes() {
        return selector.status();
    }

    /** 索引状态：链头、固化位点、已索引高度。 */
    @GetMapping("/index/state")
    public Map<String, Object> indexState() {
        return Map.of(
                "headHeight", store.headHeight(),
                "finalizedHeight", store.finalizedHeight(),
                "indexedHeights", store.indexedHeights());
    }

    /** 采集指定高度。 */
    @PostMapping("/collect")
    public IngestResult collect(@RequestParam long height) {
        return ingestion.collectOnce(height);
    }

    /** 采集当前链头。 */
    @PostMapping("/collect/latest")
    public IngestResult collectLatest() {
        return ingestion.collectLatest();
    }

    /** 固化到指定高度。 */
    @PostMapping("/index/finalize")
    public Map<String, Object> finalize(@RequestParam long height) {
        store.finalizeUpTo(height);
        return indexState();
    }

    /** 一次性追一段高度 [from, to]（含两端）。 */
    @PostMapping("/catchup")
    public CatchupResult catchUpRange(@RequestParam long from, @RequestParam long to) {
        return catchup.catchUpRange(from, to);
    }

    /** 手动触发一轮向链头追块（受批量上限约束）。 */
    @PostMapping("/catchup/head")
    public CatchupResult catchUpToHead(@RequestParam(defaultValue = "500") int batchSize) {
        return catchup.catchUpToHead(batchSize);
    }

    /** 追块进度：是否在追、当前高度、离链头差距、最近一轮统计。 */
    @GetMapping("/catchup/status")
    public CatchupStatus catchUpStatus() {
        return catchup.status(scheduler.isAutoEnabled(), scheduler.intervalMs());
    }

    /** 开关自动追块。 */
    @PostMapping("/catchup/auto")
    public Map<String, Object> setAuto(@RequestParam boolean enabled) {
        scheduler.setAutoEnabled(enabled);
        return Map.of("autoEnabled", scheduler.isAutoEnabled(), "intervalMs", scheduler.intervalMs());
    }

    /** 调整自动追块节奏（毫秒，下一轮生效）。 */
    @PostMapping("/catchup/auto/interval")
    public Map<String, Object> setInterval(@RequestParam long ms) {
        scheduler.setIntervalMs(ms);
        return Map.of("autoEnabled", scheduler.isAutoEnabled(), "intervalMs", scheduler.intervalMs());
    }
}
