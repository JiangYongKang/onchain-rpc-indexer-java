package com.github.highcumontoa.onchainrpcindexerjava.api;

import com.github.highcumontoa.onchainrpcindexerjava.follow.BlockFollower;
import com.github.highcumontoa.onchainrpcindexerjava.follow.FollowRangeResult;
import com.github.highcumontoa.onchainrpcindexerjava.follow.FollowStatus;
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
    private final BlockFollower follower;

    public IndexController(NodeSelector selector, IngestionService ingestion,
                           JournaledIndexStore store, BlockFollower follower) {
        this.selector = selector;
        this.ingestion = ingestion;
        this.store = store;
        this.follower = follower;
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

    /** 一次性把 [from, to] 这段高度补齐；已有追块在跑时本次触发被忽略。 */
    @PostMapping("/follow/range")
    public FollowRangeResult followRange(@RequestParam long from, @RequestParam long to) {
        return follower.followRange(from, to);
    }

    /** 开关自动追块。 */
    @PostMapping("/follow/auto")
    public Map<String, Object> followAuto(@RequestParam boolean enabled) {
        follower.setAutoEnabled(enabled);
        return Map.of("autoEnabled", follower.isAutoEnabled());
    }

    /** 追块进度：当前高度、目标、落后量、最近一轮写入/跳过/失败与原因分类。 */
    @GetMapping("/follow/status")
    public FollowStatus followStatus() {
        return follower.status();
    }
}
