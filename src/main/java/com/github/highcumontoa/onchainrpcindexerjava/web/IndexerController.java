package com.github.highcumontoa.onchainrpcindexerjava.web;

import com.github.highcumontoa.onchainrpcindexerjava.config.SimulationChains;
import com.github.highcumontoa.onchainrpcindexerjava.indexer.ChainIndexer;
import com.github.highcumontoa.onchainrpcindexerjava.indexer.StaleCheckpointException;
import com.github.highcumontoa.onchainrpcindexerjava.model.AdvanceResult;
import com.github.highcumontoa.onchainrpcindexerjava.model.Block;
import com.github.highcumontoa.onchainrpcindexerjava.model.RpcErrorCode;
import com.github.highcumontoa.onchainrpcindexerjava.node.NodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.ChainRpcClient;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.SimulatedRpcNode;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.SimulatedRpcNode.WireFault;
import com.github.highcumontoa.onchainrpcindexerjava.storage.IndexStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地验证与运维查询接口。
 */
@RestController
public class IndexerController {

    private final ChainIndexer indexer;
    private final NodeSelector selector;
    private final IndexStore store;
    private final Map<String, SimulatedRpcNode> nodes;

    public IndexerController(ChainIndexer indexer,
                             NodeSelector selector,
                             IndexStore store,
                             List<ChainRpcClient> clients) {
        this.indexer = indexer;
        this.selector = selector;
        this.store = store;
        Map<String, SimulatedRpcNode> map = new LinkedHashMap<>();
        for (ChainRpcClient client : clients) {
            if (client instanceof SimulatedRpcNode simulated) {
                map.put(simulated.id(), simulated);
            }
        }
        this.nodes = Map.copyOf(map);
    }

    /** 总览：位点、固化位点、节点选择依据、拒绝留痕、事件账本。 */
    @GetMapping("/api/status")
    public Map<String, Object> status() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("headHeight", store.headHeight());
        body.put("finalizedHeight", store.finalizedHeight());
        body.put("selectedNode", selector.select().orElse("<none>"));
        body.put("nodeHealth", selector.health());
        body.put("blocksIndexed", store.blocks().size());
        body.put("rejections", store.rejections());
        body.put("events", store.events());
        return body;
    }

    /** 节点择优/隔离依据（可解释）。 */
    @GetMapping("/api/nodes")
    public Object nodes() {
        return selector.health();
    }

    /** 已索引区块（去重后唯一记录）。 */
    @GetMapping("/api/blocks")
    public List<Block> blocks() {
        return store.blocks();
    }

    /** 触发一次从指定位点向链头的采集推进。 */
    @PostMapping("/api/indexer/advance")
    public AdvanceResult advance(@RequestParam(defaultValue = "0") long fromHeight) {
        return indexer.advance(fromHeight);
    }

    /** 推进固化位点。 */
    @PostMapping("/api/indexer/finalize")
    public Map<String, Object> finalizeHeight(@RequestParam long height) {
        indexer.setFinalized(height);
        return Map.of("finalizedHeight", store.finalizedHeight());
    }

    // ------------------------------------------------------------------
    // 故障注入与重组（本地模拟）
    // ------------------------------------------------------------------

    @PostMapping("/api/sim/nodes/{id}/down")
    public Map<String, Object> down(@PathVariable String id) {
        requireNode(id).setDown(true);
        return Map.of("node", id, "down", true);
    }

    @PostMapping("/api/sim/nodes/{id}/up")
    public Map<String, Object> up(@PathVariable String id) {
        SimulatedRpcNode node = requireNode(id);
        node.setDown(false);
        node.clearForcedFaults();
        return Map.of("node", id, "down", false);
    }

    /** 让指定节点从 forkHeight 起替换为分叉链（链头回退/重组）。 */
    @PostMapping("/api/sim/nodes/{id}/reorg")
    public Map<String, Object> reorg(@PathVariable String id,
                                     @RequestParam int forkHeight,
                                     @RequestParam int newHeadHeight) {
        requireNode(id).installChain(SimulationChains.reorgChain(forkHeight, newHeadHeight));
        return Map.of("node", id, "forkHeight", forkHeight, "newHeadHeight", newHeadHeight);
    }

    /** 对指定高度注入归一化 RPC 错误（TIMEOUT/UNAVAILABLE/NOT_FOUND 等）。 */
    @PostMapping("/api/sim/nodes/{id}/block/{height}/rpc-fault")
    public Map<String, Object> rpcFault(@PathVariable String id,
                                        @PathVariable long height,
                                        @RequestParam RpcErrorCode code) {
        requireNode(id).forceFault(height, code);
        return Map.of("node", id, "height", height, "code", code);
    }

    /** 对指定高度注入畸形报文（截断）。 */
    @PostMapping("/api/sim/nodes/{id}/block/{height}/malformed")
    public Map<String, Object> malformed(@PathVariable String id, @PathVariable long height) {
        installWireFault(id, height, WireFault.TRUNCATED);
        return Map.of("node", id, "height", height, "wireFault", WireFault.TRUNCATED);
    }

    /** 对指定高度注入不可解码字段。 */
    @PostMapping("/api/sim/nodes/{id}/block/{height}/undecodable")
    public Map<String, Object> undecodable(@PathVariable String id, @PathVariable long height) {
        installWireFault(id, height, WireFault.BAD_NUMBER);
        return Map.of("node", id, "height", height, "wireFault", WireFault.BAD_NUMBER);
    }

    /** 对指定高度注入内容篡改（哈希与内容不一致）。 */
    @PostMapping("/api/sim/nodes/{id}/block/{height}/hash-mismatch")
    public Map<String, Object> hashMismatch(@PathVariable String id, @PathVariable long height) {
        SimulatedRpcNode node = requireNode(id);
        List<com.github.highcumontoa.onchainrpcindexerjava.rpc.SimulatedRpcNode.BlockSpec> specs =
                new java.util.ArrayList<>();
        for (long h = 0; h <= node.getHeadHeight(); h++) {
            var spec = SimulationChains.block(h, "canonical");
            if (h == height) {
                spec = spec.withHashMismatch();
            }
            specs.add(spec);
        }
        node.installChain(specs);
        return Map.of("node", id, "height", height, "tampered", true);
    }

    @PostMapping("/api/sim/nodes/{id}/faults/clear")
    public Map<String, Object> clearFaults(@PathVariable String id) {
        SimulatedRpcNode node = requireNode(id);
        node.clearForcedFaults();
        node.installChain(SimulationChains.initialChain());
        node.setDown(false);
        return Map.of("node", id, "faultsCleared", true);
    }

    private void installWireFault(String id, long height, WireFault fault) {
        SimulatedRpcNode node = requireNode(id);
        List<com.github.highcumontoa.onchainrpcindexerjava.rpc.SimulatedRpcNode.BlockSpec> specs =
                new java.util.ArrayList<>();
        for (long h = 0; h <= node.getHeadHeight(); h++) {
            var spec = SimulationChains.block(h, "canonical");
            if (h == height) {
                spec = spec.withWireFault(fault);
            }
            specs.add(spec);
        }
        node.installChain(specs);
    }

    private SimulatedRpcNode requireNode(String id) {
        SimulatedRpcNode node = nodes.get(id);
        if (node == null) {
            throw new IllegalArgumentException("未知节点: " + id + "，可选: " + nodes.keySet());
        }
        return node;
    }

    // ------------------------------------------------------------------
    // 归一化错误响应
    // ------------------------------------------------------------------

    @ExceptionHandler(StaleCheckpointException.class)
    public ResponseEntity<Map<String, Object>> handleStale(StaleCheckpointException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "error", "STALE_CHECKPOINT",
                "message", e.getMessage(),
                "requestedHeight", e.requestedHeight(),
                "finalizedHeight", e.finalizedHeight()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegal(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of(
                "error", "INVALID_ARGUMENT", "message", e.getMessage()));
    }
}
