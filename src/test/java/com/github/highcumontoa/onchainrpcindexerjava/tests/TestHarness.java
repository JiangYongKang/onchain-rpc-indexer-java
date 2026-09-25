package com.github.highcumontoa.onchainrpcindexerjava.tests;

import com.github.highcumontoa.onchainrpcindexerjava.config.SimulationChains;
import com.github.highcumontoa.onchainrpcindexerjava.indexer.ChainIndexer;
import com.github.highcumontoa.onchainrpcindexerjava.node.DefaultNodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.node.NodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.ChainRpcClient;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.SimulatedRpcNode;
import com.github.highcumontoa.onchainrpcindexerjava.storage.IndexStore;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * 测试夹具：构造三个本地模拟节点、选择器、文件存储与索引器。
 */
final class TestHarness {

    final SimulatedRpcNode nodeA;
    final SimulatedRpcNode nodeB;
    final SimulatedRpcNode nodeC;
    final List<ChainRpcClient> clients;
    final NodeSelector selector;
    final IndexStore store;
    final ChainIndexer indexer;

    private TestHarness(SimulatedRpcNode nodeA, SimulatedRpcNode nodeB, SimulatedRpcNode nodeC,
                        List<ChainRpcClient> clients, NodeSelector selector,
                        IndexStore store, ChainIndexer indexer) {
        this.nodeA = nodeA;
        this.nodeB = nodeB;
        this.nodeC = nodeC;
        this.clients = clients;
        this.selector = selector;
        this.store = store;
        this.indexer = indexer;
    }

    static TestHarness create(Path stateFile) {
        return create(stateFile, 3, Duration.ofMillis(50), Duration.ofMillis(300));
    }

    static TestHarness create(Path stateFile, int failureThreshold,
                              Duration recoverCooldown, Duration probeTimeout) {
        SimulatedRpcNode a = new SimulatedRpcNode("node-a", 1001L);
        SimulatedRpcNode b = new SimulatedRpcNode("node-b", 2002L);
        SimulatedRpcNode c = new SimulatedRpcNode("node-c", 3003L);
        a.setLatency(Duration.ofMillis(2), Duration.ofMillis(2));
        b.setLatency(Duration.ofMillis(15), Duration.ofMillis(5));
        c.setLatency(Duration.ofMillis(40), Duration.ofMillis(10));
        for (SimulatedRpcNode node : List.of(a, b, c)) {
            node.installChain(SimulationChains.initialChain());
        }
        List<ChainRpcClient> clients = List.of(a, b, c);
        DefaultNodeSelector selector = new DefaultNodeSelector(clients,
                10, failureThreshold, recoverCooldown, probeTimeout);
        IndexStore store = new IndexStore(stateFile);
        ChainIndexer indexer = new ChainIndexer(clients, selector, store);
        return new TestHarness(a, b, c, clients, selector, store, indexer);
    }
}
