package com.github.highcumontoa.onchainrpcindexerjava.config;

import com.github.highcumontoa.onchainrpcindexerjava.indexer.ChainIndexer;
import com.github.highcumontoa.onchainrpcindexerjava.node.DefaultNodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.node.NodeSelector;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.ChainRpcClient;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.SimulatedRpcNode;
import com.github.highcumontoa.onchainrpcindexerjava.storage.IndexStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

/**
 * 本地多节点装配：三个可启动的模拟节点，延迟/成功率存在差异，用于验证择优、隔离与恢复。
 *
 * <ul>
 *     <li>node-a：低延迟、稳定（默认最优）；</li>
 *     <li>node-b：中等延迟、稳定（备选）；</li>
 *     <li>node-c：高延迟、自带少量瞬时抖动（验证短暂抖动不触发误判）。</li>
 * </ul>
 */
@Configuration
@EnableConfigurationProperties(IndexerProperties.class)
public class IndexerConfig {

    public static final String NODE_A = "node-a";
    public static final String NODE_B = "node-b";
    public static final String NODE_C = "node-c";

    @Bean
    public SimulatedRpcNode nodeA() {
        SimulatedRpcNode node = new SimulatedRpcNode(NODE_A, 1001L);
        node.setLatency(Duration.ofMillis(5), Duration.ofMillis(3));
        node.installChain(SimulationChains.initialChain());
        return node;
    }

    @Bean
    public SimulatedRpcNode nodeB() {
        SimulatedRpcNode node = new SimulatedRpcNode(NODE_B, 2002L);
        node.setLatency(Duration.ofMillis(40), Duration.ofMillis(10));
        node.installChain(SimulationChains.initialChain());
        return node;
    }

    @Bean
    public SimulatedRpcNode nodeC() {
        SimulatedRpcNode node = new SimulatedRpcNode(NODE_C, 3003L);
        node.setLatency(Duration.ofMillis(120), Duration.ofMillis(30));
        // 少量瞬时超时/不可用：应被滑动窗口吸收，不触发隔离。
        node.setTimeoutRate(0.08);
        node.installChain(SimulationChains.initialChain());
        return node;
    }

    @Bean
    public List<ChainRpcClient> chainRpcClients(SimulatedRpcNode nodeA,
                                                SimulatedRpcNode nodeB,
                                                SimulatedRpcNode nodeC) {
        return List.of(nodeA, nodeB, nodeC);
    }

    @Bean
    public NodeSelector nodeSelector(List<ChainRpcClient> clients, IndexerProperties properties) {
        return new DefaultNodeSelector(clients,
                properties.windowSize(),
                properties.failureThreshold(),
                properties.recoverCooldown(),
                properties.probeTimeout());
    }

    @Bean
    public IndexStore indexStore(IndexerProperties properties) {
        return new IndexStore(Path.of(properties.stateFile()));
    }

    @Bean
    public ChainIndexer chainIndexer(List<ChainRpcClient> clients,
                                     NodeSelector nodeSelector,
                                     IndexStore indexStore) {
        return new ChainIndexer(clients, nodeSelector, indexStore);
    }
}
