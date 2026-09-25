package com.github.highcumontoa.onchainrpcindexerjava.node;

import com.github.highcumontoa.onchainrpcindexerjava.selection.NodeSelector;
import jakarta.annotation.PostConstruct;
import java.util.List;
import org.springframework.context.annotation.Configuration;

/** 本地运行用：注册三个延迟不同的模拟节点。 */
@Configuration
public class MockNodeConfig {

    private final NodeSelector selector;
    private final List<MockChainNode> nodes;

    public MockNodeConfig(NodeSelector selector) {
        this.selector = selector;
        MockChainNode a = new MockChainNode("node-a", "mainnet-sim", 100);
        a.setLatencyMs(5);
        MockChainNode b = new MockChainNode("node-b", "mainnet-sim", 100);
        b.setLatencyMs(20);
        MockChainNode c = new MockChainNode("node-c", "mainnet-sim", 100);
        c.setLatencyMs(50);
        this.nodes = List.of(a, b, c);
    }

    @PostConstruct
    void register() {
        nodes.forEach(selector::register);
    }
}
