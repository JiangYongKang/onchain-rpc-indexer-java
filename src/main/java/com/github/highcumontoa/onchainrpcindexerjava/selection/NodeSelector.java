package com.github.highcumontoa.onchainrpcindexerjava.selection;

import com.github.highcumontoa.onchainrpcindexerjava.node.ChainNode;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** 节点探测、择优与故障隔离。 */
public interface NodeSelector {

    void register(ChainNode node);

    Optional<ChainNode> node(String nodeId);

    /** 选出一个可用节点并给出依据；无可用节点返回 null。 */
    Selection select();

    /** 选节点但排除指定节点（如单次采集内已失败的节点）。 */
    default Selection selectExcluding(Set<String> excludedNodeIds) {
        return select();
    }

    void recordSuccess(String nodeId, long latencyMs);

    void recordFailure(String nodeId, String error);

    List<NodeStatusView> status();

    /** 选择结果：节点 + 可解释依据。 */
    record Selection(ChainNode node, SelectionReport report) {
    }
}
