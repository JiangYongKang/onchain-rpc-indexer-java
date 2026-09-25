package com.github.highcumontoa.onchainrpcindexerjava.node;

import com.github.highcumontoa.onchainrpcindexerjava.model.RpcErrorCode;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * 节点探测、择优、故障隔离与恢复抽象。
 */
public interface NodeSelector {

    /** 选出当前最优健康节点（含隔离恢复判定）。 */
    Optional<String> select();

    /** 选出当前最优健康节点，排除本轮已经尝试失败的节点。 */
    Optional<String> select(Collection<String> excludeIds);

    /** 记录一次调用结果，更新滑动窗口、抖动计数与隔离状态。 */
    void recordOutcome(String nodeId, RpcErrorCode code, long latencyMillis);

    /** 查询全部节点的健康/择优依据。 */
    List<NodeHealthView> health();
}
