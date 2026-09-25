package com.github.highcumontoa.onchainrpcindexerjava.selection;

/** 单个节点的可查询状态视图。 */
public record NodeStatusView(
        String nodeId,
        boolean isolated,
        int consecutiveFailures,
        long totalSuccesses,
        long totalFailures,
        double avgLatencyMs,
        String lastError) {
}
