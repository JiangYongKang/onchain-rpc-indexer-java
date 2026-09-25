package com.github.highcumontoa.onchainrpcindexerjava.node;

/**
 * 节点健康与择优依据的只读视图。
 */
public record NodeHealthView(
        String nodeId,
        boolean healthy,
        boolean isolated,
        long consecutiveFailures,
        double avgLatencyMillis,
        double successRate,
        long isolatedAtMillis,
        long recoverAfterMillis,
        String lastError
) {
}
