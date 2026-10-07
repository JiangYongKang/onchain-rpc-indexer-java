package com.github.highcumontoa.onchainrpcindexerjava.selection;

/** 单个节点的可查询状态视图。 */
public record NodeStatusView(
        String nodeId,
        boolean isolated,
        int consecutiveFailures,
        long totalSuccesses,
        long totalFailures,
        double avgLatencyMs,
        String lastError,
        /** 当前状态：可用 / 已隔离 / 半开等待恢复确认。 */
        NodeState state,
        /** 距恢复探测的剩余冷却毫秒数；非隔离状态为 0。 */
        long remainingCooldownMs) {
}
