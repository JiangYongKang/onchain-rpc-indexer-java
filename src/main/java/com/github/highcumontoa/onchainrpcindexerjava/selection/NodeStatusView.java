package com.github.highcumontoa.onchainrpcindexerjava.selection;

/**
 * 单个节点的可查询状态视图。
 *
 * @param state               AVAILABLE（可用）/ ISOLATED（被隔离）/ HALF_OPEN（冷却期满，等待恢复探测）
 * @param recoveryRemainingMs 距恢复探测还剩多少毫秒（非隔离状态为 0）
 * @param lastError           最近一次失败原因
 */
public record NodeStatusView(
        String nodeId,
        boolean isolated,
        String state,
        long recoveryRemainingMs,
        int consecutiveFailures,
        long totalSuccesses,
        long totalFailures,
        double avgLatencyMs,
        String lastError) {
}
