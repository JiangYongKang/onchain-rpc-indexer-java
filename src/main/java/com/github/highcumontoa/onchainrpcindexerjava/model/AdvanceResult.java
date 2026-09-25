package com.github.highcumontoa.onchainrpcindexerjava.model;

import java.util.List;

/**
 * 单次采集推进的结论（可解释、可在日志中打印）。
 */
public record AdvanceResult(
        long requestedFromHeight,
        long fromHeight,
        List<Long> indexedHeights,
        List<Long> rolledBackHeights,
        List<RejectedFetch> rejectedFetches,
        String selectedNodeBefore,
        String selectedNodeAfter,
        boolean completed,
        String stopReason
) {
}
