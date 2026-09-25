package com.github.highcumontoa.onchainrpcindexerjava.model;

/**
 * 一次被拒绝数据的留痕记录（不静默丢弃）。
 */
public record RejectedFetch(
        long height,
        String nodeId,
        RejectReason reason,
        String detail,
        long atMillis
) {
}
