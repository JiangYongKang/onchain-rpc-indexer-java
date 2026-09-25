package com.github.highcumontoa.onchainrpcindexerjava.model;

/**
 * 归一化的数据拒绝原因（畸形/解码失败等），用于区分不同失败类型。
 */
public enum RejectReason {
    MALFORMED_BLOCK,
    DECODE_ERROR,
    CONTENT_MISMATCH
}
