package com.github.highcumontoa.onchainrpcindexerjava.model;

/**
 * 归一化后的 RPC 错误码，跨节点保持统一、可解释。
 */
public enum RpcErrorCode {
    OK,
    TIMEOUT,
    UNAVAILABLE,
    NOT_FOUND,
    MALFORMED_RESPONSE,
    DECODE_ERROR,
    CONTENT_MISMATCH
}
