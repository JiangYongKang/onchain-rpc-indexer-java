package com.github.highcumontoa.onchainrpcindexerjava.store;

/** 写入被拒绝的可区分原因。 */
public enum RejectionReason {
    /** 区块字段缺失或格式非法。 */
    MALFORMED_BLOCK,
    /** 日志/字段解码失败。 */
    DECODE_FAILURE,
    /** 请求位点低于已固化位点。 */
    BELOW_FINALIZED_CURSOR,
    /** 高度非正或与链头语义冲突。 */
    INVALID_HEIGHT
}
