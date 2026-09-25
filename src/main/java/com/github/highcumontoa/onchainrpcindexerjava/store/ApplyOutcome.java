package com.github.highcumontoa.onchainrpcindexerjava.store;

/** 一次写入的结果类型。 */
public enum ApplyOutcome {
    /** 新区块被追加。 */
    APPENDED,
    /** 同高度同哈希重复投递，幂等忽略。 */
    DUPLICATE_IGNORED,
    /** 检测到重组：已回滚并对齐到新链。 */
    REORG_ROLLED_BACK
}
