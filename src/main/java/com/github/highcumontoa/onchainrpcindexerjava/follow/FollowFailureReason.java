package com.github.highcumontoa.onchainrpcindexerjava.follow;

/** 一段追块失败的可区分原因。 */
public enum FollowFailureReason {
    /** 所有节点均被隔离或调用失败，无可用节点。 */
    NO_AVAILABLE_NODE,
    /** 所有可用节点在该高度都给不出可信数据（取不到/畸形/解不出）。 */
    NO_TRUSTWORTHY_DATA,
    /** 追块触及固化位点边界，被拒绝。 */
    BELOW_FINALIZED_CURSOR,
    /** 高度与本地链状态冲突（如不连续请求）。 */
    INVALID_HEIGHT,
    /** 重组对齐迭代超过上限。 */
    REORG_LIMIT_EXCEEDED
}
