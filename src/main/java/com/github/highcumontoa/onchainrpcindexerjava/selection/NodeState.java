package com.github.highcumontoa.onchainrpcindexerjava.selection;

/** 节点当前所处状态。 */
public enum NodeState {
    /** 正常参与选择。 */
    AVAILABLE,
    /** 连续失败达阈值被隔离，冷却期未满，不参与选择。 */
    ISOLATED,
    /** 冷却期已满，等待一次探测确认恢复（半开）。 */
    HALF_OPEN
}
