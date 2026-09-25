package com.github.highcumontoa.onchainrpcindexerjava.node;

import com.github.highcumontoa.onchainrpcindexerjava.model.BlockData;

/** 链节点抽象：提供链头高度与按高度取块能力。 */
public interface ChainNode {

    String id();

    /** 当前链头高度。 */
    long latestHeight();

    /** 按高度取块；不存在返回 null；失败抛 NodeCallException。 */
    BlockData getBlock(long height);
}
