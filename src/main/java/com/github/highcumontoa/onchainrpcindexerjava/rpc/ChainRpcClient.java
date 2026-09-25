package com.github.highcumontoa.onchainrpcindexerjava.rpc;

import com.github.highcumontoa.onchainrpcindexerjava.model.Block;

/**
 * 链节点 RPC 客户端抽象（本地模拟节点实现该接口）。
 */
public interface ChainRpcClient {

    /** 节点标识。 */
    String id();

    /** 当前链头高度。 */
    long getHeadHeight();

    /** 按高度拉取区块；失败时抛出 {@link RpcException}。 */
    Block getBlockByHeight(long height);
}
