package com.github.highcumontoa.onchainrpcindexerjava.node;

/** 节点调用失败（超时、连接错误、节点主动报错等）。 */
public class NodeCallException extends RuntimeException {

    public NodeCallException(String message) {
        super(message);
    }

    public NodeCallException(String message, Throwable cause) {
        super(message, cause);
    }
}
