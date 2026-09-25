package com.github.highcumontoa.onchainrpcindexerjava.rpc;

import com.github.highcumontoa.onchainrpcindexerjava.model.RpcErrorCode;

/**
 * 携带归一化错误码的 RPC 异常。
 */
public class RpcException extends RuntimeException {

    private final RpcErrorCode code;

    public RpcException(RpcErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public RpcException(RpcErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public RpcErrorCode code() {
        return code;
    }
}
