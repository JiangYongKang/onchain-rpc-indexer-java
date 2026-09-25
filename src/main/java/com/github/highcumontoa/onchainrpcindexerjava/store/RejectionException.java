package com.github.highcumontoa.onchainrpcindexerjava.store;

/** 写入被拒绝，携带可区分原因。 */
public class RejectionException extends RuntimeException {

    private final RejectionReason reason;

    public RejectionException(RejectionReason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RejectionReason reason() {
        return reason;
    }
}
