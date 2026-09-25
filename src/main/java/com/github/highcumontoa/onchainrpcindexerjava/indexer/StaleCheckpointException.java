package com.github.highcumontoa.onchainrpcindexerjava.indexer;

/**
 * 请求位点低于已固化位点时抛出：必须明确拒绝，而不是从中间继续。
 */
public class StaleCheckpointException extends RuntimeException {

    private final long requestedHeight;
    private final long finalizedHeight;

    public StaleCheckpointException(long requestedHeight, long finalizedHeight) {
        super("请求位点 " + requestedHeight + " 低于已固化位点 " + finalizedHeight
                + "，拒绝从中间继续；请从不低于固化位点的位置重新采集");
        this.requestedHeight = requestedHeight;
        this.finalizedHeight = finalizedHeight;
    }

    public long requestedHeight() {
        return requestedHeight;
    }

    public long finalizedHeight() {
        return finalizedHeight;
    }
}
