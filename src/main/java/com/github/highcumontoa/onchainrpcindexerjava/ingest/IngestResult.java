package com.github.highcumontoa.onchainrpcindexerjava.ingest;

import com.github.highcumontoa.onchainrpcindexerjava.store.RejectionReason;

/** 一次采集的结果（成功含写入结论，失败含归一化原因）。 */
public record IngestResult(
        boolean success,
        Long height,
        String nodeId,
        String outcome,
        RejectionReason rejectionReason,
        String error) {

    public static IngestResult ok(long height, String nodeId, String outcome) {
        return new IngestResult(true, height, nodeId, outcome, null, null);
    }

    public static IngestResult rejected(long height, RejectionReason reason, String error) {
        return new IngestResult(false, height, null, null, reason, error);
    }

    /** 数据类拒绝但携带数据来源节点，供追块层切换节点重试。 */
    public static IngestResult rejectedAtNode(long height, String nodeId, RejectionReason reason, String error) {
        return new IngestResult(false, height, nodeId, null, reason, error);
    }

    public static IngestResult failed(Long height, String error) {
        return new IngestResult(false, height, null, null, null, error);
    }
}
