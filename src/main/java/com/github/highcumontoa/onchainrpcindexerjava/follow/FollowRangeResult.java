package com.github.highcumontoa.onchainrpcindexerjava.follow;

import java.util.List;
import java.util.Map;

/**
 * 一段追块的结论：要么本地链头对齐到目标高度，要么给出可区分的失败原因。
 *
 * @param started       本轮是否真正开始执行（已有追块在跑时为 false，重复触发只算一次）
 * @param success       是否已对齐到目标高度
 * @param from          请求起始高度
 * @param to            目标高度
 * @param finalHead     结束时本地链头
 * @param written       本轮真实写入的块数
 * @param skipped       因已索引/重复而跳过的块数
 * @param failureReason 失败分类（成功时为 null）
 * @param error         失败详情（成功时为 null）
 * @param nodeErrors    各节点最近一条不可信记录（排障用）
 */
public record FollowRangeResult(
        boolean started,
        boolean success,
        long from,
        long to,
        long finalHead,
        long written,
        long skipped,
        FollowFailureReason failureReason,
        String error,
        Map<String, List<String>> nodeErrors) {

    public static FollowRangeResult notStarted(long from, long to) {
        return new FollowRangeResult(false, false, from, to, -1, 0, 0, null,
                "已有追块任务在执行，本次触发被忽略（只算一次）", Map.of());
    }
}
