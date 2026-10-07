package com.github.highcumontoa.onchainrpcindexerjava.catchup;

import java.util.Map;

/**
 * 一段追块的结果。
 *
 * @param success        本地链头是否已对齐目标高度
 * @param fromHeight     段起点（含）
 * @param targetHeight   段目标（含）
 * @param headAfter      追块结束后的本地链头
 * @param appended       本轮真实写入的块数
 * @param duplicates     因重复投递被幂等跳过的块数
 * @param failed         失败的高度数
 * @param failureReasons 失败原因分类计数（可区分）
 * @param error          整体失败说明（成功时为 null）
 */
public record CatchupResult(
        boolean success,
        long fromHeight,
        long targetHeight,
        long headAfter,
        long appended,
        long duplicates,
        long failed,
        Map<String, Long> failureReasons,
        String error) {
}
