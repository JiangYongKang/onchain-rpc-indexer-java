package com.github.highcumontoa.onchainrpcindexerjava.follow;

import java.util.Map;

/**
 * 追块过程的可查询状态。
 *
 * @param autoEnabled        自动追块开关当前状态
 * @param running            当前是否有追块任务在执行
 * @param currentHeight      本地链头高度（已追到的位置）
 * @param finalizedHeight    固化位点
 * @param targetHeight       最近一轮观察到的链头目标，未知为 -1
 * @param lag                距目标还差多少高度，未知为 -1
 * @param lastRoundWritten   最近一轮真实写入块数
 * @param lastRoundSkipped   最近一轮因重复跳过的块数
 * @param lastRoundFailed    最近一轮失败块数
 * @param failureReasons     最近一轮失败原因分类计数
 * @param totalWritten       累计写入块数
 * @param totalSkipped       累计跳过块数
 * @param totalFailed        累计失败块数
 * @param lastError          最近一次失败详情
 * @param lastRoundAtEpochMs 最近一轮结束时间
 */
public record FollowStatus(
        boolean autoEnabled,
        boolean running,
        long currentHeight,
        long finalizedHeight,
        long targetHeight,
        long lag,
        long lastRoundWritten,
        long lastRoundSkipped,
        long lastRoundFailed,
        Map<String, Long> failureReasons,
        long totalWritten,
        long totalSkipped,
        long totalFailed,
        String lastError,
        long lastRoundAtEpochMs) {
}
