package com.github.highcumontoa.onchainrpcindexerjava.catchup;

import java.util.Map;

/**
 * 追块进度视图（查询入口返回）。
 *
 * @param autoEnabled        自动追块是否开启
 * @param intervalMs         当前追块节奏
 * @param running            当前是否有一轮追块在执行
 * @param currentHeight      本轮正在追的高度（未在追为 -1）
 * @param targetHeight       本轮目标高度（未在追为 -1）
 * @param localHead          本地链头
 * @param lag                离最近已知链头还差多少（未知为 -1）
 * @param lastAppended       最近一轮写入块数
 * @param lastDuplicates     最近一轮幂等跳过块数
 * @param lastFailed         最近一轮失败块数
 * @param lastFailureReasons 最近一轮失败原因分类计数
 */
public record CatchupStatus(
        boolean autoEnabled,
        long intervalMs,
        boolean running,
        long currentHeight,
        long targetHeight,
        long localHead,
        long lag,
        long lastAppended,
        long lastDuplicates,
        long lastFailed,
        Map<String, Long> lastFailureReasons) {
}
