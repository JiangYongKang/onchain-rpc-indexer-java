package com.github.highcumontoa.onchainrpcindexerjava.store;

/**
 * 一次写入的结果说明。
 *
 * @param appended     该区块是否已写入（深重组时可能仅回滚未写入，需调用方从重滚位点继续采集）
 * @param rolledBackTo 本次回滚到的最高高度，无回滚为 -1
 */
public record ApplyResult(ApplyOutcome outcome, long height, String hash, boolean appended, long rolledBackTo) {
}
