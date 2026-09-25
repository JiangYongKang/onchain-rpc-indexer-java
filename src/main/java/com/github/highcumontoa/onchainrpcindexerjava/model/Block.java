package com.github.highcumontoa.onchainrpcindexerjava.model;

import java.util.List;

/**
 * 区块数据模型。
 *
 * @param body 区块体内容（参与规范哈希计算，使校验方可独立重算哈希以识别内容篡改）
 */
public record Block(long height, String hash, String parentHash, String body, List<LogEvent> logs) {
}
