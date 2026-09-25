package com.github.highcumontoa.onchainrpcindexerjava.model;

import java.util.List;

/** 一个待索引的区块及其日志。 */
public record BlockData(long height, String hash, String parentHash, List<LogEntry> logs) {
}
