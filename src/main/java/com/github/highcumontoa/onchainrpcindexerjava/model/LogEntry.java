package com.github.highcumontoa.onchainrpcindexerjava.model;

import java.util.List;

/** 一条链上日志（已归一化）。 */
public record LogEntry(String address, List<String> topics, String data) {
}
