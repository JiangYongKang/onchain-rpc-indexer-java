package com.github.highcumontoa.onchainrpcindexerjava.model;

/**
 * 日志事件数据模型。
 */
public record LogEvent(int index, String address, String data) {
}
