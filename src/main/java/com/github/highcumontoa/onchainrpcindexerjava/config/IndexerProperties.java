package com.github.highcumontoa.onchainrpcindexerjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * 索引器配置。
 *
 * @param windowSize        节点成功率/延迟滑动窗口大小
 * @param failureThreshold  连续失败多少次后隔离（短暂抖动不达阈值不隔离）
 * @param recoverCooldown   隔离后多久允许恢复探测
 * @param probeTimeout      恢复探测超时
 * @param stateFile         索引快照文件路径（重启复现的依据）
 */
@ConfigurationProperties(prefix = "indexer")
public record IndexerProperties(
        @DefaultValue("10") int windowSize,
        @DefaultValue("3") int failureThreshold,
        @DefaultValue("2s") Duration recoverCooldown,
        @DefaultValue("500ms") Duration probeTimeout,
        @DefaultValue("data/index-state.json") String stateFile
) {
}
