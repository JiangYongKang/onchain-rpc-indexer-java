package com.github.highcumontoa.onchainrpcindexerjava.follow;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 自动追块配置。 */
@ConfigurationProperties(prefix = "indexer.follow")
public class FollowerProperties {

    /** 是否在后台按节奏自动追块（可用 REST 运行时开关覆盖）。 */
    private boolean enabled = false;

    /** 自动追块节奏：每轮间隔毫秒数。 */
    private long intervalMs = 5_000;

    /** 每轮自动追块最多推进的高度数，防止单轮过长。 */
    private int maxBatchPerRound = 500;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    public void setIntervalMs(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    public int getMaxBatchPerRound() {
        return maxBatchPerRound;
    }

    public void setMaxBatchPerRound(int maxBatchPerRound) {
        this.maxBatchPerRound = maxBatchPerRound;
    }
}
