package com.github.highcumontoa.onchainrpcindexerjava.catchup;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** 自动追块配置：开关、节奏、单轮批量。 */
@Component
@ConfigurationProperties(prefix = "indexer.catchup")
public class CatchupProperties {

    /** 是否开启后台自动追块。 */
    private boolean autoEnabled = false;
    /** 自动追块节奏（毫秒）。 */
    private long intervalMs = 5_000;
    /** 单轮最多追的高度数，防止一轮跑太久。 */
    private int batchSize = 500;

    public boolean isAutoEnabled() {
        return autoEnabled;
    }

    public void setAutoEnabled(boolean autoEnabled) {
        this.autoEnabled = autoEnabled;
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    public void setIntervalMs(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
