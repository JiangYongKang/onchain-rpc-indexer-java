package com.github.highcumontoa.onchainrpcindexerjava.catchup;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 自动追块调度器：按可配置节奏在后台触发追块。
 * - 开关与节奏来自 CatchupProperties，可运行时调整（下一轮生效）；
 * - 每轮结束后按当前节奏重新排程，节奏调小会及时提速；
 * - 单线程排程，且 CatchupService 自身互斥，不会叠加并发追块。
 */
@Component
public class CatchupScheduler {

    private static final Logger log = LoggerFactory.getLogger(CatchupScheduler.class);

    private final CatchupService catchup;
    private final CatchupProperties properties;
    private final ScheduledExecutorService executor;
    private volatile boolean stopped;

    public CatchupScheduler(CatchupService catchup, CatchupProperties properties) {
        this.catchup = catchup;
        this.properties = properties;
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "catchup-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    @PostConstruct
    void start() {
        log.info("自动追块调度器启动: enabled={} intervalMs={} batchSize={}",
                properties.isAutoEnabled(), properties.getIntervalMs(), properties.getBatchSize());
        executor.schedule(this::tick, properties.getIntervalMs(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    void stop() {
        stopped = true;
        executor.shutdownNow();
    }

    /** 运行时开关自动追块。 */
    public void setAutoEnabled(boolean enabled) {
        properties.setAutoEnabled(enabled);
        log.info("自动追块开关切换: enabled={}", enabled);
    }

    /** 运行时调整追块节奏（下一轮生效）。 */
    public void setIntervalMs(long intervalMs) {
        properties.setIntervalMs(intervalMs);
        log.info("自动追块节奏调整: intervalMs={}", intervalMs);
    }

    public boolean isAutoEnabled() {
        return properties.isAutoEnabled();
    }

    public long intervalMs() {
        return properties.getIntervalMs();
    }

    private void tick() {
        try {
            if (properties.isAutoEnabled()) {
                catchup.catchUpToHead(properties.getBatchSize());
            }
        } catch (Exception e) {
            log.warn("自动追块一轮异常: {}", e.getMessage());
        } finally {
            if (!stopped) {
                executor.schedule(this::tick, Math.max(100, properties.getIntervalMs()),
                        TimeUnit.MILLISECONDS);
            }
        }
    }
}
