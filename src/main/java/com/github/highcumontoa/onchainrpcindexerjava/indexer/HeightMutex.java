package com.github.highcumontoa.onchainrpcindexerjava.indexer;

/**
 * 按高度分段的互斥锁：同一高度的并发采集串行化，不同高度不互相阻塞。
 *
 * <p>配合存储层的按高度幂等写入，保证“同一高度并发请求/重复投递不产生重复记录”。
 */
public class HeightMutex {

    private static final int STRIPES = 256;
    private final Object[] locks;

    public HeightMutex() {
        locks = new Object[STRIPES];
        for (int i = 0; i < STRIPES; i++) {
            locks[i] = new Object();
        }
    }

    public Object lockFor(long height) {
        int index = (int) Math.floorMod(height, (long) STRIPES);
        return locks[index];
    }
}
