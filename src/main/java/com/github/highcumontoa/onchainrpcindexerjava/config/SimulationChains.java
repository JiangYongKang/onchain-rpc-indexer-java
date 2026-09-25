package com.github.highcumontoa.onchainrpcindexerjava.config;

import com.github.highcumontoa.onchainrpcindexerjava.rpc.SimulatedRpcNode.BlockSpec;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.SimulatedRpcNode.LogSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * 模拟链的确定性构造：所有节点初始装载同一条链；重组场景从指定高度起替换为分叉链。
 */
public final class SimulationChains {

    public static final int INITIAL_HEIGHTS = 8;

    private SimulationChains() {
    }

    /** 初始链：高度 0..{@value #INITIAL_HEIGHTS}-1，部分高度带日志。 */
    public static List<BlockSpec> initialChain() {
        return chainUpTo(INITIAL_HEIGHTS - 1, "canonical");
    }

    /**
     * 从 {@code forkHeight} 起替换为分叉链（此前高度保持一致，从而共同祖先为 forkHeight-1）。
     * 分叉区块使用不同 body，因此哈希不同，可用于触发链头回退与重组回滚。
     */
    public static List<BlockSpec> reorgChain(int forkHeight, int newHeadHeight) {
        List<BlockSpec> chain = new ArrayList<>(chainUpTo(forkHeight - 1, "canonical"));
        for (long h = forkHeight; h <= newHeadHeight; h++) {
            chain.add(block(h, "reorg"));
        }
        return List.copyOf(chain);
    }

    /** 构造高度 0..head 的规范链。 */
    public static List<BlockSpec> chainUpTo(long head, String bodyTag) {
        List<BlockSpec> chain = new ArrayList<>();
        for (long h = 0; h <= head; h++) {
            chain.add(block(h, bodyTag));
        }
        return List.copyOf(chain);
    }

    /** 单个区块的确定性内容；偶数高度带一条日志。 */
    public static BlockSpec block(long height, String bodyTag) {
        String body = bodyTag + "-body-" + height;
        if (height % 2 == 0) {
            return BlockSpec.of(height, body,
                    LogSpec.of(0, "0x" + "ab".repeat(20), "evt-" + bodyTag + "-" + height));
        }
        return BlockSpec.of(height, body);
    }
}
