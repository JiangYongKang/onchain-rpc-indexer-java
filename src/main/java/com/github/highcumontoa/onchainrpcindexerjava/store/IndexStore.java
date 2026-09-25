package com.github.highcumontoa.onchainrpcindexerjava.store;

import com.github.highcumontoa.onchainrpcindexerjava.model.BlockData;
import java.util.Optional;

/** 索引存储：原子写入、幂等、重组回滚、固化位点。 */
public interface IndexStore {

    /** 原子写入一个区块；校验失败抛 RejectionException。 */
    ApplyResult apply(BlockData block);

    Optional<BlockData> blockAt(long height);

    /** 当前已索引的链头高度，无数据返回 -1。 */
    long headHeight();

    long finalizedHeight();

    /** 固化到指定高度（低于该高度的请求将被拒绝）。 */
    void finalizeUpTo(long height);

    /** 回滚到指定高度（含）以下，返回被回滚的最高高度；无操作返回当前头。 */
    long rollbackTo(long heightInclusive);
}
