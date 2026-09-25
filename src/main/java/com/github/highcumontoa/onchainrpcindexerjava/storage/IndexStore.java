package com.github.highcumontoa.onchainrpcindexerjava.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.onchainrpcindexerjava.model.Block;
import com.github.highcumontoa.onchainrpcindexerjava.model.LogEvent;
import com.github.highcumontoa.onchainrpcindexerjava.model.RejectReason;
import com.github.highcumontoa.onchainrpcindexerjava.model.RejectedFetch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 基于 JSON 快照的索引存储。
 *
 * <h3>一致性保证</h3>
 * <ul>
 *     <li>{@link IndexStore} 的对外方法全部 {@code synchronized}，与 {@code ChainIndexer} 的写锁配合，
 *         保证并发采集下无数据竞争；</li>
 *     <li>每次状态变更后，以“临时文件 + 原子 move”方式落盘，单次采集的写入要么整体可见、要么不可见，
 *         不会出现半写；</li>
 *     <li>区块以高度为键去重，同一高度重复/并发写入不会产生重复记录；</li>
 *     <li>回滚、拒绝留痕、位点推进、固化位点、事件账本都在同一份快照内，重启后结论可完整复现。</li>
 * </ul>
 */
public class IndexStore {

    private static final Logger log = LoggerFactory.getLogger(IndexStore.class);

    private record LogDto(@JsonProperty("index") int index,
                          @JsonProperty("address") String address,
                          @JsonProperty("data") String data) {
    }

    private record BlockDto(@JsonProperty("height") long height,
                            @JsonProperty("hash") String hash,
                            @JsonProperty("parentHash") String parentHash,
                            @JsonProperty("body") String body,
                            @JsonProperty("logs") List<LogDto> logs) {
        @JsonCreator
        BlockDto {
        }
    }

    private record RejectedDto(@JsonProperty("height") long height,
                               @JsonProperty("nodeId") String nodeId,
                               @JsonProperty("reason") String reason,
                               @JsonProperty("detail") String detail,
                               @JsonProperty("atMillis") long atMillis) {
    }

    private record State(@JsonProperty("blocks") List<BlockDto> blocks,
                         @JsonProperty("finalizedHeight") long finalizedHeight,
                         @JsonProperty("rejections") List<RejectedDto> rejections,
                         @JsonProperty("events") List<String> events) {
        @JsonCreator
        State {
        }
    }

    private final Path stateFile;
    private final ObjectMapper mapper = new ObjectMapper();

    private final TreeMap<Long, Block> blocks = new TreeMap<>();
    private long finalizedHeight = -1L;
    private final List<RejectedFetch> rejections = new ArrayList<>();
    private final List<String> events = new ArrayList<>();

    public IndexStore(Path stateFile) {
        this.stateFile = stateFile;
        load();
    }

    // ------------------------------------------------------------------
    // 查询
    // ------------------------------------------------------------------

    public synchronized long headHeight() {
        return blocks.isEmpty() ? -1L : blocks.lastKey();
    }

    public synchronized long finalizedHeight() {
        return finalizedHeight;
    }

    public synchronized Optional<Block> getBlock(long height) {
        return Optional.ofNullable(blocks.get(height));
    }

    public synchronized List<Block> blocks() {
        return List.copyOf(blocks.values());
    }

    // ------------------------------------------------------------------
    // 写入
    // ------------------------------------------------------------------

    /**
     * 幂等写入：相同（高度+哈希）直接返回 false 且不产生重复记录；
     * 同高度不同哈希属于调用方需先回滚的冲突，这里直接拒绝。
     */
    public synchronized boolean putBlock(Block block) {
        Block existing = blocks.get(block.height());
        if (existing != null) {
            if (!existing.hash().equals(block.hash())) {
                throw new IllegalStateException("高度 " + block.height()
                        + " 已存在不同哈希的区块（已索引=" + existing.hash()
                        + "，本次=" + block.hash() + "），必须先执行重组回滚再重放");
            }
            return false;
        }
        blocks.put(block.height(), block);
        persist();
        return true;
    }

    /**
     * 回滚所有高度大于 keepHeight 的已索引区块，返回被移除的高度（降序，便于审计）。
     * 不允许移除固化高度及以下的数据。
     */
    public synchronized List<Long> removeBlocksAbove(long keepHeight) {
        if (keepHeight < finalizedHeight) {
            throw new IllegalArgumentException("回滚位点 " + keepHeight
                    + " 低于已固化位点 " + finalizedHeight + "，已固化数据不可回滚");
        }
        List<Long> removed = new ArrayList<>(blocks.tailMap(keepHeight + 1L, true)
                .descendingKeySet().stream().toList());
        for (Long height : removed) {
            blocks.remove(height);
        }
        persist();
        return removed;
    }

    public synchronized void updateFinalized(long newFinalizedHeight) {
        if (newFinalizedHeight < finalizedHeight) {
            throw new IllegalArgumentException("固化位点不可回退：当前=" + finalizedHeight
                    + "，请求=" + newFinalizedHeight);
        }
        if (newFinalizedHeight == finalizedHeight) {
            return;
        }
        finalizedHeight = newFinalizedHeight;
        persist();
    }

    public synchronized void recordRejection(RejectedFetch rejection) {
        rejections.add(rejection);
        persist();
    }

    public synchronized List<RejectedFetch> rejections() {
        return List.copyOf(rejections);
    }

    public synchronized void appendEvent(String event) {
        events.add(event);
        persist();
    }

    public synchronized List<String> events() {
        return List.copyOf(events);
    }

    // ------------------------------------------------------------------
    // 快照持久化
    // ------------------------------------------------------------------

    private void persist() {
        try {
            List<BlockDto> blockDtos = new ArrayList<>(blocks.size());
            for (Block block : blocks.values()) {
                List<LogDto> logs = block.logs().stream()
                        .map(l -> new LogDto(l.index(), l.address(), l.data()))
                        .toList();
                blockDtos.add(new BlockDto(block.height(), block.hash(), block.parentHash(),
                        block.body(), logs));
            }
            List<RejectedDto> rejectedDtos = rejections.stream()
                    .map(r -> new RejectedDto(r.height(), r.nodeId(), r.reason().name(),
                            r.detail(), r.atMillis()))
                    .toList();
            State state = new State(blockDtos, finalizedHeight, rejectedDtos, List.copyOf(events));

            Path parent = stateFile.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
            Files.write(tmp, mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(state).getBytes(StandardCharsets.UTF_8));
            try {
                Files.move(tmp, stateFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicUnsupported) {
                Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("索引快照落盘失败: " + stateFile, e);
        }
    }

    private void load() {
        if (!Files.exists(stateFile)) {
            log.info("未发现既有索引快照，从空状态启动 stateFile={}", stateFile);
            return;
        }
        try {
            State state = mapper.readValue(Files.readString(stateFile, StandardCharsets.UTF_8), State.class);
            blocks.clear();
            if (state.blocks != null) {
                for (BlockDto dto : state.blocks) {
                    List<LogEvent> logs = dto.logs() == null ? List.of()
                            : dto.logs().stream()
                            .map(l -> new LogEvent(l.index(), l.address(), l.data()))
                            .toList();
                    blocks.put(dto.height(), new Block(dto.height(), dto.hash(), dto.parentHash(),
                            dto.body(), logs));
                }
            }
            finalizedHeight = state.finalizedHeight;
            rejections.clear();
            if (state.rejections != null) {
                for (RejectedDto dto : state.rejections) {
                    rejections.add(new RejectedFetch(dto.height(), dto.nodeId(),
                            RejectReason.valueOf(dto.reason()), dto.detail(), dto.atMillis()));
                }
            }
            events.clear();
            if (state.events != null) {
                events.addAll(state.events);
            }
            log.info("加载索引快照完成 stateFile={} head={} finalized={} blocks={} rejections={} events={}",
                    stateFile, headHeight(), finalizedHeight, blocks.size(),
                    rejections.size(), events.size());
        } catch (IOException e) {
            throw new IllegalStateException("索引快照读取失败（拒绝以空状态启动以免覆盖既有结论）: " + stateFile, e);
        }
    }
}
