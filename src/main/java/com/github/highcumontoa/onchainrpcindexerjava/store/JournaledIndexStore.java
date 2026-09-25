package com.github.highcumontoa.onchainrpcindexerjava.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.highcumontoa.onchainrpcindexerjava.model.BlockData;
import com.github.highcumontoa.onchainrpcindexerjava.model.LogEntry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 索引存储：
 * - 所有写操作串行化（synchronized），单次采集写入原子、无半写、无数据竞争；
 * - 同高度同哈希重复投递幂等忽略；
 * - 父哈希不匹配时按重组处理：回滚到分叉点以下再对齐；
 * - 低于/触及固化位点的写请求被明确拒绝；
 * - 每次变更先写追加日志（apply/rollback/finalize），重启后回放，结论可复现。
 */
@Component
public class JournaledIndexStore implements IndexStore {

    private static final Logger log = LoggerFactory.getLogger(JournaledIndexStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path journalPath;
    private final TreeMap<Long, BlockData> blocks = new TreeMap<>();
    private long finalizedHeight = -1;

    public JournaledIndexStore(@Value("${indexer.journal.path:./data/index-journal.jsonl}") Path journalPath) {
        this.journalPath = journalPath;
        replay();
    }

    // ---- 写入路径（全部串行，保证原子性） ----

    @Override
    public synchronized ApplyResult apply(BlockData block) {
        validate(block);
        long height = block.height();
        long head = headHeight();

        if (height <= finalizedHeight) {
            BlockData stored = blocks.get(height);
            if (stored != null && stored.hash().equals(block.hash())) {
                return new ApplyResult(ApplyOutcome.DUPLICATE_IGNORED, height, block.hash(), false, -1);
            }
            throw new RejectionException(RejectionReason.BELOW_FINALIZED_CURSOR,
                    "高度 " + height + " 低于或触及固化位点 " + finalizedHeight + "，拒绝从中间继续");
        }

        if (height <= head) {
            BlockData stored = blocks.get(height);
            if (stored.hash().equals(block.hash())) {
                return new ApplyResult(ApplyOutcome.DUPLICATE_IGNORED, height, block.hash(), false, -1);
            }
            // 重组：先回滚冲突段（>= height），再尝试对齐
            doRollback(height);
            long rolledTo = headHeight();
            if (parentMatches(block)) {
                doAppend(block);
                log.info("重组回滚并对齐: height={} rolledBackTo={} newHash={}", height, rolledTo, block.hash());
                return new ApplyResult(ApplyOutcome.REORG_ROLLED_BACK, height, block.hash(), true, rolledTo);
            }
            // 分叉点更深：再回滚一层，由调用方从重滚位点继续采集
            if (rolledTo <= finalizedHeight) {
                throw new RejectionException(RejectionReason.BELOW_FINALIZED_CURSOR,
                        "重组深度超过固化位点 " + finalizedHeight + "，拒绝继续回滚");
            }
            doRollback(rolledTo);
            log.info("重组分叉点更深: height={} rolledBackTo={}", height, headHeight());
            return new ApplyResult(ApplyOutcome.REORG_ROLLED_BACK, height, block.hash(), false, headHeight());
        }

        if (height != head + 1) {
            throw new RejectionException(RejectionReason.INVALID_HEIGHT,
                    "高度 " + height + " 不连续，当前链头 " + head + "，期望 " + (head + 1));
        }
        if (!parentMatches(block)) {
            // 链头回退/采信节点切换：新区块父哈希与本地链头不符，回滚一层交由调用方重采
            if (head < 0) {
                throw new RejectionException(RejectionReason.MALFORMED_BLOCK,
                        "首区块父哈希无法校验: " + block.parentHash());
            }
            if (head <= finalizedHeight) {
                throw new RejectionException(RejectionReason.BELOW_FINALIZED_CURSOR,
                        "重组深度超过固化位点 " + finalizedHeight + "，拒绝继续回滚");
            }
            doRollback(head);
            log.info("父哈希不匹配，回滚一层: height={} rolledBackTo={}", height, headHeight());
            return new ApplyResult(ApplyOutcome.REORG_ROLLED_BACK, height, block.hash(), false, headHeight());
        }
        doAppend(block);
        return new ApplyResult(ApplyOutcome.APPENDED, height, block.hash(), true, -1);
    }

    @Override
    public synchronized Optional<BlockData> blockAt(long height) {
        return Optional.ofNullable(blocks.get(height));
    }

    @Override
    public synchronized long headHeight() {
        return blocks.isEmpty() ? -1 : blocks.lastKey();
    }

    @Override
    public synchronized long finalizedHeight() {
        return finalizedHeight;
    }

    @Override
    public synchronized void finalizeUpTo(long height) {
        if (height > finalizedHeight) {
            finalizedHeight = Math.min(height, headHeight());
            appendJournal("finalize", null, finalizedHeight);
        }
    }

    @Override
    public synchronized long rollbackTo(long heightInclusive) {
        if (heightInclusive <= finalizedHeight) {
            throw new RejectionException(RejectionReason.BELOW_FINALIZED_CURSOR,
                    "回滚目标 " + heightInclusive + " 低于或触及固化位点 " + finalizedHeight);
        }
        doRollback(heightInclusive);
        return headHeight();
    }

    // ---- 内部原语 ----

    private void doAppend(BlockData block) {
        appendJournal("apply", block, -1);
        blocks.put(block.height(), block);
    }

    private void doRollback(long heightInclusive) {
        if (blocks.isEmpty() || heightInclusive > blocks.lastKey()) {
            return;
        }
        appendJournal("rollback", null, heightInclusive);
        blocks.tailMap(heightInclusive, true).clear();
    }

    private boolean parentMatches(BlockData block) {
        if (block.height() == 0) {
            return true;
        }
        BlockData parent = blocks.get(block.height() - 1);
        return parent != null && parent.hash().equals(block.parentHash());
    }

    // ---- 校验：畸形与解码失败必须可区分 ----

    private void validate(BlockData block) {
        if (block == null) {
            throw new RejectionException(RejectionReason.MALFORMED_BLOCK, "区块为 null");
        }
        if (block.height() < 0) {
            throw new RejectionException(RejectionReason.INVALID_HEIGHT, "高度为负: " + block.height());
        }
        if (block.hash() == null || !block.hash().startsWith("0x")) {
            throw new RejectionException(RejectionReason.MALFORMED_BLOCK,
                    "区块哈希缺失或非法: height=" + block.height());
        }
        if (block.parentHash() == null || !block.parentHash().startsWith("0x")) {
            throw new RejectionException(RejectionReason.MALFORMED_BLOCK,
                    "父哈希缺失或非法: height=" + block.height());
        }
        if (block.logs() == null) {
            throw new RejectionException(RejectionReason.MALFORMED_BLOCK,
                    "日志列表缺失: height=" + block.height());
        }
        for (LogEntry entry : block.logs()) {
            if (entry == null || entry.address() == null || entry.topics() == null || entry.data() == null) {
                throw new RejectionException(RejectionReason.MALFORMED_BLOCK,
                        "日志字段缺失: height=" + block.height());
            }
            if (!isHex(entry.data())) {
                throw new RejectionException(RejectionReason.DECODE_FAILURE,
                        "日志 data 非十六进制，解码失败: height=" + block.height());
            }
        }
    }

    private static boolean isHex(String data) {
        if (!data.startsWith("0x")) {
            return false;
        }
        for (int i = 2; i < data.length(); i++) {
            char c = data.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return false;
            }
        }
        return true;
    }

    // ---- 日志持久化与回放 ----

    private void appendJournal(String type, BlockData block, long arg) {
        try {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("type", type);
            if (block != null) {
                node.set("block", MAPPER.valueToTree(block));
            }
            if (arg >= 0) {
                node.put("arg", arg);
            }
            Files.createDirectories(journalPath.getParent() == null ? Path.of(".") : journalPath.getParent());
            Files.writeString(journalPath, MAPPER.writeValueAsString(node) + "\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("写索引日志失败: " + journalPath, e);
        }
    }

    private void replay() {
        if (!Files.exists(journalPath)) {
            return;
        }
        try {
            int applied = 0;
            for (String line : Files.readAllLines(journalPath)) {
                if (line.isBlank()) {
                    continue;
                }
                JsonNode node = MAPPER.readTree(line);
                switch (node.get("type").asText()) {
                    case "apply" -> {
                        BlockData block = MAPPER.treeToValue(node.get("block"), BlockData.class);
                        blocks.put(block.height(), block);
                        applied++;
                    }
                    case "rollback" -> blocks.tailMap(node.get("arg").asLong(), true).clear();
                    case "finalize" -> finalizedHeight = node.get("arg").asLong();
                    default -> throw new IllegalStateException("未知日志类型: " + line);
                }
            }
            if (applied > 0 || finalizedHeight >= 0) {
                log.info("索引日志回放完成: 链头={} 固化位点={}", headHeight(), finalizedHeight);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("回放索引日志失败: " + journalPath, e);
        }
    }

    /** 当前已索引高度的只读视图（测试与排障用）。 */
    public synchronized List<Long> indexedHeights() {
        return new ArrayList<>(blocks.keySet());
    }
}
