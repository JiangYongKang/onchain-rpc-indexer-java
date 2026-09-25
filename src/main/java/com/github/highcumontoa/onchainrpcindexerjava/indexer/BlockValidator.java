package com.github.highcumontoa.onchainrpcindexerjava.indexer;

import com.github.highcumontoa.onchainrpcindexerjava.model.Block;
import com.github.highcumontoa.onchainrpcindexerjava.model.LogEvent;
import com.github.highcumontoa.onchainrpcindexerjava.model.RejectReason;
import com.github.highcumontoa.onchainrpcindexerjava.rpc.SimulatedRpcNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 区块内容校验。
 *
 * <p>区分三类可解释的拒绝原因：
 * <ul>
 *     <li>{@link RejectReason#MALFORMED_BLOCK}：区块结构畸形（空哈希、父哈希缺失、日志缺字段、索引乱序等）；</li>
 *     <li>{@link RejectReason#DECODE_ERROR}：字段类型/格式无法解析（当前编解码层已先拦截，
 *         此处兜底非十六进制哈希等格式问题）；</li>
 *     <li>{@link RejectReason#CONTENT_MISMATCH}：结构合法但内容与承诺不符
 *         （哈希对不上内容、父哈希接不上、日志索引有洞/重复）。</li>
 * </ul>
 */
public class BlockValidator {

    public Optional<RejectReason> validate(Block block, Block previous, long expectedHeight) {
        return validateWithDetail(block, previous, expectedHeight).map(Rejected::reason);
    }

    /** 校验并返回（原因 + 细节），细节写入拒绝留痕，保证失败原因可区分。 */
    public Optional<Rejected> validateWithDetail(Block block, Block previous, long expectedHeight) {
        // 1) 畸形：结构性缺失
        if (block == null) {
            return Optional.of(new Rejected(RejectReason.MALFORMED_BLOCK, "区块为空"));
        }
        if (block.hash() == null || block.hash().isBlank()) {
            return Optional.of(new Rejected(RejectReason.MALFORMED_BLOCK,
                    "高度 " + expectedHeight + " 的区块哈希为空"));
        }
        if (block.parentHash() == null || block.parentHash().isBlank()) {
            return Optional.of(new Rejected(RejectReason.MALFORMED_BLOCK,
                    "高度 " + expectedHeight + " 的父哈希为空"));
        }
        if (block.height() != expectedHeight) {
            return Optional.of(new Rejected(RejectReason.MALFORMED_BLOCK,
                    "区块自带高度 " + block.height() + " 与采集位点 " + expectedHeight + " 不一致"));
        }
        if (!isHexHash(block.hash()) || !isHexHash(block.parentHash())) {
            return Optional.of(new Rejected(RejectReason.DECODE_ERROR,
                    "高度 " + expectedHeight + " 的哈希不是 0x + 64 位十六进制：hash="
                            + block.hash() + " parentHash=" + block.parentHash()));
        }

        // 2) 日志序列必须索引连续、字段完整
        List<LogEvent> logs = block.logs() == null ? List.of() : block.logs();
        for (LogEvent event : logs) {
            if (event == null || event.address() == null || event.address().isBlank()
                    || event.data() == null) {
                return Optional.of(new Rejected(RejectReason.MALFORMED_BLOCK,
                        "高度 " + expectedHeight + " 存在字段缺失的日志条目"));
            }
        }
        for (int i = 0; i < logs.size(); i++) {
            if (logs.get(i).index() != i) {
                return Optional.of(new Rejected(RejectReason.CONTENT_MISMATCH,
                        "高度 " + expectedHeight + " 日志索引不连续：期望序号 " + i
                                + "，实际 index=" + logs.get(i).index()));
            }
        }

        // 3) 父子衔接
        if (previous != null && !block.parentHash().equals(previous.hash())) {
            return Optional.of(new Rejected(RejectReason.CONTENT_MISMATCH,
                    "高度 " + expectedHeight + " 的父哈希 " + block.parentHash()
                            + " 与已索引父块哈希 " + previous.hash() + " 不一致（可能发生重组）"));
        }

        // 4) 内容哈希一致性：用与节点相同的规范哈希算法，依据区块内容独立重算。
        // 节点伪造 claimedHash（hashMismatch 注入）在此暴露为 CONTENT_MISMATCH。
        if (block.body() == null) {
            return Optional.of(new Rejected(RejectReason.MALFORMED_BLOCK,
                    "高度 " + expectedHeight + " 的区块体为空"));
        }
        List<SimulatedRpcNode.LogSpec> specs = new ArrayList<>(logs.size());
        for (LogEvent event : logs) {
            specs.add(SimulatedRpcNode.LogSpec.of(event.index(), event.address(), event.data()));
        }
        String recomputed = SimulatedRpcNode.canonicalHash(
                block.height(), block.parentHash(), block.body(), specs);
        if (!recomputed.equals(block.hash())) {
            return Optional.of(new Rejected(RejectReason.CONTENT_MISMATCH,
                    "高度 " + expectedHeight + " 区块哈希与内容重算结果不一致：claimed="
                            + block.hash() + " recomputed=" + recomputed));
        }
        return Optional.empty();
    }

    private static boolean isHexHash(String value) {
        if (value == null || !value.startsWith("0x") || value.length() != 66) {
            return false;
        }
        for (int i = 2; i < value.length(); i++) {
            char c = value.charAt(i);
            boolean hex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
            if (!hex) {
                return false;
            }
        }
        return true;
    }

    /** 校验拒绝结论。 */
    public record Rejected(RejectReason reason, String detail) {
    }
}
