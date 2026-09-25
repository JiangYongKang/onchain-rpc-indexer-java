package com.github.highcumontoa.onchainrpcindexerjava.rpc;

import com.github.highcumontoa.onchainrpcindexerjava.model.Block;
import com.github.highcumontoa.onchainrpcindexerjava.model.LogEvent;
import com.github.highcumontoa.onchainrpcindexerjava.model.RpcErrorCode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 可本地启动的模拟链节点。
 *
 * <p>节点持有一条确定性的链（区块内容、日志、哈希均可复现），并支持以下故障注入：
 * <ul>
 *     <li>可配置基础延迟与随机抖动，用于节点择优；</li>
 *     <li>随机瞬时超时/不可用，用于验证“短暂抖动不触发误判”；</li>
 *     <li>整节点下线；</li>
 *     <li>按高度定点返回归一化错误；</li>
 *     <li>按高度返回截断报文（畸形）、不可解码字段（解码失败）、哈希与内容不一致（内容篡改）；</li>
 *     <li>整条链替换，用于模拟链头回退与重组。</li>
 * </ul>
 */
public class SimulatedRpcNode implements ChainRpcClient {

    /** 报文层面的故障类型。 */
    public enum WireFault {
        /** 报文正常。 */
        NONE,
        /** 报文被截断/字段缺失：归一化为 MALFORMED_RESPONSE。 */
        TRUNCATED,
        /** 字段存在但数字不可解码：归一化为 DECODE_ERROR。 */
        BAD_NUMBER
    }

    /** 日志规格（测试/装链用）。 */
    public record LogSpec(int index, String address, String data) {
        public static LogSpec of(int index, String address, String data) {
            return new LogSpec(index, address, data);
        }
    }

    /** 区块规格（测试/装链用）。 */
    public record BlockSpec(long height, String body, List<LogSpec> logs,
                            WireFault wireFault, boolean hashMismatch) {

        public static BlockSpec of(long height, String body, LogSpec... logs) {
            return new BlockSpec(height, body, List.of(logs), WireFault.NONE, false);
        }

        public BlockSpec withWireFault(WireFault fault) {
            return new BlockSpec(height, body, logs, fault, hashMismatch);
        }

        public BlockSpec withHashMismatch() {
            return new BlockSpec(height, body, logs, wireFault, true);
        }
    }

    private record StoredBlock(long height, String body, List<LogSpec> logs,
                               WireFault wireFault, boolean hashMismatch,
                               String claimedHash, String parentHash) {
    }

    private record Decoded(long height, String body, List<LogEvent> logs) {
    }

    private final String id;
    private final Random random;

    private volatile Duration baseLatency = Duration.ZERO;
    private volatile Duration jitterLatency = Duration.ZERO;
    private volatile double failureRate;
    private volatile double timeoutRate;
    private volatile boolean down;

    private final Object chainLock = new Object();
    private List<StoredBlock> chain = List.of();
    private final Map<Long, RpcErrorCode> forcedFaults = new ConcurrentHashMap<>();

    public SimulatedRpcNode(String id, long seed) {
        this.id = id;
        this.random = new Random(seed);
    }

    @Override
    public String id() {
        return id;
    }

    // ------------------------------------------------------------------
    // 链与故障注入控制
    // ------------------------------------------------------------------

    /**
     * 安装（替换）整条链。区块高度必须从 0 连续递增；哈希按父子关系确定性计算。
     */
    public void installChain(List<BlockSpec> specs) {
        List<StoredBlock> built = new ArrayList<>(specs.size());
        String parentHash = zeroHash();
        long expected = 0L;
        for (BlockSpec spec : specs) {
            if (spec.height() != expected) {
                throw new IllegalArgumentException(
                        "节点 " + id + " 装链失败：期望高度 " + expected + "，实际 " + spec.height());
            }
            String canonical = canonicalHash(spec.height(), parentHash, spec.body(), spec.logs());
            String claimed = spec.hashMismatch() ? fakeHash(spec.height()) : canonical;
            built.add(new StoredBlock(spec.height(), spec.body(), List.copyOf(spec.logs()),
                    spec.wireFault(), spec.hashMismatch(), claimed, parentHash));
            parentHash = claimed;
            expected++;
        }
        synchronized (chainLock) {
            this.chain = List.copyOf(built);
        }
    }

    public void setLatency(Duration base, Duration jitter) {
        this.baseLatency = base;
        this.jitterLatency = jitter == null ? Duration.ZERO : jitter;
    }

    public void setFailureRate(double failureRate) {
        this.failureRate = failureRate;
    }

    public void setTimeoutRate(double timeoutRate) {
        this.timeoutRate = timeoutRate;
    }

    public void setDown(boolean down) {
        this.down = down;
    }

    public boolean isDown() {
        return down;
    }

    /** 对指定高度定点注入归一化错误。 */
    public void forceFault(long height, RpcErrorCode code) {
        forcedFaults.put(height, code);
    }

    public void clearForcedFaults() {
        forcedFaults.clear();
    }

    // ------------------------------------------------------------------
    // RPC
    // ------------------------------------------------------------------

    @Override
    public long getHeadHeight() {
        sleepLatency();
        if (down) {
            throw new RpcException(RpcErrorCode.UNAVAILABLE,
                    "节点 " + id + " 不可用：连接被拒绝");
        }
        return chain.size() - 1L;
    }

    @Override
    public Block getBlockByHeight(long height) {
        sleepLatency();
        if (down) {
            throw new RpcException(RpcErrorCode.UNAVAILABLE,
                    "节点 " + id + " 不可用：连接被拒绝");
        }
        double draw = random.nextDouble();
        if (draw < timeoutRate) {
            throw new RpcException(RpcErrorCode.TIMEOUT,
                    "节点 " + id + " 在 " + height + " 高度请求超时（注入的瞬时抖动）");
        }
        if (draw < timeoutRate + failureRate) {
            throw new RpcException(RpcErrorCode.UNAVAILABLE,
                    "节点 " + id + " 在 " + height + " 高度临时不可用（注入的瞬时抖动）");
        }

        StoredBlock stored;
        synchronized (chainLock) {
            if (height < 0 || height >= chain.size()) {
                throw new RpcException(RpcErrorCode.NOT_FOUND,
                        "节点 " + id + " 不存在高度 " + height + " 的区块（链头=" + (chain.size() - 1) + "）");
            }
            stored = chain.get((int) height);
        }

        RpcErrorCode forced = forcedFaults.get(height);
        if (forced != null) {
            throw new RpcException(forced,
                    "节点 " + id + " 在高度 " + height + " 返回定点注入错误 " + forced);
        }

        // 模拟“线上报文 -> 解码”的完整过程，畸形/解码失败在此被识别。
        String wire = encodeWire(stored);
        Decoded decoded = decodeWire(id, height, wire);
        return new Block(decoded.height(), stored.claimedHash(), stored.parentHash(),
                decoded.body(), decoded.logs());
    }

    // ------------------------------------------------------------------
    // 报文编解码（模拟真实 RPC 的序列化层）
    // ------------------------------------------------------------------

    private String encodeWire(StoredBlock b) {
        StringBuilder sb = new StringBuilder("v1|");
        sb.append("h=").append(b.height()).append('|');
        sb.append("p=").append(b.parentHash()).append('|');
        sb.append("b=").append(b.body().length()).append(':').append(b.body()).append('|');
        sb.append("l=").append(b.logs().size());
        for (LogSpec logSpec : b.logs()) {
            sb.append('|').append("i=").append(logSpec.index());
            sb.append(',').append("a=").append(logSpec.address().length()).append(':').append(logSpec.address());
            sb.append(',').append("d=").append(logSpec.data().length()).append(':').append(logSpec.data());
        }
        String wire = sb.toString();
        return switch (b.wireFault()) {
            case NONE -> wire;
            // 截断：字段缺失，解码阶段判定为畸形响应。
            case TRUNCATED -> wire.substring(0, Math.min(12, wire.length()));
            // 高度字段篡改为非数字：字段齐全但不可解码。
            case BAD_NUMBER -> wire.replace("h=" + b.height(), "h=NaN");
        };
    }

    private static Decoded decodeWire(String nodeId, long requestedHeight, String wire) {
        if (wire == null || wire.isBlank() || !wire.startsWith("v1|")) {
            throw new RpcException(RpcErrorCode.MALFORMED_RESPONSE,
                    "节点 " + nodeId + " 高度 " + requestedHeight + " 的响应为空或协议头缺失");
        }
        String[] tokens = wire.substring(3).split("\\|", -1);
        Long height = null;
        String body = null;
        List<LogEvent> logs = new ArrayList<>();
        Integer logCount = null;
        try {
            for (String token : tokens) {
                if (token.startsWith("h=")) {
                    height = Long.parseLong(token.substring(2));
                } else if (token.startsWith("b=")) {
                    int colon = token.indexOf(':');
                    if (colon < 0) {
                        throw new RpcException(RpcErrorCode.MALFORMED_RESPONSE,
                                "节点 " + nodeId + " 高度 " + requestedHeight + " 的 body 字段缺少长度分隔符");
                    }
                    body = token.substring(colon + 1);
                } else if (token.startsWith("l=")) {
                    logCount = Integer.parseInt(token.substring(2));
                } else if (token.startsWith("i=")) {
                    logs.add(parseLog(nodeId, requestedHeight, token));
                }
            }
        } catch (NumberFormatException e) {
            throw new RpcException(RpcErrorCode.DECODE_ERROR,
                    "节点 " + nodeId + " 高度 " + requestedHeight + " 的数值字段不可解码：" + e.getMessage(), e);
        }
        if (height == null || body == null || logCount == null) {
            throw new RpcException(RpcErrorCode.MALFORMED_RESPONSE,
                    "节点 " + nodeId + " 高度 " + requestedHeight + " 的响应字段缺失（h/b/l）");
        }
        if (height != requestedHeight) {
            throw new RpcException(RpcErrorCode.MALFORMED_RESPONSE,
                    "节点 " + nodeId + " 响应高度 " + height + " 与请求高度 " + requestedHeight + " 不一致");
        }
        if (logCount != logs.size()) {
            throw new RpcException(RpcErrorCode.MALFORMED_RESPONSE,
                    "节点 " + nodeId + " 高度 " + requestedHeight
                            + " 声明日志数 " + logCount + " 与实际 " + logs.size() + " 不符");
        }
        return new Decoded(height, body, List.copyOf(logs));
    }

    private static LogEvent parseLog(String nodeId, long height, String token) {
        // 形如 i=1,a=5:0xabc,d=3:foo
        String[] parts = token.split(",", -1);
        int index = -1;
        String address = null;
        String data = null;
        for (String part : parts) {
            if (part.startsWith("i=")) {
                index = Integer.parseInt(part.substring(2));
            } else if (part.startsWith("a=")) {
                int colon = part.indexOf(':');
                address = colon < 0 ? null : part.substring(colon + 1);
            } else if (part.startsWith("d=")) {
                int colon = part.indexOf(':');
                data = colon < 0 ? null : part.substring(colon + 1);
            }
        }
        if (address == null || data == null || index < 0) {
            throw new RpcException(RpcErrorCode.MALFORMED_RESPONSE,
                    "节点 " + nodeId + " 高度 " + height + " 的日志字段缺失");
        }
        return new LogEvent(index, address, data);
    }

    private void sleepLatency() {
        long base = baseLatency.toMillis();
        long jitter = jitterLatency.toMillis();
        long sleep = base + (jitter <= 0 ? 0 : random.nextLong(jitter));
        if (sleep > 0) {
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RpcException(RpcErrorCode.TIMEOUT, "节点 " + id + " 请求被中断", e);
            }
        }
    }

    // ------------------------------------------------------------------
    // 确定性哈希
    // ------------------------------------------------------------------

    /** 依据区块内容计算规范哈希；真实链中该值由区块头承载，此处用 SHA-256 模拟。 */
    public static String canonicalHash(long height, String parentHash, String body, List<LogSpec> logs) {
        StringBuilder canonical = new StringBuilder();
        canonical.append(height).append('|').append(parentHash).append('|').append(body);
        for (LogSpec logSpec : logs) {
            canonical.append('|').append(logSpec.index()).append(':')
                    .append(logSpec.address()).append(':').append(logSpec.data());
        }
        return sha256Hex(canonical.toString());
    }

    private static String fakeHash(long height) {
        return sha256Hex("forged-" + height);
    }

    public static String zeroHash() {
        return "0x" + "00".repeat(32);
    }

    private static String sha256Hex(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return "0x" + HexFormat.of().formatHex(hash);
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 不可用", e);
        }
    }
}
