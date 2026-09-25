package com.github.highcumontoa.onchainrpcindexerjava.selection;

import java.util.List;

/** 一次节点选择的可解释结论。 */
public record SelectionReport(String selectedNodeId, List<String> rationale) {
}
