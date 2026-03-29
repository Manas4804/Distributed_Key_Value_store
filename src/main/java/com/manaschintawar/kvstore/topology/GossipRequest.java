package com.manaschintawar.kvstore.topology;

import java.util.List;

public class GossipRequest {
    private String senderNodeId;
    private List<NodeInfo> knownNodes;

    public GossipRequest() {}

    public String getSenderNodeId() { return senderNodeId; }
    public void setSenderNodeId(String senderNodeId) { this.senderNodeId = senderNodeId; }

    public List<NodeInfo> getKnownNodes() { return knownNodes; }
    public void setKnownNodes(List<NodeInfo> knownNodes) { this.knownNodes = knownNodes; }
}
