package com.manaschintawar.kvstore.topology;

import java.util.Objects;

public class NodeInfo {
    private String nodeId;
    private String host;
    private int port;
    private long lastHeartbeat;

    public NodeInfo() {}

    public NodeInfo(String nodeId, String host, int port, long lastHeartbeat) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.lastHeartbeat = lastHeartbeat;
    }

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public long getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(long lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }

    public String getBaseUrl() {
        return "http://" + host + ":" + port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeInfo nodeInfo = (NodeInfo) o;
        return Objects.equals(nodeId, nodeInfo.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }
}
