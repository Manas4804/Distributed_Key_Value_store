package com.manaschintawar.kvstore.topology;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ClusterManager {
    private final ConcurrentHashMap<String, NodeInfo> nodes = new ConcurrentHashMap<>();
    private final ConsistentHashRing hashRing;

    @Value("${server.port:8080}")
    private int port;

    @Value("${kvstore.node.host:localhost}")
    private String host;

    @Value("${kvstore.cluster.seed:}")
    private String seedNodes; // Comma-separated like localhost:8080,localhost:8081

    private NodeInfo selfNode;

    public ClusterManager(ConsistentHashRing hashRing) {
        this.hashRing = hashRing;
    }

    @PostConstruct
    public void init() {
        String nodeId = "node-" + port;
        selfNode = new NodeInfo(nodeId, host, port, System.currentTimeMillis());
        addOrUpdateNode(selfNode);
        
        if (seedNodes != null && !seedNodes.trim().isEmpty()) {
            String[] seeds = seedNodes.split(",");
            for (String seed : seeds) {
                String[] parts = seed.split(":");
                if (parts.length == 2) {
                    String seedHost = parts[0];
                    int seedPort = Integer.parseInt(parts[1]);
                    if (!(seedHost.equals(host) && seedPort == port)) {
                        String sNodeId = "node-" + seedPort;
                        NodeInfo n = new NodeInfo(sNodeId, seedHost, seedPort, System.currentTimeMillis() + 30000); 
                        addOrUpdateNode(n);
                    }
                }
            }
        }
    }

    public void addOrUpdateNode(NodeInfo node) {
        nodes.put(node.getNodeId(), node);
        hashRing.addNode(node);
    }

    public void removeNode(String nodeId) {
        NodeInfo node = nodes.remove(nodeId);
        if (node != null) {
            hashRing.removeNode(node);
        }
    }

    public NodeInfo getSelfNode() {
        return selfNode;
    }

    public List<NodeInfo> getAllNodes() {
        return new ArrayList<>(nodes.values());
    }

    public NodeInfo getNode(String nodeId) {
        return nodes.get(nodeId);
    }
    
    public List<NodeInfo> getReplicasFor(String key, int count) {
        int required = Math.min(count, nodes.size());
        return hashRing.getNodes(key, required);
    }
}
