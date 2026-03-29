package com.manaschintawar.kvstore.topology;

import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Component
public class ConsistentHashRing {

    private final int numberOfReplicas; // Virtual nodes per physical node
    private final ConcurrentSkipListMap<Long, NodeInfo> ring = new ConcurrentSkipListMap<>();
    private final Set<NodeInfo> activeNodes = new HashSet<>();

    public ConsistentHashRing() {
        this.numberOfReplicas = 100; // 100 virtual nodes
    }

    public ConsistentHashRing(int numberOfReplicas) {
        this.numberOfReplicas = numberOfReplicas;
    }

    public synchronized void addNode(NodeInfo node) {
        if (activeNodes.add(node)) {
            for (int i = 0; i < numberOfReplicas; i++) {
                ring.put(hash(node.getNodeId() + "-VN" + i), node);
            }
        }
    }

    public synchronized void removeNode(NodeInfo node) {
        if (activeNodes.remove(node)) {
            for (int i = 0; i < numberOfReplicas; i++) {
                ring.remove(hash(node.getNodeId() + "-VN" + i));
            }
        }
    }

    public NodeInfo getNode(String key) {
        if (ring.isEmpty()) {
            return null;
        }
        long hash = hash(key);
        Map.Entry<Long, NodeInfo> entry = ring.ceilingEntry(hash);
        if (entry == null) {
            // wrap around
            return ring.firstEntry().getValue();
        }
        return entry.getValue();
    }

    public List<NodeInfo> getNodes(String key, int count) {
        if (ring.isEmpty()) {
            return Collections.emptyList();
        }
        List<NodeInfo> uniqueNodes = new ArrayList<>();
        long hash = hash(key);
        
        ConcurrentNavigableMap<Long, NodeInfo> tailMap = ring.tailMap(hash);
        
        for (NodeInfo node : tailMap.values()) {
            if (!uniqueNodes.contains(node)) {
                uniqueNodes.add(node);
                if (uniqueNodes.size() == count) return uniqueNodes;
            }
        }
        
        // Wrap around
        for (NodeInfo node : ring.values()) {
            if (!uniqueNodes.contains(node)) {
                uniqueNodes.add(node);
                if (uniqueNodes.size() == count) return uniqueNodes;
            }
        }
        
        return uniqueNodes;
    }

    private long hash(String key) {
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not supported", e);
        }
        md5.reset();
        md5.update(key.getBytes());
        byte[] digest = md5.digest();
        // Convert first 4 bytes to long
        long h = 0;
        for (int i = 0; i < 4; i++) {
            h <<= 8;
            h |= ((int) digest[i]) & 0xFF;
        }
        return h & 0xffffffffL;
    }
}
