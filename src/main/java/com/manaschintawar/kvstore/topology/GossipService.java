package com.manaschintawar.kvstore.topology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Random;

@Service
@EnableScheduling
public class GossipService {
    private static final Logger log = LoggerFactory.getLogger(GossipService.class);

    private final ClusterManager clusterManager;
    private final RestTemplate restTemplate = new RestTemplate();
    private final Random random = new Random();

    // 15 seconds without heartbeat = node is dead
    private static final long FAILURE_TIMEOUT_MS = 15000;

    public GossipService(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @Scheduled(fixedRate = 2000) // Gossip every 2 seconds
    public void gossip() {
        NodeInfo self = clusterManager.getSelfNode();
        if (self != null) {
            self.setLastHeartbeat(System.currentTimeMillis());
            clusterManager.addOrUpdateNode(self);
        }

        List<NodeInfo> nodes = clusterManager.getAllNodes();
        if (nodes.isEmpty() || nodes.size() == 1) {
            return; // No one to gossip with
        }

        // Check for failures
        long now = System.currentTimeMillis();
        for (NodeInfo node : nodes) {
            if (!node.getNodeId().equals(self.getNodeId())) {
                if (now - node.getLastHeartbeat() > FAILURE_TIMEOUT_MS) {
                    if (clusterManager.getNode(node.getNodeId()) != null) {
                        log.warn("Node {} suspected to be dead. Removing from cluster.", node.getNodeId());
                        clusterManager.removeNode(node.getNodeId());
                    }
                }
            }
        }

        // Fetch nodes again in case some were removed
        nodes = clusterManager.getAllNodes();
        if (nodes.size() <= 1) return;

        // Pick a random target node (not self)
        NodeInfo targetNode = null;
        for (int i = 0; i < 5; i++) {
            targetNode = nodes.get(random.nextInt(nodes.size()));
            if (!targetNode.getNodeId().equals(self.getNodeId())) {
                break;
            }
            targetNode = null; // Try again
        }

        if (targetNode != null) {
            GossipRequest req = new GossipRequest();
            req.setSenderNodeId(self.getNodeId());
            req.setKnownNodes(nodes);

            try {
                String url = targetNode.getBaseUrl() + "/internal/gossip";
                restTemplate.postForObject(url, req, Void.class);
            } catch (Exception e) {
                log.debug("Failed to gossip with node {}: {}", targetNode.getNodeId(), e.getMessage());
            }
        }
    }

    public void receiveGossip(GossipRequest request) {
        long now = System.currentTimeMillis();
        for (NodeInfo incomingNode : request.getKnownNodes()) {
            NodeInfo existingNode = clusterManager.getNode(incomingNode.getNodeId());
            if (existingNode == null) {
                // If it's a seed node that hasn't communicated, it might have last heartbeat = 0.
                if (now - incomingNode.getLastHeartbeat() < FAILURE_TIMEOUT_MS) {
                    clusterManager.addOrUpdateNode(incomingNode);
                    log.info("Discovered new node via gossip: {}", incomingNode.getNodeId());
                }
            } else {
                if (incomingNode.getLastHeartbeat() > existingNode.getLastHeartbeat()) {
                    existingNode.setLastHeartbeat(incomingNode.getLastHeartbeat());
                    existingNode.setHost(incomingNode.getHost());
                    existingNode.setPort(incomingNode.getPort());
                }
            }
        }
    }
}
