package com.manaschintawar.kvstore.quorum;

import com.manaschintawar.kvstore.topology.ClusterManager;
import com.manaschintawar.kvstore.topology.NodeInfo;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class QuorumCoordinator {
    private static final Logger log = LoggerFactory.getLogger(QuorumCoordinator.class);

    private final ClusterManager clusterManager;
    private final RestTemplate restTemplate;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Value("${kvstore.quorum.n:3}")
    private int N;

    @Value("${kvstore.quorum.w:2}")
    private int W;

    @Value("${kvstore.quorum.r:2}")
    private int R;

    public QuorumCoordinator(ClusterManager clusterManager, RestTemplate restTemplate) {
        this.clusterManager = clusterManager;
        this.restTemplate = restTemplate;
    }

    public boolean write(String key, String value) {
        List<NodeInfo> replicas = clusterManager.getReplicasFor(key, N);
        if (replicas.size() < W) {
            log.warn("Not enough nodes for write quorum. Required: {}, Available: {}", W, replicas.size());
            return false;
        }

        AtomicInteger successes = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(W);

        for (NodeInfo node : replicas) {
            executor.submit(() -> {
                try {
                    String url = node.getBaseUrl() + "/internal/kv/" + key;
                    restTemplate.put(url, value);
                    successes.incrementAndGet();
                    latch.countDown();
                } catch (Exception e) {
                    log.error("Failed to write to replica {}: {}", node.getNodeId(), e.getMessage());
                }
            });
        }

        try {
            boolean wrote = latch.await(2000, TimeUnit.MILLISECONDS);
            if (!wrote && successes.get() >= W) {
                return true; 
            }
            return successes.get() >= W;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public String read(String key) {
        List<NodeInfo> replicas = clusterManager.getReplicasFor(key, N);
        if (replicas.size() < R) {
            throw new RuntimeException("Not enough nodes for read quorum");
        }

        ConcurrentHashMap<String, Integer> valueCounts = new ConcurrentHashMap<>();
        AtomicInteger successes = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(R);

        for (NodeInfo node : replicas) {
            executor.submit(() -> {
                try {
                    String url = node.getBaseUrl() + "/internal/kv/" + key;
                    ResponseEntity<String> response = restTemplate.getForEntity(url, String.class);
                    if (response.getStatusCode() == HttpStatus.OK) {
                        String value = response.getBody();
                        valueCounts.merge(value, 1, Integer::sum);
                    }
                    successes.incrementAndGet();
                    latch.countDown();
                } catch (Exception e) {
                    if (e.getMessage() != null && e.getMessage().contains("404")) {
                        valueCounts.merge("NOT_FOUND", 1, Integer::sum);
                        successes.incrementAndGet();
                        latch.countDown();
                    } else {
                        log.error("Failed to read from replica {}: {}", node.getNodeId(), e.getMessage());
                    }
                }
            });
        }

        try {
            latch.await(2000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Read interrupted", e);
        }

        if (successes.get() < R) {
            throw new RuntimeException("Failed to satisfy read quorum. Reached " + successes.get() + " of " + R);
        }

        String bestValue = null;
        int maxCount = 0;
        for (Map.Entry<String, Integer> entry : valueCounts.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                bestValue = entry.getKey();
            }
        }

        if ("NOT_FOUND".equals(bestValue)) {
            return null;
        }
        return bestValue;
    }

    public boolean delete(String key) {
        List<NodeInfo> replicas = clusterManager.getReplicasFor(key, N);
        if (replicas.size() < W) {
            log.warn("Not enough nodes for delete quorum. Required: {}, Available: {}", W, replicas.size());
            return false;
        }

        AtomicInteger successes = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(W);

        for (NodeInfo node : replicas) {
            executor.submit(() -> {
                try {
                    String url = node.getBaseUrl() + "/internal/kv/" + key;
                    restTemplate.delete(url);
                    successes.incrementAndGet();
                    latch.countDown();
                } catch (Exception e) {
                    log.error("Failed to delete at replica {}: {}", node.getNodeId(), e.getMessage());
                }
            });
        }

        try {
            latch.await(2000, TimeUnit.MILLISECONDS);
            return successes.get() >= W;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}
