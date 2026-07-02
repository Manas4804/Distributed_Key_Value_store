package com.manaschintawar.kvstore.quorum;

import com.manaschintawar.kvstore.storage.VersionedValue;
import com.manaschintawar.kvstore.topology.ClusterManager;
import com.manaschintawar.kvstore.topology.NodeInfo;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Coordinates quorum reads and writes across the N replicas that own a key.
 *
 * Every write is stamped with a coordinator-assigned timestamp; reads collect
 * versioned records from R replicas, return the record with the highest
 * timestamp (last-write-wins), and repair any replica that returned a stale or
 * missing record. Combined with R + W > N, this guarantees reads observe the
 * latest acknowledged write.
 */
@Service
public class QuorumCoordinator {
    private static final Logger log = LoggerFactory.getLogger(QuorumCoordinator.class);

    public static final String TIMESTAMP_HEADER = "X-KV-Timestamp";
    private static final long QUORUM_TIMEOUT_MS = 2000;

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
        return replicate(key, value, System.currentTimeMillis(), false);
    }

    public boolean delete(String key) {
        return replicate(key, null, System.currentTimeMillis(), true);
    }

    private boolean replicate(String key, String value, long timestamp, boolean tombstone) {
        List<NodeInfo> replicas = clusterManager.getReplicasFor(key, N);
        if (replicas.size() < W) {
            log.warn("Not enough nodes for write quorum. Required: {}, Available: {}", W, replicas.size());
            return false;
        }

        CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executor);
        for (NodeInfo node : replicas) {
            completionService.submit(() -> sendToReplica(node, key, value, timestamp, tombstone));
        }

        int successes = 0;
        int failures = 0;
        int total = replicas.size();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(QUORUM_TIMEOUT_MS);

        // Stop as soon as the quorum is reached or mathematically unreachable —
        // no pointless waiting out the timeout when replicas have already failed.
        while (successes < W && failures <= total - W) {
            try {
                Future<Boolean> future = completionService.poll(remainingNanos(deadline), TimeUnit.NANOSECONDS);
                if (future == null) break; // timed out
                if (Boolean.TRUE.equals(future.get())) successes++; else failures++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (ExecutionException e) {
                failures++;
            }
        }
        return successes >= W;
    }

    private boolean sendToReplica(NodeInfo node, String key, String value, long timestamp, boolean tombstone) {
        try {
            String url = node.getBaseUrl() + "/internal/kv/" + key;
            HttpHeaders headers = new HttpHeaders();
            headers.set(TIMESTAMP_HEADER, String.valueOf(timestamp));
            if (tombstone) {
                restTemplate.exchange(url, HttpMethod.DELETE, new HttpEntity<>(headers), Void.class);
            } else {
                headers.setContentType(MediaType.TEXT_PLAIN);
                restTemplate.exchange(url, HttpMethod.PUT, new HttpEntity<>(value, headers), Void.class);
            }
            return true;
        } catch (Exception e) {
            log.warn("Failed to replicate to {}: {}", node.getNodeId(), e.getMessage());
            return false;
        }
    }

    public String read(String key) {
        List<NodeInfo> replicas = clusterManager.getReplicasFor(key, N);
        if (replicas.size() < R) {
            throw new RuntimeException("Not enough nodes for read quorum");
        }

        CompletionService<ReadResult> completionService = new ExecutorCompletionService<>(executor);
        for (NodeInfo node : replicas) {
            completionService.submit(() -> fetchFromReplica(node, key));
        }

        List<ReadResult> responses = new ArrayList<>();
        int failures = 0;
        int total = replicas.size();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(QUORUM_TIMEOUT_MS);

        while (responses.size() < R && failures <= total - R) {
            try {
                Future<ReadResult> future = completionService.poll(remainingNanos(deadline), TimeUnit.NANOSECONDS);
                if (future == null) break; // timed out
                ReadResult result = future.get();
                if (result != null) responses.add(result); else failures++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Read interrupted", e);
            } catch (ExecutionException e) {
                failures++;
            }
        }

        // Drain any replies that have already arrived (no extra waiting) so they
        // participate in conflict resolution and read repair too.
        Future<ReadResult> extra;
        while ((extra = completionService.poll()) != null) {
            try {
                ReadResult result = extra.get();
                if (result != null) responses.add(result);
            } catch (Exception ignored) {
            }
        }

        if (responses.size() < R) {
            throw new RuntimeException("Failed to satisfy read quorum. Reached " + responses.size() + " of " + R);
        }

        // Last-write-wins: the record with the highest timestamp is authoritative.
        VersionedValue winner = null;
        for (ReadResult response : responses) {
            if (response.record != null && response.record.isNewerThan(winner)) {
                winner = response.record;
            }
        }

        readRepair(key, winner, responses);

        return (winner == null || winner.isTombstone()) ? null : winner.getValue();
    }

    /** A replica's reply: record is null when the replica has never seen the key. */
    private record ReadResult(NodeInfo node, VersionedValue record) {}

    private ReadResult fetchFromReplica(NodeInfo node, String key) {
        try {
            String url = node.getBaseUrl() + "/internal/kv/" + key;
            ResponseEntity<VersionedValue> response = restTemplate.getForEntity(url, VersionedValue.class);
            return new ReadResult(node, response.getBody());
        } catch (HttpClientErrorException.NotFound e) {
            return new ReadResult(node, null); // definitive answer: key absent
        } catch (Exception e) {
            log.warn("Failed to read from replica {}: {}", node.getNodeId(), e.getMessage());
            return null; // replica unreachable
        }
    }

    /** Pushes the winning record to every replica that returned a stale or missing one. */
    private void readRepair(String key, VersionedValue winner, List<ReadResult> responses) {
        if (winner == null) {
            return; // no replica has the key; nothing to repair
        }
        for (ReadResult response : responses) {
            boolean stale = winner.isNewerThan(response.record);
            boolean missingTombstone = response.record == null && winner.isTombstone();
            if (stale && !missingTombstone) { // don't push tombstones to replicas that never had the key
                log.info("Read repair: updating replica {} for key '{}'", response.node.getNodeId(), key);
                sendToReplica(response.node, key, winner.getValue(), winner.getTimestamp(), winner.isTombstone());
            }
        }
    }

    private long remainingNanos(long deadline) {
        return Math.max(0, deadline - System.nanoTime());
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }
}
