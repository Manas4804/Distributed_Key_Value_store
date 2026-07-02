package com.manaschintawar.kvstore;

import com.manaschintawar.kvstore.quorum.QuorumCoordinator;
import com.manaschintawar.kvstore.storage.LocalStorage;
import com.manaschintawar.kvstore.storage.SnapshotManager;
import com.manaschintawar.kvstore.storage.StorageEngine;
import com.manaschintawar.kvstore.storage.WALManager;
import com.manaschintawar.kvstore.topology.ClusterManager;
import com.manaschintawar.kvstore.topology.ConsistentHashRing;
import com.manaschintawar.kvstore.topology.NodeInfo;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.client.ExpectedCount.between;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.content;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withNoContent;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;
import static org.springframework.http.HttpStatus.NOT_FOUND;

class DistributedKvStoreApplicationTests {

    @Test
    void restoresStructuredWalEntriesAcrossRestart() throws Exception {
        Path baseDir = Files.createTempDirectory("kvstore-recovery-");

        try {
            StorageFixture firstStart = createStorageFixture(baseDir, 19090);
            firstStart.engine.restore();
            firstStart.engine.put("special,key", "line1,line2\nline3");
            firstStart.engine.delete("missing,key");
            firstStart.engine.shutdown();
            firstStart.walManager.close();

            StorageFixture secondStart = createStorageFixture(baseDir, 19090);
            secondStart.engine.restore();

            assertEquals("line1,line2\nline3", secondStart.engine.get("special,key"));
            assertNull(secondStart.engine.get("missing,key"));

            secondStart.engine.shutdown();
            secondStart.walManager.close();
        } finally {
            deleteRecursively(baseDir);
        }
    }

    @Test
    void staleWritesDoNotClobberNewerData() throws Exception {
        Path baseDir = Files.createTempDirectory("kvstore-lww-");

        try {
            StorageFixture fixture = createStorageFixture(baseDir, 19091);
            fixture.engine.restore();

            fixture.engine.put("key", "newer", 200L);
            fixture.engine.put("key", "older", 100L); // stale replica write arrives late
            assertEquals("newer", fixture.engine.get("key"));

            fixture.engine.delete("key", 300L);
            assertNull(fixture.engine.get("key"));
            fixture.engine.put("key", "resurrected?", 250L); // older than the tombstone
            assertNull(fixture.engine.get("key")); // tombstone wins, no resurrection

            fixture.engine.shutdown();
            fixture.walManager.close();
        } finally {
            deleteRecursively(baseDir);
        }
    }

    @Test
    void satisfiesQuorumOperationsAgainstReplicas() {
        RestTemplate restTemplate = new RestTemplate();
        MockRestServiceServer server = MockRestServiceServer.bindTo(restTemplate).ignoreExpectOrder(true).build();
        ClusterManager clusterManager = createClusterManager();
        QuorumCoordinator coordinator = createCoordinator(clusterManager, restTemplate, 3, 2, 2);

        List<NodeInfo> replicas = clusterManager.getReplicasFor("cluster-key", 3);
        assertEquals(3, replicas.size());

        for (NodeInfo replica : replicas) {
            server.expect(once(), requestTo(replica.getBaseUrl() + "/internal/kv/cluster-key"))
                    .andExpect(method(HttpMethod.PUT))
                    .andExpect(content().string("cluster-value"))
                    .andRespond(withSuccess());
        }
        assertTrue(coordinator.write("cluster-key", "cluster-value"));
        // The coordinator returns as soon as W replicas ack; wait for the in-flight straggler.
        server.verify(Duration.ofSeconds(2));

        server.reset();
        String record = "{\"value\":\"cluster-value\",\"timestamp\":100,\"tombstone\":false}";
        server.expect(once(), requestTo(replicas.get(0).getBaseUrl() + "/internal/kv/cluster-key"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(record, MediaType.APPLICATION_JSON));
        server.expect(once(), requestTo(replicas.get(1).getBaseUrl() + "/internal/kv/cluster-key"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess(record, MediaType.APPLICATION_JSON));
        server.expect(once(), requestTo(replicas.get(2).getBaseUrl() + "/internal/kv/cluster-key"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(NOT_FOUND));
        // Read repair may push the winning record to the replica that returned 404,
        // depending on whether its reply arrived before the quorum was decided.
        server.expect(between(0, 1), requestTo(replicas.get(2).getBaseUrl() + "/internal/kv/cluster-key"))
                .andExpect(method(HttpMethod.PUT))
                .andRespond(withSuccess());
        assertEquals("cluster-value", coordinator.read("cluster-key"));
        server.verify(Duration.ofSeconds(2));

        server.reset();
        for (NodeInfo replica : replicas) {
            server.expect(once(), requestTo(replica.getBaseUrl() + "/internal/kv/cluster-key"))
                    .andExpect(method(HttpMethod.DELETE))
                    .andRespond(withNoContent());
        }
        assertTrue(coordinator.delete("cluster-key"));
        server.verify(Duration.ofSeconds(2));

        coordinator.shutdown();
    }

    @Test
    void readReturnsLatestValueAndRepairsStaleReplicas() {
        RestTemplate restTemplate = new RestTemplate();
        MockRestServiceServer server = MockRestServiceServer.bindTo(restTemplate).ignoreExpectOrder(true).build();
        ClusterManager clusterManager = createClusterManager();
        // R = 3 so the coordinator waits for every replica: deterministic repair.
        QuorumCoordinator coordinator = createCoordinator(clusterManager, restTemplate, 3, 2, 3);

        List<NodeInfo> replicas = clusterManager.getReplicasFor("cluster-key", 3);
        String url0 = replicas.get(0).getBaseUrl() + "/internal/kv/cluster-key";
        String url1 = replicas.get(1).getBaseUrl() + "/internal/kv/cluster-key";
        String url2 = replicas.get(2).getBaseUrl() + "/internal/kv/cluster-key";

        server.expect(once(), requestTo(url0)).andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"value\":\"old\",\"timestamp\":100,\"tombstone\":false}", MediaType.APPLICATION_JSON));
        server.expect(once(), requestTo(url1)).andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"value\":\"new\",\"timestamp\":200,\"tombstone\":false}", MediaType.APPLICATION_JSON));
        server.expect(once(), requestTo(url2)).andExpect(method(HttpMethod.GET))
                .andRespond(withStatus(NOT_FOUND));

        // The stale replica and the missing replica must both be repaired with the winner.
        server.expect(once(), requestTo(url0)).andExpect(method(HttpMethod.PUT))
                .andExpect(header(QuorumCoordinator.TIMESTAMP_HEADER, "200"))
                .andExpect(content().string("new"))
                .andRespond(withSuccess());
        server.expect(once(), requestTo(url2)).andExpect(method(HttpMethod.PUT))
                .andExpect(header(QuorumCoordinator.TIMESTAMP_HEADER, "200"))
                .andExpect(content().string("new"))
                .andRespond(withSuccess());

        assertEquals("new", coordinator.read("cluster-key"));
        server.verify();

        coordinator.shutdown();
    }

    @Test
    void refreshesHashRingWhenNodeMetadataChanges() {
        ClusterManager clusterManager = createClusterManager();

        clusterManager.addOrUpdateNode(new NodeInfo("node-8081", "127.0.0.1", 9081, System.currentTimeMillis()));

        List<NodeInfo> replicas = clusterManager.getReplicasFor("cluster-key", 3);
        assertTrue(replicas.stream().anyMatch(node -> "127.0.0.1".equals(node.getHost()) && node.getPort() == 9081));
        assertFalse(replicas.stream().anyMatch(node -> "localhost".equals(node.getHost()) && node.getPort() == 8081));
    }

    private StorageFixture createStorageFixture(Path baseDir, int port) {
        LocalStorage localStorage = new LocalStorage();

        WALManager walManager = new WALManager();
        ReflectionTestUtils.setField(walManager, "walDir", baseDir.resolve("wal").toString());
        ReflectionTestUtils.setField(walManager, "port", port);
        walManager.init();

        SnapshotManager snapshotManager = new SnapshotManager();
        ReflectionTestUtils.setField(snapshotManager, "snapshotDir", baseDir.resolve("snapshots").toString());
        ReflectionTestUtils.setField(snapshotManager, "port", port);
        snapshotManager.init();

        StorageEngine storageEngine = new StorageEngine(localStorage, walManager, snapshotManager);
        return new StorageFixture(storageEngine, walManager);
    }

    private ClusterManager createClusterManager() {
        ClusterManager clusterManager = new ClusterManager(new ConsistentHashRing());
        ReflectionTestUtils.setField(clusterManager, "port", 8080);
        ReflectionTestUtils.setField(clusterManager, "host", "localhost");
        ReflectionTestUtils.setField(clusterManager, "seedNodes", "localhost:8080,localhost:8081,localhost:8082");
        clusterManager.init();
        return clusterManager;
    }

    private QuorumCoordinator createCoordinator(ClusterManager clusterManager, RestTemplate restTemplate, int n, int w, int r) {
        QuorumCoordinator coordinator = new QuorumCoordinator(clusterManager, restTemplate);
        ReflectionTestUtils.setField(coordinator, "N", n);
        ReflectionTestUtils.setField(coordinator, "W", w);
        ReflectionTestUtils.setField(coordinator, "R", r);
        return coordinator;
    }

    private void deleteRecursively(Path baseDir) throws IOException {
        if (!Files.exists(baseDir)) {
            return;
        }
        try (var paths = Files.walk(baseDir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        assertTrue(Files.notExists(baseDir));
    }

    private record StorageFixture(StorageEngine engine, WALManager walManager) {
    }
}
