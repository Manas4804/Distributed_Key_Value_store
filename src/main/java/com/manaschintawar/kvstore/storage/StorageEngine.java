package com.manaschintawar.kvstore.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.BufferedReader;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class StorageEngine {
    private static final Logger log = LoggerFactory.getLogger(StorageEngine.class);

    private final LocalStorage localStorage;
    private final WALManager walManager;
    private final SnapshotManager snapshotManager;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public StorageEngine(LocalStorage localStorage, WALManager walManager, SnapshotManager snapshotManager) {
        this.localStorage = localStorage;
        this.walManager = walManager;
        this.snapshotManager = snapshotManager;
    }

    @PostConstruct
    public void restore() {
        log.info("Starting restoration process...");
        // 1. Load Snapshot
        Map<String, String> snapshot = snapshotManager.loadSnapshot();
        if (snapshot != null) {
            localStorage.restore(snapshot);
            log.info("Restored {} keys from snapshot.", snapshot.size());
        }

        // 2. Replay WAL
        try {
            BufferedReader reader = walManager.getReader();
            if (reader != null) {
                int count = 0;
                String line;
                while ((line = reader.readLine()) != null) {
                    WALManager.WalEntry entry = parseWalEntry(line);
                    if (entry != null) {
                        applyWalEntry(entry);
                        count++;
                    }
                }
                reader.close();
                log.info("Replayed {} operations from WAL.", count);
            }
        } catch (Exception e) {
            log.error("Error replaying WAL", e);
        }

        // 3. Start periodic snapshot task (every 5 minutes)
        scheduler.scheduleAtFixedRate(this::takeSnapshot, 5, 5, TimeUnit.MINUTES);
    }

    public synchronized void put(String key, String value) {
        walManager.append("PUT", key, value);
        localStorage.put(key, value);
    }

    public String get(String key) {
        return localStorage.get(key);
    }

    public synchronized void delete(String key) {
        walManager.append("DELETE", key, null);
        localStorage.delete(key);
    }

    public synchronized void takeSnapshot() {
        log.info("Taking background snapshot...");
        snapshotManager.saveSnapshot(localStorage.getAll());
        walManager.clearWAL();
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
    }

    private WALManager.WalEntry parseWalEntry(String line) {
        try {
            return objectMapper.readValue(line, WALManager.WalEntry.class);
        } catch (Exception ignored) {
            String[] parts = line.split(",", 3);
            if (parts.length < 2) {
                log.warn("Skipping malformed WAL entry: {}", line);
                return null;
            }
            String value = parts.length > 2 ? parts[2] : "";
            return new WALManager.WalEntry(parts[0], parts[1], value);
        }
    }

    private void applyWalEntry(WALManager.WalEntry entry) {
        if ("PUT".equals(entry.getOperation())) {
            localStorage.put(entry.getKey(), entry.getValue());
        } else if ("DELETE".equals(entry.getOperation())) {
            localStorage.delete(entry.getKey());
        } else {
            log.warn("Skipping unknown WAL operation: {}", entry.getOperation());
        }
    }
}
