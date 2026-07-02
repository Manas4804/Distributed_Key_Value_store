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
        Map<String, VersionedValue> snapshot = snapshotManager.loadSnapshot();
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

    /** Convenience overload: coordinator normally supplies the timestamp. */
    public void put(String key, String value) {
        put(key, value, System.currentTimeMillis());
    }

    /**
     * Applies a versioned write using last-write-wins: an incoming record older
     * than the one already stored is ignored. This makes replication and
     * read-repair idempotent and prevents stale writes from clobbering newer data.
     */
    public synchronized void put(String key, String value, long timestamp) {
        VersionedValue existing = localStorage.getRecord(key);
        if (existing != null && existing.getTimestamp() >= timestamp) {
            return; // stale write, ignore
        }
        walManager.append("PUT", key, value, timestamp);
        localStorage.putRecord(key, new VersionedValue(value, timestamp, false));
    }

    /** Client-facing read: returns null for missing keys and tombstones. */
    public String get(String key) {
        return localStorage.get(key);
    }

    /** Replica-facing read: returns the full record, including tombstones. */
    public VersionedValue getRecord(String key) {
        return localStorage.getRecord(key);
    }

    /** Convenience overload: coordinator normally supplies the timestamp. */
    public void delete(String key) {
        delete(key, System.currentTimeMillis());
    }

    /** Deletes are stored as tombstones so missed deletes cannot resurrect values. */
    public synchronized void delete(String key, long timestamp) {
        VersionedValue existing = localStorage.getRecord(key);
        if (existing != null && existing.getTimestamp() >= timestamp) {
            return; // stale delete, ignore
        }
        walManager.append("DELETE", key, null, timestamp);
        localStorage.putRecord(key, new VersionedValue(null, timestamp, true));
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
            return new WALManager.WalEntry(parts[0], parts[1], value, 0L);
        }
    }

    private void applyWalEntry(WALManager.WalEntry entry) {
        VersionedValue existing = localStorage.getRecord(entry.getKey());
        // Legacy entries (timestamp 0) are applied in log order; versioned entries use LWW.
        if (entry.getTimestamp() > 0 && existing != null && existing.getTimestamp() >= entry.getTimestamp()) {
            return;
        }
        if ("PUT".equals(entry.getOperation())) {
            localStorage.putRecord(entry.getKey(), new VersionedValue(entry.getValue(), entry.getTimestamp(), false));
        } else if ("DELETE".equals(entry.getOperation())) {
            localStorage.putRecord(entry.getKey(), new VersionedValue(null, entry.getTimestamp(), true));
        } else {
            log.warn("Skipping unknown WAL operation: {}", entry.getOperation());
        }
    }
}
