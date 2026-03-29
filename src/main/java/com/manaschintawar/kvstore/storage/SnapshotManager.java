package com.manaschintawar.kvstore.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Map;

@Component
public class SnapshotManager {
    private static final Logger log = LoggerFactory.getLogger(SnapshotManager.class);

    @Value("${kvstore.storage.snapshot.dir:./data}")
    private String snapshotDir;

    @Value("${server.port:8080}")
    private int port;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private File snapshotFile;

    @PostConstruct
    public void init() {
        File dir = new File(snapshotDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        snapshotFile = new File(dir, "snapshot_" + port + ".json");
    }

    public void saveSnapshot(Map<String, String> data) {
        try {
            objectMapper.writeValue(snapshotFile, data);
            log.info("Snapshot saved successfully to {}", snapshotFile.getAbsolutePath());
        } catch (IOException e) {
            log.error("Failed to save snapshot", e);
        }
    }

    public Map<String, String> loadSnapshot() {
        if (!snapshotFile.exists()) {
            return null;
        }
        try {
            return objectMapper.readValue(snapshotFile, new TypeReference<Map<String, String>>() {});
        } catch (IOException e) {
            log.error("Failed to load snapshot", e);
            return null;
        }
    }
}
