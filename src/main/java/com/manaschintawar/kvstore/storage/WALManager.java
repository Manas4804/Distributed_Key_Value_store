package com.manaschintawar.kvstore.storage;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Component
public class WALManager {
    private static final Logger log = LoggerFactory.getLogger(WALManager.class);

    @Value("${kvstore.storage.wal.dir:./data}")
    private String walDir;

    @Value("${server.port:8080}")
    private int port;

    private BufferedWriter writer;
    private Path walFilePath;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @PostConstruct
    public void init() {
        try {
            Path dir = Paths.get(walDir);
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
            walFilePath = dir.resolve("wal_" + port + ".log");
            writer = Files.newBufferedWriter(walFilePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            log.info("WAL Initialized at {}", walFilePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize WAL", e);
        }
    }

    public void append(String operation, String key, String value) {
        lock.writeLock().lock();
        try {
            String entry = String.format("%s,%s,%s\n", operation, key, value == null ? "" : value);
            writer.write(entry);
            writer.flush(); // Ensure it's immediately written to disk
        } catch (IOException e) {
            log.error("Failed to append to WAL", e);
            throw new RuntimeException("Failed to append to WAL", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public BufferedReader getReader() throws IOException {
        if (!Files.exists(walFilePath)) {
            return null;
        }
        return Files.newBufferedReader(walFilePath);
    }

    public void clearWAL() {
        lock.writeLock().lock();
        try {
            if (writer != null) {
                writer.close();
            }
            Files.deleteIfExists(walFilePath);
            writer = Files.newBufferedWriter(walFilePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Failed to clear WAL", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @PreDestroy
    public void close() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            log.error("Error closing WAL writer", e);
        }
    }
}
