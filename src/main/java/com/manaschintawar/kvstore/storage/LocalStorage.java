package com.manaschintawar.kvstore.storage;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class LocalStorage {
    private final Map<String, VersionedValue> data = new ConcurrentHashMap<>();

    public void putRecord(String key, VersionedValue record) {
        data.put(key, record);
    }

    public VersionedValue getRecord(String key) {
        return data.get(key);
    }

    /** Client-facing read: tombstones and missing keys are both null. */
    public String get(String key) {
        VersionedValue record = data.get(key);
        return (record == null || record.isTombstone()) ? null : record.getValue();
    }

    public boolean containsKey(String key) {
        VersionedValue record = data.get(key);
        return record != null && !record.isTombstone();
    }

    public Map<String, VersionedValue> getAll() {
        return data;
    }

    public void clear() {
        data.clear();
    }

    public void restore(Map<String, VersionedValue> snapshot) {
        data.clear();
        data.putAll(snapshot);
    }
}
