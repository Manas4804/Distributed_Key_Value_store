package com.manaschintawar.kvstore.storage;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class LocalStorage {
    private final Map<String, String> data = new ConcurrentHashMap<>();

    public void put(String key, String value) {
        data.put(key, value);
    }

    public String get(String key) {
        return data.get(key);
    }

    public void delete(String key) {
        data.remove(key);
    }

    public boolean containsKey(String key) {
        return data.containsKey(key);
    }
    
    public Map<String, String> getAll() {
        return data;
    }
    
    public void clear() {
        data.clear();
    }
    
    public void restore(Map<String, String> snapshot) {
        data.clear();
        data.putAll(snapshot);
    }
}
