package com.manaschintawar.kvstore.api;

import com.manaschintawar.kvstore.storage.StorageEngine;
import com.manaschintawar.kvstore.topology.GossipRequest;
import com.manaschintawar.kvstore.topology.GossipService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/internal")
public class InternalNodeController {

    private final GossipService gossipService;
    private final StorageEngine storageEngine;

    public InternalNodeController(GossipService gossipService, StorageEngine storageEngine) {
        this.gossipService = gossipService;
        this.storageEngine = storageEngine;
    }

    @PostMapping("/gossip")
    public void receiveGossip(@RequestBody GossipRequest request) {
        gossipService.receiveGossip(request);
    }

    @PutMapping("/kv/{key}")
    public void putInternal(@PathVariable String key, @RequestBody String value) {
        storageEngine.put(key, value);
    }

    @GetMapping("/kv/{key}")
    public ResponseEntity<String> getInternal(@PathVariable String key) {
        String value = storageEngine.get(key);
        if (value == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(value);
    }

    @DeleteMapping("/kv/{key}")
    public void deleteInternal(@PathVariable String key) {
        storageEngine.delete(key);
    }
}
