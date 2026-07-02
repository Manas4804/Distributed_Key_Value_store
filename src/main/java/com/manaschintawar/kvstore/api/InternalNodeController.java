package com.manaschintawar.kvstore.api;

import com.manaschintawar.kvstore.quorum.QuorumCoordinator;
import com.manaschintawar.kvstore.storage.StorageEngine;
import com.manaschintawar.kvstore.storage.VersionedValue;
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
    public void putInternal(@PathVariable String key,
                            @RequestBody String value,
                            @RequestHeader(value = QuorumCoordinator.TIMESTAMP_HEADER, required = false) Long timestamp) {
        storageEngine.put(key, value, timestamp != null ? timestamp : System.currentTimeMillis());
    }

    /** Returns the full versioned record (including tombstones) so the coordinator can resolve conflicts. */
    @GetMapping("/kv/{key}")
    public ResponseEntity<VersionedValue> getInternal(@PathVariable String key) {
        VersionedValue record = storageEngine.getRecord(key);
        if (record == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(record);
    }

    @DeleteMapping("/kv/{key}")
    public void deleteInternal(@PathVariable String key,
                               @RequestHeader(value = QuorumCoordinator.TIMESTAMP_HEADER, required = false) Long timestamp) {
        storageEngine.delete(key, timestamp != null ? timestamp : System.currentTimeMillis());
    }
}
