package com.manaschintawar.kvstore.api;

import com.manaschintawar.kvstore.quorum.QuorumCoordinator;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/kv")
public class ClientController {

    private final QuorumCoordinator quorumCoordinator;

    public ClientController(QuorumCoordinator quorumCoordinator) {
        this.quorumCoordinator = quorumCoordinator;
    }

    @PutMapping("/{key}")
    public ResponseEntity<Void> put(@PathVariable String key, @RequestBody String value) {
        boolean success = quorumCoordinator.write(key, value);
        if (success) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }
    }

    @GetMapping("/{key}")
    public ResponseEntity<String> get(@PathVariable String key) {
        try {
            String value = quorumCoordinator.read(key);
            if (value == null) {
                return ResponseEntity.notFound().build();
            }
            return ResponseEntity.ok(value);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(e.getMessage());
        }
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<Void> delete(@PathVariable String key) {
        boolean success = quorumCoordinator.delete(key);
        if (success) {
            return ResponseEntity.noContent().build();
        } else {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build();
        }
    }
}
