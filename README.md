# Distributed Key-Value Store

A highly available, fault-tolerant distributed key-value store inspired by Amazon's DynamoDB, implemented in Java and Spring Boot. This system trades strict consistency for high availability using a **quorum-based replication model** and a **decentralized gossip protocol** to manage cluster state without a single point of failure.

## 🌟 Core Features

- **Consistent Hashing with Virtual Nodes**: Efficient $O(k/n)$ key redistribution. By assigning multiple virtual nodes to each physical server on a hash ring (using MD5), the system achieves uniform data distribution and minimal shuffling when nodes join or leave the cluster.
- **Quorum-Based Consistency ($R+W>N$)**: Tunable read ($R$) and write ($W$) quorums across replicated nodes ($N$). Guarantees that at least one node in any read quorum has the latest successfully written data, ensuring eventual consistency and high availability even during network partitions.
- **Gossip Protocol Failure Detection**: Decentralized cluster membership. Nodes periodically communicate their state to random peers. If a node fails to send a heartbeat within a threshold, it is suspected dead and automatically evicted from the cluster routing table.
- **Durability (WAL & Snapshots)**: Zero data loss on crashes. Every mutation is immediately appended to a Write-Ahead Log (WAL) on disk before being applied to the in-memory `ConcurrentHashMap`. Background snapshotting periodically dumps memory to JSON and truncates the WAL for fast recovery.

## 🏗 Architecture & Components

The system is broken down into several modular subsystems:

### 1. Storage Engine
- **`LocalStorage`**: The ultra-fast, in-memory `ConcurrentHashMap` where data resides.
- **`WALManager`**: Handles append-only logging of `PUT` and `DELETE` operations.
- **`SnapshotManager`**: Periodically serializes `LocalStorage` to disk (`snapshot.json`).
- **`StorageEngine`**: Orchestrates the above three. On startup, it loads the last snapshot and replays the WAL to perfectly restore state.

### 2. Cluster Topology
- **`ConsistentHashRing`**: Maps keys to the physical nodes using Virtual Nodes.
- **`ClusterManager`**: Maintains the list of known nodes and handles bootstrapping (seed nodes).

### 3. Distributed Protocols
- **`GossipService`**: Runs a background cron job (`@Scheduled`). Every 2 seconds, it picks a random peer and exchanges known node heartbeats, effectively propagating failure detection across the network in $O(\log N)$ time.
- **`QuorumCoordinator`**: The router for client requests. When a client performs a `GET` or `PUT`, the coordinator determines the $N$ replicas from the `ConsistentHashRing`, sends parallel REST requests to them, and waits for exactly $R$ or $W$ successful responses before acking the client.

---

## 🚀 Getting Started

### Prerequisites
- Java 21+
- Maven 3.9+

### Building the Project
Clone the repository and build the executable JAR:
```bash
./mvnw clean package -DskipTests
```

### Running a Local Cluster (3 Nodes)
To demonstrate the distributed nature of the application, start 3 independent Spring Boot instances on different ports. They are pre-configured to automatically discover each other using the seed nodes defined in `application.properties`.

**Open 3 separate terminals:**
```bash
# Node 1
java -jar target/distributed-kv-store-0.0.1-SNAPSHOT.jar --server.port=8080

# Node 2
java -jar target/distributed-kv-store-0.0.1-SNAPSHOT.jar --server.port=8081

# Node 3
java -jar target/distributed-kv-store-0.0.1-SNAPSHOT.jar --server.port=8082
```

Wait roughly 15-30 seconds. You will see logs indicating that the nodes have used the **Gossip Protocol** to discover each other!

---

## 📖 API Reference

Clients can send requests to **any** node in the cluster. The receiving node will act as the Coordinator for that specific request.

### Write a Key (`PUT`)
```bash
curl -X PUT http://localhost:8080/api/v1/kv/myKey \
     -H "Content-Type: text/plain" \
     -d "Hello Distributed World!"
```
*Returns `200 OK` once the write is successfully replicated to $W$ nodes.*

### Read a Key (`GET`)
```bash
curl http://localhost:8081/api/v1/kv/myKey
```
*Returns `200 OK` with the value once $R$ nodes have agreed on the result.*

### Delete a Key (`DELETE`)
```bash
curl -X DELETE http://localhost:8082/api/v1/kv/myKey
```
*Returns `204 No Content` once the tombstone/deletion is replicated to $W$ nodes.*

---

## 🧪 Testing Fault Tolerance & Recovery

### 1. High Availability (Node Crash)
- Write some data to the cluster using `PUT`.
- Kill Node 8081 (`Ctrl+C`).
- Execute a `GET` against Node 8080. The data will still successfully return because the Quorum ($R=2$, $N=3$) can still be satisfied by Node 8080 and Node 8082!
- Write a **new** key to Node 8080. It will also succeed ($W=2$).

### 2. Durability (Total Cluster Crash)
- Terminate **all** running nodes (simulating a data center power loss).
- Restart the nodes.
- When the Spring Boot application boots, the `StorageEngine` will parse the `.log` files in the `./data` directory.
- Perform a `GET` for the data you wrote previously. It will be 100% intact!
