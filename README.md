# Distributed Vector Search System

High-throughput distributed similarity search engine with FAISS-powered sharded indices, in-memory query caching, OpenMP-parallel query fan-out, and a Raft-backed control plane for fault-tolerant replication.

## Highlights
- **Sharded FAISS indices** with per-shard persistence and hot reloading.
- **Parallel fan-out + merge**: OpenMP accelerates multi-shard top-*k* aggregation and request batching.
- **Adaptive query cache**: LRU with TTL to accelerate repeated lookups.
- **Raft consensus** over gRPC for leader election and write-ahead command replication.
- **gRPC APIs** for search, batch search, warm-up, and cluster mutations.

## Architecture
```
                +----------------------+          +----------------------+
                |   Client Libraries   |          |   Admin Tooling      |
                |  (gRPC / HTTP Gate)  |          |  (Warm / Upsert CLI) |
                +----------+-----------+          +----------+-----------+
                           |                               |
                           | gRPC                          | gRPC
                           v                               v
                +-----------------------------------------------+
                |         Vector Search Node (This Repo)        |
                |                                               |
                |  +----------------------+   +---------------+ |
                |  |  RPC Layer           |   | Raft Consensus| |
                |  |  - SearchService     |   |  - Leader Elec| |
                |  |  - Control RPCs      |   |  - Log Replic | |
                |  +----------+-----------+   +-------+-------+ |
                |             |                       |         |
                |             v                       v         |
                |  +----------------------+   +---------------+ |
                |  | Shard Manager        |<->| Raft Log      | |
                |  |  - Query Planner     |   |  - CommitIdx  | |
                |  |  - Batch Executor    |   +---------------+ |
                |  +----------+-----------+                     |
                |             |                                 |
                |             v                                 |
                |  +----------------------+                     |
                |  |  Cache + FAISS Pool  |                     |
                |  |  - LRU Query Cache   |                     |
                |  |  - Vector Shards     |                     |
                |  +----------+-----------+                     |
                +-------------|---------------------------------+
                              |
                              | Persistent Index Files
                              v
                +----------------------+   +----------------------+
                |  indices/*.faiss     |   |  Config *.textproto  |
                +----------------------+   +----------------------+
```

## Repository Layout
```
CMakeLists.txt          # Build definition (requires Protobuf, gRPC, FAISS, OpenMP)
proto/                  # Protobuf service definitions
include/                # Public headers for indices, caching, raft, utilities
src/                    # Implementations and node entrypoint
config/                 # Sample configuration (protobuf text format)
```

## Build Prerequisites
- CMake ≥ 3.20
- A C++20 compiler (Clang or GCC recommended)
- [gRPC](https://grpc.io/) with C++ plugin (`grpc_cpp_plugin` on PATH)
- Protocol Buffers (libprotobuf + `protoc`)
- [FAISS](https://github.com/facebookresearch/faiss) with C++ headers and libraries
- OpenMP runtime (`libomp`/`libgomp`)

On macOS (Homebrew example):
```bash
brew install cmake protobuf grpc faiss libomp
```
On Ubuntu/Debian (apt + source for gRPC/FAISS if needed):
```bash
sudo apt-get install build-essential cmake protobuf-compiler libprotobuf-dev libgrpc++-dev grpc-proto libfaiss-dev libomp-dev
```

## Building
```bash
mkdir -p build
cd build
cmake ..
cmake --build . --target vector_search_node -j
```
Artifacts:
- `vector_search_node`: cluster node binary
- `generated/`: auto-generated protobuf sources

## Configuration
Nodes load a protobuf text-format config (`distributed.NodeConfig`). See `config/node1.textproto` for a full example. Key fields:
- `node_id`: unique Raft member id
- `bind_address`: `host:port` the node listens on
- `peers`: list of other nodes `{ node_id, address }`
- `shards`: shard definitions `{ shard_id, dimension, index_path }`
- `cache`: LRU cache sizing and TTL
- `batching`: preferred batch size and maximum in-flight delay

Create one file per node, adjusting `node_id`, `bind_address`, and `peers` accordingly.

## Running a Cluster
From three terminals (or tmux panes), launch each node with its config:
```bash
./build/vector_search_node --config=../config/node1.textproto
./build/vector_search_node --config=../config/node2.textproto
./build/vector_search_node --config=../config/node3.textproto
```
Each node will:
1. Load or initialize FAISS shards (persisted at the configured `index_path`).
2. Start the gRPC server for search and Raft endpoints.
3. Participate in Raft leader election, then serve traffic once a leader is elected.

Stop nodes with `Ctrl+C`; a signal-aware shutdown thread flushes Raft and FAISS state before exit.

## Client APIs
The gRPC interface is defined in `proto/search.proto`:
- `Search`: single-vector top-*k* query with consistency hints.
- `BatchSearch`: multi-request batch to amortize fan-out and merging.
- `Upsert`/`Delete`: mutating RPCs (leader-only, internally replicated via Raft).
- `WarmCache`: pre-warm cache entries.

Generate stubs in your preferred language using `protoc`. Example (Python):
```bash
python -m grpc_tools.protoc -Iproto --python_out=. --grpc_python_out=. proto/search.proto
```
Then target the leader (see `leader_hint` fields when a follower responds).

## Operational Notes
- **Persistence**: Each shard writes to `index_path` after successful mutations.
- **Cache invalidation**: Any applied mutation invalidates the query cache to keep results consistent.
- **Consensus**: The Raft log serializes `CommandEnvelope` entries so followers replay the same mutations.
- **Scaling shards**: Add additional `ShardConfig` entries per node; adjust `ShardManager` wiring if you distribute shards unevenly.

## Development Workflow
- Protobuf changes: edit `proto/*.proto`, then rebuild (`cmake --build .`).
- Unit testing hooks can be added via new CMake targets; the repository currently focuses on functional services.
- Logging/tracing: integrate your preferred framework inside the service classes.

## Troubleshooting
- **Missing dependencies**: Re-run `cmake` to check detection messages.
- **Leader not elected**: ensure all peers can reach each other and `node_id`s are unique.
- **FAISS index mismatch**: clear stale index files or align the `dimension` in configs.

Happy searching!
