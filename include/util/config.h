#pragma once

#include <string>
#include <vector>

#include "config.pb.h"

namespace dvs {

struct PeerInfo {
  std::string node_id;
  std::string address;
};

struct ShardInfo {
  std::string shard_id;
  uint32_t dimension;
  std::string index_path;
};

struct CacheSettings {
  uint32_t max_entries;
  uint32_t ttl_seconds;
};

struct BatchingSettings {
  uint32_t preferred_batch_size;
  uint32_t max_batch_delay_ms;
};

struct NodeRuntimeConfig {
  std::string node_id;
  std::string bind_address;
  std::vector<PeerInfo> peers;
  std::vector<ShardInfo> shards;
  CacheSettings cache;
  BatchingSettings batching;
};

class ConfigLoader {
 public:
  // Loads node configuration from a protobuf text-format file.
  static NodeRuntimeConfig FromFile(const std::string& path);
  // Converts a protobuf NodeConfig into runtime configuration.
  static NodeRuntimeConfig FromProto(const distributed::NodeConfig& proto);
};

}  // namespace dvs
