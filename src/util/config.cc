#include "util/config.h"

#include <fstream>
#include <stdexcept>

#include <google/protobuf/text_format.h>

namespace dvs {

NodeRuntimeConfig ConfigLoader::FromFile(const std::string& path) {
  std::ifstream stream(path);
  if (!stream.is_open()) {
    throw std::runtime_error("Failed to open config file: " + path);
  }

  std::string content{std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>()};
  distributed::NodeConfig proto;
  if (!google::protobuf::TextFormat::ParseFromString(content, &proto)) {
    throw std::runtime_error("Unable to parse node config at: " + path);
  }
  return FromProto(proto);
}

NodeRuntimeConfig ConfigLoader::FromProto(const distributed::NodeConfig& proto) {
  NodeRuntimeConfig cfg;
  cfg.node_id = proto.node_id();
  cfg.bind_address = proto.bind_address();
  cfg.peers.reserve(proto.peers_size());
  for (const auto& peer : proto.peers()) {
    cfg.peers.push_back(PeerInfo{.node_id = peer.node_id(), .address = peer.address()});
  }
  cfg.shards.reserve(proto.shards_size());
  for (const auto& shard : proto.shards()) {
    cfg.shards.push_back(ShardInfo{
        .shard_id = shard.shard_id(),
        .dimension = shard.dimension(),
        .index_path = shard.index_path(),
    });
  }
  cfg.cache = CacheSettings{
      .max_entries = proto.has_cache() ? proto.cache().max_entries() : 1024,
      .ttl_seconds = proto.has_cache() ? proto.cache().default_ttl_seconds() : 60,
  };
  cfg.batching = BatchingSettings{
      .preferred_batch_size = proto.has_batching() ? proto.batching().preferred_batch_size() : 8,
      .max_batch_delay_ms = proto.has_batching() ? proto.batching().max_batch_delay_ms() : 2,
  };
  return cfg;
}

}  // namespace dvs
