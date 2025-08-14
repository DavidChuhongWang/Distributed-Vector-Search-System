#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cache/query_cache.h"
#include "index/vector_shard.h"
#include "util/config.h"

#include "search.pb.h"

namespace dvs {

struct SearchContext {
  std::vector<float> query;
  uint32_t top_k;
};

class ShardManager {
 public:
  explicit ShardManager(const NodeRuntimeConfig& config);

  std::vector<SearchResult> Search(const distributed::SearchRequest& request, bool* served_from_cache);
  std::vector<std::vector<SearchResult>> BatchSearch(const distributed::BatchSearchRequest& request, std::vector<bool>* served_from_cache);

  void Upsert(const distributed::UpsertRequest& request);
  bool Remove(const distributed::DeleteRequest& request);
  void WarmCache(const distributed::WarmRequest& request);

  uint32_t Dimension() const { return dimension_; }

 private:
  static std::vector<float> NormalizeQuery(const distributed::QueryVector& query, uint32_t expected_dim);
  static std::vector<SearchResult> MergeTopK(const std::vector<std::vector<SearchResult>>& per_shard, size_t top_k);

  std::vector<std::shared_ptr<VectorShard>> shards_;
  std::unordered_map<std::string, std::shared_ptr<VectorShard>> shard_index_;
  QueryCache cache_;
  uint32_t dimension_;
  uint32_t preferred_batch_size_;
  uint32_t max_batch_delay_ms_;
};

}  // namespace dvs
