#include "index/shard_manager.h"

#include <algorithm>
#include <chrono>
#include <stdexcept>

#include <omp.h>

namespace dvs {

ShardManager::ShardManager(const NodeRuntimeConfig& config)
    : cache_(config.cache.max_entries, std::chrono::seconds(config.cache.ttl_seconds)),
      dimension_(0),
      preferred_batch_size_(std::max<uint32_t>(1, config.batching.preferred_batch_size)),
      max_batch_delay_ms_(config.batching.max_batch_delay_ms) {
  if (config.shards.empty()) {
    throw std::invalid_argument("At least one shard must be configured");
  }
  dimension_ = config.shards.front().dimension;
  for (const auto& shard_cfg : config.shards) {
    if (shard_cfg.dimension != dimension_) {
      throw std::invalid_argument("Inconsistent vector dimensions across shards");
    }
    auto shard = std::make_shared<VectorShard>(shard_cfg.shard_id, shard_cfg.dimension, shard_cfg.index_path);
    shard_index_.emplace(shard_cfg.shard_id, shard);
    shards_.push_back(std::move(shard));
  }
}

std::vector<SearchResult> ShardManager::Search(const distributed::SearchRequest& request, bool* served_from_cache) {
  if (request.top_k() == 0) {
    if (served_from_cache) {
      *served_from_cache = false;
    }
    return {};
  }
  auto query = NormalizeQuery(request.query(), dimension_);
  const auto key = QueryCache::BuildKey(query.data(), query.size(), request.top_k());
  if (auto cached = cache_.Lookup(key)) {
    if (served_from_cache) {
      *served_from_cache = true;
    }
    return cached->hits;
  }

  std::vector<std::vector<SearchResult>> per_shard(shards_.size());

#pragma omp parallel for schedule(dynamic) if (shards_.size() > 1)
  for (int64_t i = 0; i < static_cast<int64_t>(shards_.size()); ++i) {
    per_shard[i] = shards_[i]->Search(query.data(), request.top_k());
  }

  auto merged = MergeTopK(per_shard, request.top_k());
  cache_.Insert(key, CachedSearchResult{.hits = merged, .exact_match = true});
  if (served_from_cache) {
    *served_from_cache = false;
  }
  return merged;
}

std::vector<std::vector<SearchResult>> ShardManager::BatchSearch(const distributed::BatchSearchRequest& request, std::vector<bool>* served_from_cache) {
  const size_t batch_size = request.requests_size();
  std::vector<std::vector<SearchResult>> responses(batch_size);
  if (served_from_cache) {
    served_from_cache->assign(batch_size, false);
  }

#pragma omp parallel for schedule(dynamic) if (batch_size > 1)
  for (int64_t i = 0; i < static_cast<int64_t>(batch_size); ++i) {
    bool cached = false;
    responses[i] = Search(request.requests(i), &cached);
    if (served_from_cache) {
      (*served_from_cache)[i] = cached;
    }
  }

  return responses;
}

void ShardManager::Upsert(const distributed::UpsertRequest& request) {
  const auto it = shard_index_.find(request.shard_id());
  if (it == shard_index_.end()) {
    throw std::invalid_argument("Unknown shard: " + request.shard_id());
  }
  const auto& values = request.vector().values();
  if (values.size() != dimension_) {
    throw std::invalid_argument("Upsert vector dimension mismatch");
  }
  it->second->Upsert(request.id(), {values.begin(), values.end()});
  cache_.Invalidate();
}

bool ShardManager::Remove(const distributed::DeleteRequest& request) {
  const auto it = shard_index_.find(request.shard_id());
  if (it == shard_index_.end()) {
    return false;
  }
  const bool removed = it->second->Remove(request.id());
  if (removed) {
    cache_.Invalidate();
  }
  return removed;
}

void ShardManager::WarmCache(const distributed::WarmRequest& request) {
  if (request.ids_size() == 0) {
    return;
  }
  // Touch shards to load specific vectors into memory by performing dummy queries.
#pragma omp parallel for schedule(static) if (shards_.size() > 1)
  for (int64_t i = 0; i < static_cast<int64_t>(shards_.size()); ++i) {
    auto shard = shards_[i];
    if (shard->size() == 0) {
      continue;
    }
    std::vector<float> probe(dimension_, 0.0f);
    shard->Search(probe.data(), 1);
  }
}

std::vector<float> ShardManager::NormalizeQuery(const distributed::QueryVector& query, uint32_t expected_dim) {
  if (static_cast<uint32_t>(query.values_size()) != expected_dim) {
    throw std::invalid_argument("Query vector dimension mismatch");
  }
  std::vector<float> data(query.values().begin(), query.values().end());
  return data;
}

std::vector<SearchResult> ShardManager::MergeTopK(const std::vector<std::vector<SearchResult>>& per_shard, size_t top_k) {
  std::vector<SearchResult> aggregate;
  for (const auto& shard_hits : per_shard) {
    aggregate.insert(aggregate.end(), shard_hits.begin(), shard_hits.end());
  }
  if (aggregate.size() <= top_k) {
    std::sort(aggregate.begin(), aggregate.end(), [](const auto& lhs, const auto& rhs) {
      return lhs.distance < rhs.distance;
    });
    return aggregate;
  }
  std::partial_sort(aggregate.begin(), aggregate.begin() + top_k, aggregate.end(), [](const auto& lhs, const auto& rhs) {
    return lhs.distance < rhs.distance;
  });
  aggregate.resize(top_k);
  return aggregate;
}

}  // namespace dvs
