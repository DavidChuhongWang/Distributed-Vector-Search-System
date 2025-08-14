#pragma once

#include <chrono>
#include <cstdint>
#include <list>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "index/vector_shard.h"

namespace dvs {

struct QueryCacheKey {
  uint64_t query_digest;
  uint32_t top_k;

  bool operator==(const QueryCacheKey& other) const {
    return query_digest == other.query_digest && top_k == other.top_k;
  }
};

struct QueryCacheKeyHash {
  size_t operator()(const QueryCacheKey& key) const noexcept {
    return std::hash<uint64_t>{}(key.query_digest) ^ (static_cast<size_t>(key.top_k) << 1);
  }
};

struct CachedSearchResult {
  std::vector<SearchResult> hits;
  bool exact_match;
};

class QueryCache {
 public:
  QueryCache(uint32_t max_entries, std::chrono::seconds ttl);

  std::optional<CachedSearchResult> Lookup(const QueryCacheKey& key);
  void Insert(const QueryCacheKey& key, const CachedSearchResult& value);
  void Invalidate();

  static QueryCacheKey BuildKey(const float* query, size_t dim, uint32_t top_k);

 private:
  using LruList = std::list<QueryCacheKey>;

  void Touch(const QueryCacheKey& key);
  void EvictIfNeeded();
  static uint64_t Fnv1a64(const float* data, size_t dim);

  const uint32_t max_entries_;
  const std::chrono::seconds ttl_;

  std::mutex mutex_;
  struct Entry {
    CachedSearchResult value;
    std::chrono::steady_clock::time_point expires_at;
    LruList::iterator it;
  };

  std::unordered_map<QueryCacheKey, Entry, QueryCacheKeyHash> entries_;
  LruList lru_order_;
};

}  // namespace dvs
