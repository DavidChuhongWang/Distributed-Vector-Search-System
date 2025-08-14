#include "cache/query_cache.h"

#include <cstring>
#include <stdexcept>

namespace dvs {

QueryCache::QueryCache(uint32_t max_entries, std::chrono::seconds ttl)
    : max_entries_(max_entries), ttl_(ttl) {
  if (max_entries_ == 0) {
    throw std::invalid_argument("QueryCache capacity must be positive");
  }
}

std::optional<CachedSearchResult> QueryCache::Lookup(const QueryCacheKey& key) {
  std::lock_guard lock(mutex_);
  auto it = entries_.find(key);
  if (it == entries_.end()) {
    return std::nullopt;
  }
  const auto now = std::chrono::steady_clock::now();
  if (it->second.expires_at < now) {
    lru_order_.erase(it->second.it);
    entries_.erase(it);
    return std::nullopt;
  }
  Touch(key);
  return it->second.value;
}

void QueryCache::Insert(const QueryCacheKey& key, const CachedSearchResult& value) {
  std::lock_guard lock(mutex_);
  auto it = entries_.find(key);
  if (it != entries_.end()) {
    lru_order_.erase(it->second.it);
    entries_.erase(it);
  }
  lru_order_.push_front(key);
  entries_.emplace(key, Entry{.value = value,
                              .expires_at = std::chrono::steady_clock::now() + ttl_,
                              .it = lru_order_.begin()});
  EvictIfNeeded();
}

void QueryCache::Invalidate() {
  std::lock_guard lock(mutex_);
  entries_.clear();
  lru_order_.clear();
}

QueryCacheKey QueryCache::BuildKey(const float* query, size_t dim, uint32_t top_k) {
  QueryCacheKey key;
  key.query_digest = Fnv1a64(query, dim);
  key.top_k = top_k;
  return key;
}

void QueryCache::Touch(const QueryCacheKey& key) {
  auto it = entries_.find(key);
  if (it == entries_.end()) {
    return;
  }
  lru_order_.erase(it->second.it);
  lru_order_.push_front(key);
  it->second.it = lru_order_.begin();
}

void QueryCache::EvictIfNeeded() {
  while (entries_.size() > max_entries_) {
    const auto& key = lru_order_.back();
    entries_.erase(key);
    lru_order_.pop_back();
  }
}

uint64_t QueryCache::Fnv1a64(const float* data, size_t dim) {
  constexpr uint64_t kFnvOffset = 1469598103934665603ULL;
  constexpr uint64_t kFnvPrime = 1099511628211ULL;
  uint64_t hash = kFnvOffset;
  const auto* bytes = reinterpret_cast<const unsigned char*>(data);
  const size_t total_bytes = dim * sizeof(float);
  for (size_t i = 0; i < total_bytes; ++i) {
    hash ^= static_cast<uint64_t>(bytes[i]);
    hash *= kFnvPrime;
  }
  return hash;
}

}  // namespace dvs
