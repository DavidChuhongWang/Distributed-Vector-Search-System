#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include <faiss/Index.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexIDMap.h>

namespace dvs {

struct SearchResult {
  uint64_t id;
  float distance;
  std::string shard_id;
};

class VectorShard {
 public:
  VectorShard(std::string shard_id, uint32_t dimension, const std::string& index_path = "");

  void Upsert(uint64_t id, const std::vector<float>& vector);
  bool Remove(uint64_t id);

  std::vector<SearchResult> Search(const float* query, size_t top_k) const;

  const std::string& shard_id() const { return shard_id_; }
  uint32_t dimension() const { return dimension_; }
  size_t size() const;

 private:
  void PersistUnlocked();

  const std::string shard_id_;
  const uint32_t dimension_;
  const std::string index_path_;

  std::unique_ptr<faiss::Index> index_;

  mutable std::shared_mutex mutex_;
};

}  // namespace dvs
