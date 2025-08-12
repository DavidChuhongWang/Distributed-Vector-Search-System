#include "index/vector_shard.h"

#include <filesystem>
#include <stdexcept>

#include <faiss/IndexIDMap.h>
#include <faiss/index_io.h>
#include <faiss/impl/IDSelector.h>

namespace dvs {

namespace {
faiss::IndexIDMap2* AsIdMap(faiss::Index* index) {
  auto* id_map = dynamic_cast<faiss::IndexIDMap2*>(index);
  if (id_map == nullptr) {
    throw std::runtime_error("Index is not an IndexIDMap2 instance");
  }
  return id_map;
}

const faiss::IndexIDMap2* AsIdMapConst(const faiss::Index* index) {
  auto* id_map = dynamic_cast<const faiss::IndexIDMap2*>(index);
  if (id_map == nullptr) {
    throw std::runtime_error("Index is not an IndexIDMap2 instance");
  }
  return id_map;
}
}  // namespace

VectorShard::VectorShard(std::string shard_id, uint32_t dimension, const std::string& index_path)
    : shard_id_(std::move(shard_id)),
      dimension_(dimension),
      index_path_(index_path) {
  if (!index_path_.empty() && std::filesystem::exists(index_path_)) {
    std::unique_ptr<faiss::Index> loaded(faiss::read_index(index_path_.c_str()));
    if (loaded->d != dimension_) {
      throw std::runtime_error("Index dimension mismatch for shard " + shard_id_);
    }
    index_.swap(loaded);
  } else {
    auto* flat = new faiss::IndexFlatL2(dimension_);
    index_.reset(new faiss::IndexIDMap2(flat));
  }
}

void VectorShard::Upsert(uint64_t id, const std::vector<float>& vector) {
  if (vector.size() != dimension_) {
    throw std::invalid_argument("Vector dimension mismatch for shard " + shard_id_);
  }
  std::unique_lock lock(mutex_);
  auto* id_map = AsIdMap(index_.get());
  const faiss::idx_t faiss_id = static_cast<faiss::idx_t>(id);
  faiss::IDSelectorBatch selector(1, &faiss_id);
  id_map->remove_ids(selector);
  id_map->add_with_ids(1, vector.data(), &faiss_id);
  PersistUnlocked();
}

bool VectorShard::Remove(uint64_t id) {
  std::unique_lock lock(mutex_);
  auto* id_map = AsIdMap(index_.get());
  const faiss::idx_t faiss_id = static_cast<faiss::idx_t>(id);
  faiss::IDSelectorBatch selector(1, &faiss_id);
  auto removed = id_map->remove_ids(selector);
  if (removed > 0) {
    PersistUnlocked();
    return true;
  }
  return false;
}

std::vector<SearchResult> VectorShard::Search(const float* query, size_t top_k) const {
  std::shared_lock lock(mutex_);
  const auto* id_map = AsIdMapConst(index_.get());
  if (id_map->ntotal == 0 || top_k == 0) {
    return {};
  }
  std::vector<float> distances(top_k);
  std::vector<faiss::idx_t> labels(top_k);
  id_map->search(1, query, static_cast<faiss::idx_t>(top_k), distances.data(), labels.data());
  std::vector<SearchResult> results;
  results.reserve(top_k);
  for (size_t i = 0; i < top_k; ++i) {
    if (labels[i] < 0) {
      continue;
    }
    results.push_back(SearchResult{
        .id = static_cast<uint64_t>(labels[i]),
        .distance = distances[i],
        .shard_id = shard_id_,
    });
  }
  return results;
}

size_t VectorShard::size() const {
  std::shared_lock lock(mutex_);
  return AsIdMapConst(index_.get())->ntotal;
}

void VectorShard::PersistUnlocked() {
  if (index_path_.empty()) {
    return;
  }
  faiss::write_index(index_.get(), index_path_.c_str());
}

}  // namespace dvs
