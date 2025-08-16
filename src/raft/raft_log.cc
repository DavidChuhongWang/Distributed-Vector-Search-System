#include "raft/raft_log.h"

#include <algorithm>
#include <stdexcept>

namespace dvs {

RaftLog::RaftLog() : commit_index_(0), last_applied_(0) {}

uint64_t RaftLog::LastIndex() const {
  std::lock_guard lock(mutex_);
  if (entries_.empty()) {
    return 0;
  }
  return entries_.back().index;
}

uint64_t RaftLog::LastTerm() const {
  std::lock_guard lock(mutex_);
  if (entries_.empty()) {
    return 0;
  }
  return entries_.back().term;
}

uint64_t RaftLog::CommitIndex() const {
  std::lock_guard lock(mutex_);
  return commit_index_;
}

uint64_t RaftLog::LastApplied() const {
  std::lock_guard lock(mutex_);
  return last_applied_;
}

void RaftLog::Append(const std::vector<LocalLogEntry>& entries) {
  if (entries.empty()) {
    return;
  }
  std::lock_guard lock(mutex_);
  for (const auto& entry : entries) {
    if (!entries_.empty() && entry.index != entries_.back().index + 1) {
      throw std::runtime_error("Raft log append with non-contiguous index");
    }
    entries_.push_back(entry);
  }
}

void RaftLog::Truncate(uint64_t from_index) {
  std::lock_guard lock(mutex_);
  if (entries_.empty()) {
    return;
  }
  if (from_index <= entries_.front().index) {
    entries_.clear();
    return;
  }
  auto it = std::lower_bound(entries_.begin(), entries_.end(), from_index, [](const auto& entry, uint64_t index) {
    return entry.index < index;
  });
  entries_.erase(it, entries_.end());
}

std::vector<LocalLogEntry> RaftLog::EntriesFrom(uint64_t from_index) const {
  std::lock_guard lock(mutex_);
  std::vector<LocalLogEntry> subset;
  for (const auto& entry : entries_) {
    if (entry.index >= from_index) {
      subset.push_back(entry);
    }
  }
  return subset;
}

void RaftLog::SetCommitIndex(uint64_t commit_index) {
  std::lock_guard lock(mutex_);
  commit_index_ = std::max(commit_index_, commit_index);
}

void RaftLog::SetLastApplied(uint64_t last_applied) {
  std::lock_guard lock(mutex_);
  last_applied_ = std::max(last_applied_, last_applied);
}

std::optional<LocalLogEntry> RaftLog::EntryAt(uint64_t index) const {
  std::lock_guard lock(mutex_);
  for (const auto& entry : entries_) {
    if (entry.index == index) {
      return entry;
    }
  }
  return std::nullopt;
}

}  // namespace dvs
