#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace dvs {

struct LocalLogEntry {
  uint64_t term;
  uint64_t index;
  std::string command;  // Serialized mutation command.
};

class RaftLog {
 public:
  RaftLog();

  uint64_t LastIndex() const;
  uint64_t LastTerm() const;
  uint64_t CommitIndex() const;
  uint64_t LastApplied() const;

  void Append(const std::vector<LocalLogEntry>& entries);
  void Truncate(uint64_t from_index);

  std::vector<LocalLogEntry> EntriesFrom(uint64_t from_index) const;

  void SetCommitIndex(uint64_t commit_index);
  void SetLastApplied(uint64_t last_applied);

  std::optional<LocalLogEntry> EntryAt(uint64_t index) const;

 private:
  mutable std::mutex mutex_;
  std::vector<LocalLogEntry> entries_;
  uint64_t commit_index_;
  uint64_t last_applied_;
};

}  // namespace dvs
