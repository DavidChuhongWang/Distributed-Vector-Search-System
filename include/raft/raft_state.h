#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "index/shard_manager.h"
#include "raft/raft_log.h"
#include "util/config.h"

#include "mutation.pb.h"
#include "raft.grpc.pb.h"

namespace dvs {

enum class Role { kFollower, kCandidate, kLeader };

class RaftState {
 public:
  RaftState(const NodeRuntimeConfig& config, ShardManager* shard_manager);
  ~RaftState();

  void Start();
  void Stop();

  bool IsLeader() const;
  std::string LeaderId() const;
  uint64_t CurrentTerm() const;

  // Throws if not leader.
  uint64_t ReplicateCommand(const distributed::CommandEnvelope& command);

  distributed::RequestVoteResponse HandleVoteRequest(const distributed::RequestVoteRequest& request);
  distributed::AppendEntriesResponse HandleAppendEntries(const distributed::AppendEntriesRequest& request);
  distributed::HeartbeatResponse HandleHeartbeat(const distributed::HeartbeatRequest& request);

 private:
  void ElectionLoop();
  void BecomeFollower(uint64_t term, std::string leader_id);
  void BecomeCandidate();
  void BecomeLeader();
  void ResetElectionTimerLocked();

  void BroadcastHeartbeats();
  void BroadcastAppendEntries();
  void SendAppendEntriesToPeer(const PeerInfo& peer);

  void ApplyCommittedEntries();

  bool HasVoteQuorum(uint32_t votes) const;

  const NodeRuntimeConfig config_;
  ShardManager* shard_manager_;
  mutable std::mutex mutex_;
  Role role_;
  uint64_t current_term_;
  std::string voted_for_;
  std::string leader_id_;
  uint64_t commit_index_;
  uint64_t last_applied_;

  RaftLog log_;

  std::unordered_map<std::string, uint64_t> next_index_;
  std::unordered_map<std::string, uint64_t> match_index_;

  std::unordered_map<std::string, std::unique_ptr<distributed::RaftConsensus::Stub>> peer_stubs_;

  std::atomic<bool> running_;
  std::thread election_thread_;
  std::condition_variable cv_;
  std::chrono::steady_clock::time_point election_deadline_;

  uint32_t votes_granted_;
};

}  // namespace dvs
