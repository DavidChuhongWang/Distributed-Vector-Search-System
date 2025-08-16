#include "raft/raft_state.h"

#include <algorithm>
#include <chrono>
#include <random>
#include <stdexcept>
#include <utility>
#include <vector>

#include "search.pb.h"

namespace dvs {
namespace {
constexpr std::chrono::milliseconds kHeartbeatInterval{50};
constexpr std::chrono::milliseconds kElectionTimeoutMin{150};
constexpr std::chrono::milliseconds kElectionTimeoutMax{300};

uint64_t Majority(uint64_t members) { return members / 2 + 1; }

std::chrono::milliseconds RandomElectionTimeout() {
  static thread_local std::mt19937 rng(std::random_device{}());
  std::uniform_int_distribution<int> dist(kElectionTimeoutMin.count(), kElectionTimeoutMax.count());
  return std::chrono::milliseconds(dist(rng));
}
}  // namespace

RaftState::RaftState(const NodeRuntimeConfig& config, ShardManager* shard_manager)
    : config_(config),
      shard_manager_(shard_manager),
      role_(Role::kFollower),
      current_term_(0),
      commit_index_(0),
      last_applied_(0),
      running_(false),
      votes_granted_(0) {
  for (const auto& peer : config_.peers) {
    auto channel = grpc::CreateChannel(peer.address, grpc::InsecureChannelCredentials());
    peer_stubs_.emplace(peer.node_id, distributed::RaftConsensus::NewStub(channel));
    next_index_[peer.node_id] = 1;
    match_index_[peer.node_id] = 0;
  }
  next_index_[config_.node_id] = 1;
  match_index_[config_.node_id] = 0;
}

RaftState::~RaftState() { Stop(); }

void RaftState::Start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    return;
  }
  {
    std::lock_guard lock(mutex_);
    ResetElectionTimerLocked();
  }
  election_thread_ = std::thread(&RaftState::ElectionLoop, this);
}

void RaftState::Stop() {
  bool expected = true;
  if (!running_.compare_exchange_strong(expected, false)) {
    return;
  }
  {
    std::lock_guard lock(mutex_);
    cv_.notify_all();
  }
  if (election_thread_.joinable()) {
    election_thread_.join();
  }
}

bool RaftState::IsLeader() const {
  std::lock_guard lock(mutex_);
  return role_ == Role::kLeader;
}

std::string RaftState::LeaderId() const {
  std::lock_guard lock(mutex_);
  return leader_id_;
}

uint64_t RaftState::CurrentTerm() const {
  std::lock_guard lock(mutex_);
  return current_term_;
}

uint64_t RaftState::ReplicateCommand(const distributed::CommandEnvelope& command) {
  std::string serialized;
  if (!command.SerializeToString(&serialized)) {
    throw std::runtime_error("Failed to serialize command envelope");
  }
  std::unique_lock lock(mutex_);
  if (role_ != Role::kLeader) {
    throw std::runtime_error("Cannot replicate command when not leader");
  }
  const uint64_t new_index = log_.LastIndex() + 1;
  log_.Append({LocalLogEntry{.term = current_term_, .index = new_index, .command = serialized}});
  match_index_[config_.node_id] = new_index;
  next_index_[config_.node_id] = new_index + 1;
  lock.unlock();

  BroadcastAppendEntries();
  ApplyCommittedEntries();
  return new_index;
}

distributed::RequestVoteResponse RaftState::HandleVoteRequest(const distributed::RequestVoteRequest& request) {
  std::lock_guard lock(mutex_);
  distributed::RequestVoteResponse response;
  response.set_term(current_term_);
  if (request.term() < current_term_) {
    response.set_vote_granted(false);
    return response;
  }
  if (request.term() > current_term_) {
    BecomeFollower(request.term(), "");
  }
  bool candidate_up_to_date = false;
  const uint64_t last_term = log_.LastTerm();
  const uint64_t last_index = log_.LastIndex();
  if (request.last_log_term() > last_term) {
    candidate_up_to_date = true;
  } else if (request.last_log_term() == last_term && request.last_log_index() >= last_index) {
    candidate_up_to_date = true;
  }

  if ((voted_for_.empty() || voted_for_ == request.candidate_id()) && candidate_up_to_date) {
    voted_for_ = request.candidate_id();
    response.set_vote_granted(true);
    ResetElectionTimerLocked();
  } else {
    response.set_vote_granted(false);
  }
  response.set_term(current_term_);
  return response;
}

distributed::AppendEntriesResponse RaftState::HandleAppendEntries(const distributed::AppendEntriesRequest& request) {
  std::lock_guard lock(mutex_);
  distributed::AppendEntriesResponse response;
  response.set_term(current_term_);
  if (request.term() < current_term_) {
    response.set_success(false);
    return response;
  }
  if (request.term() > current_term_ || role_ != Role::kFollower) {
    BecomeFollower(request.term(), request.leader_id());
  } else {
    leader_id_ = request.leader_id();
  }
  ResetElectionTimerLocked();

  if (request.prev_log_index() > 0) {
    const auto prev = log_.EntryAt(request.prev_log_index());
    if (!prev || prev->term != request.prev_log_term()) {
      response.set_success(false);
      response.set_match_index(log_.LastIndex());
      return response;
    }
  }

  if (request.prev_log_index() > 0) {
    log_.Truncate(request.prev_log_index() + 1);
  }

  std::vector<LocalLogEntry> new_entries;
  new_entries.reserve(request.entries_size());
  for (const auto& entry : request.entries()) {
    new_entries.push_back(LocalLogEntry{.term = entry.term(), .index = entry.index(), .command = entry.command()});
  }
  log_.Append(new_entries);

  if (request.leader_commit() > commit_index_) {
    const auto new_commit = std::min(request.leader_commit(), log_.LastIndex());
    log_.SetCommitIndex(new_commit);
    commit_index_ = new_commit;
  }
  response.set_success(true);
  response.set_match_index(log_.LastIndex());
  cv_.notify_all();
  return response;
}

distributed::HeartbeatResponse RaftState::HandleHeartbeat(const distributed::HeartbeatRequest& request) {
  std::lock_guard lock(mutex_);
  distributed::HeartbeatResponse response;
  response.set_term(current_term_);
  if (request.term() < current_term_) {
    response.set_ok(false);
    return response;
  }
  if (request.term() > current_term_ || role_ != Role::kFollower) {
    BecomeFollower(request.term(), request.leader_id());
  } else {
    leader_id_ = request.leader_id();
  }
  ResetElectionTimerLocked();
  response.set_ok(true);
  return response;
}

void RaftState::ElectionLoop() {
  std::unique_lock lock(mutex_);
  while (running_) {
    if (role_ == Role::kLeader) {
      if (cv_.wait_for(lock, kHeartbeatInterval, [this]() { return !running_ || role_ != Role::kLeader; })) {
        if (!running_) {
          break;
        }
        if (role_ != Role::kLeader) {
          continue;
        }
      }
      lock.unlock();
      BroadcastHeartbeats();
      ApplyCommittedEntries();
      lock.lock();
      continue;
    }

    if (cv_.wait_until(lock, election_deadline_, [this]() { return !running_ || role_ == Role::kLeader; })) {
      if (!running_) {
        break;
      }
      continue;
    }

    if (!running_) {
      break;
    }

    const auto now = std::chrono::steady_clock::now();
    if (now >= election_deadline_) {
      BecomeCandidate();
      const uint64_t term = current_term_;
      const uint64_t last_term = log_.LastTerm();
      const uint64_t last_index = log_.LastIndex();
      votes_granted_ = 1;  // vote for self
      auto peers = config_.peers;
      lock.unlock();

      for (const auto& peer : peers) {
        auto stub_it = peer_stubs_.find(peer.node_id);
        if (stub_it == peer_stubs_.end()) {
          continue;
        }
        distributed::RequestVoteRequest request;
        request.set_term(term);
        request.set_candidate_id(config_.node_id);
        request.set_last_log_index(last_index);
        request.set_last_log_term(last_term);
        grpc::ClientContext ctx;
        distributed::RequestVoteResponse response;
        const auto status = stub_it->second->RequestVote(&ctx, request, &response);
        lock.lock();
        if (!running_) {
          lock.unlock();
          return;
        }
        if (!status.ok()) {
          lock.unlock();
          continue;
        }
        if (response.term() > current_term_) {
          BecomeFollower(response.term(), "");
          lock.unlock();
          break;
        }
        if (role_ != Role::kCandidate || term != current_term_) {
          lock.unlock();
          continue;
        }
        if (response.vote_granted()) {
          ++votes_granted_;
          if (HasVoteQuorum(votes_granted_)) {
            BecomeLeader();
            lock.unlock();
            BroadcastHeartbeats();
            ApplyCommittedEntries();
            lock.lock();
            break;
          }
        }
        lock.unlock();
      }
      if (!lock.owns_lock()) {
        lock.lock();
      }
    }
  }
}

void RaftState::BecomeFollower(uint64_t term, std::string leader_id) {
  role_ = Role::kFollower;
  current_term_ = term;
  voted_for_.clear();
  leader_id_ = std::move(leader_id);
  votes_granted_ = 0;
  ResetElectionTimerLocked();
}

void RaftState::BecomeCandidate() {
  role_ = Role::kCandidate;
  ++current_term_;
  voted_for_ = config_.node_id;
  leader_id_.clear();
  votes_granted_ = 1;
  ResetElectionTimerLocked();
}

void RaftState::BecomeLeader() {
  role_ = Role::kLeader;
  leader_id_ = config_.node_id;
  for (const auto& peer : config_.peers) {
    next_index_[peer.node_id] = log_.LastIndex() + 1;
    match_index_[peer.node_id] = 0;
  }
  next_index_[config_.node_id] = log_.LastIndex() + 1;
  match_index_[config_.node_id] = log_.LastIndex();
}

void RaftState::ResetElectionTimerLocked() {
  election_deadline_ = std::chrono::steady_clock::now() + RandomElectionTimeout();
  cv_.notify_all();
}

void RaftState::BroadcastHeartbeats() { BroadcastAppendEntries(); }

void RaftState::BroadcastAppendEntries() {
  for (const auto& peer : config_.peers) {
    SendAppendEntriesToPeer(peer);
  }
}

void RaftState::SendAppendEntriesToPeer(const PeerInfo& peer) {
  auto stub_it = peer_stubs_.find(peer.node_id);
  if (stub_it == peer_stubs_.end()) {
    return;
  }

  while (running_) {
    distributed::AppendEntriesRequest request;
    uint64_t term;
    uint64_t next_index;
    std::vector<LocalLogEntry> entries;
    {
      std::lock_guard lock(mutex_);
      if (role_ != Role::kLeader) {
        return;
      }
      term = current_term_;
      next_index = next_index_[peer.node_id];
      request.set_term(term);
      request.set_leader_id(config_.node_id);
      if (next_index > 1) {
        const auto prev = log_.EntryAt(next_index - 1);
        if (prev) {
          request.set_prev_log_index(prev->index);
          request.set_prev_log_term(prev->term);
        }
      }
      entries = log_.EntriesFrom(next_index);
      for (const auto& entry : entries) {
        auto* proto = request.add_entries();
        proto->set_term(entry.term);
        proto->set_index(entry.index);
        proto->set_command(entry.command);
      }
      request.set_leader_commit(log_.CommitIndex());
    }

    grpc::ClientContext ctx;
    distributed::AppendEntriesResponse response;
    auto status = stub_it->second->AppendEntries(&ctx, request, &response);
    if (!status.ok()) {
      return;
    }

    std::lock_guard lock(mutex_);
    if (response.term() > current_term_) {
      BecomeFollower(response.term(), "");
      return;
    }
    if (role_ != Role::kLeader) {
      return;
    }
    if (response.success()) {
      const uint64_t match = response.match_index();
      match_index_[peer.node_id] = match;
      next_index_[peer.node_id] = match + 1;
      std::vector<uint64_t> match_indexes;
      match_indexes.reserve(match_index_.size());
      for (const auto& kv : match_index_) {
        match_indexes.push_back(kv.second);
      }
      std::sort(match_indexes.begin(), match_indexes.end());
      const uint64_t quorum_match = match_indexes[match_indexes.size() - Majority(match_indexes.size())];
      if (quorum_match > log_.CommitIndex()) {
        log_.SetCommitIndex(quorum_match);
        commit_index_ = quorum_match;
        cv_.notify_all();
      }
      return;
    }

    if (next_index_[peer.node_id] > 1) {
      --next_index_[peer.node_id];
    } else {
      return;
    }
  }
}

void RaftState::ApplyCommittedEntries() {
  while (true) {
    uint64_t to_apply = 0;
    {
      std::lock_guard lock(mutex_);
      const uint64_t commit_index = log_.CommitIndex();
      if (commit_index <= last_applied_) {
        break;
      }
      to_apply = last_applied_ + 1;
      last_applied_ = commit_index;
    }

    for (uint64_t index = to_apply; index <= last_applied_; ++index) {
      const auto entry = log_.EntryAt(index);
      if (!entry.has_value()) {
        continue;
      }
      distributed::CommandEnvelope envelope;
      if (!envelope.ParseFromString(entry->command)) {
        continue;
      }
      switch (envelope.type()) {
        case distributed::CommandEnvelope::UPSERT: {
          distributed::UpsertRequest upsert;
          if (upsert.ParseFromString(envelope.payload())) {
            shard_manager_->Upsert(upsert);
          }
          break;
        }
        case distributed::CommandEnvelope::DELETE: {
          distributed::DeleteRequest del;
          if (del.ParseFromString(envelope.payload())) {
            shard_manager_->Remove(del);
          }
          break;
        }
        default:
          break;
      }
      log_.SetLastApplied(index);
    }
  }
}

bool RaftState::HasVoteQuorum(uint32_t votes) const {
  const uint32_t total = static_cast<uint32_t>(config_.peers.size() + 1);
  return votes >= Majority(total);
}

}  // namespace dvs
