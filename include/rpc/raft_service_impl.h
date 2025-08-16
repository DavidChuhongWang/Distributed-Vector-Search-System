#pragma once

#include <grpcpp/grpcpp.h>

#include "raft/raft_state.h"

#include "raft.grpc.pb.h"

namespace dvs {

class RaftServiceImpl final : public distributed::RaftConsensus::Service {
 public:
  explicit RaftServiceImpl(RaftState* raft_state);

  grpc::Status RequestVote(grpc::ServerContext* context, const distributed::RequestVoteRequest* request,
                           distributed::RequestVoteResponse* response) override;

  grpc::Status AppendEntries(grpc::ServerContext* context, const distributed::AppendEntriesRequest* request,
                             distributed::AppendEntriesResponse* response) override;

  grpc::Status Heartbeat(grpc::ServerContext* context, const distributed::HeartbeatRequest* request,
                         distributed::HeartbeatResponse* response) override;

 private:
  RaftState* raft_state_;
};

}  // namespace dvs
