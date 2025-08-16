#include "rpc/raft_service_impl.h"

namespace dvs {

RaftServiceImpl::RaftServiceImpl(RaftState* raft_state) : raft_state_(raft_state) {}

grpc::Status RaftServiceImpl::RequestVote(grpc::ServerContext* /*context*/, const distributed::RequestVoteRequest* request,
                                          distributed::RequestVoteResponse* response) {
  *response = raft_state_->HandleVoteRequest(*request);
  return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::AppendEntries(grpc::ServerContext* /*context*/, const distributed::AppendEntriesRequest* request,
                                            distributed::AppendEntriesResponse* response) {
  *response = raft_state_->HandleAppendEntries(*request);
  return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::Heartbeat(grpc::ServerContext* /*context*/, const distributed::HeartbeatRequest* request,
                                        distributed::HeartbeatResponse* response) {
  *response = raft_state_->HandleHeartbeat(*request);
  return grpc::Status::OK;
}

}  // namespace dvs
