#pragma once

#include <memory>

#include <grpcpp/grpcpp.h>

#include "index/shard_manager.h"
#include "raft/raft_state.h"

#include "search.grpc.pb.h"

namespace dvs {

class SearchServiceImpl final : public distributed::VectorSearch::Service {
 public:
  SearchServiceImpl(ShardManager* shard_manager, RaftState* raft_state);

  grpc::Status Search(grpc::ServerContext* context, const distributed::SearchRequest* request,
                      distributed::SearchResponse* response) override;

  grpc::Status BatchSearch(grpc::ServerContext* context, const distributed::BatchSearchRequest* request,
                           distributed::BatchSearchResponse* response) override;

  grpc::Status Upsert(grpc::ServerContext* context, const distributed::UpsertRequest* request,
                      distributed::UpsertResponse* response) override;

  grpc::Status Delete(grpc::ServerContext* context, const distributed::DeleteRequest* request,
                      distributed::DeleteResponse* response) override;

  grpc::Status WarmCache(grpc::ServerContext* context, const distributed::WarmRequest* request,
                         distributed::WarmResponse* response) override;

 private:
  bool ShouldProxyToLeader(distributed::ConsistencyLevel consistency) const;
  void PopulateLeaderHint(distributed::SearchResponse* response) const;
  void PopulateLeaderHint(distributed::UpsertResponse* response) const;
  void PopulateLeaderHint(distributed::DeleteResponse* response) const;

  ShardManager* shard_manager_;
  RaftState* raft_state_;
};

}  // namespace dvs
