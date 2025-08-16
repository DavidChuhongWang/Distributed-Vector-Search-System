#include "rpc/search_service_impl.h"

#include <stdexcept>

namespace dvs {

SearchServiceImpl::SearchServiceImpl(ShardManager* shard_manager, RaftState* raft_state)
    : shard_manager_(shard_manager), raft_state_(raft_state) {}

bool SearchServiceImpl::ShouldProxyToLeader(distributed::ConsistencyLevel consistency) const {
  return consistency == distributed::STRONG;
}

void SearchServiceImpl::PopulateLeaderHint(distributed::SearchResponse* response) const {
  response->set_leader_hint(raft_state_->LeaderId());
}

void SearchServiceImpl::PopulateLeaderHint(distributed::UpsertResponse* response) const {
  response->set_leader_hint(raft_state_->LeaderId());
}

void SearchServiceImpl::PopulateLeaderHint(distributed::DeleteResponse* response) const {
  response->set_leader_hint(raft_state_->LeaderId());
}

grpc::Status SearchServiceImpl::Search(grpc::ServerContext* /*context*/, const distributed::SearchRequest* request,
                                        distributed::SearchResponse* response) {
  if (ShouldProxyToLeader(request->consistency()) && !raft_state_->IsLeader()) {
    PopulateLeaderHint(response);
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "Node is not the Raft leader");
  }

  try {
    bool served_from_cache = false;
    auto hits = shard_manager_->Search(*request, &served_from_cache);
    for (const auto& hit : hits) {
      auto* scored = response->add_hits();
      scored->set_id(hit.id);
      scored->set_distance(hit.distance);
      scored->set_shard_id(hit.shard_id);
    }
    response->set_served_from_cache(served_from_cache);
    if (!raft_state_->IsLeader()) {
      PopulateLeaderHint(response);
    }
    return grpc::Status::OK;
  } catch (const std::exception& ex) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, ex.what());
  }
}

grpc::Status SearchServiceImpl::BatchSearch(grpc::ServerContext* /*context*/, const distributed::BatchSearchRequest* request,
                                             distributed::BatchSearchResponse* response) {
  if (!raft_state_->IsLeader()) {
    // For batch search we allow followers to serve if requests tolerate staleness.
    for (const auto& single : request->requests()) {
      if (ShouldProxyToLeader(single.consistency())) {
        response->clear_responses();
        return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "Leader routing required for strong consistency");
      }
    }
  }

  try {
    std::vector<bool> cache_hits;
    auto batch_results = shard_manager_->BatchSearch(*request, &cache_hits);
    for (size_t i = 0; i < batch_results.size(); ++i) {
      auto* single_response = response->add_responses();
      for (const auto& hit : batch_results[i]) {
        auto* scored = single_response->add_hits();
        scored->set_id(hit.id);
        scored->set_distance(hit.distance);
        scored->set_shard_id(hit.shard_id);
      }
      single_response->set_served_from_cache(cache_hits[i]);
      if (!raft_state_->IsLeader()) {
        PopulateLeaderHint(single_response);
      }
    }
    return grpc::Status::OK;
  } catch (const std::exception& ex) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, ex.what());
  }
}

grpc::Status SearchServiceImpl::Upsert(grpc::ServerContext* /*context*/, const distributed::UpsertRequest* request,
                                       distributed::UpsertResponse* response) {
  if (!raft_state_->IsLeader()) {
    response->set_success(false);
    PopulateLeaderHint(response);
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "Upsert requires leader");
  }

  distributed::CommandEnvelope envelope;
  envelope.set_type(distributed::CommandEnvelope::UPSERT);
  std::string payload;
  if (!request->SerializeToString(&payload)) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Failed to serialize upsert request");
  }
  envelope.set_payload(std::move(payload));

  try {
    raft_state_->ReplicateCommand(envelope);
    response->set_success(true);
    return grpc::Status::OK;
  } catch (const std::exception& ex) {
    response->set_success(false);
    return grpc::Status(grpc::StatusCode::ABORTED, ex.what());
  }
}

grpc::Status SearchServiceImpl::Delete(grpc::ServerContext* /*context*/, const distributed::DeleteRequest* request,
                                       distributed::DeleteResponse* response) {
  if (!raft_state_->IsLeader()) {
    response->set_success(false);
    PopulateLeaderHint(response);
    return grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "Delete requires leader");
  }

  distributed::CommandEnvelope envelope;
  envelope.set_type(distributed::CommandEnvelope::DELETE);
  std::string payload;
  if (!request->SerializeToString(&payload)) {
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Failed to serialize delete request");
  }
  envelope.set_payload(std::move(payload));

  try {
    raft_state_->ReplicateCommand(envelope);
    response->set_success(true);
    return grpc::Status::OK;
  } catch (const std::exception& ex) {
    response->set_success(false);
    return grpc::Status(grpc::StatusCode::ABORTED, ex.what());
  }
}

grpc::Status SearchServiceImpl::WarmCache(grpc::ServerContext* /*context*/, const distributed::WarmRequest* request,
                                          distributed::WarmResponse* response) {
  try {
    shard_manager_->WarmCache(*request);
    response->set_success(true);
    return grpc::Status::OK;
  } catch (const std::exception& ex) {
    response->set_success(false);
    return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, ex.what());
  }
}

}  // namespace dvs
