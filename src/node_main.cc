#include <atomic>
#include <csignal>
#include <iostream>
#include <memory>
#include <chrono>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "rpc/raft_service_impl.h"
#include "rpc/search_service_impl.h"
#include "util/config.h"

namespace {
std::atomic<bool> g_shutdown_requested{false};

std::string ParseConfigPath(int argc, char** argv) {
  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);
    const std::string prefix = "--config=";
    if (arg.rfind(prefix, 0) == 0) {
      return arg.substr(prefix.size());
    }
  }
  return {};
}

void PrintUsage() {
  std::cerr << "Usage: vector_search_node --config=/path/to/node.textproto" << std::endl;
}

void HandleSignal(int /*signal*/) {
  g_shutdown_requested.store(true);
}
}  // namespace

int main(int argc, char** argv) {
  std::string config_path = ParseConfigPath(argc, argv);
  if (config_path.empty()) {
    PrintUsage();
    return 1;
  }

  try {
    const auto config = dvs::ConfigLoader::FromFile(config_path);
    dvs::ShardManager shard_manager(config);
    dvs::RaftState raft_state(config, &shard_manager);
    dvs::SearchServiceImpl search_service(&shard_manager, &raft_state);
    dvs::RaftServiceImpl raft_service(&raft_state);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(config.bind_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&search_service);
    builder.RegisterService(&raft_service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
      std::cerr << "Failed to start gRPC server" << std::endl;
      return 1;
    }
    std::cout << "Node listening on " << config.bind_address << std::endl;

    raft_state.Start();

    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    std::thread shutdown_thread([&]() {
      while (!g_shutdown_requested.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      server->Shutdown();
    });

    server->Wait();
    std::cout << "Shutting down node" << std::endl;
    raft_state.Stop();
    g_shutdown_requested.store(true);
    shutdown_thread.join();
  } catch (const std::exception& ex) {
    std::cerr << "Fatal error: " << ex.what() << std::endl;
    return 1;
  }

  return 0;
}
