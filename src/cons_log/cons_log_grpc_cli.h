#pragma once

#include "cons_log_cli.h"
#include <grpcpp/grpcpp.h>
#include "lazylog.pb.h"
#include "lazylog.grpc.pb.h"
#include "glog/logging.h"

namespace lazylog {

class ConsensusLogGrpcCli : public ConsensusLogCli {
   public:
    ConsensusLogGrpcCli();
    virtual ~ConsensusLogGrpcCli();

    void Initialize(const Properties &p) override {
        LOG(ERROR) << "This is a client RPC transport";
        throw Exception("Wrong rpc transport type");
    }
    void InitializeConn(const Properties &p, const std::string &svr_uri, void *param = nullptr) override;
    void Finalize() override;

    void ReadEntry(const uint64_t l) override;
    void ReadEntries(const uint64_t from, const uint64_t to) override;
    uint64_t GetNumOrderedEntries() override;

   protected:
    std::unique_ptr<proto::ConsLogService::Stub> stub_;
};

}  // namespace lazylog
