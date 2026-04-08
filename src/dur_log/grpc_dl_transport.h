#pragma once

#include "../rpc/transport.h"
#include "dur_log.h"
#include <grpcpp/grpcpp.h>
#include <glog/logging.h>
#include "lazylog.pb.h"
#include "lazylog.grpc.pb.h"
#include <thread>
#include <vector>

namespace lazylog {

class GrpcDurLogTransport : public RPCTransport {
   public:
    GrpcDurLogTransport();
    ~GrpcDurLogTransport();

    void Initialize(const Properties &p) override;
    void InitializeConn(const Properties &p, const std::string &svr, void *param) override {
        LOG(ERROR) << "This is a server RPC transport";
        throw Exception("Wrong rpc transport type");
    }
    void Finalize() override;

   protected:
    std::unique_ptr<grpc::Server> server_;
    std::thread server_thread_;

    static DurabilityLog *dur_log_;
    
    friend class DurLogServiceImpl;
};

class DurLogServiceImpl final : public proto::DurLogService::Service {
    grpc::Status AppendEntry(grpc::ServerContext* context, const proto::AppendEntryRequest* request,
                             proto::AppendEntryResponse* response) override;
    grpc::Status GetNumDurEntry(grpc::ServerContext* context, const proto::Empty* request,
                                proto::GetNumDurEntryResponse* response) override;
    grpc::Status FetchUnorderedEntries(grpc::ServerContext* context, const proto::FetchUnorderedRequest* request,
                                       proto::FetchUnorderedResponse* response) override;
    grpc::Status SpecRead(grpc::ServerContext* context, const proto::SpecReadRequest* request,
                          proto::SpecReadResponse* response) override;
    grpc::Status DeleteOrderedEntries(grpc::ServerContext* context, const proto::DeleteOrderedRequest* request,
                                      proto::DeleteOrderedResponse* response) override;
};

}  // namespace lazylog
