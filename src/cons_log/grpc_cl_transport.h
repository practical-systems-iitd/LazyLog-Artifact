#pragma once

#include "../rpc/transport.h"
#include "cons_log.h"
#include <grpcpp/grpcpp.h>
#include "lazylog.pb.h"
#include "lazylog.grpc.pb.h"
#include <thread>
#include <vector>

namespace lazylog {

class GrpcConsLogTransport : public RPCTransport {
   public:
    GrpcConsLogTransport();
    ~GrpcConsLogTransport();

    void Initialize(const Properties &p) override;
    void InitializeConn(const Properties &p, const std::string &svr, void *param) override {
        throw Exception("Wrong rpc transport type");
    }
    void Finalize() override;

   protected:
    std::unique_ptr<grpc::Server> server_;
    std::thread server_thread_;

    bool pipelined_;
    std::thread fetch_th_;

    static ConsensusLog *cons_log_;
    friend class ConsLogServiceImpl;
};

class ConsLogServiceImpl final : public proto::ConsLogService::Service {
    grpc::Status ReadEntry(grpc::ServerContext* context, const proto::ReadEntryRequest* request,
                           proto::ReadEntryResponse* response) override;
    grpc::Status ReadEntries(grpc::ServerContext* context, const proto::ReadEntriesRequest* request,
                             proto::ReadEntriesResponse* response) override;
    grpc::Status GetNumOrderedEntries(grpc::ServerContext* context, const proto::Empty* request,
                                      proto::GetNumOrderedEntriesResponse* response) override;
};

}  // namespace lazylog
