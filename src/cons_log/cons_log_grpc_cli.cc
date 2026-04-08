#include "cons_log_grpc_cli.h"
#include "../rpc/rpc_factory.h"

namespace lazylog {

ConsensusLogGrpcCli::ConsensusLogGrpcCli() {}

ConsensusLogGrpcCli::~ConsensusLogGrpcCli() {}

void ConsensusLogGrpcCli::InitializeConn(const Properties &p, const std::string &svr_uri, void *param) {
    stub_ = proto::ConsLogService::NewStub(grpc::CreateChannel(svr_uri, grpc::InsecureChannelCredentials()));
}

void ConsensusLogGrpcCli::Finalize() {
    stub_.reset();
}

void ConsensusLogGrpcCli::ReadEntry(const uint64_t l) {
    proto::ReadEntryRequest req;
    req.set_idx(l);
    proto::ReadEntryResponse resp;
    grpc::ClientContext context;

    grpc::Status status = stub_->ReadEntry(&context, req, &resp);
    if (!status.ok()) {
        LOG(FATAL) << "ReadEntry gRPC failed: " << status.error_message();
    }
    // TODO: callback or signal completion (the old eRPC path did this via pollForRpcComplete)
}

void ConsensusLogGrpcCli::ReadEntries(const uint64_t from, const uint64_t to) {
    proto::ReadEntriesRequest req;
    req.set_from_idx(from);
    req.set_to_idx(to);
    proto::ReadEntriesResponse resp;
    grpc::ClientContext context;

    grpc::Status status = stub_->ReadEntries(&context, req, &resp);
    if (!status.ok()) {
        LOG(FATAL) << "ReadEntries gRPC failed: " << status.error_message();
    }
}

uint64_t ConsensusLogGrpcCli::GetNumOrderedEntries() {
    proto::Empty req;
    proto::GetNumOrderedEntriesResponse resp;
    grpc::ClientContext context;

    grpc::Status status = stub_->GetNumOrderedEntries(&context, req, &resp);
    if (status.ok()) {
        return resp.num();
    }
    LOG(FATAL) << "GetNumOrderedEntries gRPC failed: " << status.error_message();
    return 0;
}

namespace {
bool register_grpc_cl_cli() {
    return RPCFactory::RegisterRPC("grpc_cl_cli", []() -> std::shared_ptr<RPCTransport> {
        return std::make_shared<ConsensusLogGrpcCli>();
    });
}
bool dummy = register_grpc_cl_cli();
}  // namespace

}  // namespace lazylog
