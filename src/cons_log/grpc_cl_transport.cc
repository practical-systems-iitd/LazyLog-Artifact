#include "grpc_cl_transport.h"
#include "../rpc/rpc_factory.h"
#include "glog/logging.h"

namespace lazylog {

ConsensusLog *GrpcConsLogTransport::cons_log_ = nullptr;

GrpcConsLogTransport::GrpcConsLogTransport() : pipelined_(false) {
    if (!cons_log_) {
        cons_log_ = new ConsensusLog();
    }
}

GrpcConsLogTransport::~GrpcConsLogTransport() {
    if (cons_log_) delete cons_log_;
    cons_log_ = nullptr;
}

void GrpcConsLogTransport::Initialize(const Properties &p) {
    const std::string server_uri = p.GetProperty(PROP_CL_SVR_URI, PROP_CL_SVR_URI_DEFAULT);

    pipelined_ = (p.GetProperty("cons_log.pipeline", "false") == "true");

    cons_log_->Initialize(p, nullptr);

    if (pipelined_) {
        fetch_th_ = std::thread([this]() {
            LOG(INFO) << "Fetch thread started";
            while (run_) {
                cons_log_->fetch();
            }
            LOG(INFO) << "Fetch thread stopped";
        });
        LOG(INFO) << "Store thread started";
        // To be safe we should run store on another thread so we don't block Grpc setup
        std::thread store_th_([this]() {
            while (run_) {
               cons_log_->store(run_);
            }
        });
        store_th_.detach();
    } else {
        std::thread fetch_store_th_([this]() {
            while (run_) {
                cons_log_->fetchAndStore();
            }
        });
        fetch_store_th_.detach();
    }

    server_thread_ = std::thread([this, server_uri]() {
        ConsLogServiceImpl service;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(server_uri, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        server_ = builder.BuildAndStart();
        LOG(INFO) << "gRPC ConsensusLog server listening on " << server_uri;
        if (server_) {
            server_->Wait();
        }
    });
}

void GrpcConsLogTransport::Finalize() {
    if (server_) {
        server_->Shutdown();
    }
    if (server_thread_.joinable()) server_thread_.join();
    if (pipelined_ && fetch_th_.joinable()) {
        fetch_th_.join();
    }

    cons_log_->Finalize();
}

grpc::Status ConsLogServiceImpl::ReadEntry(grpc::ServerContext* context, const proto::ReadEntryRequest* request,
                                           proto::ReadEntryResponse* response) {
    return grpc::Status::OK;
}

grpc::Status ConsLogServiceImpl::ReadEntries(grpc::ServerContext* context, const proto::ReadEntriesRequest* request,
                                             proto::ReadEntriesResponse* response) {
    return grpc::Status::OK;
}

grpc::Status ConsLogServiceImpl::GetNumOrderedEntries(grpc::ServerContext* context, const proto::Empty* request,
                                                      proto::GetNumOrderedEntriesResponse* response) {
    response->set_num(GrpcConsLogTransport::cons_log_->GetNumOrderedEntries());
    return grpc::Status::OK;
}

namespace {
bool register_grpc_cl_svr() {
    return RPCFactory::RegisterRPC("grpc_cl_svr", []() -> std::shared_ptr<RPCTransport> {
        return std::make_shared<GrpcConsLogTransport>();
    });
}
bool dummy2 = register_grpc_cl_svr();
}

}  // namespace lazylog
