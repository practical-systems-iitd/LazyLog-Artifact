#include "grpc_dl_transport.h"
#include "../rpc/rpc_factory.h"
#include "../utils/utils.h"
#include "glog/logging.h"

namespace lazylog {

DurabilityLog *GrpcDurLogTransport::dur_log_ = nullptr;

GrpcDurLogTransport::GrpcDurLogTransport() {
    if (!dur_log_) {
        dur_log_ = new DurabilityLog();
    }
}

GrpcDurLogTransport::~GrpcDurLogTransport() {
    if (dur_log_) delete dur_log_;
    dur_log_ = nullptr;
}

void GrpcDurLogTransport::Initialize(const Properties &p) {
    bool is_primary_dl = p.GetProperty("dur_log.primary", "true") == "true";
    dur_log_->Initialize(p, &is_primary_dl);

    std::string server_uri = p.GetProperty(PROP_DL_SVR_URI, PROP_DL_SVR_URI_DEFAULT);

    server_thread_ = std::thread([this, server_uri]() {
        DurLogServiceImpl service;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(server_uri, grpc::InsecureServerCredentials());
        builder.RegisterService(&service);
        server_ = builder.BuildAndStart();
        LOG(INFO) << "gRPC DurabilityLog server listening on " << server_uri;
        if (server_) {
            server_->Wait();
        }
    });

    // Durability Log initialization wait mechanism from erpc
    bool is_client_also = p.GetProperty("dur_log.client", "false") == "true";
    if (is_client_also) {
        LOG(INFO) << "This process will be also a DL client";
    }
}

void GrpcDurLogTransport::Finalize() {
    if (server_) {
        server_->Shutdown();
    }
    if (server_thread_.joinable()) server_thread_.join();

    dur_log_->Finalize();
}

grpc::Status DurLogServiceImpl::AppendEntry(grpc::ServerContext* context, const proto::AppendEntryRequest* request,
                                            proto::AppendEntryResponse* response) {
    LogEntry e;
    Deserializer(e, reinterpret_cast<const uint8_t*>(request->entry().data().data()));
    uint64_t seq = GrpcDurLogTransport::dur_log_->AppendEntry(e);
    response->set_seq(seq);
    return grpc::Status::OK;
}

grpc::Status DurLogServiceImpl::GetNumDurEntry(grpc::ServerContext* context, const proto::Empty* request,
                                               proto::GetNumDurEntryResponse* response) {
    uint64_t seq = GrpcDurLogTransport::dur_log_->GetNumDurEntry();
    response->set_seq1(seq);
    response->set_seq2(0);
    response->set_num(GrpcDurLogTransport::dur_log_->GetView());
    return grpc::Status::OK;
}

grpc::Status DurLogServiceImpl::FetchUnorderedEntries(grpc::ServerContext* context, const proto::FetchUnorderedRequest* request,
                                                      proto::FetchUnorderedResponse* response) {
    std::vector<LogEntry> entries;
    GrpcDurLogTransport::dur_log_->FetchUnorderedEntries(entries, request->max_fetch_size());
    // In old eRPC code, it fetches from unordered_entries. If from>0, we just do max_entries_num
    // We didn't fully use `from` in the DurabilityLog class, it tracks state internally anyway (e.g. ordered_watermk_)

    for (const auto& entry : entries) {
        char buf[4096];
        uint32_t len = Serializer(entry, reinterpret_cast<uint8_t*>(buf));
        response->add_entries()->set_data(buf, len);
    }
    
    // In the actual system, the last entry size / last sequence may be passed some clever way. Let's just return.
    if (!entries.empty()) {
        response->set_last_client_seq(entries.back().client_seq);
    }

    return grpc::Status::OK;
}

grpc::Status DurLogServiceImpl::SpecRead(grpc::ServerContext* context, const proto::SpecReadRequest* request,
                                         proto::SpecReadResponse* response) {
    LogEntry e;
    int status = GrpcDurLogTransport::dur_log_->SpecReadEntry(request->idx(), e);
    response->set_status(status);
    if (status > 0) {
        char buf[4096];
        uint32_t len = Serializer(e, reinterpret_cast<uint8_t*>(buf));
        response->mutable_entry()->set_data(buf, len);
    }
    return grpc::Status::OK;
}

grpc::Status DurLogServiceImpl::DeleteOrderedEntries(grpc::ServerContext* context, const proto::DeleteOrderedRequest* request,
                                                     proto::DeleteOrderedResponse* response) {
    std::vector<LogEntry::ReqID> req_ids;
    for (const auto& seq : request->seqs()) {
        req_ids.push_back({seq.client_id(), seq.client_seq()});
    }
    uint32_t num = GrpcDurLogTransport::dur_log_->DelOrderedEntries(req_ids);
    response->set_max_seq(num);
    return grpc::Status::OK;
}

namespace {
bool register_dur_svr() {
    return RPCFactory::RegisterRPC("grpc_dl_svr", []() -> std::shared_ptr<RPCTransport> {
        return std::make_shared<GrpcDurLogTransport>();
    });
}
bool dummy2 = register_dur_svr();
}

}  // namespace lazylog
