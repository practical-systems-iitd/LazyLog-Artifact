#include "dur_log_grpc_cli.h"
#include "../rpc/rpc_factory.h"
#include "../utils/utils.h"

namespace lazylog {

DurabilityLogGrpcCli::DurabilityLogGrpcCli() : is_primary_(false) {}

DurabilityLogGrpcCli::~DurabilityLogGrpcCli() {}

void DurabilityLogGrpcCli::InitializeConn(const Properties &p, const std::string &svr, void *param) {
    stub_ = proto::DurLogService::NewStub(grpc::CreateChannel(svr, grpc::InsecureChannelCredentials()));
    if (param != nullptr) {
        is_primary_ = *reinterpret_cast<bool *>(param);
    }
}

void DurabilityLogGrpcCli::Finalize() {
    stub_.reset();
}

uint64_t DurabilityLogGrpcCli::AppendEntry(const LogEntry &e) {
    proto::AppendEntryRequest req;
    char buf[4096];
    uint32_t len = Serializer(e, reinterpret_cast<uint8_t*>(buf));
    req.mutable_entry()->set_data(buf, len);

    proto::AppendEntryResponse resp;
    grpc::ClientContext context;

    grpc::Status status = stub_->AppendEntry(&context, req, &resp);
    if (!status.ok()) {
        LOG(FATAL) << "AppendEntry gRPC failed: " << status.error_message();
    }
    return resp.seq();
}

bool DurabilityLogGrpcCli::AppendEntryAsync(const LogEntry &e, std::shared_ptr<RPCToken> &token) {
    if (!token) {
        token = std::make_shared<RPCToken>();
    }
    token->Reset();
    
    // We simulate async by spawning a detached thread since gRPC async requires polling CQ
    // which significantly changes the codebase structure.
    LogEntry e_copy = e;
    std::thread([this, e_copy, token]() {
        uint64_t seq = AppendEntry(e_copy);
        // The token needs to know it completed
        token->SetComplete();
    }).detach();

    return true;
}

std::tuple<uint64_t, uint64_t, uint16_t> DurabilityLogGrpcCli::GetNumDurEntry() {
    proto::Empty req;
    proto::GetNumDurEntryResponse resp;
    grpc::ClientContext context;
    grpc::Status status = stub_->GetNumDurEntry(&context, req, &resp);
    if (status.ok()) {
        return {resp.seq1(), resp.seq2(), resp.num()};
    }
    return {0, 0, 0};
}

uint32_t DurabilityLogGrpcCli::FetchUnorderedEntries(std::vector<LogEntry> &es, uint32_t max_entries_num) {
    return FetchUnorderedEntries(es, 0, max_entries_num);
}

uint32_t DurabilityLogGrpcCli::FetchUnorderedEntries(std::vector<LogEntry> &es, uint64_t from, uint32_t max_entries_num) {
    proto::FetchUnorderedRequest req;
    req.set_from_idx(from);
    req.set_max_fetch_size(max_entries_num);

    proto::FetchUnorderedResponse resp;
    grpc::ClientContext context;

    grpc::Status status = stub_->FetchUnorderedEntries(&context, req, &resp);
    if (!status.ok()) {
        return 0;
    }

    uint32_t fetched = 0;
    for (const auto& raw : resp.entries()) {
        LogEntry e;
        Deserializer(e, reinterpret_cast<const uint8_t*>(raw.data().data()));
        es.push_back(e);
        fetched += raw.data().size();
    }

    if (fetched > 0) {
        es.emplace_back();
        es.back().client_seq = resp.last_client_seq();
    }

    return fetched;
}

uint64_t DurabilityLogGrpcCli::DeleteOrderedEntries(std::vector<LogEntry::ReqID> &req_ids) {
    proto::DeleteOrderedRequest req;
    for (auto& r : req_ids) {
        auto* rid = req.add_seqs();
        rid->set_client_id(r.first);
        rid->set_client_seq(r.second);
    }
    proto::DeleteOrderedResponse resp;
    grpc::ClientContext context;
    grpc::Status status = stub_->DeleteOrderedEntries(&context, req, &resp);
    if (status.ok()) {
        return resp.max_seq();
    }
    return 0;
}

void DurabilityLogGrpcCli::DeleteOrderedEntriesAsync(std::vector<LogEntry::ReqID> &req_ids) {
    std::thread([this, req_ids]() {
        auto copies = req_ids;
        DeleteOrderedEntries(copies);
    }).detach();
}

int DurabilityLogGrpcCli::SpecRead(const uint64_t idx, LogEntry &e) {
    proto::SpecReadRequest req;
    req.set_idx(idx);
    proto::SpecReadResponse resp;
    grpc::ClientContext context;
    grpc::Status status = stub_->SpecRead(&context, req, &resp);
    if (status.ok()) {
        if (resp.status() > 0 && resp.entry().data().size() > 0) {
            Deserializer(e, reinterpret_cast<const uint8_t*>(resp.entry().data().data()));
        }
        return resp.status();
    }
    return -1;
}

uint64_t DurabilityLogGrpcCli::ProcessFetchedEntries(const std::vector<LogEntry> &es, std::vector<LogEntry::ReqID> &req_ids) {
    if (IsPrimary()) {
        uint64_t end_sequence = es.back().client_seq;
        req_ids.emplace_back(end_sequence, 0);
        uint64_t end_idx = (es.end() - 2)->log_idx + 1;
        req_ids.emplace_back(end_idx, 0);
    } else {
        std::map<uint64_t, uint64_t> maxSeqForClient;
        for (const auto& entry : es) {
            if (maxSeqForClient.find(entry.client_id) == maxSeqForClient.end() ||
                entry.client_seq > maxSeqForClient[entry.client_id]) {
                maxSeqForClient[entry.client_id] = entry.client_seq;
            }
        }
        maxSeqForClient.erase(0);
        for (const auto& [client_id, cli_seq] : maxSeqForClient) {
            req_ids.emplace_back(client_id, cli_seq);
        }
    }
    return (es.end() - 2)->log_idx;
}

bool DurabilityLogGrpcCli::IsPrimary() {
    return is_primary_;
}

bool DurabilityLogGrpcCli::CheckAndRunOnce() {
    return true; // No event loop needed for synchronous gRPC or detached asyncs
}

void DurabilityLogGrpcCli::AddPendingReq(std::shared_ptr<RPCToken> &token) {
    pending_reqs_.push(token);
}

void DurabilityLogGrpcCli::CheckPendingReq() {
    while (!pending_reqs_.empty()) {
        if (pending_reqs_.front()->Complete()) {
            pending_reqs_.pop();
        } else {
            break;
        }
    }
}

namespace {
bool register_dur_cli() {
    return RPCFactory::RegisterRPC("grpc_dl_cli", []() -> std::shared_ptr<RPCTransport> {
        return std::make_shared<DurabilityLogGrpcCli>();
    });
}
bool dummy = register_dur_cli();
}

}  // namespace lazylog
