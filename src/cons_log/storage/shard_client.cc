#include "shard_client.h"

#include "../../rpc/common.h"
#include <thread>

namespace lazylog {

ShardClient::ShardClient() : del_nexus_on_finalize_(true) {}

ShardClient::~ShardClient() {}

void ShardClient::InitializeConn(const Properties& p, const std::string& svr_uri, void* param) {
    channel_ = grpc::CreateChannel(svr_uri, grpc::InsecureChannelCredentials());
    stub_ = lazylog::proto::ShardService::NewStub(channel_);
    LOG(INFO) << "Connected to shard server at " << svr_uri;
}

void ShardClient::Finalize() {
    // Channel handles its own cleanup
}

void ShardClient::AppendBatchAsync(const std::vector<LogEntry>& es, uint64_t from, uint32_t num, RPCToken& token) {
    std::thread([this, es, from, num, &token]() {
        grpc::ClientContext context;
        lazylog::proto::AppendBatchRequest request;
        lazylog::proto::ShardResponse response;

        std::string buf;
        buf.resize(2 * 1024 * 1024); // 2MB buffer for serialization
        for (uint32_t i = 0; i < num; ++i) {
            auto* entry = request.add_entries();
            size_t len = Serializer(es[from + i], (uint8_t*)buf.data());
            entry->set_data(buf.data(), len);
        }

        grpc::Status status = stub_->AppendBatch(&context, request, &response);
        token.SetComplete();
    }).detach();
}

uint64_t ShardClient::AppendBatchAsync(const std::vector<LogEntry>& es, uint64_t from, uint64_t to, uint32_t itrv, RPCToken& token) {
    uint64_t actual_end = to;
    std::thread([this, es, from, actual_end, itrv, &token]() {
        grpc::ClientContext context;
        lazylog::proto::AppendBatchRequest request;
        lazylog::proto::ShardResponse response;
        
        std::string buf;
        buf.resize(2 * 1024 * 1024); 
        for (uint64_t i = from; i <= actual_end; i += itrv) {
            auto* entry = request.add_entries();
            size_t len = Serializer(es[i], (uint8_t*)buf.data());
            entry->set_data(buf.data(), len);
        }

        grpc::Status status = stub_->AppendBatch(&context, request, &response);
        token.SetComplete();
    }).detach();
    return actual_end;
}

void ShardClient::UpdateGlobalIdxAsync(const uint64_t idx, RPCToken& tkn) {
    std::thread([this, idx, &tkn]() {
        grpc::ClientContext context;
        lazylog::proto::UpdateGlobalIdxRequest request;
        lazylog::proto::ShardResponse response;
        
        request.set_idx(idx);
        stub_->UpdateGlobalIdx(&context, request, &response);
        tkn.SetComplete();
    }).detach();
}

bool ShardClient::ReadEntry(const uint64_t idx, LogEntry& e) {
    grpc::ClientContext context;
    lazylog::proto::ReadEntryRequest request;
    lazylog::proto::ReadEntryResponse response;
    
    request.set_idx(idx);
    
    grpc::Status status = stub_->ReadEntry(&context, request, &response);
    
    if (status.ok() && response.has_entry()) {
        Deserializer(e, (const uint8_t*)response.entry().data().data());
        return true;
    }
    return false;
}

void ShardClient::ReplicateBatchAsync(const uint8_t* buf, size_t size, RPCToken& token) {
    // Make a copy of buf to avoid lifetime issues
    std::string dataCopy((const char*)buf, size);
    std::thread([this, dataCopy, &token]() {
        grpc::ClientContext context;
        lazylog::proto::ReplicateBatchRequest request;
        lazylog::proto::ShardResponse response;
        
        request.set_data(dataCopy);
        stub_->ReplicateBatch(&context, request, &response);
        token.SetComplete();
    }).detach();
}

#ifdef CORFU
bool ShardClient::AppendEntry(const LogEntry& e) {
    return false; // Not fully implemented in experimental
}
void ShardClient::ReadBatchAsync(const uint64_t start_idx, const uint64_t end_idx, RPCToken &token) {
    token.SetComplete();
}
void ShardClient::SetRemoteIdOffset(int input) {}
#endif

}  // namespace lazylog
