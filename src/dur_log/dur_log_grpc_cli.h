#pragma once

#include "dur_log_cli.h"
#include <grpcpp/grpcpp.h>
#include "lazylog.pb.h"
#include "lazylog.grpc.pb.h"
#include "glog/logging.h"
#include <queue>
#include <thread>
#include <future>

namespace lazylog {

class DurabilityLogGrpcCli : public DurabilityLogCli {
   public:
    DurabilityLogGrpcCli();
    virtual ~DurabilityLogGrpcCli();

    void Initialize(const Properties &p) override {
        LOG(ERROR) << "This is a client RPC transport";
        throw Exception("Wrong rpc transport type");
    }
    void InitializeConn(const Properties &p, const std::string &svr, void *param) override;
    void Finalize() override;

    uint64_t AppendEntry(const LogEntry &e) override;
    bool AppendEntryAsync(const LogEntry &e, std::shared_ptr<RPCToken> &token) override;
    std::tuple<uint64_t, uint64_t, uint16_t> GetNumDurEntry() override;
    uint32_t FetchUnorderedEntries(std::vector<LogEntry> &e, uint32_t max_entries_num) override;
    uint32_t FetchUnorderedEntries(std::vector<LogEntry> &e, uint64_t from, uint32_t max_entries_num) override;
    uint64_t DeleteOrderedEntries(std::vector<LogEntry::ReqID> &req_ids) override;
    int SpecRead(const uint64_t idx, LogEntry &e) override;
    void DeleteOrderedEntriesAsync(std::vector<LogEntry::ReqID> &req_ids) override;
    uint64_t ProcessFetchedEntries(const std::vector<LogEntry> &es, std::vector<LogEntry::ReqID> &req_ids) override;
    bool IsPrimary() override;
    bool CheckAndRunOnce() override;

#ifdef CORFU
    virtual uint64_t getGSN() { return 0; }
    virtual uint64_t getGSNBatch(uint64_t batchSize) { return 0; }
#endif

   public:
    void AddPendingReq(std::shared_ptr<RPCToken> &token) override;
    void CheckPendingReq() override;

   protected:
    std::unique_ptr<proto::DurLogService::Stub> stub_;
    bool is_primary_;
    std::queue<std::shared_ptr<RPCToken>> pending_reqs_;
};

}  // namespace lazylog
