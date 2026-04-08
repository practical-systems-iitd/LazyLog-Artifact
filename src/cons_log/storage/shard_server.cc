#include "shard_server.h"

#include "../../rpc/common.h"
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <sys/types.h>

namespace lazylog {

std::unordered_map<std::string, std::shared_ptr<ShardClient>> ShardServer::backups_;
std::unordered_map<uint64_t, std::vector<LogEntry>> ShardServer::entries_cache_set_;
std::map<uint64_t, int> ShardServer::entries_fd_set_;
std::unordered_map<uint64_t, size_t> ShardServer::cache_size_;
std::shared_mutex ShardServer::cache_rw_lock_;
std::condition_variable_any ShardServer::cache_write_cv_;
ShardServerMetrics ShardServer::metrics_ = {};
size_t ShardServer::stripe_unit_size_ = 0;
std::string ShardServer::folder_path_ = "";
uint64_t ShardServer::global_index_ = 0;
uint64_t ShardServer::replicated_index_ = 0;
int ShardServer::shard_num_ = 0;
int ShardServer::shard_id_ = 0;
bool ShardServer::terminate_ = false;
std::unique_ptr<grpc::Server> ShardServer::grpc_server_;

ShardServer::ShardServer() : is_primary_(false) {}

std::ostream &operator<<(std::ostream &out, ShardServerMetrics const &metrics) {
    out << "metrics: " << std::endl
        << "\tnum_slow_path_reads: " << metrics.num_slow_path_reads << std::endl
        << "\tnum_fast_path_reads: " << metrics.num_fast_path_reads << std::endl;
    return out;
}

ShardServer::~ShardServer() {
    if (is_primary_) {
        std::cout << metrics_;
    }
}

void ShardServer::backgroundFsync() {
    while (!terminate_) {
        int latest_fd = -1;
        {
            std::unique_lock<std::shared_mutex> lock(cache_rw_lock_);
            if (!entries_fd_set_.empty()) {
                latest_fd = entries_fd_set_.rbegin()->second;
            }
        }
        if (latest_fd != -1) fsync(latest_fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void ShardServer::Initialize(const Properties &p) {
    const std::string server_uri = p.GetProperty(PROP_SHD_SVR_URI, PROP_SHD_SVR_URI_DEFAULT);

    stripe_unit_size_ = std::stoi(p.GetProperty(PROP_SHD_STRIPE_SIZE, PROP_SHD_STRIPE_SIZE_DEFAULT));
    shard_num_ = std::stoi(p.GetProperty("shard.num", "1"));
    shard_id_ = std::stoi(p.GetProperty("shard.id", "0"));
    folder_path_ = p.GetProperty(PROP_SHD_FOLDER_PATH, PROP_SHD_FOLDER_PATH_DEFAULT);
    replicated_index_ = std::stoull(p.GetProperty("shard.replicated_index", "0"));
    global_index_ = std::stoull(p.GetProperty("shard.global_index", "0"));

    struct stat info;
    if (::stat(folder_path_.c_str(), &info) != 0 || !S_ISDIR(info.st_mode)) {
        if (::mkdir(folder_path_.c_str(), 0777) != 0) {
            LOG(ERROR) << "Can't make directory, error: " << strerror(errno);
            throw Exception("Can't make directory");
        }
    }

    if (p.GetProperty("leader", "false") != "true") {
        LOG(INFO) << "Not a shard server leader";
    } else {
        is_primary_ = true;
        LOG(INFO) << "This is shard server leader";

        const std::vector<std::string> backup_uri =
            SeparateValue(p.GetProperty(PROP_SHD_BACKUP_URI, PROP_SHD_BACKUP_URI_DEFAULT), ',');
        for (auto &b : backup_uri) {
            backups_[b] = std::make_shared<ShardClient>();
            backups_[b]->InitializeConn(p, b, nullptr);
        }
    }

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_uri, grpc::InsecureServerCredentials());
    static ShardServerImpl service;
    builder.RegisterService(&service);
    grpc_server_ = builder.BuildAndStart();
    LOG(INFO) << "gRPC ShardServer listening on " << server_uri;
}

void ShardServer::Finalize() {
    terminate_ = true;
    if (grpc_server_) {
        grpc_server_->Shutdown();
    }
    for (auto &b : backups_) b.second->Finalize();
}

grpc::Status ShardServerImpl::AppendBatch(grpc::ServerContext* context, const lazylog::proto::AppendBatchRequest* request, lazylog::proto::ShardResponse* response) {
    std::string buf;
    uint32_t num = request->entries_size();
    buf.append((const char*)&num, sizeof(uint32_t));
    for (uint32_t i = 0; i < num; ++i) {
        buf.append(request->entries(i).data());
    }

    LogEntry first_e_in_batch;
    Deserializer(first_e_in_batch, (const uint8_t*)buf.data() + sizeof(uint32_t));
    uint64_t big_stripe_unit_size = ShardServer::stripe_unit_size_ * ShardServer::shard_num_;
    uint64_t base_idx = first_e_in_batch.log_idx / big_stripe_unit_size * big_stripe_unit_size;

    std::vector<RPCToken> tokens;
    tokens.reserve(ShardServer::backups_.size());
    for (auto &b : ShardServer::backups_) {
        tokens.emplace_back();
        b.second->ReplicateBatchAsync((const uint8_t*)buf.data(), buf.size(), tokens.back());
    }

    {
        std::unique_lock<std::shared_mutex> write_lock(ShardServer::cache_rw_lock_);
        ShardServer::addToEntryCache(base_idx, (const uint8_t*)buf.data());
        if (ShardServer::entries_cache_set_[base_idx].size() >= ShardServer::stripe_unit_size_) {
            ShardServer::writeFromCacheToDisk(base_idx);
        }
    }

    // Wait for backups replicating
    while (!ShardServer::allRPCCompleted(tokens)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1)); // busy wait emulation
    }

    {
        std::unique_lock<std::shared_mutex> write_lock(ShardServer::cache_rw_lock_);
        ShardServer::replicated_index_ = ShardServer::entries_cache_set_[base_idx].back().log_idx;
    }
    ShardServer::cache_write_cv_.notify_all();

    response->set_status(0);
    return grpc::Status::OK;
}

grpc::Status ShardServerImpl::ReplicateBatch(grpc::ServerContext* context, const lazylog::proto::ReplicateBatchRequest* request, lazylog::proto::ShardResponse* response) {
    const std::string& buf = request->data();
    uint32_t num = *reinterpret_cast<const uint32_t*>(buf.data());
    
    LogEntry first_e_in_batch;
    Deserializer(first_e_in_batch, (const uint8_t*)buf.data() + sizeof(uint32_t));
    uint64_t big_stripe_unit_size = ShardServer::stripe_unit_size_ * ShardServer::shard_num_;
    uint64_t base_idx = first_e_in_batch.log_idx / big_stripe_unit_size * big_stripe_unit_size;

    ShardServer::addToEntryCache(base_idx, (const uint8_t*)buf.data());

    if (ShardServer::entries_cache_set_[base_idx].size() >= ShardServer::stripe_unit_size_) {
        if (ShardServer::writeFromCacheToDisk(base_idx) < 0) {
            response->set_status(-1);
            return grpc::Status::OK;
        }
    }

    response->set_status(0);
    return grpc::Status::OK;
}

grpc::Status ShardServerImpl::ReadEntry(grpc::ServerContext* context, const lazylog::proto::ReadEntryRequest* request, lazylog::proto::ReadEntryResponse* response) {
    uint64_t idx = request->idx();
    uint64_t big_stripe_unit_size = ShardServer::stripe_unit_size_ * ShardServer::shard_num_;
    uint64_t base_idx = idx / big_stripe_unit_size * big_stripe_unit_size;
    uint64_t local_cache_idx = (idx - base_idx) / ShardServer::shard_num_;

    bool slow_path_exercized = false;
    std::string resp_buf;
    resp_buf.resize(2 * 1024 * 1024);
    size_t len = 0;

    {
        std::shared_lock<std::shared_mutex> read_lock(ShardServer::cache_rw_lock_);
        while (1) {
            if (ShardServer::entries_cache_set_.find(base_idx) == ShardServer::entries_cache_set_.end()) {
                read_lock.unlock();
                bool loaded_by_me = false;
                bool loaded = false;
                {
                    std::unique_lock<std::shared_mutex> write_lock(ShardServer::cache_rw_lock_);
                    if (ShardServer::entries_cache_set_.find(base_idx) == ShardServer::entries_cache_set_.end()) {
                        loaded_by_me = loaded = (ShardServer::loadFromDiskToCache(base_idx) == 0);
                    } else {
                        loaded = true;
                        loaded_by_me = false;
                    }
                }
                if (loaded_by_me) {
                    ShardServer::cache_write_cv_.notify_all();
                }
                read_lock.lock();
                if (!loaded && ShardServer::entries_cache_set_.find(base_idx) == ShardServer::entries_cache_set_.end()) {
                    ShardServer::cache_write_cv_.wait(read_lock);
                    slow_path_exercized = true;
                    continue;
                }
            }

            if (local_cache_idx >= ShardServer::entries_cache_set_[base_idx].size() || idx > ShardServer::replicated_index_ || idx > ShardServer::global_index_) {
                slow_path_exercized = true;
                ShardServer::cache_write_cv_.wait(read_lock);
                continue;
            }

            len = Serializer(ShardServer::entries_cache_set_[base_idx][local_cache_idx], (uint8_t*)resp_buf.data());
            break;
        }
    }

    if (slow_path_exercized) {
        ShardServer::metrics_.num_slow_path_reads++;
    } else {
        ShardServer::metrics_.num_fast_path_reads++;
    }

    resp_buf.resize(len);
    response->mutable_entry()->set_data(resp_buf);
    return grpc::Status::OK;
}

grpc::Status ShardServerImpl::UpdateGlobalIdx(grpc::ServerContext* context, const lazylog::proto::UpdateGlobalIdxRequest* request, lazylog::proto::ShardResponse* response) {
    {
        std::unique_lock<std::shared_mutex> lock(ShardServer::cache_rw_lock_);
        ShardServer::global_index_ = request->idx();
    }
    ShardServer::cache_write_cv_.notify_all();

    response->set_status(0);
    return grpc::Status::OK;
}

void ShardServer::addToEntryCache(uint64_t base_idx, const uint8_t *buf) {
    int fd = 0;
    if (entries_cache_set_.find(base_idx) == entries_cache_set_.end()) {
        if (loadFromDiskToCache(base_idx) < 0) {
            entries_cache_set_[base_idx] = {};
            cache_size_[base_idx] = 0;
            fd = open(getDataFilePath(base_idx).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
            if (fd < 0) {
                LOG(ERROR) << "Can't open file " << getDataFilePath(base_idx);
                return;
            }
            entries_fd_set_[base_idx] = fd;
        }
    } else {
        fd = entries_fd_set_[base_idx];
    }

    size_t batch_len = MultiDeserializer(entries_cache_set_[base_idx], buf) - sizeof(uint32_t);
    cache_size_[base_idx] += batch_len;

    if (write(fd, buf + sizeof(uint32_t), batch_len) != batch_len) {
        LOG(WARNING) << "Writing less bytes than expected";
    }
}

std::string ShardServer::getDataFilePath(uint64_t base_idx) {
    return folder_path_ + "/entries_" + std::to_string(base_idx) + "_r_" + std::to_string(shard_id_) + ".dat";
}

int ShardServer::writeFromCacheToDisk(uint64_t base_idx) {
    auto it = entries_fd_set_.find(base_idx);
    if (it == entries_fd_set_.end()) {
        LOG(ERROR) << "File not open for base idx " << base_idx;
        return -1;
    }
    close(it->second);
    return 0;
}

int ShardServer::loadFromDiskToCache(uint64_t base_idx) {
    int fd = open(getDataFilePath(base_idx).c_str(), O_RDONLY);
    if (fd < 0) return fd;

    struct stat info;
    if (fstat(fd, &info) < 0) {
        close(fd);
        return errno;
    }
    if (info.st_size == 0) {
        close(fd);
        return -1;
    }
    uint8_t *buf = static_cast<uint8_t *>(mmap(0, info.st_size, PROT_READ, MAP_FILE | MAP_PRIVATE, fd, 0));
    if (buf == MAP_FAILED) {
        return -1;
    }
    entries_cache_set_[base_idx].clear();
    MultiDeserializer(entries_cache_set_[base_idx], buf, info.st_size);
    if (info.st_size < stripe_unit_size_) {
        cache_size_[base_idx] = info.st_size;
        entries_fd_set_[base_idx] = fd;
    } else {
        close(fd);
    }

    munmap(buf, info.st_size);
    return 0;
}

bool ShardServer::allRPCCompleted(std::vector<RPCToken> &tokens) {
    for (auto &t : tokens) {
        if (!t.Complete()) return false;
    }
    return true;
}

// Stubs for legacy interfaces
void ShardServer::server_func(const Properties &p) {}
void ShardServer::read_server_func(const Properties &p, int t_id) {}
bool ShardServer::processExistingDataFiles() { return true; }

}  // namespace lazylog
