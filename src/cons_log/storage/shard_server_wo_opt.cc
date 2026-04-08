#include "shard_server_wo_opt.h"

#include <unistd.h>
#include <cerrno>
#include <sys/stat.h>
#include <fcntl.h>
#include "../../rpc/common.h"
#include "sys/mman.h"

namespace lazylog {

std::unordered_map<std::string, std::shared_ptr<ShardClient>> ShardServerUnoptimized::backups_;
std::unordered_map<uint64_t, int> ShardServerUnoptimized::entries_fd_set_;
std::unordered_map<uint64_t, size_t> ShardServerUnoptimized::cache_size_;
std::unordered_map<uint64_t, size_t> ShardServerUnoptimized::num_entries_;
std::unordered_map<uint64_t, std::map<uint64_t, uint64_t>> ShardServerUnoptimized::gsn_to_file_offset_map_;
std::shared_mutex ShardServerUnoptimized::cache_rw_lock_;
std::condition_variable_any ShardServerUnoptimized::cache_write_cv_;
uint64_t ShardServerUnoptimized::replicated_index_ = 0;
ShardServerMetrics ShardServerUnoptimized::metrics_ = {};
size_t ShardServerUnoptimized::stripe_unit_size_ = 0;
std::string ShardServerUnoptimized::folder_path_ = "";
int ShardServerUnoptimized::shard_num_ = 0;
int ShardServerUnoptimized::shard_id_ = 0;
std::unique_ptr<grpc::Server> ShardServerUnoptimized::grpc_server_;

ShardServerUnoptimized::ShardServerUnoptimized() : is_primary_(false) {}

ShardServerUnoptimized::~ShardServerUnoptimized() {
    if (is_primary_) {
        std::cout << metrics_;
    }
}

void ShardServerUnoptimized::Initialize(const Properties &p) {
    const std::string server_uri = p.GetProperty(PROP_SHD_SVR_URI, PROP_SHD_SVR_URI_DEFAULT);

    stripe_unit_size_ = std::stoi(p.GetProperty(PROP_SHD_STRIPE_SIZE, PROP_SHD_STRIPE_SIZE_DEFAULT));
    shard_num_ = std::stoi(p.GetProperty("shard.num", "1"));
    shard_id_ = std::stoi(p.GetProperty("shard.id", "0"));
    folder_path_ = p.GetProperty(PROP_SHD_FOLDER_PATH, PROP_SHD_FOLDER_PATH_DEFAULT);
    replicated_index_ = std::stoull(p.GetProperty("shard.replicated_index", "0"));

    struct stat info;
    if (::stat(folder_path_.c_str(), &info) != 0 || !S_ISDIR(info.st_mode)) {
        if (::mkdir(folder_path_.c_str(), 0777) != 0) {
            LOG(ERROR) << "Can't make directory, error: " << errno;
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
    static ShardServerUnoptImpl service;
    builder.RegisterService(&service);
    grpc_server_ = builder.BuildAndStart();
    LOG(INFO) << "gRPC ShardServerUnoptimized listening on " << server_uri;
}

void ShardServerUnoptimized::Finalize() {
    if (grpc_server_) {
        grpc_server_->Shutdown();
    }
    for (auto &b : backups_) b.second->Finalize();
}

grpc::Status ShardServerUnoptImpl::AppendBatch(grpc::ServerContext* context, const lazylog::proto::AppendBatchRequest* request, lazylog::proto::ShardResponse* response) {
    std::string buf;
    uint32_t num = request->entries_size();
    buf.append((const char*)&num, sizeof(uint32_t));
    for (uint32_t i = 0; i < num; ++i) {
        buf.append(request->entries(i).data());
    }

    LogEntry first_e_in_batch;
    Deserializer(first_e_in_batch, (const uint8_t*)buf.data() + sizeof(uint32_t));
    uint64_t big_stripe_unit_size = ShardServerUnoptimized::stripe_unit_size_ * ShardServerUnoptimized::shard_num_;
    uint64_t base_idx = first_e_in_batch.log_idx / big_stripe_unit_size * big_stripe_unit_size;

    std::vector<RPCToken> tokens;
    tokens.reserve(ShardServerUnoptimized::backups_.size());
    for (auto &b : ShardServerUnoptimized::backups_) {
        tokens.emplace_back();
        b.second->ReplicateBatchAsync((const uint8_t*)buf.data(), buf.size(), tokens.back());
    }

    {
        std::unique_lock<std::shared_mutex> write_lock(ShardServerUnoptimized::cache_rw_lock_);
        ShardServerUnoptimized::processEntriesAndBuildMap(base_idx, (const uint8_t*)buf.data());
        if (ShardServerUnoptimized::num_entries_[base_idx] >= ShardServerUnoptimized::stripe_unit_size_) {
            ShardServerUnoptimized::writeFromCacheToDisk(base_idx);
        }
    }

    while (!ShardServerUnoptimized::allRPCCompleted(tokens)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    {
        std::unique_lock<std::shared_mutex> write_lock(ShardServerUnoptimized::cache_rw_lock_);
        ShardServerUnoptimized::replicated_index_ = std::prev(ShardServerUnoptimized::gsn_to_file_offset_map_[base_idx].end())->first;
    }
    ShardServerUnoptimized::cache_write_cv_.notify_all();

    response->set_status(0);
    return grpc::Status::OK;
}

grpc::Status ShardServerUnoptImpl::ReplicateBatch(grpc::ServerContext* context, const lazylog::proto::ReplicateBatchRequest* request, lazylog::proto::ShardResponse* response) {
    const std::string& buf = request->data();
    uint32_t num = *reinterpret_cast<const uint32_t*>(buf.data());

    LogEntry first_e_in_batch;
    Deserializer(first_e_in_batch, (const uint8_t*)buf.data() + sizeof(uint32_t));
    uint64_t big_stripe_unit_size = ShardServerUnoptimized::stripe_unit_size_ * ShardServerUnoptimized::shard_num_;
    uint64_t base_idx = first_e_in_batch.log_idx / big_stripe_unit_size * big_stripe_unit_size;

    ShardServerUnoptimized::processEntriesAndBuildMap(base_idx, (const uint8_t*)buf.data());

    if (ShardServerUnoptimized::num_entries_[base_idx] >= ShardServerUnoptimized::stripe_unit_size_) {
        if (ShardServerUnoptimized::writeFromCacheToDisk(base_idx) < 0) {
            response->set_status(-1);
            return grpc::Status::OK;
        }
    }

    response->set_status(0);
    return grpc::Status::OK;
}

grpc::Status ShardServerUnoptImpl::ReadEntry(grpc::ServerContext* context, const lazylog::proto::ReadEntryRequest* request, lazylog::proto::ReadEntryResponse* response) {
    uint64_t idx = request->idx();
    uint64_t big_stripe_unit_size = ShardServerUnoptimized::stripe_unit_size_ * ShardServerUnoptimized::shard_num_;
    uint64_t base_idx = idx / big_stripe_unit_size * big_stripe_unit_size;
    uint64_t local_cache_idx = (idx - base_idx) / ShardServerUnoptimized::shard_num_;
    
    bool slow_path_exercized = false;
    std::string resp_buf;
    resp_buf.resize(2 * 1024 * 1024);
    size_t len = 0;

    {
        std::shared_lock<std::shared_mutex> read_lock(ShardServerUnoptimized::cache_rw_lock_);
        while (1) {
            if (ShardServerUnoptimized::gsn_to_file_offset_map_.find(base_idx) == ShardServerUnoptimized::gsn_to_file_offset_map_.end()) {
                read_lock.unlock();
                bool loaded_by_me = false;
                bool loaded = false;
                {
                    std::unique_lock<std::shared_mutex> write_lock(ShardServerUnoptimized::cache_rw_lock_);
                    if (ShardServerUnoptimized::gsn_to_file_offset_map_.find(base_idx) == ShardServerUnoptimized::gsn_to_file_offset_map_.end()) {
                        loaded_by_me = loaded = (ShardServerUnoptimized::loadFromDiskToCache(base_idx) == 0);
                    } else {
                        loaded = true;
                        loaded_by_me = false;
                    }
                }
                if (loaded_by_me) {
                    ShardServerUnoptimized::cache_write_cv_.notify_all();
                }
                read_lock.lock();
                if (!loaded && ShardServerUnoptimized::gsn_to_file_offset_map_.find(base_idx) == ShardServerUnoptimized::gsn_to_file_offset_map_.end()) {
                    ShardServerUnoptimized::cache_write_cv_.wait(read_lock);
                    slow_path_exercized = true;
                    continue;
                }
            }

            if (local_cache_idx >= ShardServerUnoptimized::num_entries_[base_idx] || idx > ShardServerUnoptimized::replicated_index_) {
                slow_path_exercized = true;
                ShardServerUnoptimized::cache_write_cv_.wait(read_lock);
                continue;
            }

            auto &offset_map = ShardServerUnoptimized::gsn_to_file_offset_map_[base_idx];
            len = offset_map.find(idx + ShardServerUnoptimized::shard_num_) != offset_map.end() 
                ? offset_map[idx + ShardServerUnoptimized::shard_num_] - offset_map[idx]
                : ShardServerUnoptimized::cache_size_[base_idx] - offset_map[idx];

            int got = ShardServerUnoptimized::readEntryFromDisk(base_idx, offset_map[idx], (uint8_t*)resp_buf.data(), len);
            if (len != got) {
                LOG(ERROR) << "not able to read length, expected " << len << ", got " << got;
            }
            break;
        }
    }
    if (slow_path_exercized) {
        ShardServerUnoptimized::metrics_.num_slow_path_reads++;
    } else {
        ShardServerUnoptimized::metrics_.num_fast_path_reads++;
    }
    resp_buf.resize(len);
    response->mutable_entry()->set_data(resp_buf);
    return grpc::Status::OK;
}

void ShardServerUnoptimized::processEntriesAndBuildMap(uint64_t base_idx, const uint8_t *buf) {
    int fd = 0;
    if (gsn_to_file_offset_map_.find(base_idx) == gsn_to_file_offset_map_.end()) {
        if (loadFromDiskToCache(base_idx) < 0) {
            gsn_to_file_offset_map_[base_idx] = std::map<uint64_t, uint64_t>();
            cache_size_[base_idx] = 0;
            num_entries_[base_idx] = 0;
            fd = open(getDataFilePath(base_idx).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
            if (fd < 0) {
                LOG(ERROR) << "Can't open file for write of base idx " << base_idx;
                return;
            }
            entries_fd_set_[base_idx] = fd;
        }
    } else {
        fd = entries_fd_set_[base_idx];
    }

    size_t batch_len = ProcessAndBuildMap(gsn_to_file_offset_map_[base_idx], buf, cache_size_[base_idx]) - sizeof(uint32_t);
    cache_size_[base_idx] += batch_len;
    num_entries_[base_idx] += (*reinterpret_cast<const uint32_t *>(buf));

    if (write(fd, buf + sizeof(uint32_t), batch_len) != batch_len) {
        LOG(WARNING) << "Writing less bytes than expected";
    }
}

std::string ShardServerUnoptimized::getDataFilePath(uint64_t base_idx) {
    return folder_path_ + "/entries_" + std::to_string(base_idx) + "_r_" + std::to_string(shard_id_) + ".dat";
}

int ShardServerUnoptimized::writeFromCacheToDisk(uint64_t base_idx) {
    auto it = entries_fd_set_.find(base_idx);
    if (it == entries_fd_set_.end()) {
        LOG(ERROR) << "File not open for base idx " << base_idx;
        return -1;
    }
    close(it->second);
    return 0;
}

int ShardServerUnoptimized::readEntryFromDisk(uint64_t base_idx, uint64_t file_offset, uint8_t *buf, size_t len) {
    int fd = open(getDataFilePath(base_idx).c_str(), O_RDONLY);
    if (fd < 0) return fd;
    int ret = pread(fd, buf, len, file_offset);
    close(fd);
    return ret;
}

int ShardServerUnoptimized::loadFromDiskToCache(uint64_t base_idx) {
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
    if (buf == MAP_FAILED) return -1;
    
    gsn_to_file_offset_map_[base_idx].clear();
    cache_size_[base_idx] = 0;
    auto p = ProcessAndBuildMap(gsn_to_file_offset_map_[base_idx], buf, info.st_size, cache_size_[base_idx]);
    cache_size_[base_idx] += p.first;
    num_entries_[base_idx] += p.second;
    if (info.st_size >= stripe_unit_size_) {
        close(fd);
    }
    munmap(buf, info.st_size);
    return 0;
}

bool ShardServerUnoptimized::allRPCCompleted(std::vector<RPCToken> &tokens) {
    for (auto &t : tokens) {
        if (!t.Complete()) return false;
    }
    return true;
}

void ShardServerUnoptimized::server_func(const Properties &p) {}
void ShardServerUnoptimized::read_server_func(const Properties &p, int t_id) {}

std::ostream &operator<<(std::ostream &out, const ShardServerMetrics &B) {
    out << "ShardServerMetrics: \n"
        << "\tnum_slow_path_reads: " << B.num_slow_path_reads << "\n"
        << "\tnum_fast_path_reads: " << B.num_fast_path_reads << "\n";
    return out;
}

}  // namespace lazylog
