#pragma once
#include <string>

namespace lazylog {

using rpc_run_func_type = void(*)();

class RPCToken {
   public:
    RPCToken();

    bool Complete();
    void SetComplete();
    void Reset();

    std::string resp_msgbuf;

   protected:
    volatile bool completed_;
    // rpc_run_func_type run_func_;
};

}  // namespace lazylog
