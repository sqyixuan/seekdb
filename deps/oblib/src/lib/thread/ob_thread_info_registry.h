/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OCEANBASE_LIB_THREAD_OB_THREAD_INFO_REGISTRY_H_
#define OCEANBASE_LIB_THREAD_OB_THREAD_INFO_REGISTRY_H_

#include <pthread.h>
#include <stdint.h>

#ifdef __APPLE__
#include <map>
#include <mutex>
#include "lib/ob_define.h"
#include "lib/lock/ob_latch.h"
#include "lib/net/ob_addr.h"
#include "lib/profile/ob_trace_id.h"
#include "rpc/obrpc/ob_rpc_packet.h"

namespace oceanbase {
namespace lib {
class Thread;
}

namespace common {

// Structure to hold pointers to a thread's TLS variables
// Used on macOS where pthread/TLS offset calculations don't work
struct ObThreadTLSInfo {
  pthread_t pth;
  int64_t* tid_ptr;
  uint64_t* tenant_id_ptr;
  char* tname_ptr;
  uint32_t** current_wait_ptr;
  pthread_t* thread_joined_ptr;
  int64_t* sleep_us_ptr;
  int64_t* blocking_ts_ptr;
  ObAddr* rpc_dest_addr_ptr;
  obrpc::ObRpcPacketCode* pcode_ptr;
  uint8_t* wait_event_ptr;
  int64_t* loop_ts_ptr;
  int* numa_node_ptr;
  uint32_t** current_locks_ptr;
  uint8_t* max_lock_slot_idx_ptr;
  ObCurTraceId::TraceId* trace_id_ptr;

  ObThreadTLSInfo() : pth(0), tid_ptr(nullptr), tenant_id_ptr(nullptr), tname_ptr(nullptr),
    current_wait_ptr(nullptr), thread_joined_ptr(nullptr), sleep_us_ptr(nullptr),
    blocking_ts_ptr(nullptr), rpc_dest_addr_ptr(nullptr), pcode_ptr(nullptr),
    wait_event_ptr(nullptr), loop_ts_ptr(nullptr), numa_node_ptr(nullptr),
    current_locks_ptr(nullptr), max_lock_slot_idx_ptr(nullptr), trace_id_ptr(nullptr) {}
};

// Thread-safe registry for thread TLS information
// Used on macOS to enable reading other threads' TLS variables
class ObThreadInfoRegistry {
public:
  static ObThreadInfoRegistry& instance() {
    static ObThreadInfoRegistry inst;
    return inst;
  }

  void register_thread(pthread_t pth, const ObThreadTLSInfo& info) {
    std::lock_guard<std::mutex> lock(mutex_);
    registry_[pth] = info;
  }

  void unregister_thread(pthread_t pth) {
    std::lock_guard<std::mutex> lock(mutex_);
    registry_.erase(pth);
  }

  bool get_thread_info(pthread_t pth, ObThreadTLSInfo& info) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = registry_.find(pth);
    if (it != registry_.end()) {
      info = it->second;
      return true;
    }
    return false;
  }

  std::map<pthread_t, ObThreadTLSInfo> get_all_threads() {
    std::lock_guard<std::mutex> lock(mutex_);
    return registry_;
  }

private:
  ObThreadInfoRegistry() = default;
  std::mutex mutex_;
  std::map<pthread_t, ObThreadTLSInfo> registry_;
};

// Helper to register the current thread's TLS info
// Call this early in thread startup
void ob_register_thread_tls_info();

// Helper to unregister the current thread's TLS info
// Call this before thread exit
void ob_unregister_thread_tls_info();

// Ensure the current thread's TLS info is registered (idempotent)
void ob_ensure_thread_tls_registered();

} // namespace common
} // namespace oceanbase

#endif // __APPLE__

#endif // OCEANBASE_LIB_THREAD_OB_THREAD_INFO_REGISTRY_H_
