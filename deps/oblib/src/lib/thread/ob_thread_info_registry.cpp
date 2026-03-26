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

#include "lib/thread/ob_thread_info_registry.h"

#ifdef __APPLE__
#include "lib/ob_define.h"
#include "lib/lock/ob_latch.h"
#include "lib/thread/thread.h"
#include "lib/resource/ob_affinity_ctrl.h"

namespace oceanbase {
namespace common {

void ob_register_thread_tls_info()
{
  ObThreadTLSInfo info;
  info.pth = pthread_self();
  info.tid_ptr = &get_tid_cache();
  info.tenant_id_ptr = &ob_get_tenant_id();
  info.tname_ptr = &(ob_get_tname()[0]);
  info.current_wait_ptr = &ObLatch::current_wait;
  info.thread_joined_ptr = &lib::Thread::thread_joined_;
  info.sleep_us_ptr = &lib::Thread::sleep_us_;
  info.blocking_ts_ptr = &lib::Thread::blocking_ts_;
  info.rpc_dest_addr_ptr = &lib::Thread::rpc_dest_addr_;
  info.pcode_ptr = &lib::Thread::pcode_;
  info.wait_event_ptr = &lib::Thread::wait_event_;
  info.loop_ts_ptr = &lib::Thread::loop_ts_;
  info.numa_node_ptr = &lib::ObAffinityCtrl::get_tls_node();
  info.current_locks_ptr = &(ObLatch::current_locks[0]);
  info.max_lock_slot_idx_ptr = &ObLatch::max_lock_slot_idx;
  info.trace_id_ptr = ObCurTraceId::get_trace_id();
  ObThreadInfoRegistry::instance().register_thread(info.pth, info);
}

void ob_unregister_thread_tls_info()
{
  ObThreadInfoRegistry::instance().unregister_thread(pthread_self());
}

// RAII guard to automatically register/unregister thread TLS info
class ObThreadTLSInfoGuard {
public:
  ObThreadTLSInfoGuard() {
    ob_register_thread_tls_info();
  }
  ~ObThreadTLSInfoGuard() {
    ob_unregister_thread_tls_info();
  }
};

// Thread-local guard ensures each thread is registered when it first uses this
static thread_local ObThreadTLSInfoGuard g_thread_tls_info_guard;

// Function to force initialization of the thread-local guard
void ob_ensure_thread_tls_registered()
{
  (void)g_thread_tls_info_guard;
}

} // namespace common
} // namespace oceanbase

#endif // __APPLE__
