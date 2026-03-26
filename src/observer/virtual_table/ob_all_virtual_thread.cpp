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

#include "ob_all_virtual_thread.h"
#include "lib/file/file_directory_utils.h"
#include "lib/thread/protected_stack_allocator.h"
#include "lib/resource/ob_affinity_ctrl.h"

#ifdef __APPLE__
#include <sys/uio.h>
#include "lib/thread/ob_thread_info_registry.h"

// macOS doesn't have process_vm_readv, but we can read from our own process memory directly
// since we're in the same address space
static ssize_t process_vm_readv(pid_t pid, const struct iovec *local_iov, unsigned long liovcnt,
                                const struct iovec *remote_iov, unsigned long riovcnt, unsigned long flags) {
  (void)pid;
  (void)flags;
  if (liovcnt == 0 || riovcnt == 0 || local_iov == nullptr || remote_iov == nullptr) {
    return -1;
  }
  // For same-process memory access, we can just memcpy
  size_t to_copy = std::min(local_iov[0].iov_len, remote_iov[0].iov_len);
  if (remote_iov[0].iov_base != nullptr && local_iov[0].iov_base != nullptr) {
    memcpy(local_iov[0].iov_base, remote_iov[0].iov_base, to_copy);
    return static_cast<ssize_t>(to_copy);
  }
  return -1;
}
#endif

#ifndef __APPLE__
// Linux: use pthread/TLS offset calculation
#define GET_OTHER_TSI_ADDR(var_name, addr) \
const int64_t var_name##_offset = ((int64_t)addr - (int64_t)pthread_self()); \
decltype(*addr) var_name = *(decltype(addr))(thread_base + var_name##_offset);
#endif

namespace oceanbase
{
using namespace lib;
namespace observer
{
ObAllVirtualThread::ObAllVirtualThread() : is_inited_(false), is_config_cgroup_(false)
{
}

ObAllVirtualThread::~ObAllVirtualThread()
{
  reset();
}

int ObAllVirtualThread::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))
              == false)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ip_to_string() fail", K(ret));
  }
  return ret;
}

void ObAllVirtualThread::reset()
{
  is_inited_ = false;
}

int ObAllVirtualThread::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    const char *cgroup_path = "cgroup";
    if (OB_FAIL(FileDirectoryUtils::is_exists(cgroup_path, is_config_cgroup_))) {
      SERVER_LOG(WARN, "fail check file exist", K(cgroup_path), K(ret));
    }
    #ifdef OB_BUILD_EMBED_MODE
    ret = OB_NOT_SUPPORTED;
    return ret;
    #endif
    const int64_t col_count = output_column_ids_.count();
    pid_t pid = getpid();
#ifdef __APPLE__
    // macOS: use thread registry to access other threads' TLS variables
    // First, ensure current thread is registered
    ob_register_thread_tls_info();

    StackMgr::Guard guard(g_stack_mgr);
    for (oceanbase::lib::ObStackHeader* header = *guard; OB_NOT_NULL(header); header = guard.next()) {
      pthread_t thread_pth = (pthread_t)(header->pth_);
      if (thread_pth == 0) {
        continue;
      }

      ObThreadTLSInfo tls_info;
      if (!ObThreadInfoRegistry::instance().get_thread_info(thread_pth, tls_info)) {
        // Thread not registered, skip it
        continue;
      }

      // Read TLS values from the registered pointers
      int64_t tid = (tls_info.tid_ptr != nullptr) ? *tls_info.tid_ptr : 0;
      if (tid <= 0) {
        continue;  // Invalid tid, skip
      }

      uint64_t tenant_id = (tls_info.tenant_id_ptr != nullptr) ? *tls_info.tenant_id_ptr : 0;
      if (!is_sys_tenant(effective_tenant_id_) && tenant_id != effective_tenant_id_) {
        continue;
      }

      uint32_t* wait_addr = (tls_info.current_wait_ptr != nullptr) ? *tls_info.current_wait_ptr : nullptr;
      pthread_t join_addr = (tls_info.thread_joined_ptr != nullptr) ? *tls_info.thread_joined_ptr : 0;
      int64_t sleep_us = (tls_info.sleep_us_ptr != nullptr) ? *tls_info.sleep_us_ptr : 0;
      int64_t blocking_ts = (tls_info.blocking_ts_ptr != nullptr) ? *tls_info.blocking_ts_ptr : 0;

      for (int64_t i = 0; i < col_count && OB_SUCC(ret); ++i) {
        const uint64_t col_id = output_column_ids_.at(i);
        ObObj *cells = cur_row_.cells_;
        switch (col_id) {
          case SVR_IP: {
            cells[i].set_varchar(ip_buf_);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SVR_PORT: {
            cells[i].set_int(GCONF.self_addr_.get_port());
            break;
          }
          case TENANT_ID: {
            cells[i].set_int(0 == tenant_id ? OB_SERVER_TENANT_ID : tenant_id);
            break;
          }
          case TID: {
            cells[i].set_int(tid);
            break;
          }
          case TNAME: {
            if (tls_info.tname_ptr != nullptr) {
              MEMCPY(tname_, tls_info.tname_ptr, sizeof(tname_));
            } else {
              tname_[0] = '\0';
            }
            cells[i].set_varchar(tname_);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case STATUS: {
            const char* status_str = nullptr;
            if (0 != join_addr) {
              status_str = "Join";
            } else if (0 != sleep_us) {
              status_str = "Sleep";
            } else if (0 != blocking_ts) {
              status_str = "Wait";
            } else {
              status_str = "Run";
            }
            cells[i].set_varchar(status_str);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case WAIT_EVENT: {
            ObAddr rpc_dest_addr = (tls_info.rpc_dest_addr_ptr != nullptr) ? *tls_info.rpc_dest_addr_ptr : ObAddr();
            uint8_t event = (tls_info.wait_event_ptr != nullptr) ? *tls_info.wait_event_ptr : 0;
            wait_event_[0] = '\0';
            size_t buf_size = sizeof(wait_event_);
            if (0 != join_addr) {
              // On macOS, we can't easily get the joined thread's tid
              // Try to look it up in our registry
              ObThreadTLSInfo joined_info;
              if (ObThreadInfoRegistry::instance().get_thread_info(join_addr, joined_info) &&
                  joined_info.tid_ptr != nullptr) {
                IGNORE_RETURN snprintf(wait_event_, buf_size, "thread %ld", *joined_info.tid_ptr);
              } else {
                IGNORE_RETURN snprintf(wait_event_, buf_size, "thread (unknown)");
              }
            } else if (OB_NOT_NULL(wait_addr)) {
              uint32_t val = *wait_addr;
              if (0 != (val & (1<<30))) {
                IGNORE_RETURN snprintf(wait_event_, buf_size, "wrlock on %u", val & 0x3fffffff);
              } else {
                IGNORE_RETURN snprintf(wait_event_, buf_size, "%u rdlocks", val & 0x3fffffff);
              }
            } else if (rpc_dest_addr.is_valid()) {
              obrpc::ObRpcPacketCode pcode = (tls_info.pcode_ptr != nullptr) ?
                  *tls_info.pcode_ptr : obrpc::ObRpcPacketCode::OB_INVALID_RPC_CODE;
              int64_t pos1 = 0;
              int64_t pos2 = 0;
              if (((pos1 = snprintf(wait_event_, 37, "rpc 0x%X(%s", pcode,
                    obrpc::ObRpcPacketSet::instance().name_of_idx(
                        obrpc::ObRpcPacketSet::instance().idx_of_pcode(pcode)) + 3)) > 0)
                  && ((pos2 = snprintf(wait_event_ + std::min(static_cast<int64_t>(36L), pos1), 6, ") to ")) > 0)) {
                int64_t pos = std::min(static_cast<int64_t>(36L), pos1) + std::min(static_cast<int64_t>(5L), pos2);
                pos += rpc_dest_addr.to_string(wait_event_ + pos, buf_size - pos);
              }
            } else if (0 != blocking_ts && (0 != (Thread::WAIT_IN_TENANT_QUEUE & event))) {
              IGNORE_RETURN snprintf(wait_event_, buf_size, "tenant worker requests");
            } else if (0 != blocking_ts && (0 != (Thread::WAIT_FOR_IO_EVENT & event))) {
              IGNORE_RETURN snprintf(wait_event_, buf_size, "IO events");
            } else if (0 != blocking_ts && (0 != (Thread::WAIT_FOR_LOCAL_RETRY & event))) {
              IGNORE_RETURN snprintf(wait_event_, buf_size, "local retry");
            } else if (0 != blocking_ts && (0 != (Thread::WAIT_FOR_PX_MSG & event))) {
              IGNORE_RETURN snprintf(wait_event_, buf_size, "px message");
            } else if (0 != sleep_us) {
              IGNORE_RETURN snprintf(wait_event_, buf_size, "%ld us", sleep_us);
            } else if (0 != blocking_ts) {
              IGNORE_RETURN snprintf(wait_event_, buf_size, "%ld us", common::ObTimeUtility::fast_current_time() - blocking_ts);
            }
            cells[i].set_varchar(wait_event_);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case LATCH_WAIT: {
            if (OB_ISNULL(wait_addr)) {
              cells[i].set_varchar("");
            } else {
              IGNORE_RETURN snprintf(wait_addr_, 16, "%p", wait_addr);
              cells[i].set_varchar(wait_addr_);
            }
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case LATCH_HOLD: {
            locks_addr_[0] = 0;
            if (tls_info.current_locks_ptr != nullptr && tls_info.max_lock_slot_idx_ptr != nullptr) {
              uint8_t slot_cnt = *tls_info.max_lock_slot_idx_ptr;
              const int64_t cnt = std::min(ARRAYSIZEOF(ObLatch::current_locks), (int64_t)slot_cnt);
              uint32_t** locks = tls_info.current_locks_ptr;
              for (int64_t ii = 0, j = 0; ii < cnt; ++ii) {
                int64_t idx = (slot_cnt + ii) % ARRAYSIZEOF(ObLatch::current_locks);
                if (OB_NOT_NULL(locks[idx]) && j < 256) {
                  uint32_t val = *locks[idx];
                  if (0 != val) {
                    j += snprintf(locks_addr_ + j, 256 - j, "%p ", locks[idx]);
                  }
                }
              }
            }
            cells[i].set_varchar(locks_addr_);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case TRACE_ID: {
            if (tls_info.trace_id_ptr != nullptr) {
              IGNORE_RETURN tls_info.trace_id_ptr->to_string(trace_id_buf_, sizeof(trace_id_buf_));
            } else {
              trace_id_buf_[0] = '\0';
            }
            cells[i].set_varchar(trace_id_buf_);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case LOOP_TS: {
            int64_t loop_ts = (tls_info.loop_ts_ptr != nullptr) ? *tls_info.loop_ts_ptr : 0;
            cells[i].set_timestamp(loop_ts);
            break;
          }
          case CGROUP_PATH: {
            // cgroups not supported on macOS
            cells[i].set_varchar("");
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case NUMA_NODE: {
            int numa_node = (tls_info.numa_node_ptr != nullptr) ? *tls_info.numa_node_ptr : OB_NUMA_SHARED_INDEX;
            int64_t numa_node_display = -1;
            if (numa_node != OB_NUMA_SHARED_INDEX) {
              numa_node_display = numa_node;
            }
            cells[i].set_int(numa_node_display);
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
            break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(scanner_.add_row(cur_row_))) {
          SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
          if (OB_SIZE_OVERFLOW == ret) {
            ret = OB_SUCCESS;
          }
        }
      }
    }
#else
    // Linux: use pthread/TLS offset calculation
    StackMgr::Guard guard(g_stack_mgr);
    for (oceanbase::lib::ObStackHeader* header = *guard; OB_NOT_NULL(header); header = guard.next()) {
      char* thread_base = (char*)(header->pth_);
      if (OB_NOT_NULL(thread_base)) {
        GET_OTHER_TSI_ADDR(tid, &get_tid_cache());
        {
          char path[64];
          IGNORE_RETURN snprintf(path, 64, "/proc/self/task/%ld", tid);
          if (-1 == access(path, F_OK)) {
            // thread not exist, may have exited.
            continue;
          }
        }
        GET_OTHER_TSI_ADDR(tenant_id, &ob_get_tenant_id());
        if (!is_sys_tenant(effective_tenant_id_)
            && tenant_id != effective_tenant_id_) {
          continue;
        }
        GET_OTHER_TSI_ADDR(wait_addr, &ObLatch::current_wait);
        GET_OTHER_TSI_ADDR(join_addr, &Thread::thread_joined_);
        GET_OTHER_TSI_ADDR(sleep_us, &Thread::sleep_us_);
        GET_OTHER_TSI_ADDR(blocking_ts, &Thread::blocking_ts_);
        for (int64_t i = 0; i < col_count && OB_SUCC(ret); ++i) {
          const uint64_t col_id = output_column_ids_.at(i);
          ObObj *cells = cur_row_.cells_;
          switch (col_id) {
            case SVR_IP: {
              cells[i].set_varchar(ip_buf_);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case SVR_PORT: {
              cells[i].set_int(GCONF.self_addr_.get_port());
              break;
            }
            case TENANT_ID: {
              cells[i].set_int(0 == tenant_id ? OB_SERVER_TENANT_ID : tenant_id);
              break;
            }
            case TID: {
              cells[i].set_int(tid);
              break;
            }
            case TNAME: {
              GET_OTHER_TSI_ADDR(tname, &(ob_get_tname()[0]));
              // PAY ATTENTION HERE
              MEMCPY(tname_, thread_base + tname_offset, sizeof(tname_));
              cells[i].set_varchar(tname_);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case STATUS: {
              const char* status_str = nullptr;
              if (0 != join_addr) {
                status_str = "Join";
              } else if (0 != sleep_us) {
                status_str = "Sleep";
              } else if (0 != blocking_ts) {
                status_str = "Wait";
              } else {
                status_str = "Run";
              }
              cells[i].set_varchar(status_str);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case WAIT_EVENT: {
              GET_OTHER_TSI_ADDR(rpc_dest_addr, &Thread::rpc_dest_addr_);
              GET_OTHER_TSI_ADDR(event, &Thread::wait_event_);
              ObAddr addr;
              struct iovec local_iov = {&addr, sizeof(ObAddr)};
              struct iovec remote_iov = {thread_base + rpc_dest_addr_offset, sizeof(ObAddr)};
              wait_event_[0] = '\0';
              size_t buf_size = sizeof(wait_event_);
              if (0 != join_addr) {
                IGNORE_RETURN snprintf(wait_event_, buf_size, "thread %u", *(uint32_t*)(join_addr + tid_offset));
              } else if (OB_NOT_NULL(wait_addr)) {
                uint32_t val = 0;
                struct iovec local_iov = {&val, sizeof(val)};
                struct iovec remote_iov = {wait_addr, sizeof(val)};
                ssize_t n = process_vm_readv(pid, &local_iov, 1, &remote_iov, 1, 0);
                if (n != sizeof(val)) {
                } else if (0 != (val & (1<<30))) {
                  IGNORE_RETURN snprintf(wait_event_, buf_size, "wrlock on %u", val & 0x3fffffff);
                } else {
                  IGNORE_RETURN snprintf(wait_event_, buf_size, "%u rdlocks", val & 0x3fffffff);
                }
              } else if (sizeof(ObAddr) == process_vm_readv(pid, &local_iov, 1, &remote_iov, 1, 0)
                         && addr.is_valid()) {
                GET_OTHER_TSI_ADDR(pcode, &Thread::pcode_);
                int64_t pos1 = 0;
                int64_t pos2 = 0;
                if (((pos1 = snprintf(wait_event_, 37, "rpc 0x%X(%s", pcode, obrpc::ObRpcPacketSet::instance().name_of_idx(obrpc::ObRpcPacketSet::instance().idx_of_pcode(pcode)) + 3)) > 0)
                    && ((pos2 = snprintf(wait_event_ + std::min(static_cast<int64_t>(36L), pos1), 6, ") to ")) > 0)) {
                  int64_t pos = std::min(static_cast<int64_t>(36L), pos1) + std::min(static_cast<int64_t>(5L), pos2);
                  pos += addr.to_string(wait_event_ + pos, buf_size - pos);
                }
              } else if (0 != blocking_ts && (0 != (Thread::WAIT_IN_TENANT_QUEUE & event))) {
                IGNORE_RETURN snprintf(wait_event_, buf_size, "tenant worker requests");
              } else if (0 != blocking_ts && (0 != (Thread::WAIT_FOR_IO_EVENT & event))) {
                IGNORE_RETURN snprintf(wait_event_, buf_size, "IO events");
              } else if (0 != blocking_ts && (0 != (Thread::WAIT_FOR_LOCAL_RETRY & event))) {
                IGNORE_RETURN snprintf(wait_event_, buf_size, "local retry");
              } else if (0 != blocking_ts && (0 != (Thread::WAIT_FOR_PX_MSG & event))) {
                IGNORE_RETURN snprintf(wait_event_, buf_size, "px message");
              } else if (0 != sleep_us) {
                IGNORE_RETURN snprintf(wait_event_, buf_size, "%ld us", sleep_us);
              } else if (0 != blocking_ts) {
                IGNORE_RETURN snprintf(wait_event_, buf_size, "%ld us", common::ObTimeUtility::fast_current_time() - blocking_ts);
              }
              cells[i].set_varchar(wait_event_);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case LATCH_WAIT: {
              if (OB_ISNULL(wait_addr)) {
                cells[i].set_varchar("");
              } else {
                IGNORE_RETURN snprintf(wait_addr_, 16, "%p", wait_addr);
                cells[i].set_varchar(wait_addr_);
              }
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case LATCH_HOLD: {
              GET_OTHER_TSI_ADDR(locks_addr, &(ObLatch::current_locks[0]));
              GET_OTHER_TSI_ADDR(slot_cnt, &ObLatch::max_lock_slot_idx)
              const int64_t cnt = std::min(ARRAYSIZEOF(ObLatch::current_locks), (int64_t)slot_cnt);
              decltype(&locks_addr) locks = (decltype(&locks_addr))(thread_base + locks_addr_offset);
              locks_addr_[0] = 0;
              for (int64_t i = 0, j = 0; i < cnt; ++i) {
                int64_t idx = (slot_cnt + i) % ARRAYSIZEOF(ObLatch::current_locks);
                if (OB_NOT_NULL(locks[idx]) && j < 256) {
                  uint32_t val = 0;
                  struct iovec local_iov = {&val, sizeof(val)};
                  struct iovec remote_iov = {locks[idx], sizeof(val)};
                  ssize_t n = process_vm_readv(pid, &local_iov, 1, &remote_iov, 1, 0);
                  if (n != sizeof(val)) {
                  } else if (0 != val) {
                    j += snprintf(locks_addr_ + j, 256 - j, "%p ", locks[idx]);
                  }
                }
              }
              cells[i].set_varchar(locks_addr_);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case TRACE_ID: {
              GET_OTHER_TSI_ADDR(trace_id, ObCurTraceId::get_trace_id());
              IGNORE_RETURN trace_id.to_string(trace_id_buf_, sizeof(trace_id_buf_));
              cells[i].set_varchar(trace_id_buf_);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case LOOP_TS: {
              GET_OTHER_TSI_ADDR(loop_ts, &oceanbase::lib::Thread::loop_ts_);
              cells[i].set_timestamp(loop_ts);
              break;
            }
            case CGROUP_PATH: {
              if (!is_config_cgroup_) {
                cells[i].set_varchar("");
              } else {
                int64_t pid = getpid();
                snprintf(cgroup_path_buf_, PATH_BUFSIZE, "/proc/%ld/task/%ld/cgroup", pid, tid);
                cells[i].set_varchar(cgroup_path_buf_);
              }
              break;
            }
            case NUMA_NODE: {
              GET_OTHER_TSI_ADDR(numa_node, &ObAffinityCtrl::get_tls_node());
              int64_t numa_node_display = -1;
              if (numa_node == OB_NUMA_SHARED_INDEX) {
              } else {
                numa_node_display = numa_node;
              }
              cells[i].set_int(numa_node_display);
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
              break;
            }
          }
        }
        if (OB_SUCC(ret)) {
          // scanner maximum supports 64M, therefore overflow is not considered for now
          if (OB_FAIL(scanner_.add_row(cur_row_))) {
            SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
            if (OB_SIZE_OVERFLOW == ret) {
              ret = OB_SUCCESS;
            }
          }
        }
      }
    }
#endif
    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      is_inited_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else if (OB_FAIL(read_real_cgroup_path())){
      SERVER_LOG(WARN, "fail to get cgroup path real path", K(ret));
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualThread::read_real_cgroup_path()
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  for (int64_t i = 0; i < col_count && OB_SUCC(ret); ++i) {
    const uint64_t col_id = output_column_ids_.at(i);
    ObObj *cells = cur_row_.cells_;
    if (col_id == CGROUP_PATH) {
      char path[PATH_BUFSIZE];
      snprintf(path, cells[i].get_val_len() + 1, "%s", cells[i].get_varchar().ptr());
      FILE *file = fopen(path, "r");
      if (NULL == file) {
        cells[i].set_varchar("");
      } else {
        /*
        file content
        7:perf_event:/
        6:cpuset,cpu,cpuacct:/oceanbase/tenant_0001
        5:blkio:/system.slice/staragentctl.service
        4:devices:/system.slice/staragentctl.service
        3:hugetlb:/
        cgroup_path = /tenant_0001
        */
        bool is_find = false;
        int min_len = 2;
        int discard_len = 1;
        char read_buff[PATH_BUFSIZE];
        cgroup_path_buf_[0] = '\0';
        while (fgets(read_buff, sizeof(read_buff), file) != NULL && !is_find) {
          const char* match_begin =  strstr(read_buff, ":/");
          const char* match_cpu =  strstr(read_buff, "cpu");
          if (match_begin != NULL && match_cpu != NULL) {
            is_find = true;
            match_begin += discard_len;
            snprintf(cgroup_path_buf_, PATH_BUFSIZE, "%s", match_begin);
          }
        }
        if (is_find) {
          int cgroup_path_len = strlen(cgroup_path_buf_);
          if (min_len < cgroup_path_len) {
            if (cgroup_path_buf_[cgroup_path_len - 1] == '\n') {
              cgroup_path_buf_[cgroup_path_len - 1] = '\0';
            }
            cells[i].set_varchar(cgroup_path_buf_);
          } else {
            cells[i].set_varchar("");
          }
        } else {
          cells[i].set_varchar("");
        }
      }
      cells[i].set_collation_type(
          ObCharset::get_default_collation(ObCharset::get_default_charset()));
      if (NULL != file) {
        fclose(file);
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
