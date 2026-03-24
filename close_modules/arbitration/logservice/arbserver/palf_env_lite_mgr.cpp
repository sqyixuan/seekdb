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

#include "palf_env_lite_mgr.h"
#include <regex>
#include "logservice/ob_log_service.h"

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace arbserver;
namespace palflite
{

int DummyBlockPool::create_block_at(const palf::FileDesc &dir_fd,
                                    const char *block_path,
                                    const int64_t block_size)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  do {
    if (-1 == (fd = ::openat(dir_fd, block_path, palf::LOG_WRITE_FLAG | O_CREAT,
            palf::FILE_OPEN_MODE))) {
      ret = convert_sys_errno();
      CLOG_LOG(ERROR, "open block failed", K(dir_fd), K(block_path));
    } else if (-1 == ::fallocate(fd, 0, 0, block_size)) {
      ret = convert_sys_errno();
      CLOG_LOG(ERROR, "fallocate failed", K(dir_fd), K(block_path));
    } else {
      ret = OB_SUCCESS;
    }
    if (OB_FAIL(ret)) {
      PALF_LOG(ERROR, "create block failed", K(errno), KR(ret), K(block_path));
      ob_usleep(100*1000);
    }
    if (-1 != fd) {
      ::close(fd);
    }
  } while (OB_FAIL(ret));
  return ret;
}

int DummyBlockPool::remove_block_at(const palf::FileDesc &dir_fd,
                                    const char *block_path)
{
  int ret = OB_SUCCESS;
  do {
    if (-1 == ::unlinkat(dir_fd, block_path, 0)) {
      ret = convert_sys_errno();
      if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        CLOG_LOG(WARN, "remove block failed, block not exist", K(errno), KR(ret), K(dir_fd), K(block_path));
        ret = OB_SUCCESS;
      } else {
        CLOG_LOG(ERROR, "remove block failed", K(errno), KR(ret), K(dir_fd), K(block_path));
        ob_usleep(100*1000);
      }
    }
  } while(OB_FAIL(ret));
  return ret;
}

ClusterMetaInfo::ClusterMetaInfo()
  : epoch_(),
    tenant_count_(0)
{
  MEMSET(cluster_name_, '\0', OB_MAX_CLUSTER_NAME_LENGTH + 1);
}

ClusterMetaInfo::ClusterMetaInfo(const ClusterMetaInfo &meta)
{
  epoch_ = meta.epoch_;
  tenant_count_ = meta.tenant_count_;
  strncpy(cluster_name_, meta.cluster_name_, OB_MAX_CLUSTER_NAME_LENGTH);
}

int ClusterMetaInfo::assign(const ClusterMetaInfo &meta)
{
  epoch_ = meta.epoch_;
  tenant_count_ = meta.tenant_count_;
  strncpy(cluster_name_, meta.cluster_name_, OB_MAX_CLUSTER_NAME_LENGTH);
  return OB_SUCCESS;
}

ClusterMetaInfo::~ClusterMetaInfo()
{
  epoch_.reset();
  tenant_count_ = 0;
  MEMSET(cluster_name_, '\0', OB_MAX_CLUSTER_NAME_LENGTH + 1);
}

void ClusterMetaInfo::generate_by_default()
{
  epoch_.proposal_id_ = 0;
  epoch_.seq_ = 0;
  tenant_count_ = 0;
  MEMSET(cluster_name_, '\0', OB_MAX_CLUSTER_NAME_LENGTH + 1);
}

bool ClusterMetaInfo::is_valid() const
{
  return epoch_.is_valid();
}

PalfEnvLite *PalfEnvImplFactory::alloc()
{
  return OB_NEW(PalfEnvLite, "PalfEnvLite");
}

void PalfEnvImplFactory::free(PalfEnvLite *palf_env_lite)
{
  OB_DELETE(PalfEnvLite, "PalfEnvLite", palf_env_lite);
}

PalfEnvLite *PalfEnvImplAlloc::alloc_value()
{
  return NULL;
}

void PalfEnvImplAlloc::free_value(PalfEnvLite *palf_env_lite)
{
  PalfEnvImplFactory::free(palf_env_lite);
}

PalfEnvImplAlloc::Node *PalfEnvImplAlloc::alloc_node(PalfEnvLite *palf_env_lite)
{
  return op_reclaim_alloc(Node);
}

void PalfEnvImplAlloc::free_node(PalfEnvImplAlloc::Node *node)
{
  op_reclaim_free(node);
  node = NULL;
}

PalfEnvLiteMgr::PalfEnvLiteMgr() 
  : self_(),
    transport_(NULL),
    allocator_(OB_SERVER_TENANT_ID),
    monitor_(),
    io_adapter_(),
    is_inited_(false)
{
}

PalfEnvLiteMgr::~PalfEnvLiteMgr()
{
  destroy();
}

PalfEnvLiteMgr &PalfEnvLiteMgr::get_instance()
{
  static PalfEnvLiteMgr palf_env_mgr;
  return palf_env_mgr;
}

int PalfEnvLiteMgr::init(const char *base_dir, 
                         const ObAddr &self,
                         rpc::frame::ObReqTransport *transport,
                         share::ObLocalDevice *log_local_device,
                         share::ObResourceManager *resource_manager,
                         common::ObIOManager *io_manager)
{
  int ret = OB_SUCCESS;
  int pret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "PalfEnvLiteMgr has inited twice", KR(ret), K(base_dir));
  } else if (OB_ISNULL(base_dir) || !self.is_valid() || OB_ISNULL(transport) 
             || OB_ISNULL(log_local_device) || OB_ISNULL(resource_manager) || OB_ISNULL(io_manager)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argumets", KR(ret), KP(base_dir), K(self), KP(transport), KP(log_local_device), KP(resource_manager), KP(io_manager));
  } else if (0 >= (pret = snprintf(base_dir_, OB_MAX_FILE_NAME_LENGTH, "%s/clog", base_dir))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprinf failed", KR(ret), K(base_dir));
  } else if (OB_FAIL(palf_env_lite_map_.init("ARB_HASH_MAP", OB_SYS_TENANT_ID))) {
    CLOG_LOG(ERROR, "palf_env_lite_map_ init failed", KR(ret));
  } else if(OB_FAIL(cluster_meta_info_map_.create(DEFAULT_CLUSTER_COUNT, "PalfEnvLiteMgr"))) {
    CLOG_LOG(ERROR, "create ObHashMap failed", KR(ret));
  } else if (OB_FAIL(monitor_.init())) {
    CLOG_LOG(ERROR, "monitor_ init failed", KR(ret));
  } else if (OB_FAIL(io_adapter_.init(OB_SERVER_TENANT_ID, log_local_device, resource_manager, io_manager))) {
    CLOG_LOG(ERROR, "io_adapter_ init failed", KR(ret));
  } else if (OB_FAIL(do_load_(base_dir_, self, transport, &allocator_, &log_block_pool_))) {
    CLOG_LOG(WARN, "do_load_ failed", KR(ret), K(base_dir), K(self));
  } else {
    self_ = self;
    transport_ = transport;
    is_inited_ = true;
    CLOG_LOG(INFO, "PalfEnvLiteMgr init success", KPC(this));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void PalfEnvLiteMgr::destroy()
{
  CLOG_LOG_RET(WARN, OB_SUCCESS, "PalfEnvLiteMgr destroy");
  is_inited_ = false;
  // The used memory is smaller than SMALL_OBJ_MAX_SIZE of ObFunction, therefore,
  // no need to alloc memory from heap
  auto func = [] (const PalfEnvKey key, PalfEnvLite* value) -> bool {
    value->destroy();
    return true;
  };
  palf_env_lite_map_.for_each(func);
  palf_env_lite_map_.destroy();
  cluster_meta_info_map_.destroy();
  transport_ = NULL;
  self_.reset();
  io_adapter_.destroy();
  memset(base_dir_, '\0', OB_MAX_FILE_NAME_LENGTH);
}

int PalfEnvLiteMgr::check_and_prepare_dir(const char *dir)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  char tmp_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  if (0 > (pret = snprintf(tmp_dir, MAX_PATH_SIZE, "%s%s", dir, TMP_SUFFIX))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprintf failed", KR(ret), K(dir), K(tmp_dir));
  } else {
    if (OB_FAIL(common::FileDirectoryUtils::create_directory(tmp_dir))) {
      CLOG_LOG(WARN, "create_directory failed", KR(ret), K(dir));
    } else if (OB_FAIL(rename_with_retry(tmp_dir, dir))) {
      CLOG_LOG(ERROR, "rename tmp_dir to dir failed", KR(ret), K(dir), K(tmp_dir));
    } else if (OB_FAIL(common::FileDirectoryUtils::fsync_dir(base_dir_))) {
      CLOG_LOG(WARN, "fsync_dir failed", KR(ret), K(dir), KPC(this), K(errno));
    } else {
      CLOG_LOG(INFO, "check_and_prepare_dir success", KR(ret), K(dir), KPC(this));
    }
    if (OB_FAIL(ret)) {
      remove_dir_while_exist(dir);
      remove_dir_while_exist(tmp_dir);
    }
  }
  return ret;
}

int PalfEnvLiteMgr::remove_dir(const char *dir)
{
  return remove_dir_(dir, false);
}

int PalfEnvLiteMgr::remove_dir_while_exist(const char *dir)
{
  return remove_dir_(dir, true);
}

bool PalfEnvLiteMgr::is_cluster_placeholder_exists(const int64_t cluster_id) const
{
  int ret = OB_SUCCESS;
  int pret = 0;
  char dir_prefix[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  DIR *base_dir = NULL;
  struct dirent *entry = NULL;
  bool bool_ret = false;
  if (0 >= (pret = snprintf(dir_prefix, OB_MAX_FILE_NAME_LENGTH,
      "cluster_%ld_clustername_", cluster_id))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "snprintf failed", KR(ret), KPC(this), K(dir_prefix), K(cluster_id));
  } else if (NULL == (base_dir = ::opendir(base_dir_))) {
    ret = OB_ERR_SYS;
    CLOG_LOG(WARN, "opendir failed", KR(ret), KPC(this));
  } else {
    while ((entry = readdir(base_dir)) != NULL && OB_SUCC(ret)) {
      if (prefix_match(dir_prefix, entry->d_name)) {
        bool_ret = true;
        CLOG_LOG(INFO, "cluster placeholder exists", KR(ret), KPC(this), K(cluster_id), K(dir_prefix), K(entry->d_name));
        break;
      } else {
        CLOG_LOG(TRACE, "prefix do not match", KR(ret), KPC(this), K(cluster_id), K(dir_prefix), K(entry->d_name));
      }
    }
  }
  if (NULL != base_dir) {
    closedir(base_dir);
  }
  return bool_ret;
}

int PalfEnvLiteMgr::create_palf_env_lite(const PalfEnvKey &palf_env_key)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  ClusterMetaInfo cluster_meta_info;
  char log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "PalfEnvLiteMgr not init", KR(ret), KPC(this), K(palf_env_key));
  } else if (!palf_env_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", KR(ret), KPC(this), K(palf_env_key));
  } else if (OB_FAIL(palf_env_lite_map_.contains_key(palf_env_key)) 
             && OB_ENTRY_NOT_EXIST != ret
             && OB_ENTRY_EXIST != ret) {
    CLOG_LOG(WARN, "contains key failed", KR(ret), KPC(this), K(palf_env_key));
  } else if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
    CLOG_LOG(INFO, "directory has existed", KR(ret), KPC(this), K(palf_env_key));
  } else if (0 >= (pret = snprintf(log_dir, OB_MAX_FILE_NAME_LENGTH, "%s/cluster_%ld_tenant_%ld",
                  base_dir_, palf_env_key.cluster_id_, palf_env_key.tenant_id_))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprintf failed", KR(ret), KPC(this), K(palf_env_key));
  } else if (OB_FAIL(check_and_prepare_dir(log_dir))) {
    CLOG_LOG(WARN, "FileDirectoryUtils create_directory failed", KR(ret), K(log_dir));
    // NB: if create_palf_env_lite_guarded_by_lock_ failed:
    // 1) if enable arbitration retry, the log directory will be used correctly.
    // 2) if not enable arbitration, the log directory will be leaked, need delete it by human.
  } else if (OB_FAIL(create_palf_env_lite_not_guarded_by_lock_(
                     palf_env_key, log_dir, self_,  transport_, &allocator_, &log_block_pool_))) {
    CLOG_LOG(WARN, "create_palf_env_lite_guarded_by_lock_ failed", KR(ret), KPC(this));
    // remove dir if palf_env_lite has not insert into map successfully, otherwise delete tenant in gc.
    if (OB_ENTRY_NOT_EXIST == palf_env_lite_map_.contains_key(palf_env_key)) {
      remove_dir(log_dir);
    }
  } else {
    CLOG_LOG(INFO, "create_palf_env_lite success", KR(ret), KPC(this), K(palf_env_key));
    monitor_.record_create_or_delete_event(palf_env_key.cluster_id_, palf_env_key.tenant_id_,
        ObLSID::INVALID_LS_ID, true /*is_create*/, log_dir);
  }
  return ret;
}
// TODO by runlin: temporarily remove directly from map, delete directory
int PalfEnvLiteMgr::remove_palf_env_lite(const PalfEnvKey &palf_env_key)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "PalfEnvLiteMgr not inited", KR(ret), K(palf_env_key));
  } else if (OB_FAIL(remove_palf_env_lite_not_guarded_by_lock_(palf_env_key))) {
    CLOG_LOG(WARN, "remove_palf_env_lite_guarded_by_lock_ failed", KR(ret), K(palf_env_key));
  } else {
    CLOG_LOG(WARN, "remove_palf_env_lite success", KR(ret), KPC(this), K(palf_env_key));
  }
  return ret;
}

int PalfEnvLiteMgr::get_palf_env_lite(const PalfEnvKey &palf_env_key, 
                                      PalfEnvLite *&palf_env_lite)
{
  int ret = OB_SUCCESS;
  palf_env_lite = NULL;
  if (!palf_env_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(palf_env_lite_map_.get(palf_env_key, palf_env_lite))) {
    CLOG_LOG(WARN, "get from map failed", KR(ret), KPC(this), K(palf_env_key));
  } else if (true == palf_env_lite->has_set_deleted()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
  }
  if (OB_FAIL(ret) && NULL != palf_env_lite) {
    palf_env_lite_map_.revert(palf_env_lite);
    palf_env_lite = NULL;
  }
  return ret;
}

void PalfEnvLiteMgr::revert_palf_env_lite(PalfEnvLite *palf_env_lite)
{
  if (NULL != palf_env_lite) {
    palf_env_lite_map_.revert(palf_env_lite);
  }
}

int PalfEnvLiteMgr::do_load_(const char *base_dir,
                             const common::ObAddr &self,
                             rpc::frame::ObReqTransport *transport,
                             common::ObILogAllocator *alloc_mgr,
                             palf::ILogBlockPool *log_block_pool)
{
  int ret = OB_SUCCESS;
  DIR *dir = NULL;
  struct dirent *entry = NULL;
  std::regex pattern_log("cluster_[1-9]\\d*_tenant_[1-9]\\d*");
  std::regex pattern_placeholder("cluster_[1-9]\\d*_clustername_.*");
  if (OB_FAIL(FileDirectoryUtils::delete_tmp_file_or_directory_at(base_dir))) {
    CLOG_LOG(WARN, "delete_tmp_file_or_directory_at failed", KR(ret), K(base_dir));
  } else if (NULL == (dir = opendir(base_dir))) {
    ret = OB_ERR_SYS;
    CLOG_LOG(WARN, "opendir failed", KR(ret), KPC(this));
  } else {
    char current_file_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    while ((entry = readdir(dir)) != NULL && OB_SUCC(ret)) {
      bool is_dir = false;
      MEMSET(current_file_path, '\0', OB_MAX_FILE_NAME_LENGTH);
      if (0 == strcmp(entry->d_name, ".") || 0 == strcmp(entry->d_name, "..")) {
        // do nothing
      } else if (0 >= snprintf(current_file_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s",
                               base_dir, entry->d_name)) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "snprintf failed", KR(ret), KPC(this), K(current_file_path),
                K(entry->d_name));
      } else if (OB_FAIL(FileDirectoryUtils::is_directory(current_file_path, is_dir))) {
        CLOG_LOG(WARN, "is_directory failed", KR(ret), K(entry->d_name));
      } else if (false == is_dir) {
        CLOG_LOG(WARN, "unexpeced file", K(current_file_path), KPC(this));
      } else if (true == std::regex_match(entry->d_name, pattern_placeholder)) {
        ret = load_cluster_placeholder_(entry->d_name, base_dir);
      } else if (true == std::regex_match(entry->d_name, pattern_log)) {
        ret = create_palf_env_lite_(entry->d_name, base_dir, self, transport, alloc_mgr, log_block_pool);
      } else {
      }
    }
    // regenerate cluster placeholder dir for upgrading scenarios (e.g., v4.1 -> v4.2)
    auto generate_placeholder_dir = [this, base_dir](MapPair &pair) -> int {
      int ret = OB_SUCCESS;
      const int64_t cluster_id = pair.first;
      const common::ObString cluster_name("");
      if (OB_FAIL(this->generate_cluster_placeholder_(cluster_id, cluster_name, base_dir))) {
        CLOG_LOG(WARN, "generate_cluster_placeholder_ failed", KR(ret), K(cluster_id), K(cluster_name));
      } else {
        CLOG_LOG(INFO, "generate_cluster_placeholder_ success", KR(ret), K(cluster_id), K(cluster_name));
      }
      return OB_SUCCESS;
    };
    if (FAILEDx(iterate_cluster_meta_info_(generate_placeholder_dir))) {
      CLOG_LOG(WARN, "generate_placeholder_dir failed", KR(ret), KPC(this));
    }
  }
  if (NULL != dir) {
    closedir(dir);
  }
  return ret;
}

int PalfEnvLiteMgr::load_cluster_placeholder_(const char *cluster_placeholder_str,
                                              const char *base_dir)
{
  int ret = OB_SUCCESS;
  int pret = -1;
  char *saveptr = NULL;
  char file_name_for_id[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  MEMCPY(file_name_for_id, cluster_placeholder_str, strlen(cluster_placeholder_str));
  char *cluster_str = strtok_r(file_name_for_id, "_", &saveptr);
  char *cluster_id_str= NULL;
  char *clustern_str= NULL;
  char *cluster_name_str = NULL;
  if (NULL == cluster_str ||
      NULL == (cluster_id_str = strtok_r(NULL, "_", &saveptr)) || 
      NULL == (clustern_str = strtok_r(NULL, "_", &saveptr)) ||
      false == ObString(cluster_id_str).is_numeric()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected format", KR(ret), KP(cluster_str), KP(cluster_id_str),
        K(clustern_str), K(file_name_for_id));
  } else {
    cluster_name_str = strtok_r(NULL, "\0", &saveptr);
    const int64_t cluster_id = atol(cluster_id_str);
    const common::ObString cluster_name = OB_ISNULL(cluster_name_str)? \
        ObString::make_string(""): ObString::make_string(cluster_name_str);
    auto update_cluster_name = [&cluster_name](MapPair &pair) -> void {
      auto &cluster_meta_info = pair.second;
      strncpy(cluster_meta_info.cluster_name_, cluster_name.ptr(), OB_MAX_CLUSTER_NAME_LENGTH);
    };
    if (OB_FAIL(try_create_cluster_meta_info_(cluster_id))) {
      CLOG_LOG(WARN, "try_create_cluster_meta_info_failed", KR(ret), KPC(this), K(cluster_id), K(cluster_name));
    } else if (OB_FAIL(update_cluster_meta_info_(cluster_id, update_cluster_name))) {
      CLOG_LOG(WARN, "update_cluster_meta_info_ failed", KR(ret), KPC(this), K(cluster_id), K(cluster_name));
    } else {
      CLOG_LOG(INFO, "load_cluster_placeholder_ finish", KR(ret), KPC(this), K(cluster_placeholder_str), K(base_dir),
        K(cluster_id_str), K(cluster_name_str), K(cluster_id), K(cluster_name));
    }
  }
  return ret;
}

int PalfEnvLiteMgr::generate_cluster_placeholder_(const int64_t cluster_id,
                                                  const common::ObString &cluster_name,
                                                  const char *base_dir = NULL)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  char log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  const char *actual_base_dir = (OB_ISNULL(base_dir))? base_dir_: base_dir;
  if (false == is_cluster_placeholder_exists(cluster_id)) {
    if (0 >= (pret = snprintf(log_dir, OB_MAX_FILE_NAME_LENGTH, "%s/cluster_%ld_clustername_%s",
                    actual_base_dir, cluster_id, cluster_name.ptr()))) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "snprintf failed", KR(ret), KPC(this), K(cluster_id));
    } else if (OB_FAIL(check_and_prepare_dir(log_dir))) {
      CLOG_LOG(WARN, "FileDirectoryUtils create_directory failed", KR(ret), K(log_dir));
    } else {
      CLOG_LOG(INFO, "generate_cluster_placeholder_ success", KR(ret), K(cluster_id), K(cluster_name), K(log_dir));
    }
  }
  return ret;
}

int PalfEnvLiteMgr::create_palf_env_lite_(const char *palf_env_lite_id_str,
                                          const char *base_dir,
                                          const common::ObAddr &self,
                                          rpc::frame::ObReqTransport *transport,
                                          common::ObILogAllocator *alloc_mgr,
                                          palf::ILogBlockPool *log_block_pool)
{
  int ret = OB_SUCCESS;
  int pret = -1;
  char *saveptr = NULL;
  char palf_env_log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char file_name[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  MEMCPY(file_name, palf_env_lite_id_str, strlen(palf_env_lite_id_str));
  char *cluster_str = strtok_r(file_name, "_", &saveptr);
  char *cluster_id_str= NULL;
  char *tenant_str = NULL;
  char *tenant_id_str = NULL;
  if (0 > (pret = snprintf(palf_env_log_dir, MAX_PATH_SIZE, "%s/%s", base_dir, palf_env_lite_id_str))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprinf failed", KR(ret), K(base_dir), K(palf_env_log_dir));
  } else if (NULL == cluster_str 
      || NULL == (cluster_id_str = strtok_r(NULL, "_", &saveptr))
      || false == ObString(cluster_id_str).is_numeric()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected format", KR(ret), KP(cluster_str), KP(cluster_id_str), K(file_name));
  } else if (NULL == (tenant_str = strtok_r(NULL, "_", &saveptr)) 
      || NULL == (tenant_id_str = strtok_r(NULL, "_", &saveptr))
      || false == ObString(tenant_id_str).is_numeric()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "unexpected format", KR(ret), KP(tenant_str), KP(tenant_id_str), K(file_name));
  } else {
    int64_t cluster_id = atol(cluster_id_str);
    int64_t tenant_id = atol(tenant_id_str);
    PalfEnvKey palf_env_key(cluster_id, tenant_id);
    if (OB_FAIL(create_palf_env_lite_not_guarded_by_lock_(palf_env_key, 
            palf_env_log_dir, self, transport,  alloc_mgr, log_block_pool))) {
      CLOG_LOG(WARN, "create_palf_env_lite_ failed", KR(ret), KPC(this));
    } 
  }
  return ret;
}

int PalfEnvLiteMgr::create_palf_env_lite_not_guarded_by_lock_(const PalfEnvKey &palf_env_key,
                                                              const char *palf_env_log_dir,
                                                              const common::ObAddr &self,
                                                              rpc::frame::ObReqTransport *transport,
                                                              common::ObILogAllocator *alloc_mgr,
                                                              palf::ILogBlockPool *log_block_pool)
{
  int ret = OB_SUCCESS;
  auto inc_tenant_count = [](MapPair &pair) -> void {
    auto &info = pair.second;
    info.tenant_count_++;
  };
  PalfDiskOptions dummy;
  dummy.log_disk_usage_limit_size_ = 2 * 1024 * 1024 * 1024ul;
  dummy.log_disk_utilization_threshold_ = 80;
  dummy.log_disk_utilization_limit_threshold_= 95;
  dummy.log_disk_throttling_percentage_= 100;
  dummy.log_disk_throttling_maximum_duration_= 2 * 60 * 60 * 1000 * 1000L;//2h
  PalfEnvLite *palf_env_lite= NULL;
  // The purpose of locking is to avoid handling the issue of concurrent creation discovering that the directory already exists, otherwise, we need to handle the directory existence, but due to concurrent creation
  // Failure leading to the deletion of PalfEnvLite.
  if (NULL == (palf_env_lite = PalfEnvImplFactory::alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc palf_env_lite failed", KR(ret), KPC(this), K(palf_env_key));
  } else if (OB_FAIL(palf_env_lite->init(palf_env_log_dir, self, palf_env_key.cluster_id_,
          palf_env_key.tenant_id_, transport, alloc_mgr, log_block_pool, this))) {
    CLOG_LOG(WARN, "init palf_env_lite failed", KR(ret), KPC(this), K(palf_env_key));
  } else if (OB_FAIL(palf_env_lite->start())) {
    CLOG_LOG(WARN, "start palf_env_lite failed", KR(ret), KPC(this), K(palf_env_key));
  // Note: we reserve the logic for creating cluster meta info for following scenarios:
  // 1. In-place upgrade. If we restart a new v4.2 arb server to upgrade arb server,
  //    clusters created by v4.1 do not have a placeholder, therefore, we generate meta info
  //    and placeholder for these clusters
  // 2. Replacement upgrade. If we start another v4.2 arb server and replace arbitration service
  //    of OceanBase clusters to upgrade arb server, arb server should not reject creating tenants
  //    requests even if the cluster placeholder does not exist, and the v4.2 arb server should generate
  //    meta info and placeholder for new created tenants.
  // Safety: the atomicity of add_cluster protects the arb server from being used by two clusters
  //    with the same cluster_id
  } else if (OB_FAIL(try_create_cluster_meta_info_(palf_env_key.cluster_id_))) {
    CLOG_LOG(WARN, "try_create_cluster_meta_info_failed", KR(ret), KPC(this));
  } else if (OB_FAIL(update_cluster_meta_info_(palf_env_key.cluster_id_, inc_tenant_count))) {
    CLOG_LOG(WARN, "update_cluster_meta_info_ failed", KR(ret), KPC(this), K(palf_env_key));
  // it is an optimization. If we upgrade arb server with a replacement way, arb server(v4.2) should
  // generate a cluster placeholder for new created tenants
  } else if (OB_FAIL(generate_cluster_placeholder_(palf_env_key.cluster_id_, common::ObString("")))) {
    (void) try_remove_cluster_meta_info_(palf_env_key.cluster_id_);
    CLOG_LOG(WARN, "generate_cluster_placeholder_ failed", KR(ret), KPC(this), K(palf_env_key));
  } else if (OB_FAIL(palf_env_lite_map_.insert_and_get(palf_env_key, palf_env_lite))) {
    CLOG_LOG(WARN, "insert_and_get failed", KR(ret), KPC(this), K(palf_env_key));
  } else {
    palf_env_lite_map_.revert(palf_env_lite);
  }
  if (OB_FAIL(ret) && NULL != palf_env_lite) {
    PalfEnvImplFactory::free(palf_env_lite);
  } 
  return ret;
}

int PalfEnvLiteMgr::remove_palf_env_lite_not_guarded_by_lock_(const PalfEnvKey &palf_env_key)
{
  int ret = OB_SUCCESS;
  // The used memory is smaller than SMALL_OBJ_MAX_SIZE of ObFunction, therefore,
  // no need to alloc memory from heap
  auto set_delete_func = [](const PalfEnvKey &key, PalfEnvLite *value) {
    UNUSED(key);
    value->set_deleted();
  };
  int64_t tenant_count = 0;
  auto dec_tenant_count = [&tenant_count](MapPair &pair) -> void {
    auto &info = pair.second;
    info.tenant_count_--;
    tenant_count = info.tenant_count_;
  };
  if (OB_FAIL(palf_env_lite_map_.operate(palf_env_key, set_delete_func))) {
    CLOG_LOG(WARN, "operate palf_handle_impl_map_ failed", KR(ret), K(palf_env_key), KPC(this));
  } else if (OB_FAIL(palf_env_lite_map_.del(palf_env_key))) {
    CLOG_LOG(WARN, "palf_handle_impl_map_ del failed", KR(ret), K(palf_env_key));
  } else if (OB_FAIL(wait_until_reference_count_to_zero_(palf_env_key))) {
    CLOG_LOG(WARN, "wait_until_reference_count_to_zero_ failed", KR(ret), K(palf_env_key));
  } else if (OB_FAIL(update_cluster_meta_info_(palf_env_key.cluster_id_, dec_tenant_count))) {
    CLOG_LOG(WARN, "update_cluster_meta_info_ failed", KR(ret), KPC(this), K(palf_env_key));
  } else {
    CLOG_LOG(INFO, "remove_palf_env_lite_guarded_by_lock_ success", KR(ret), K(palf_env_key));
    PALF_REPORT_INFO_KV(K(tenant_count));
    monitor_.record_create_or_delete_event(palf_env_key.cluster_id_, palf_env_key.tenant_id_,
        ObLSID::INVALID_LS_ID, false /*is_create*/, EXTRA_INFOS);
  }
  return ret;
}

int PalfEnvLiteMgr::wait_until_reference_count_to_zero_(const PalfEnvKey &palf_env_key)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  char base_dir[MAX_PATH_SIZE] = {'\0'};
  char tmp_base_dir[MAX_PATH_SIZE] = {'\0'};
  if (false == palf_env_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid arguments", KR(ret), K(palf_env_key));
  } else if (0 > (pret = snprintf(base_dir, MAX_PATH_SIZE, "%s/cluster_%ld_tenant_%ld", 
          base_dir_, palf_env_key.cluster_id_, palf_env_key.tenant_id_))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprinf failed", KR(ret), K(pret), K(palf_env_key));
  } else if (0 > (pret = snprintf(tmp_base_dir, MAX_PATH_SIZE, "%s/cluster_%ld_tenant_%ld%s", 
          base_dir_, palf_env_key.cluster_id_, palf_env_key.tenant_id_, TMP_SUFFIX))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprinf failed", KR(ret), K(pret), K(palf_env_key));
  } else {
    bool normal_dir_exist = true;
    bool tmp_dir_exist = true;
    while (OB_SUCC(FileDirectoryUtils::is_exists(base_dir, normal_dir_exist))
           && OB_SUCC(FileDirectoryUtils::is_exists(tmp_base_dir, tmp_dir_exist))) {
      if (!normal_dir_exist && !tmp_dir_exist) {
        break;
      }
      PALF_LOG(INFO, "wait_until_reference_count_to_zero_ failed, may be reference count has leaked", K(palf_env_key),
          K(normal_dir_exist), K(tmp_dir_exist), K(base_dir), K(tmp_base_dir));
      ob_usleep(1000);
    }
  }
  return ret;
}

int PalfEnvLiteMgr::add_cluster(const common::ObAddr &src_server,
                                const int64_t cluster_id,
                                const common::ObString &cluster_name,
                                const arbserver::GCMsgEpoch &epoch)
{
  #define FUNC_ARGS KPC(this), K(src_server), K(cluster_id), K(cluster_name), K(epoch)
  int ret = OB_SUCCESS;
  int pret = 0;
  int contains_meta = OB_SUCCESS;
  bool contains_tenant = false;
  char log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  ClusterMetaInfo cluster_meta_info;
  auto update_epoch = [&epoch](MapPair &pair) -> void {
    auto &cluster_meta_info = pair.second;
    cluster_meta_info.epoch_ = epoch;
  };
  auto update_cluster_name = [&cluster_name](MapPair &pair) -> void {
    auto &cluster_meta_info = pair.second;
    strncpy(cluster_meta_info.cluster_name_, cluster_name.ptr(), OB_MAX_CLUSTER_NAME_LENGTH);
  };
  ObSpinLockGuard guard(lock_);
  // Note: for reentrancy, we accept add_cluster req only when:
  // 1. empty meta_map and palf_env_map
  // 2. empty palf_env_map and matched (cluster_name,epoch)
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (!src_server.is_valid() || OB_INVALID_CLUSTER_ID == cluster_id || !epoch.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", FUNC_ARGS);
  } else if (FALSE_IT(contains_tenant = contains_any_tenant_(cluster_id))) {
  } else if (FALSE_IT(contains_meta = get_cluster_meta_info_(cluster_id, cluster_meta_info))) {
  } else if (contains_tenant) {
    ret = OB_ARBITRATION_SERVICE_ALREADY_EXIST;
    CLOG_LOG(WARN, "another cluster already exists, can not add_cluster", FUNC_ARGS, K(contains_meta), K(cluster_meta_info));
  } else if (OB_SUCC(contains_meta)) {
    if (0 == strcmp(cluster_meta_info.cluster_name_, cluster_name.ptr()) && !(epoch < cluster_meta_info.epoch_)) {
      CLOG_LOG(INFO, "cluster has already been added", FUNC_ARGS, K(cluster_meta_info));
    } else {
      ret = OB_ARBITRATION_SERVICE_ALREADY_EXIST;
      CLOG_LOG(INFO, "another cluster already exists, can not add_cluster", FUNC_ARGS, K(cluster_meta_info));
    }
  } else if (true == is_cluster_placeholder_exists(cluster_id)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "cluster placeholder exists, but do not contain meta", FUNC_ARGS, K(contains_meta));
  } else if (OB_FAIL(generate_cluster_placeholder_(cluster_id, cluster_name))) {
    CLOG_LOG(WARN, "generate_cluster_placeholder_ failed", KR(ret), K(cluster_id), K(cluster_name));
  } else if (OB_FAIL(try_create_cluster_meta_info_(cluster_id))) {
    CLOG_LOG(WARN, "try_create_cluster_meta_info_failed", FUNC_ARGS);
  } else if (OB_FAIL(update_cluster_meta_info_(cluster_id, update_cluster_name))) {
    CLOG_LOG(WARN, "update_cluster_meta_info_ failed", FUNC_ARGS);
  } else if (OB_FAIL(update_cluster_meta_info_(cluster_id, update_epoch))) {
    CLOG_LOG(WARN, "update_cluster_meta_info_ failed", FUNC_ARGS);
  }
  CLOG_LOG(INFO, "add_cluster finished", FUNC_ARGS);
  #undef FUNC_ARGS
  return ret;
}

int PalfEnvLiteMgr::remove_cluster(const common::ObAddr &src_server,
                                   const int64_t cluster_id,
                                   const common::ObString &cluster_name,
                                   const arbserver::GCMsgEpoch &epoch)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(remove_cluster_(src_server, cluster_id, cluster_name, epoch))) {
    CLOG_LOG(WARN, "remove_cluster_ failed", KPC(this), K(src_server), K(cluster_id), K(cluster_name), K(epoch));
  } else {
    CLOG_LOG(INFO, "remove_cluster success", KPC(this), K(src_server), K(cluster_id), K(cluster_name), K(epoch));
  }
  return ret;
}

int PalfEnvLiteMgr::remove_cluster_(const common::ObAddr &src_server,
                                    const int64_t cluster_id,
                                    const common::ObString &cluster_name,
                                    const arbserver::GCMsgEpoch &epoch)
{
  #define FUNC_ARGS KPC(this), K(src_server), K(cluster_id), K(cluster_name), K(epoch)
  int ret = OB_SUCCESS;
  int pret = 0;
  int contains_meta = OB_SUCCESS;
  TenantLSIDSArray ls_ids;
  ClusterMetaInfo cluster_meta_info;
  char log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  ls_ids.set_max_tenant_id(UINT64_MAX);
  // Note: we compare epoch, but do not compare the cluster_name arg with the cluster_name
  //       recorded in cluster_meta_info_map_
  // 1. arb server may be upgraded from v4.1 to v4.2, therefore the cluster_name recorded in
  //    cluster_meta_info_map_ will be empty
  // 2. the cluster_name of a cluster may be changed during running, if the cluster_name has
  //    been changed, users should clear dir in the arb server manually.
  if (!src_server.is_valid() || OB_INVALID_CLUSTER_ID == cluster_id || !epoch.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", FUNC_ARGS);
  } else if (FALSE_IT(contains_meta = get_cluster_meta_info_(cluster_id, cluster_meta_info))) {
  } else if (OB_SUCCESS != contains_meta) {
    CLOG_LOG(WARN, "cluster do not exist, skip remove_cluster", K(contains_meta), FUNC_ARGS);
  } else if (OB_FAIL(handle_gc_message_(epoch, src_server, cluster_id, ls_ids))) {
    CLOG_LOG(WARN, "handle_gc_message_ failed", K(ls_ids), FUNC_ARGS);
  } else if (0 >= (pret = snprintf(log_dir, OB_MAX_FILE_NAME_LENGTH, "%s/cluster_%ld_clustername_%s",
      base_dir_, cluster_id, cluster_meta_info.cluster_name_))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "snprintf failed", K(cluster_meta_info), FUNC_ARGS);
  } else if (OB_FAIL(try_remove_cluster_meta_info_(cluster_id))) {
    CLOG_LOG(WARN, "try_remove_cluster_meta_info_ failed", KPC(this), K(cluster_id));
    // Note: the remove_cluster_ function should be reentrant, so we check if the cluster placeholder dir exists.
  } else if (is_cluster_placeholder_exists(cluster_id) && OB_FAIL(remove_dir(log_dir))) {
    CLOG_LOG(WARN, "remove_dir failed", K(log_dir), K(cluster_meta_info), FUNC_ARGS);
  } else {
    CLOG_LOG(INFO, "remove_cluster_ success", FUNC_ARGS, K(cluster_meta_info));
  }
  #undef FUNC_ARGS
  return ret;
}

int PalfEnvLiteMgr::create_arbitration_instance(const PalfEnvKey &palf_env_key,
                                                const ObAddr &src_server,
                                                const int64_t id,
                                                const share::ObTenantRole &tenant_role)
{
  int ret = OB_SUCCESS;
  PalfEnvLite *palf_env_lite = NULL;
  // NB: create_palf_env_lite has guarded by lock
  if (OB_FAIL(create_palf_env_lite(palf_env_key))) {
    CLOG_LOG(WARN, "create_palf_env_lite failed", KR(ret), KPC(this), K(palf_env_key), K(src_server),
          K(id), K(tenant_role));
  } else if (OB_FAIL(get_palf_env_lite(palf_env_key, palf_env_lite))) {
    CLOG_LOG(WARN, "get_palf_env_lite failed", KR(ret), KPC(this), K(palf_env_key), K(src_server),
          K(id), K(tenant_role));
  } else {
    PalfBaseInfo palf_base_info;
    palf_base_info.generate_by_default();
    IPalfHandleImpl *palf_handle_impl = NULL;
    palf::AccessMode access_mode = logservice::ObLogService::get_palf_access_mode(tenant_role);
    if (OB_FAIL(palf_env_lite->create_palf_handle_impl(id, access_mode, palf_base_info, palf_handle_impl))
        && OB_ENTRY_EXIST != ret) {
      CLOG_LOG(WARN, "create_palf_handle_impl failed", KR(ret), KPC(this), K(palf_env_key), K(src_server),
          K(id), K(tenant_role));
    } else {
      CLOG_LOG(INFO, "create_arbitration_instance success", KR(ret), KPC(this), K(palf_env_key), K(src_server),
          K(id), K(tenant_role));
      ret = OB_SUCCESS;
      PALF_REPORT_INFO_KV(K(src_server), K(tenant_role));
      monitor_.record_create_or_delete_event(palf_env_key.cluster_id_, palf_env_key.tenant_id_,
          id, true /*is_create*/, EXTRA_INFOS);
    }
    if (OB_NOT_NULL(palf_handle_impl)) {
      palf_env_lite->revert_palf_handle_impl(palf_handle_impl);
      palf_handle_impl = NULL;
    }
  }
  if (OB_NOT_NULL(palf_env_lite)) {
    revert_palf_env_lite(palf_env_lite);
    palf_env_lite = NULL;
  }
  return ret;
}

int PalfEnvLiteMgr::delete_arbitration_instance(const PalfEnvKey &palf_env_key,
                                                const ObAddr &src_server,
                                                const int64_t &id)
{
  int ret = OB_SUCCESS;
  PalfEnvLite *palf_env_lite = NULL;
  if (OB_FAIL(get_palf_env_lite(palf_env_key, palf_env_lite))
      && OB_ENTRY_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "get_palf_env_lite failed", KR(ret), KPC(this), K(palf_env_key), 
        K(src_server), K(id));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  } else if (OB_FAIL(palf_env_lite->remove_palf_handle_impl(id))) {
    CLOG_LOG(WARN, "remove_palf_handle_impl failed", KR(ret), KPC(this), K(palf_env_key), 
        K(src_server), K(id));
  } else {
    CLOG_LOG(INFO, "delete_arbitration_instance success", KR(ret), KPC(this), K(palf_env_key),
        K(src_server), K(id));
    PALF_REPORT_INFO_KV(K(src_server));
    monitor_.record_create_or_delete_event(palf_env_key.cluster_id_, palf_env_key.tenant_id_,
        id, false /*is_create*/, EXTRA_INFOS);
  }
  if (NULL != palf_env_lite) {
    revert_palf_env_lite(palf_env_lite);
    palf_env_lite = NULL;
  }
  return ret;
}

int PalfEnvLiteMgr::set_initial_member_list(const PalfEnvKey &palf_env_key,
                                            const ObAddr &src_server,
                                            const int64_t id,
                          	                const common::ObMemberList &member_list,
                                            const common::ObMember &arb_member,
                                            const int64_t paxos_replica_num,
                                            const common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  PalfEnvLite *palf_env_lite = NULL;
  IPalfHandleImplGuard guard;
  if (OB_FAIL(get_palf_env_lite(palf_env_key, palf_env_lite))) {
    CLOG_LOG(WARN, "get_palf_env_lite failed", KR(ret), KPC(this), K(palf_env_key), K(src_server), 
        K(id), K(member_list), K(arb_member), K(paxos_replica_num));
  } else if (OB_FAIL(palf_env_lite->get_palf_handle_impl(id, guard))) {
    CLOG_LOG(WARN, "get_palf_handle_impl failed", KR(ret), KPC(this), K(palf_env_key), K(src_server), 
        K(id), K(member_list), K(arb_member), K(paxos_replica_num));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->set_initial_member_list(
          member_list, arb_member, paxos_replica_num, learner_list))) {
    CLOG_LOG(WARN, "set_initial_member_list failed", KR(ret), KPC(this), K(palf_env_key), K(src_server), 
        K(id), K(member_list), K(arb_member), K(paxos_replica_num), K(learner_list));
  } else {
    CLOG_LOG(INFO, "set_initial_member_list failed", KR(ret), KPC(this), K(palf_env_key), K(src_server), 
        K(id), K(member_list), K(arb_member), K(paxos_replica_num), K(learner_list));
  }
  if (OB_NOT_NULL(palf_env_lite)) {
    revert_palf_env_lite(palf_env_lite);
    palf_env_lite = NULL;
  }
  return ret;
}


int PalfEnvLiteMgr::handle_gc_message(const GCMsgEpoch &epoch,
                                      const ObAddr &src_server,
                                      const int64_t src_cluster_id,
                                      TenantLSIDSArray &ls_ids)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "PalfEnvLiteMgr not init", KR(ret), KPC(this), K(epoch), K(src_server), K(ls_ids));
  } else if (!epoch.is_valid() || !src_server.is_valid() || !ls_ids.is_valid() || -1 == src_cluster_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argumets", KR(ret), KPC(this), K(epoch), K(src_server), K(ls_ids), K(src_cluster_id));
  } else if (OB_FAIL(handle_gc_message_(epoch, src_server, src_cluster_id, ls_ids))) {
    CLOG_LOG(WARN, "handle_gc_message_ failed", KR(ret), KPC(this), K(epoch), K(src_server), K(ls_ids));
  } else {
    CLOG_LOG(INFO, "handle_gc_message success", KR(ret), KPC(this), K(epoch), K(src_server), K(ls_ids));
  }
  return ret;
}

int PalfEnvLiteMgr::handle_gc_message_(const GCMsgEpoch &epoch,
                                       const ObAddr &src_server,
                                       const int64_t src_cluster_id,
                                       TenantLSIDSArray &ls_ids)
{
  int ret = OB_SUCCESS;
  ClusterMetaInfo cluster_meta_info;
  auto update_epoch = [&epoch](MapPair &pair) -> void {
    auto &cluster_meta_info = pair.second;
    cluster_meta_info.epoch_ = epoch;
  };
  if (OB_FAIL(get_cluster_meta_info_(src_cluster_id, cluster_meta_info))) {
    ret = (OB_HASH_NOT_EXIST)? OB_ARBITRATION_SERVICE_NOT_EXIST: ret;
    CLOG_LOG(WARN, "get_cluster_meta_info_ failed", KR(ret), KPC(this), K(epoch), K(src_server), K(src_cluster_id));
  } else if (epoch < cluster_meta_info.epoch_) {
    ret = OB_OP_NOT_ALLOW;
    CLOG_LOG(WARN, "stale message, no need handle it", KR(ret), KPC(this), K(epoch), K(src_server), 
        K(ls_ids), K(src_cluster_id), K(cluster_meta_info));
  } else if (OB_FAIL(handle_gc_message_for_one_cluster_(src_cluster_id, ls_ids))) {
    CLOG_LOG(WARN, "handle_gc_message_for_one_cluster_ failed", KR(ret), KPC(this), K(epoch), K(src_server), K(ls_ids), K(src_cluster_id));
  } else if (OB_FAIL(update_cluster_meta_info_(src_cluster_id, update_epoch))) {
    CLOG_LOG(WARN, "update_cluster_meta_info_ failed", KR(ret), KPC(this), K(epoch), K(src_server), K(src_cluster_id));
  } else {
    CLOG_LOG(INFO, "handle_gc_message_ success", KPC(this), K(epoch), K(src_server), K(ls_ids), K(src_cluster_id), K(cluster_meta_info));
  }
  return ret;
}

int PalfEnvLiteMgr::handle_force_clear_arb_cluster_info_message(const common::ObAddr &src_server,
                                                                const int64_t src_cluster_id)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  ClusterMetaInfo cluster_meta_info;
  cluster_meta_info.generate_by_default();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "PalfEnvLiteMgr not init", KPC(this), K(src_server));
  } else if (false == is_valid_cluster_id(src_cluster_id)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argumets", KR(ret), KPC(this), K(src_server), K(src_cluster_id));
  } else if (OB_FAIL(get_cluster_meta_info_(src_cluster_id, cluster_meta_info)) && OB_HASH_NOT_EXIST != ret) {
    CLOG_LOG(WARN, "get_cluster_meta_info_ failed", KR(ret), KPC(this), K(src_server), K(src_cluster_id));
  } else if (OB_FAIL(remove_cluster_(src_server, src_cluster_id, cluster_meta_info.cluster_name_, cluster_meta_info.epoch_))) {
    CLOG_LOG(WARN, "remove_cluster_ failed", KPC(this), K(src_server), K(src_cluster_id), K(cluster_meta_info));
  } else {
    CLOG_LOG(INFO, "handle_force_clear_arb_cluster_info_message success", KPC(this), K(src_server), K(src_cluster_id));
    PALF_REPORT_INFO_KV(K(src_server), "force_clear_arb", true);
    monitor_.record_create_or_delete_event(src_cluster_id, OB_INVALID_TENANT_ID,
        ObLSID::INVALID_LS_ID, false /*is_create*/, EXTRA_INFOS);
  }
  return ret;
}

int PalfEnvLiteMgr::get_arb_member_info(const PalfEnvKey &palf_env_key,
                                        const common::ObAddr &src_server,
                                        const int64_t id,
                                        palf::ArbMemberInfo &arb_member_info)
{
  int ret = OB_SUCCESS;
  PalfEnvLite *palf_env_lite = NULL;
  IPalfHandleImplGuard guard;
  if (OB_FAIL(get_palf_env_lite(palf_env_key, palf_env_lite))) {
    CLOG_LOG(WARN, "get_palf_env_lite failed", KR(ret), KPC(this), K(palf_env_key), K(src_server), K(id));
  } else if (OB_FAIL(palf_env_lite->get_palf_handle_impl(id, guard))) {
    CLOG_LOG(WARN, "get_palf_handle_impl failed", KR(ret), KPC(this), K(palf_env_key), K(src_server), K(id));
  } else if (OB_FAIL(guard.get_palf_handle_impl()->get_arb_member_info(arb_member_info))) {
    CLOG_LOG(WARN, "get_arb_member_info failed", KR(ret), KPC(this), K(palf_env_key), K(src_server), K(id));
  } else {
    CLOG_LOG(INFO, "get_arb_member_info succ", KR(ret), KPC(this), K(palf_env_key), K(src_server),
        K(id), K(arb_member_info));
  }
  if (OB_NOT_NULL(palf_env_lite)) {
    revert_palf_env_lite(palf_env_lite);
    palf_env_lite = NULL;
  }
  return ret;
}

int PalfEnvLiteMgr::handle_gc_message_for_one_cluster_(const int64_t src_cluster_id,
                                                       arbserver::TenantLSIDSArray &ls_ids)
{
  // Tow steps:
  // step1. iterate each tenant in palf_env_lite_map, no need handle any tenant which
  //        tenant id greater than max tenant id;
  // step2. iterate each ls in palf_handle_impl_map, no need handle any ls which ls
  //        is greater than max ls id;
  int ret = OB_SUCCESS;
  const uint64_t max_tenant_id = ls_ids.get_max_tenant_id();
  PalfEnvKey max_palf_env_key(src_cluster_id, max_tenant_id);
  common::ObSEArray<PalfEnvKey, DEFAULT_CLUSTER_COUNT> deleted_tenants;
  // The used memory is smaller than SMALL_OBJ_MAX_SIZE of ObFunction, therefore,
  // no need to alloc memory from heap
  // define operate functor for ObLinkHashMap
  auto operate_map_func = [&max_palf_env_key, &ls_ids, &deleted_tenants, this](
      const PalfEnvKey &palf_env_key,
      PalfEnvLite *palf_env_lite) -> bool
  {
    int ret = OB_SUCCESS;
    // only need handle same cluster
    if (max_palf_env_key.cluster_id_ != palf_env_key.cluster_id_) {
      return true;
    } 
    // No need to handle the tenant which tenant id greater than max tenant_ id
    else if (max_palf_env_key < palf_env_key) {
      CLOG_LOG(INFO, "max_palf_env_key of remote is smaller than local, no need handle it",
          KR(ret), K(max_palf_env_key), K(palf_env_key));
    // When tenant not exist in ls_ids, need remove it from PalfEnvLiteMgr;
    } else if (!ls_ids.exist(palf_env_key.tenant_id_)) {
      // remove tenant from map
      // NB: we can not use 'remove_palf_env_lite', because the ref count of palf_env_lite still greater than zero.
      // We record deleted tenants and delete them later, because we need ensure their dirs are deleted.
      if (OB_FAIL(deleted_tenants.push_back(palf_env_key))) {
        PALF_LOG(WARN, "push_back failed", KR(ret), KPC(this), K(palf_env_key), K(deleted_tenants));
        palf_env_lite->set_deleted();
        ret = palf_env_lite_map_.del(palf_env_key);
        PALF_LOG(INFO, "drop tenant finished", KR(ret), KPC(this), K(palf_env_key), K(max_palf_env_key), K(ls_ids));
      }
    } else {
      // define operate functor for TenantLSIDArray
      arbserver::TenantLSIDSArray::Functor functor = [&palf_env_lite, this](
          const TenantLSIDS &tenant_ls_ids) -> int {
        int ret = OB_SUCCESS;
        if (OB_FAIL(handle_gc_message_for_one_tenant_(tenant_ls_ids, palf_env_lite))) {
          CLOG_LOG(WARN, "handle_gc_message_for_one_tenant_ failed", KR(ret), KPC(this), KPC(palf_env_lite));
        } else {
        }
        return ret;
      };
      // step2. iterate each ls in palf_env_lite secondly.
      if (OB_FAIL(ls_ids.operate(palf_env_key.tenant_id_, functor))) {
        CLOG_LOG(WARN, "operate ls_ids failed", KR(ret), KPC(this), K(palf_env_key));
      }
    }
    // even if operate in one tenant failed, we alse need to handle next tenant.
    return true;
  };

  // step1. iterate each tenant in palf_env_lite_map_ firstly.
  if (OB_FAIL(palf_env_lite_map_.for_each(operate_map_func))) {
    CLOG_LOG(WARN, "ObLinkHashMap for each failed", KR(ret), KPC(this), K(max_tenant_id), K(src_cluster_id), K(ls_ids));
  } else {
    // step2. delete tenants actually
    for (int i = 0; i < deleted_tenants.count(); i++) {
      int tmp_ret = OB_SUCCESS;
      const PalfEnvKey &palf_env_key = deleted_tenants.at(i);
      if (OB_SUCCESS != (tmp_ret = remove_palf_env_lite_not_guarded_by_lock_(deleted_tenants.at(i)))) {
        PALF_LOG(WARN, "drop tenant failed", KR(ret), KPC(this), K(palf_env_key), K(max_palf_env_key), K(ls_ids));
      } else {
        PALF_LOG(INFO, "drop tenant finished", KR(ret), KPC(this), K(palf_env_key), K(max_palf_env_key), K(ls_ids));
      }
    }
    CLOG_LOG(INFO, "handle_gc_message_for_one_cluster_ success", KR(ret), KPC(this), K(max_tenant_id), K(src_cluster_id), K(ls_ids));
  }
  return ret;
}

int PalfEnvLiteMgr::handle_gc_message_for_one_tenant_(const arbserver::TenantLSIDS &tenant_ls_ids,
                                                      PalfEnvLite *palf_env_lite)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &max_ls_id = tenant_ls_ids.get_max_ls_id().ls_id_;
  // The used memory is smaller than SMALL_OBJ_MAX_SIZE of ObFunction, therefore,
  // no need to alloc memory from heap
  auto operate_palf_env_func = [&palf_env_lite, &max_ls_id, &tenant_ls_ids](
      const palf::LSKey &key,
      IPalfHandleImpl *value) -> bool
  {
    int ret = OB_SUCCESS;
    const share::ObLSID ls_id(key.id_);
    if (max_ls_id < ls_id) {
      CLOG_LOG(INFO, "max_ls_id of remote is smaller than local, no need handle it",
          KR(ret), K(max_ls_id), K(ls_id));
    // When ls not exist in tenant_ls_ids, need remove it from PalfEnvLite;
    } else if (!tenant_ls_ids.exist(ls_id)) {
      // remove ls from map
      // NB: we can not use 'remove_palf_handle_impl', because the ref count of palf_handle_impl still greater than zero.
      // NB: for arb server, if a palf instance has been deleted, there is no possibility create it again.
      value->set_deleted();
      ret = palf_env_lite->palf_handle_impl_map_.del(key);
      CLOG_LOG(WARN, "remove_palf_handle_impl success", KR(ret), K(key));
    } else {
    }
    // even if operate in one ls failed, we alse need to handle next ls.
    return true;
  };
  if (OB_FAIL(palf_env_lite->palf_handle_impl_map_.for_each(operate_palf_env_func))) {
    CLOG_LOG(WARN, "ObLinkHashMap for_each failed", K(ret), KPC(this), KPC(palf_env_lite));
  } else {
    CLOG_LOG(INFO, "handle_gc_message_for_one_tenant_ success", K(ret), KPC(this), K(tenant_ls_ids));
  }
  return ret;
}

int PalfEnvLiteMgr::try_create_cluster_meta_info_(const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(cluster_meta_lock_);
  if (NULL != cluster_meta_info_map_.get(cluster_id)) {
  } else {
    CLOG_LOG(INFO, "there is no cluster meta info, need create", KR(ret), KPC(this), K(cluster_id));
    ClusterMetaInfo cluster_meta_info;
    cluster_meta_info.generate_by_default();
    if (OB_FAIL(cluster_meta_info_map_.set_refactored(cluster_id, cluster_meta_info))) {
      CLOG_LOG(WARN, "set_refactored failed", KR(ret), KPC(this), K(cluster_id));
    } else {
    }
  }
  return ret;
}

int PalfEnvLiteMgr::try_remove_cluster_meta_info_(const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(cluster_meta_lock_);
  if (OB_FAIL(cluster_meta_info_map_.erase_refactored(cluster_id))) {
    CLOG_LOG(WARN, "erase_refactored failed", KR(ret), KPC(this), K(cluster_id));
  }
  return ret;
}

int PalfEnvLiteMgr::get_cluster_meta_info_(const int64_t cluster_id,
                                           ClusterMetaInfo &cluster_meta_info) const
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(cluster_meta_lock_);
  if (OB_FAIL(cluster_meta_info_map_.get_refactored(cluster_id, cluster_meta_info))) {
    CLOG_LOG(INFO, "get_refactored failed", KR(ret), KPC(this), K(cluster_id));
  }
  return ret;
}

bool PalfEnvLiteMgr::contains_any_tenant_(const int64_t cluster_id)
{
  int ret = OB_SUCCESS;
  bool contains_tenant = false;
  auto check_contains_cluster = [cluster_id, &contains_tenant](const PalfEnvKey key, PalfEnvLite* value) -> bool {
    if (key.cluster_id_ == cluster_id) {
      contains_tenant = true;
      return false;
    }
    return true;
  };

  (void) palf_env_lite_map_.for_each(check_contains_cluster);
  CLOG_LOG(INFO, "contains_cluster_", K(contains_tenant), KPC(this));
  return contains_tenant;
}

int PalfEnvLiteMgr::remove_dir_(const char *dir,
                                const bool need_check_exist)
{
  int ret = OB_SUCCESS;
  int pret = 0;
  char tmp_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  bool exist = true;
  if (need_check_exist && OB_FAIL(FileDirectoryUtils::is_exists(dir, exist))) {
    PALF_LOG(WARN, "check directory exists failed", KPC(this), K(dir));
  } else if (!exist) {
    ret = OB_SUCCESS;
    PALF_LOG(INFO, "dir not exist", KPC(this), K(dir), K(need_check_exist));
  } else if (0 > (pret = snprintf(tmp_dir, MAX_PATH_SIZE, "%s%s", dir, TMP_SUFFIX))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "snprintf failed", KR(ret), K(dir), K(tmp_dir));
  } else if (OB_FAIL(rename_with_retry(dir, tmp_dir))) {
    CLOG_LOG(ERROR, "rename dir to tmp_dir failed", KR(ret), K(dir), K(tmp_dir), K(errno));
  } else {
    do {
      if (OB_FAIL(FileDirectoryUtils::is_exists(tmp_dir, exist))) {
        PALF_LOG(WARN, "check directory exists failed", KPC(this), K(dir));
      } else if (!exist) {
        ret = OB_SUCCESS;
        PALF_LOG(INFO, "dir not exist", KPC(this), K(tmp_dir), K(need_check_exist));
      } else {
        if (OB_FAIL(FileDirectoryUtils::delete_directory_rec(tmp_dir))) {
          CLOG_LOG(WARN, "delete_directory failed", KR(ret), K(dir));
        } else {
          CLOG_LOG(INFO, "remove_dir success", K(tmp_dir), KPC(this));
        }
        if (OB_FAIL(ret)) {
          CLOG_LOG(WARN, "remove directory failed, may be physical disk full", K(ret), K(tmp_dir), K(dir), K(errno));
          ob_usleep(100 * 1000);
        }
      }
    } while (OB_FAIL(ret));
    (void)FileDirectoryUtils::fsync_dir(base_dir_);
  }
  return ret;
}
} // end namespace palflite
} // end namespace oceanbase
