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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_EXTERNAL_UTILS_
#define OCEANBASE_LOGSERVICE_OB_LOG_EXTERNAL_UTILS_
#include <stdint.h>                               // uint64_t
#include "lib/utility/ob_macro_utils.h"           // DISALLOW_COPY_AND_ASSIGN
#include "lib/function/ob_function.h"             // ObFunction
#include "common/storage/ob_device_common.h"      // ObBaseDirEntryOperator
#include "share/scn.h"                            // SCN
#include "logservice/palf/log_define.h"           // block_id_t
#include "lib/container/ob_bit_set.h"             // ObBitSet
namespace oceanbase
{
namespace share
{
class SCN;
class ObDeviceConfig;
class ObBackupDest;
class ObLSID;
}
namespace palf
{
class LogBlockHeader;
}
namespace logservice
{
class ObSharedLogUtils {
public:
  // @brief get oldest object on specified log stream.
  // @param[in]  tenant_id.
  // @param[in]  ls_id.
  // @param[out] oldest_block_id.
  // @return value
  //   OB_SUCCESS.
  //   OB_INVALID_ARGUMENT.
  //   OB_ALLOCATE_MEMORY_FAILED.
  //   OB_ENTRY_NOT_EXIST, there is no such block on external storage.
  //   OB_OBJECT_STORAGE_IO_ERROR.
  static int get_oldest_block(const uint64_t tenant_id,
                              const share::ObLSID &ls_id,
                              palf::block_id_t &oldest_block_id);

  // @brief get newest object after specified start_block_id(include this block).
  // @param[in]  tenant_id.
  // @param[in]  ls_id.
  // @param[in]  start_block_id, used to avoid list too many objects.
  // @param[out] out_block_id, the newest block.
  // @return value
  //   OB_SUCCESS.
  //   OB_INVALID_ARGUMENT.
  //   OB_ALLOCATE_MEMORY_FAILED.
  //   OB_ENTRY_NOT_EXIST, there is no such block whose names is greater than or equal to start_block_id.
  //   OB_OBJECT_STORAGE_IO_ERROR.
  //
  // NB: there mustn't be holes in range of [start_block_id, LOG_MAX_BLOCK_ID_ON_SHARED) if
  //     block which names 'start_block_id' exist on shared storage.
  static int get_newest_block(const uint64_t tenant_id,
                              const share::ObLSID &ls_id,
                              const palf::block_id_t &start_block_id,
                              palf::block_id_t &out_block_id);

  // @brief get the last block in the range [start_block_id, end_block_id) such that the min scn
  //        of this block is smaller or equal to 'scn'.
  // @param[in]  tenant_id.
  // @param[in]  ls_id.
  // @param[in]  start_block_id, starting point of binary search(inclusive range).
  // @param[in]  end_block_id, ending point of binary search(exclusive range).
  // @param[in]  scn, specified scn.
  // @param[out] out_block_id, the last block whose min scn is smaller than or equal to 'scn'
  // @param[out] out_block_min_scn, min scn of the block.
  // @return value
  //   OB_SUCCESS.
  //   OB_INVALID_ARGUMENT.
  //   OB_ALLOCATE_MEMORY_FAILED.
  //   OB_ENTRY_NOT_EXIST, there is no such block in the range on external storage.
  //   OB_ENTRY_OUTOF_LOWER_BOUND, the min scn of oldest block is greater than 'scn'.
  //   OB_OBJECT_STORAGE_IO_ERROR.
  // NB: there may be holes in range of [start_block_id, end_block_id).
  static int locate_by_scn_coarsely(const uint64_t tenant_id,
                                    const share::ObLSID &ls_id,
                                    const palf::block_id_t &start_block_id,
                                    const palf::block_id_t &end_block_id,
                                    const share::SCN &scn,
                                    palf::block_id_t &out_block_id,
                                    share::SCN &out_block_min_scn);

  // @brief get min scn of specified block.
  // @param[in]  tenant_id.
  // @param[in]  ls_id.
  // @param[in]  block_id, the id of specified block.
  // @param[out] block_min_scn, min scn of the block.
  // @return value
  //   OB_SUCCESS.
  //   OB_INVALID_ARGUMENT
  //   OB_ALLOCATE_MEMORY_FAILED.
  //   OB_ENTRY_NOT_EXIST, the specified block is not exist.
  //   OB_OBJECT_STORAGE_IO_ERROR.
  static int get_block_min_scn(const uint64_t tenant_id,
                               const share::ObLSID &ls_id,
                               const palf::block_id_t &block_id,
                               share::SCN &block_min_scn);

  // @brief delete all blocks in range of [start_block_id, end_block_id)
  // @param[in] tenant_id.
  // @param[in] ls_id.
  // @param[in] start_block_id, starting point to be deleted(inclusive range).
  // @param[in] end_block_id, ending point of to be deleted(exclusive range).
  // @return value
  //   OB_SUCCESS.
  //   OB_INVALID_ARGUMENT.
  //   OB_ALLOCATE_MEMORY_FAILED.
  //   OB_OBJECT_STORAGE_IO_ERROR.
  // NB: please retry when delete_blocks failed.
  static int delete_blocks(const uint64_t tenant_id,
                           const share::ObLSID &ls_id,
                           const palf::block_id_t &start_block_id,
                           const palf::block_id_t &end_block_id);

  // @brief construct uri for specified block_id and get access info(e.g. upload single block to object storage)
  // @param[in] tenant_id.
  // @param[in] ls_id.
  // @param[in] block_id.
  // @param[out] out_uri, the memory of out_uri need allocate by caller.
  // @param[out] out_uri_buf_len, the valid length of out_uri.
  // @param[out] storage_dest.
  // @return value
  //   OB_SUCCESS
  //   OB_INVALID_ARGUMENT.
  //   OB_ALLOCATE_MEMORY_FAILED
  static int construct_external_storage_access_info(const uint64_t tenant_id,
                                                    const share::ObLSID &ls_id,
                                                    const palf::block_id_t &block_id,
                                                    char *out_uri,
                                                    const int64_t out_uri_buf_len,
                                                    share::ObBackupDest &storage_dest,
                                                    uint64_t &storage_id);

  // @brief check ls whether exist on object storage.
  // @param[in] tenant_id.
  // @param[in] ls_id.
  // @param[out] ls_exist.
  // @return value
  //   OB_SUCCESS
  //   OB_INVALID_ARGUMENT
  //   OB_ALLOCATE_MEMORY_FAILED
  //   OB_OBJECT_STORAGE_IO_ERROR
  static int check_ls_exist(const uint64_t tenant_id,
                            const share::ObLSID &ls_id,
                            bool &ls_exist);

  // @brief check tenant whehter exist on object storage.
  // @param[in] tenant_id.
  // @param[out] tenant_exist.
  // @return value
  //   OB_SUCCESS
  //   OB_INVALID_ARGUMENT
  //   OB_ALLOCATE_MEMORY_FAILED
  //   OB_OBJECT_STORAGE_IO_ERROR
  static int check_tenant_exist(const uint64_t tenant_id,
                                bool &tenant_exist);

  // @brief delete tenant directory on object storage.
  // @param[in] tenant_id.
  // @return value
  //   OB_SUCCESS
  //   OB_INVALID_ARGUMENT
  //   OB_ALLOCATE_MEMORY_FAILED
  //   OB_OBJECT_STORAGE_IO_ERROR
  // NB: if uri is beginning with 'file://', before delete_tenant, we must
  //     need keep there is no object under tenant.
  static int delete_tenant(const uint64_t tenant_id);

  // @brief delete ls directory on object storage.
  // @param[in] tenant_id.
  // @param[in] ls_id.
  // @return value
  //   OB_SUCCESS
  //   OB_INVALID_ARGUMENT
  //   OB_ALLOCATE_MEMORY_FAILED
  //   OB_OBJECT_STORAGE_IO_ERROR
  // NB: if uri is beginning with 'file://', before delete_tenant, we must
  //     need keep there is no object under ls.
  static int delete_ls(const uint64_t tenant_id,
                       const share::ObLSID &ls_id);
};

// ======================================= Implementions =============================================
class ObSharedLogDirOp : public ObBaseDirEntryOperator {
public:
  typedef ObFunction<int(const char *uri)> UserFunction;
  ObSharedLogDirOp();
  ~ObSharedLogDirOp();
  int set_used_for_marker(const char *marker,
                          const int64_t limited_num,
                          const UserFunction &user_function);
  virtual int func(const dirent *entry) final;
private:
  UserFunction function_;
  DISABLE_COPY_ASSIGN(ObSharedLogDirOp);
};
class ObSharedLogUtilsImpl {
public:
  static ObSharedLogUtilsImpl& get_instance();
  ObSharedLogUtilsImpl() = default;
  ~ObSharedLogUtilsImpl() = default;
  int get_oldest_block(const uint64_t tenant_id,
                       const share::ObLSID &ls_id,
		                   palf::block_id_t &oldest_block_id);

  int get_newest_block(const uint64_t tenant_id,
                       const share::ObLSID &ls_id,
                       const palf::block_id_t &start_block_id,
                       palf::block_id_t &out_block_id);

  int locate_by_scn_coarsely(const uint64_t tenant_id,
                             const share::ObLSID &ls_id,
                             const palf::block_id_t &start_block_id,
                             const palf::block_id_t &end_block_id,
                             const share::SCN &scn,
                             palf::block_id_t &out_block_id,
                             share::SCN &out_block_min_scn);

  int get_block_min_scn(const uint64_t tenant_id,
                        const share::ObLSID &ls_id,
                        const palf::block_id_t &block_id,
                        share::SCN &block_min_scn);

  int delete_blocks(const uint64_t tenant_id,
                    const share::ObLSID &ls_id,
                    const palf::block_id_t &start_block_id,
                    const palf::block_id_t &end_block_id);

  int construct_external_storage_access_info(const uint64_t tenant_id,
                                             const share::ObLSID &ls_id,
                                             const palf::block_id_t &block_id,
                                             char *out_uri,
                                             const int64_t out_uri_buf_len,
                                             share::ObBackupDest &storage_dest,
                                             uint64_t &storage_id);

  int check_tenant_exist(const uint64_t tenant_id,
                         bool &tenant_exist);

  int check_ls_exist(const uint64_t tenant_id,
                     const share::ObLSID &ls_id,
                     bool &ls_exist);

  int delete_tenant(const uint64_t tenant_id);

  int delete_ls(const uint64_t tenant_id,
                const share::ObLSID &ls_id);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSharedLogUtilsImpl);
private:
  static const char *TENANT_FORMAT;
  static const char *LS_FORMAT;
  static const char *BLOCK_FORMAT;
  static const int64_t DEFAULT_BATCH_COUNT;
  static const int64_t RIGHT_ALIGN_COUNT;

  int64_t binary_search_count_;

private:
  int get_oldest_block_impl_(const uint64_t tenant_id,
                             const share::ObLSID &ls_id,
                             palf::block_id_t &oldest_block_id);

  int get_newest_block_impl_(const uint64_t tenant_id,
                             const share::ObLSID &ls_id,
                             const palf::block_id_t &start_block_id,
                             palf::block_id_t &newest_block_id);

  int list_blocks_impl_(const share::ObBackupDest &dest,
                        const char *uri,
                        const char *marker,
                        const int64_t limited_num,
                        ObSharedLogDirOp::UserFunction &user_function);
  
  int construct_tenant_str_(const share::ObBackupDest &dest,
                            const uint64_t tenant_id,
                            char *output_str,
                            const int64_t output_str_buf_len);

  int construct_ls_str_(const share::ObBackupDest &dest,
                        const uint64_t tenant_id,
                        const share::ObLSID &ls_id,
                        char *output_str,
                        const int64_t output_str_buf_len);

  int construct_block_str_(const share::ObBackupDest &dest,
                           const uint64_t tenant_id,
                           const share::ObLSID &ls_id,
                           const palf::block_id_t &block_id,
                           char *out_str,
                           const int64_t out_str_buf_len);

  int get_block_header_(const uint64_t tenant_id,
                        const share::ObLSID &ls_id,
                        const palf::block_id_t &block_id,
                        palf::LogBlockHeader &block_header);

  int get_storage_dest_and_id_(share::ObBackupDest &dest, uint64_t &storage_id);

  int delete_blocks_impl_(const uint64_t tenant_id,
                          const share::ObLSID &ls_id,
                          const palf::block_id_t &start_block_id,
                          const int64_t delete_count);

  int convert_last_non_zero_char_to_zero_(char *uri);

  int convert_ret_code_(const int ret);

  int binary_search_block_by_scn_(const uint64_t tenant_id,
                                  const share::ObLSID &ls_id,
                                  const share::SCN &scn,
                                  const palf::block_id_t start_block_id,
                                  const palf::block_id_t end_block_id,
                                  palf::block_id_t &out_block_id,
                                  share::SCN &out_block_min_scn);

  int relocate_mid_block_(const uint64_t tenant_id,
                          const share::ObLSID &ls_id,
                          palf::block_id_t &start_block_id,
                          palf::block_id_t &end_block_id,
                          palf::block_id_t &existed_block_id,
                          common::ObBitSet<> &non_existed_block_set);
  
  int is_block_existed_(const uint64_t tenant_id,
                        const share::ObLSID &ls_id,
                        const palf::block_id_t &block_id,
                        bool &existed,
                        common::ObBitSet<> &non_existed_block_set);
};

#define SHARED_LOG_GLOBAL_UTILS ObSharedLogUtilsImpl::get_instance()
// ===================================================================================================
} // end namespace logservice
} // end namespace oceanbase
#endif

