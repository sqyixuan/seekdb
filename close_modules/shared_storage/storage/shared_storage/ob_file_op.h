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
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_OP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_OP_H_

#include "common/storage/ob_device_common.h"
#include "deps/oblib/src/lib/container/ob_array.h"
#include "deps/oblib/src/lib/container/ob_se_array.h"
#include "share/ob_io_device_helper.h"

namespace oceanbase
{
namespace storage
{

// ObSharedTenantDirListOp is used for list shared tenant dir in object storage, example tenant_xxx
class ObSharedTenantDirListOp : public ObBaseDirEntryOperator
{
public:
  ObSharedTenantDirListOp(const uint64_t tenant_id);
  virtual int func(const dirent *entry) override;
  int get_file_list(common::ObIArray<uint64_t> &files) const;
  TO_STRING_KV(K_(file_list));

public:
  ObArray<uint64_t> file_list_;
};

// ObSingleNumFileListOp is used for list single number name file, for example tablet data dir,tablet meta dir,tmp file dir,segment file
class ObSingleNumFileListOp : public ObBaseDirEntryOperator
{
public:
  static const int64_t OB_DEFAULT_ARRAY_CAPACITY = 128;
  ObSingleNumFileListOp();
  virtual int func(const dirent *entry) override;
  int get_file_list(common::ObIArray<int64_t> &files) const;
  TO_STRING_KV(K_(file_list));

public:
  ObSEArray<int64_t, OB_DEFAULT_ARRAY_CAPACITY> file_list_;
};

// ObDoubleNumFileListOp is used for list double number name file, for example private_data_macro_file,private_meta_macro_file,ls_dir
class ObDoubleNumFileListOp : public ObBaseDirEntryOperator
{
public:
  ObDoubleNumFileListOp();
  virtual int func(const dirent *entry) override;
  int get_file_list(common::ObIArray<std::pair<int64_t, int64_t>> &files) const;
  TO_STRING_KV(K_(file_list));

public:
  ObSEArray<std::pair<int64_t, int64_t>, ObSingleNumFileListOp::OB_DEFAULT_ARRAY_CAPACITY> file_list_;
};

class ObDirCalcSizeOp : public share::ObScanDirOp
{
public:
  static const int64_t DEFAULT_TMP_SEQ_FILE_DELETE_SECURITY_TIME_S = 24L * 60L * 60L; // 24h, second level
  ObDirCalcSizeOp(volatile bool &is_stop, const char *dir = nullptr);
  virtual int func(const dirent *entry) override;
  OB_INLINE int64_t get_total_file_size() const { return total_file_size_; }
  OB_INLINE int64_t get_total_tmp_file_read_cache_size() const { return total_tmp_file_read_cache_size_; }
  int set_start_calc_size_time(const int64_t start_calc_size_time_s);
  // if .tm.seq file's modify time exceeds the security time(second level) compared with the current time, it will be deleted
  int set_tmp_seq_file_del_secy_time_s(const int64_t del_secy_time_s);
  void set_tmp_file_calc() { is_tmp_file_calc_ = true; }
  TO_STRING_KV(K_(is_stop), K_(total_file_size), K_(start_calc_size_time_s),
               K_(is_tmp_file_calc), K_(total_tmp_file_read_cache_size), K_(tmp_seq_file_del_secy_time_s));

private:
  volatile bool &is_stop_;
  int64_t total_file_size_;
  int64_t start_calc_size_time_s_;
  bool is_tmp_file_calc_;
  int64_t total_tmp_file_read_cache_size_;
  int64_t tmp_seq_file_del_secy_time_s_;  // if .tm.seq file's modify time exceeds the security time(second level) compared with the current time, it will be deleted
};

class ObMajorDataDeleteOp : public ObBaseDirEntryOperator
{
public:
  ObMajorDataDeleteOp(const char *dir = nullptr,
                      const int64_t time_stamp = ObTimeUtility::current_time());
  virtual int func(const dirent *entry) override;
  OB_INLINE int64_t get_total_file_count() const { return total_file_count_; }
  int set_dir(const char *dir);
  int set_time_stamp(const int64_t time_stamp);
  TO_STRING_KV(K_(time_stamp), K_(total_file_count));

private:
  const char *dir_;
  int64_t time_stamp_;
  int64_t total_file_count_;
};

class ObRMLogicalDeletedFileOp : public share::ObScanDirOp
{
public:
  ObRMLogicalDeletedFileOp(volatile bool &is_stop, const char *dir = nullptr)
  : ObScanDirOp(dir), is_stop_(is_stop), total_file_cnt_(0) {}
  virtual int func(const dirent *entry) override;
  OB_INLINE int64_t get_total_file_count() const { return total_file_cnt_; }
  TO_STRING_KV(K_(is_stop), K_(total_file_cnt));
private:
  volatile bool &is_stop_;
  int64_t total_file_cnt_;
};

class ObDelTmpFileDirOp : public share::ObScanDirOp
{
public:
  ObDelTmpFileDirOp(volatile bool &is_stop, const char *dir = nullptr)
  : ObScanDirOp(dir), is_stop_(is_stop) {}
  virtual int func(const dirent *entry) override;
  TO_STRING_KV(K_(is_stop));
private:
  volatile bool &is_stop_;
};

} // namespace storage
} // namespace oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_OP_H_ */
