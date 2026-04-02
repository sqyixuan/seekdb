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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_TMP_FILE_IO_CALLBACK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_TMP_FILE_IO_CALLBACK_H_

#include <stdint.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/utility/ob_print_utils.h"
#include "share/io/ob_io_define.h"
#include "storage/shared_storage/ob_segment_file_manager.h"

namespace oceanbase
{
namespace storage
{

enum class ObSSTmpFileSegDeleteType : uint8_t
{
  NONE = 0,             // no need to delete file
  LOCAL = 1,            // need to delete local file
  REMOTE = 2,           // need to delete remote object
  LOCAL_AND_REMOTE = 3  // need to delete both local file and remote object
};

enum class ObSSTmpFileSegMetaOpType : uint8_t
{
  INSERT = 0, // insert meta
  DELETE = 1, // delete meta
  UPDATE = 2  // update meta
};

class ObSSTmpFileIOCallback : public common::ObIOCallback
{
public:
  ObSSTmpFileIOCallback();
  virtual ~ObSSTmpFileIOCallback() {}
  virtual ObIAllocator *get_allocator() override { return allocator_; }
  virtual const char *get_data() override { return nullptr; }
  virtual int64_t size() const override { return 0; }
  virtual int alloc_data_buf(const char *io_data_buffer, const int64_t data_size) override
  {
    return OB_NOT_SUPPORTED;
  }
  virtual int inner_process(const char *data_buffer, const int64_t size) override;
  int set_ss_tmpfile_io_callback(const TmpFileSegId &seg_id,
                                 const TmpFileMetaHandle &meta_handle,
                                 const ObSSTmpFileSegMetaOpType seg_meta_op_type,
                                 const ObSSTmpFileSegDeleteType seg_del_type,
                                 const int64_t del_seg_valid_len,
                                 common::ObIAllocator *allocator);
  const char *get_cb_name() const override { return "SSTmpFileIOCallback"; }
  TO_STRING_KV(K_(seg_id), K_(meta_handle), K_(seg_meta_op_type), K_(seg_del_type),
               K_(del_seg_valid_len), KP_(allocator));

private:
  int inner_process_();
  int process_seg_meta();
  int process_seg_deletion();
  int delete_local_seg_file(const TmpFileSegId &del_seg_id);
  int push_del_seg_file_to_remove_queue(const TmpFileSegId &del_seg_id, const int64_t &del_seg_valid_len);
  DISALLOW_COPY_AND_ASSIGN(ObSSTmpFileIOCallback);

public:
  TmpFileSegId seg_id_;
  TmpFileMetaHandle meta_handle_;
  ObSSTmpFileSegMetaOpType seg_meta_op_type_;
  ObSSTmpFileSegDeleteType seg_del_type_;
  int64_t del_seg_valid_len_; // record valid length of the segment to delete on object storage
  common::ObIAllocator *allocator_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_TMP_FILE_IO_CALLBACK_H_ */
