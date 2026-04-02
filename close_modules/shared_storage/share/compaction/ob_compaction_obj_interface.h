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
#ifndef OB_SHARED_STORAGE_SHARE_COMPACTION_OB_COMPACTION_OBJ_INTERFACE_H_
#define OB_SHARED_STORAGE_SHARE_COMPACTION_OB_COMPACTION_OBJ_INTERFACE_H_
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace blocksstable
{
class ObStorageObjectOpt;
class MacroBlockId;
}
namespace compaction
{
struct ObCompactionObjBuffer;
struct ObCompactionObjInterface
{
  ObCompactionObjInterface()
    : last_refresh_ts_(0),
      is_inited_(false),
      is_reloaded_(false)
  {}
  ~ObCompactionObjInterface() = default;
  virtual bool is_valid() const = 0;
  int write_object(ObCompactionObjBuffer &buf);
  int read_object(ObCompactionObjBuffer &buf);
  static int read_object_in_buffer(
    const blocksstable::MacroBlockId &block_id,
    ObCompactionObjBuffer &buf);
  int delete_object();
  int check_exist(bool &exist) const;
  int try_reload_obj(const bool alloc_big_buf = false); // deal with restart scene
  bool is_reload_obj() const { return is_reloaded_; }
  VIRTUAL_TO_STRING_KV(K_(last_refresh_ts));
protected:
  virtual void set_obj_opt(blocksstable::ObStorageObjectOpt &opt) const = 0;
  PURE_VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
  int64_t last_refresh_ts_;
  bool is_inited_;
private:
  bool is_reloaded_; // check whether the obj is reloaded from s2 obj
};

struct ObCompactionObjBuffer
{
  ObCompactionObjBuffer();
  ~ObCompactionObjBuffer() { destroy(); }
  bool is_valid() const { return NULL != buf_; }
  int init(const bool alloc_big_buf = false);
  void destroy();
  char *get_buf() { return buf_; }
  int64_t get_buf_len() const { return buf_len_; }
  int ensure_space(const int64_t size);
  TO_STRING_KV(K_(buf_len), KCSTRING_(buf));
private:
  int64_t get_aligned_size(const int64_t size) const;
  void free();
public:
  static const int64_t INITIAL_BUF_SIZE = OB_MALLOC_NORMAL_BLOCK_SIZE; // 8 KB
  static const int64_t BIG_BUF_SIZE = OB_MALLOC_BIG_BLOCK_SIZE; // 2MB
  static const int64_t ALIGNMENT = OB_MALLOC_NORMAL_BLOCK_SIZE; // 8 KB
private:
  char *buf_;
  int64_t buf_len_;
  DefaultPageAllocator allocator_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARED_STORAGE_SHARE_COMPACTION_OB_COMPACTION_OBJ_INTERFACE_H_
