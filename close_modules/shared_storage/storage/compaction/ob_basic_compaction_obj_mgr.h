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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGE_COMPACTION
#endif
#ifndef OB_SHARE_STORAGE_COMPACTION_BASIC_COMPACTION_OBJ_MGR_H_
#define OB_SHARE_STORAGE_COMPACTION_BASIC_COMPACTION_OBJ_MGR_H_

#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/literals/ob_literals.h"
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/container/ob_se_array.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx_storage/ob_ls_handle.h"

namespace oceanbase
{
namespace compaction
{
struct ObCompactionObjBuffer;
struct ObLSObj;
struct ObCompactionReportObj;
template <typename T>
class ObBasicObjHandle;

struct ObBasicObj
{
public:
  ObBasicObj() : ref_cnt_(0) {}
  virtual ~ObBasicObj() { destroy(); }
  virtual void destroy() { ref_cnt_ = 0; }
  void inc_ref() { ATOMIC_INC(&ref_cnt_); }
  void dec_ref() { ATOMIC_DEC(&ref_cnt_); }
  uint32_t get_ref() const { return ATOMIC_LOAD(&ref_cnt_); }
  VIRTUAL_TO_STRING_KV(K_(ref_cnt));
protected:
  uint32_t ref_cnt_;
};

struct ObBasicObjHandleHelper
{
  void reset_obj_for_ObLSObj(ObBasicObjHandle<ObLSObj> *obj);
  void reset_obj_for_ObCompactionReportObj(ObBasicObjHandle<ObCompactionReportObj> *obj);
};
template<typename T>
struct ObBasicObjHandle : public ObBasicObjHandleHelper
{
  friend class ObBasicObjHandleHelper;
  ObBasicObjHandle()
    : obj_ptr_(nullptr)
  {}
  ~ObBasicObjHandle() { reset(); }
  bool is_valid() const { return nullptr != obj_ptr_; }
  void reset();
  void set(T &obj);
  int assign(const ObBasicObjHandle &other);
  T *get_obj() const { return obj_ptr_; }
  TO_STRING_KV(KPC_(obj_ptr));
private:
  T *obj_ptr_;
};

template<typename KEY, typename VALUE>
class ObBasicCompactionObjMgr
{
public:
  ObBasicCompactionObjMgr();
  virtual ~ObBasicCompactionObjMgr() { destroy(); }
  int init();
  int get_obj_handle(const KEY &key,
                     ObBasicObjHandle<VALUE> &obj_handle,
                     const bool add_if_not_exist = true);
  void release_handle(ObBasicObjHandle<VALUE> &handle);
  int refresh(ObCompactionObjBuffer &obj_buf);
  void destroy();
  VIRTUAL_TO_STRING_KV(K_(alloc_obj_cnt), "map_cnt", obj_map_.size());
protected:
  typedef hash::ObHashSet<KEY> KeySet;
  int inner_clear_obj_map();
  int remove_obj(const KEY &key);
  int get_exist_key_set(KeySet &key_set);
  int inner_add_obj(const KEY &key, VALUE *&obj);
  void inner_free_obj(VALUE *obj);
  int obj_refresh(const KEY &key, const bool is_leader, ObCompactionObjBuffer &obj_buf);
  // pure virtual func
  virtual int get_valid_key_array(ObIArray<KEY> &keys) = 0;
  virtual int check_obj_valid(const KEY &key, bool &is_valid, bool &is_leader) = 0;
  virtual bool is_valid_key(const KEY &key) = 0;
  virtual int init_obj(const KEY &key, VALUE &value) = 0;
  virtual int64_t get_default_hash_bucket_cnt() const = 0;
  static int erase_from_exist_set(const KEY &key, KeySet &key_set);
protected:
  typedef hash::ObHashMap<KEY, VALUE *> ObjMap;
  static const int64_t DEFAULT_ARRAY_SIZE = 10;
protected:
  bool is_inited_;
  uint32_t alloc_obj_cnt_;
  mutable obsys::ObRWLock lock_;
  common::DefaultPageAllocator allocator_;
  ObjMap obj_map_;
};

/**
 * -------------------------------------------------------------------ObBasicObjHandle-------------------------------------------------------------------
 */
template<typename T>
int ObBasicObjHandle<T>::assign(const ObBasicObjHandle<T> &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(other));
  } else {
    set(*other.obj_ptr_);
  }
  return ret;
}

template<>
inline void ObBasicObjHandle<ObLSObj>::reset()
{
  if (is_valid()) {
    ObBasicObjHandleHelper::reset_obj_for_ObLSObj(this);
    obj_ptr_ = nullptr;
  }
}

template<>
inline void ObBasicObjHandle<ObCompactionReportObj>::reset()
{
  if (is_valid()) {
    ObBasicObjHandleHelper::reset_obj_for_ObCompactionReportObj(this);
    obj_ptr_ = nullptr;
  }
}

template<typename T>
void ObBasicObjHandle<T>::set(T &obj)
{
  reset();
  obj_ptr_ = &obj;
  obj.inc_ref();
}


/**
 * -------------------------------------------------------------------ObBasicCompactionObjMgr-------------------------------------------------------------------
 */
template<typename KEY, typename VALUE>
ObBasicCompactionObjMgr<KEY, VALUE>::ObBasicCompactionObjMgr()
  : is_inited_(false),
    alloc_obj_cnt_(0),
    lock_(),
    allocator_(),
    obj_map_()
{}

template<typename KEY, typename VALUE>
void ObBasicCompactionObjMgr<KEY, VALUE>::destroy()
{
  int ret = OB_SUCCESS;
  obsys::ObWLockGuard guard(lock_);
  if (is_inited_) {
    is_inited_ = false;
    if (obj_map_.created()) {
      (void) inner_clear_obj_map();
      obj_map_.destroy();
    }
    allocator_.reset();
    STORAGE_LOG(INFO, "CompactionObjMgr destroyed", K_(alloc_obj_cnt));
    if (OB_UNLIKELY(alloc_obj_cnt_ != 0)) {
      STORAGE_LOG(ERROR, "may exists unfree obj", KR(ret), K_(alloc_obj_cnt));
    }
  }
}

template<typename KEY, typename VALUE>
int ObBasicCompactionObjMgr<KEY, VALUE>::inner_clear_obj_map()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSEArray<KEY, 32> keys;

  if (OB_TMP_FAIL(keys.reserve(obj_map_.size()))) {
    STORAGE_LOG(WARN, "failed to reserve key array", K(tmp_ret));
  }

  typename ObjMap::const_iterator iter = obj_map_.begin();
  for ( ; OB_SUCC(ret) && iter != obj_map_.end(); ++iter) {
    if (OB_FAIL(keys.push_back(iter->first))) {
      STORAGE_LOG(WARN, "failed to push back key", KR(ret), "key", iter->first);
    }
  }

  for (int64_t idx = 0; OB_SUCC(ret) && idx < keys.count(); ++idx) {
    // rest obj in set is invalid or not exist
    const KEY &key = keys.at(idx);
    VALUE *obj = NULL;
    if (OB_TMP_FAIL(obj_map_.get_refactored(key, obj))) {
      if (OB_HASH_NOT_EXIST != tmp_ret) {
        STORAGE_LOG(WARN, "failed to get from map", KR(tmp_ret), K(key));
      }
    } else if (OB_TMP_FAIL(obj_map_.erase_refactored(key))) {
      STORAGE_LOG(ERROR, "failed to erase from map", KR(tmp_ret), K(key));
    } else {
      obj->dec_ref();
      inner_free_obj(obj);
    }
  }
  return ret;
}

template<typename KEY, typename VALUE>
int ObBasicCompactionObjMgr<KEY, VALUE>::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObBasicCompactionObjMgr has inited", K(ret));
  } else if (OB_FAIL(obj_map_.create(get_default_hash_bucket_cnt(), "CompObjMap", "CompObjMap", MTL_ID()))) {
    STORAGE_LOG(WARN, "failed to create obj map", KR(ret));
  } else {
    allocator_.set_attr(ObMemAttr(MTL_ID(), "CompObjMgr"));
    is_inited_ = true;
  }
  return ret;
}

// call func under lock protection
template<typename KEY, typename VALUE>
int ObBasicCompactionObjMgr<KEY, VALUE>::inner_add_obj(
  const KEY &key,
  VALUE *&input_obj)
{
  input_obj = NULL;
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (OB_UNLIKELY(!is_valid_key(key))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(key));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(VALUE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc obj", KR(ret));
  } else {
    VALUE *obj = new (buf) VALUE();
    if (OB_FAIL(init_obj(key, *obj))) {
      STORAGE_LOG(WARN, "failed to init obj", KR(ret), K(key));
    } else if (OB_FAIL(obj_map_.set_refactored(key, obj))) {
      STORAGE_LOG(WARN, "failed to set refactor", KR(ret));
    } else {
      obj->inc_ref();
      input_obj = obj;
      ++alloc_obj_cnt_;
    }
    if (OB_FAIL(ret)) {
      obj->~VALUE();
      allocator_.free(obj);
    }
  }
  return ret;
}

template<typename KEY, typename VALUE>
int ObBasicCompactionObjMgr<KEY, VALUE>::get_obj_handle(
  const KEY &key,
  ObBasicObjHandle<VALUE> &obj_handle,
  const bool add_if_not_exist)
{
  int ret = OB_SUCCESS;
  obj_handle.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObBasicCompactionObjMgr has not been inited", K(ret));
  } else if (OB_UNLIKELY(!is_valid_key(key))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(key));
  } else {
    VALUE *obj = NULL;
    obsys::ObWLockGuard guard(lock_);
    if (OB_FAIL(obj_map_.get_refactored(key, obj))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get from map", KR(ret), K(key));
      } else if (add_if_not_exist) {
        ret = OB_SUCCESS;
        if (OB_FAIL(inner_add_obj(key, obj))) {
          LOG_WARN("failed to add obj", KR(ret), K(key));
        }
      } else { // !add_if_not_exist
        LOG_WARN("failed to get from map", KR(ret), K(key));
      }
    }
    if (OB_SUCC(ret)) {
      obj_handle.set(*obj);
    }
  }
  return ret;
}

template<typename KEY, typename VALUE>
int ObBasicCompactionObjMgr<KEY, VALUE>::remove_obj(const KEY &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_key(key))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(key));
  } else {
    VALUE *obj = NULL;
    obsys::ObWLockGuard guard(lock_);
    if (OB_FAIL(obj_map_.get_refactored(key, obj))) {
      if (OB_HASH_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "failed to get from map", KR(ret), K(key));
      }
    } else if (OB_FAIL(obj_map_.erase_refactored(key))) {
      STORAGE_LOG(ERROR, "failed to erase from map", KR(ret), K(key));
    } else {
      obj->dec_ref();
      inner_free_obj(obj);
    }
  }
  return ret;
}

template<typename KEY, typename VALUE>
void ObBasicCompactionObjMgr<KEY, VALUE>::inner_free_obj(VALUE *obj)
{
  if (OB_NOT_NULL(obj) && 0 == obj->get_ref()) {
    obj->~VALUE();
    allocator_.free(obj);
    --alloc_obj_cnt_;
  }
}

template<typename KEY, typename VALUE>
int ObBasicCompactionObjMgr<KEY, VALUE>::get_exist_key_set(hash::ObHashSet<KEY> &key_set)
{
  int ret = OB_SUCCESS;
  obsys::ObRLockGuard guard(lock_);
  for (typename ObjMap::const_iterator iter = obj_map_.begin();
       OB_SUCC(ret) && iter != obj_map_.end(); ++iter) {
    if (OB_FAIL(key_set.set_refactored(iter->first))) {
      STORAGE_LOG(WARN, "failed to push back key", KR(ret), "key", iter->first);
    }
  }
  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "[SharedStorage] success to get exist key set", KR(ret), K(key_set));
  }
  return ret;
}

template<typename KEY, typename VALUE>
int ObBasicCompactionObjMgr<KEY, VALUE>::erase_from_exist_set(const KEY &key, KeySet &exist_key_set)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(exist_key_set.erase_refactored(key))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "failed to erase from key set", KR(ret), K(key));
    }
  }
  return ret;
}

template<typename KEY, typename VALUE>
int ObBasicCompactionObjMgr<KEY, VALUE>::refresh(ObCompactionObjBuffer &obj_buf)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  storage::ObLSHandle ls_handle;
  ObSEArray<KEY, DEFAULT_ARRAY_SIZE> keys;
  KeySet exist_key_set;
  bool is_valid = false;
  bool is_leader = false;
  int64_t remove_obj_cnt = 0;
  int64_t cost_ts = ObTimeUtility::fast_current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObBasicCompactionObjMgr has not been inited", K(ret));
  } else if (OB_FAIL(get_valid_key_array(keys))) {
    STORAGE_LOG(WARN, "failed to get valid key array", K(ret));
  } else if (OB_FAIL(exist_key_set.create(get_default_hash_bucket_cnt(), ObMemAttr(MTL_ID(), "RefreshSet")))) {
    STORAGE_LOG(WARN, "failed to create exist key set", K(ret));
  } else if (OB_FAIL(get_exist_key_set(exist_key_set))) {
    STORAGE_LOG(WARN, "failed to get exist key set", K(ret));
  }
  // get valid keys
  for (int64_t idx = 0; OB_SUCC(ret) && idx < keys.count(); ++idx) {
    const KEY &key = keys.at(idx);
    is_valid = false;
    is_leader = false;
    if (OB_FAIL(check_obj_valid(key, is_valid, is_leader))) {
      STORAGE_LOG(WARN, "failed to check obj valid", KR(ret), K(key));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(erase_from_exist_set(key, exist_key_set))) {
      STORAGE_LOG(WARN, "failed to erase from key set", KR(ret), K(key));
    } else if (OB_TMP_FAIL(obj_refresh(key, is_leader, obj_buf))) {
      STORAGE_LOG_RET(WARN, tmp_ret, "failed to refresh obj", K(key));
    }
  } // end of for
  for (typename KeySet::iterator iter = exist_key_set.begin();
       OB_SUCC(ret) && iter != exist_key_set.end(); ++iter) {
    // rest obj in set is invalid or not exist
    if (OB_FAIL(remove_obj(iter->first))) {
      STORAGE_LOG(WARN, "failed to remove obj", KR(ret), "key", iter->first);
    } else {
      ++remove_obj_cnt;
      STORAGE_LOG(INFO, "[SharedStorage] success to remove obj", KR(ret), "key", iter->first);
    }
  }
  cost_ts = ObTimeUtility::fast_current_time() - cost_ts;
  STORAGE_LOG(INFO, "[SharedStorage] refresh obj", K(ret), K(cost_ts), K(keys), "remove_key", exist_key_set, K(remove_obj_cnt));
  return ret;
}

template<typename KEY, typename VALUE>
void ObBasicCompactionObjMgr<KEY, VALUE>::release_handle(ObBasicObjHandle<VALUE> &handle)
{
  if (handle.is_valid()) {
    VALUE *obj = handle.get_obj();
    obsys::ObWLockGuard guard(lock_);
    obj->dec_ref();
    inner_free_obj(obj);
  }
}

template<typename KEY, typename VALUE>
int ObBasicCompactionObjMgr<KEY, VALUE>::obj_refresh(
  const KEY &key,
  const bool is_leader,
  ObCompactionObjBuffer &obj_buf)
{
  int ret = OB_SUCCESS;
  ObBasicObjHandle<VALUE> handle;
  if (OB_FAIL(get_obj_handle(key, handle))) {
    STORAGE_LOG(WARN, "failed to get obj", KR(ret));
  } else {
    // use handle to protect obj memory, release lock in mgr
    ret = handle.get_obj()->refresh(is_leader, obj_buf);
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_STORAGE_COMPACTION_BASIC_COMPACTION_OBJ_MGR_H_
