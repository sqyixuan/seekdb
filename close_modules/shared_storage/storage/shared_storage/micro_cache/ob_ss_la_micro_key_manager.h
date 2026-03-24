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

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_LA_MICRO_KEY_MANAGER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_LA_MICRO_KEY_MANAGER_H_

#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace storage
{

class ObLS;

class ObSSLAMicroKeyManager
{
public:
  static const int64_t OB_LATEST_ACCESS_MICRO_KEY_BUCKET_NUM = 10001;
  ObSSLAMicroKeyManager();
  virtual ~ObSSLAMicroKeyManager();
  int init();
  void destroy();
  bool is_inited() const { return is_inited_; }
  int push_latest_access_micro_key_to_hashset(const ObSSMicroBlockCacheKeyMeta &micro_meta);
  int get_batch_micro_keys(ObLS *ls, ObIArray<ObSSMicroBlockCacheKeyMeta> &keys);
  int is_tablet_id_need_filter(ObLS *ls, const uint64_t tablet_id, bool &is_filter);
  int check_stop_record_la_micro_key();

private:
  class ObGetMicroKeyOp
  {
  public:
    ObGetMicroKeyOp(common::ObIArray<ObSSMicroBlockCacheKeyMeta> &keys,
                    ObLS *ls,
                    ObSSLAMicroKeyManager &la_micro_key_manager)
      : keys_(keys), ls_(ls), la_micro_key_manager_(la_micro_key_manager) {}
    ~ObGetMicroKeyOp() {}
    int operator()(common::hash::HashSetTypes<ObSSMicroBlockCacheKeyMeta>::pair_type &pair);
  public:
    common::ObIArray<ObSSMicroBlockCacheKeyMeta> &keys_;
    ObLS *ls_;
    ObSSLAMicroKeyManager &la_micro_key_manager_;
    DISALLOW_COPY_AND_ASSIGN(ObGetMicroKeyOp);
  };
  int batch_erase_micro_keys(const common::ObIArray<ObSSMicroBlockCacheKeyMeta> &keys);
  int filter_micro_block_cache_key_by_object_type(const ObSSMicroBlockCacheKeyMeta &micro_meta, bool &is_filter);
  int filter_micro_block_cache_key_by_tablet_id(ObLS *ls, const ObSSMicroBlockCacheKeyMeta &micro_meta, bool &is_filter);

private:
  static const uint64_t MAX_MICRO_BLOCK_KEY_SET_CAPACITY = 100000; // 10w

private:
  bool is_inited_;
  // store latest accessed micro_meta from ss_micro_cache, used for ls replica prewarm.
  common::hash::ObHashSet<ObSSMicroBlockCacheKeyMeta> latest_access_micro_key_set_;
  bool is_stop_record_la_micro_key_; // is ls replica prewarm stop
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_LA_MICRO_KEY_MANAGER_H_ */
