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
#ifndef OB_SHARE_STORAGE_COMPACTION_SVR_COMPACTION_OBJ_MGR_H_
#define OB_SHARE_STORAGE_COMPACTION_SVR_COMPACTION_OBJ_MGR_H_
#include "lib/net/ob_addr.h"
#include "storage/compaction/ob_basic_compaction_obj_mgr.h"
#include "storage/compaction/ob_compaction_report.h"
namespace oceanbase
{
namespace compaction
{

struct ObSvrPair
{
public:
  ObSvrPair() { reset(); }
  ObSvrPair(const uint64_t svr_id, const common::ObAddr &addr)
    : svr_id_(svr_id),
      svr_addr_(addr)
  {}
  ~ObSvrPair() = default;
  void reset() { svr_id_ = 0; svr_addr_.reset(); }
  bool is_valid() const { return 0 != svr_id_ && svr_addr_.is_valid(); }
  TO_STRING_KV(K_(svr_id), K_(svr_addr));
public:
  uint64_t svr_id_;
  common::ObAddr svr_addr_;
};


class ObSvrIDCache
{
public:
  ObSvrIDCache();
  ~ObSvrIDCache();
  int init();
  int refresh(const bool force_refresh);
  int get_svr_ids(ObIArray<uint64_t> &input_svr_ids);
  int get_svr_infos(ObIArray<ObSvrPair> &svr_infos);
  int get_svr_info(const uint64_t svr_id, ObSvrPair &svr_info);
  static int get_svr_id_by_addr(
    const ObIArray<ObSvrPair> &svr_infos,
    const common::ObAddr &addr,
    uint64_t &svr_id);
  void destroy();
private:
  int inner_refresh();
  static const int64_t REFRESH_SVR_ID_INTERVAL = 30 * 1000L * 1000L; // 30s
  static const int64_t DEFAULT_ARRAY_SIZE = 10;
private:
  bool is_inited_;
  lib::ObMutex lock_;
  int64_t last_refresh_ts_;
  ObSEArray<ObSvrPair, DEFAULT_ARRAY_SIZE> svr_infos_;
};

// record CompactionStatus for every svr in leader
class ObSvrCompactionObjMgr : public ObBasicCompactionObjMgr<uint64_t, ObCompactionReportObj>
{
public:
  ObSvrCompactionObjMgr() : ObBasicCompactionObjMgr(), exist_ls_leader_flag_(false) {}
  virtual ~ObSvrCompactionObjMgr() {}
  // leader: record obj for all server
  // follower: record obj for cur server
  int refresh(const bool exist_ls_leader_flag, ObCompactionObjBuffer &obj_buf);
  INHERIT_TO_STRING_KV("ObSvrCompactionObjMgr", ObBasicCompactionObjMgr, K_(exist_ls_leader_flag));
protected:
  typedef uint64_t KEY;
  virtual int get_valid_key_array(ObIArray<KEY> &keys) override;
  virtual int check_obj_valid(const KEY &key, bool &is_valid, bool &is_leader) override;
  virtual bool is_valid_key(const KEY &key) override { return true; }
  virtual int init_obj(const KEY &key, ObCompactionReportObj &value) override;
  virtual int64_t get_default_hash_bucket_cnt() const override { return 1024; }

  bool exist_ls_leader_flag_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_STORAGE_COMPACTION_SVR_COMPACTION_OBJ_MGR_H_
