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

#ifndef OCEANBASE_SRC_LIBOBLOG_OB_LOG_BINLOG_RECORD_POOL_
#define OCEANBASE_SRC_LIBOBLOG_OB_LOG_BINLOG_RECORD_POOL_

#include "ob_log_binlog_record.h"               // ObLogBR
#include "lib/objectpool/ob_small_obj_pool.h"   // ObSmallObjPool

namespace oceanbase
{
namespace libobcdc
{
class IObLogBRPool
{
public:
  virtual ~IObLogBRPool() {}

public:
  // If host is valid, then set host to binlog record: ObLogBR::set_host()
  virtual int alloc(ObLogBR *&br, void *host = nullptr, void *stmt_task = nullptr) = 0;
  virtual void free(ObLogBR *br) = 0;
  virtual void print_stat_info() const = 0;
};

//////////////////////////////////////////////////////////////////////////////

class ObLogBRPool : public IObLogBRPool
{
  typedef common::ObSmallObjPool<ObLogUnserilizedBR> UnserilizedBRObjPool;

public:
  ObLogBRPool();
  virtual ~ObLogBRPool();

public:
  int alloc(ObLogBR *&br, void *host = nullptr, void *stmt_task = nullptr);
  void free(ObLogBR *br);
  void print_stat_info() const;

public:
  int init(const int64_t fixed_br_count);
  void destroy();

private:
  bool        inited_;
  UnserilizedBRObjPool   unserilized_pool_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogBRPool);
};

} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_SRC_LIBOBLOG_OB_LOG_BINLOG_RECORD_POOL_ */
