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

#ifndef OCEANBASE_LIBOBCDC_LOB_AUX_TABLE_PARSE_H_
#define OCEANBASE_LIBOBCDC_LOB_AUX_TABLE_PARSE_H_

#include "common/object/ob_object.h"        // ObLobId
#include "lib/ob_define.h"                  // OB_APP_MIN_COLUMN_ID
#include "ob_log_part_trans_task.h"         // ColValueList

namespace oceanbase
{
namespace libobcdc
{
class ObCDCLobAuxMetaParse
{
public:
  static const int64_t AUX_LOB_META_TABLE_LOB_ID_COLUMN_ID = common::OB_APP_MIN_COLUMN_ID;
  static const int64_t AUX_LOB_META_TABLE_LOB_DATA_COLUMN_ID = common::OB_APP_MIN_COLUMN_ID + 5;

  static int parse_aux_lob_meta_table_row(
      ColValueList &cols,
      ObLobId &lob_id,
      const char *&lob_data,
      int64_t &lob_data_len);

private:
  static int get_lob_id_(
      ColValue &col_val,
      ObLobId &lob_id);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
