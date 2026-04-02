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

#include <gtest/gtest.h>
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
using namespace common;
using namespace arbserver;

namespace unittest
{
TEST(TestArbGCUtils, test_gc_msg_epoch)
{
  GCMsgEpoch epoch1, epoch2, epoch3;
  // test valid 
  EXPECT_EQ(false, epoch1.is_valid());
  // test compare invalid
  EXPECT_EQ(true, epoch1 == epoch2);

  // test compare
  epoch1.proposal_id_ = 1;
  EXPECT_EQ(false, epoch1.is_valid());
  epoch1.seq_ = 1;

  EXPECT_EQ(true, epoch1 > epoch2);

  epoch2.proposal_id_ = 1;
  epoch2.seq_ = 2;

  EXPECT_EQ(true, epoch1 < epoch2);

  epoch2.proposal_id_ = 2;
  epoch2.seq_ = 1;
  EXPECT_EQ(true, epoch1 < epoch2);

  epoch2.proposal_id_ = 1;
  epoch2.seq_ = 2;
  EXPECT_EQ(true, epoch1 < epoch2);

  EXPECT_EQ(false, epoch1 > epoch2);
  epoch2.seq_ = 0;
  EXPECT_EQ(false, epoch1 < epoch2);
  epoch2.seq_ = 1;
  EXPECT_EQ(true, epoch1 == epoch2);

  // test serialzie 
  char buf[4096];
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, epoch1.serialize(buf, 4096, pos));
  EXPECT_EQ(pos, epoch1.get_serialize_size());
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, epoch3.deserialize(buf, 4096, pos));
  EXPECT_EQ(epoch3, epoch1);
}

TEST(TestArbGCUtils, test_tenant_ls_id)
{
  TenantLSID tenant_ls_id1, tenant_ls_id2;
  // test valid 
  EXPECT_EQ(false, tenant_ls_id1.is_valid());
  tenant_ls_id1.tenant_id_ = 1;
  EXPECT_EQ(false, tenant_ls_id1.is_valid());
  tenant_ls_id1.ls_id_ = 2;
  EXPECT_EQ(true, tenant_ls_id1.is_valid());

  // test serialzie 
  char buf[4096];
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, tenant_ls_id1.serialize(buf, 4096, pos));
  EXPECT_EQ(pos, tenant_ls_id1.get_serialize_size());
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, tenant_ls_id2.deserialize(buf, 4096, pos));
  EXPECT_EQ(tenant_ls_id1.tenant_id_, tenant_ls_id2.tenant_id_);
  EXPECT_EQ(tenant_ls_id1.ls_id_, tenant_ls_id2.ls_id_);
}

TEST(TestArbGCUtils, test_tenant_ls_ids)
{
  TenantLSIDS tenant_ls_ids1, tenant_ls_ids2;
  TenantLSID tenant_ls_id1;
  tenant_ls_id1.tenant_id_ = 1;
  tenant_ls_id1.ls_id_ = 2;
  // test valid
  EXPECT_EQ(false, tenant_ls_ids1.is_valid());
  tenant_ls_ids1.set_max_ls_id(tenant_ls_id1);
  EXPECT_EQ(false, tenant_ls_ids1.is_valid());
  EXPECT_EQ(OB_SUCCESS, tenant_ls_ids1.push_back(tenant_ls_id1.ls_id_));
  EXPECT_EQ(true, tenant_ls_ids1.is_valid());
  EXPECT_EQ(OB_SUCCESS, tenant_ls_ids1.push_back(tenant_ls_id1.ls_id_));
  
  // test serialize
  char buf[4096];
  int64_t pos = 0; 
  EXPECT_EQ(OB_SUCCESS, tenant_ls_ids1.serialize(buf, 4096, pos));
  EXPECT_EQ(pos, tenant_ls_ids1.get_serialize_size());
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, tenant_ls_ids2.deserialize(buf, 4096, pos));
  EXPECT_EQ(tenant_ls_ids1, tenant_ls_ids2);
  EXPECT_EQ(true, tenant_ls_ids1.exist(tenant_ls_id1.ls_id_));
  EXPECT_EQ(false, tenant_ls_ids1.exist(share::ObLSID(1000)));

}

TEST(TestArbGCUtils, test_tenant_ls_id_array)
{
  TenantLSIDS tenant_ls_ids;
  TenantLSID tenant_ls_id(1, share::ObLSID(2));
  tenant_ls_ids.set_max_ls_id(tenant_ls_id);
  EXPECT_EQ(OB_SUCCESS, tenant_ls_ids.push_back(tenant_ls_id.ls_id_));
  TenantLSIDSArray ls_ids, ls_ids2;
  // test valid
  EXPECT_EQ(false, ls_ids.is_valid());
  EXPECT_EQ(OB_SUCCESS, ls_ids.push_back(tenant_ls_ids));
  EXPECT_EQ(false, ls_ids.is_valid());
  ls_ids.set_max_tenant_id(1);
  EXPECT_EQ(true, ls_ids.is_valid());

  // test serialize
  char buf[4096];
  int64_t pos = 0; 
  EXPECT_EQ(OB_SUCCESS, ls_ids.serialize(buf, 4096, pos));
  EXPECT_EQ(pos, ls_ids.get_serialize_size());
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, ls_ids2.deserialize(buf, 4096, pos));
  EXPECT_EQ(ls_ids, ls_ids2);
  EXPECT_EQ(true, ls_ids.exist(1));
  EXPECT_EQ(false, ls_ids.exist(1000));
  EXPECT_EQ(1, ls_ids.get_max_tenant_id());

}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_arb_gc_utils.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_arb_gc_utils");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
