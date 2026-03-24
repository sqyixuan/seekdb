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
#define private public
#include "test_object_storage.h"
#include "object_storage_authorization_info.h"

namespace oceanbase
{
namespace unittest
{

extern std::vector<Config> all_configs;
INSTANTIATE_TEST_CASE_P(
  ConfigCombinations,
  TestObjectStorage,
  ::testing::ValuesIn(all_configs),
  ObObjectStorageUnittestUtil::custom_test_name
);
}
}

int main(int argc, char **argv)
{
  oceanbase::unittest::ObObjectStorageUnittestUtil::generate_configs_with_checksum(
    oceanbase::unittest::all_configs,
    oceanbase::unittest::Config {
      "FILE",                             /*storage_type*/
      oceanbase::common::OB_FILE_PREFIX,  /*bucket*/
      nullptr,                            /*enpoint*/
      nullptr,                            /*ak*/
      nullptr,                            /*sk*/
      nullptr,                            /*region*/
      nullptr,                            /*appid*/
      nullptr,                            /*entension*/
    },
    {nullptr}
  );

  system("rm -f test_object_storage_list_nfs.log*");
  OB_LOGGER.set_file_name("test_object_storage_list_nfs.log", true, true);
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
