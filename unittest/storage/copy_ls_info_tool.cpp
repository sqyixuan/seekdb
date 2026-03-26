/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <iostream>
#include <string>
#include "storage/ob_storage_grpc.h"
#include "lib/net/ob_addr.h"
#include "share/ob_errno.h"
#include "lib/ob_errno.h"

using namespace oceanbase;
using namespace oceanbase::storage;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;

int main(int argc, char **argv) {
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <ip> <port>" << std::endl;
    return 1;
  }

  const char* ip = argv[1];
  int port = std::atoi(argv[2]);

  ObAddr addr;
  if (!addr.set_ip_addr(ip, port)) {
    std::cerr << "Invalid IP/Port: " << ip << ":" << port << std::endl;
    return 1;
  }

  std::cout << "Connecting to " << ip << ":" << port << "..." << std::endl;

  ObStorageGrpcClient client;
  // Initialize with 5 second timeout
  int ret = client.init(addr, 5000); 
  
  if (ret != OB_SUCCESS) {
    std::cerr << "Failed to init ObStorageGrpcClient, ret=" << ret << std::endl;
    return 1;
  }

  std::cout << "Client initialized. Calling copy_ls_info..." << std::endl;

  ObCopyLSInfo result;
  // This calls the gRPC method copy_ls_info
  ret = client.copy_ls_info(result);

  if (ret != OB_SUCCESS) {
    std::cerr << "copy_ls_info failed, ret=" << ret << std::endl;
    return 1;
  }

  std::cout << "copy_ls_info succeeded!" << std::endl;
  std::cout << "Version: " << result.version_ << std::endl;
  std::cout << "Is log sync: " << (result.is_log_sync_ ? "true" : "false") << std::endl;
  std::cout << "Tablet count: " << result.tablet_id_array_.count() << std::endl;
  
  if (result.tablet_id_array_.count() > 0) {
    std::cout << "First 10 tablets: ";
    for (int i = 0; i < std::min((int64_t)10, result.tablet_id_array_.count()); ++i) {
      std::cout << result.tablet_id_array_.at(i) << " ";
    }
    std::cout << std::endl;
  }

  return 0;
}
