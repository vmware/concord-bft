// UTT Client API
//
// Copyright (c) 2020-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the sub-component's license, as noted in the LICENSE
// file.

#include <iostream>
#include <string>
#include <sstream>

#include "wallet.hpp"

int main(int argc, char* argv[]) {
  (void)argc;
  (void)argv;

  if (argc != 2) {
    std::cout << "Provide GRPC server host:port\n";
    return 0;
  }
  std::cout << "Privacy library service grpc server starting at : " << argv[1] << std::endl;
  try {
    PrivacyWalletService svc;
    svc.StartServer(argv[1]);
    svc.Wait();
  } catch (const std::runtime_error& e) {
    std::cout << "Error (exception): " << e.what() << '\n';
    return 1;
  }
  return 0;
}