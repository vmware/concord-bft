// Concord
//
// Copyright (c) 2018-2022 VMware, Inc. All Rights Reserved.
//
// This product is licensed to you under the Apache 2.0 license (the "License").
// You may not use this product except in compliance with the Apache 2.0
// License.
//
// This product may include a number of subcomponents with separate copyright
// notices and license terms. Your use of these subcomponents is subject to the
// terms and conditions of the subcomponent's license, as noted in the LICENSE
// file.

#pragma once

#include <sstream>

#include "Logger.hpp"

#include "UTTClientApp.hpp"

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UTTDataViewerError : std::runtime_error {
  UTTDataViewerError(const std::string& msg) : std::runtime_error(msg) {}
};

struct UnexpectedPathTokenError : UTTDataViewerError {
  UnexpectedPathTokenError(const std::string& token) : UTTDataViewerError("Unexpected path token: " + token) {}
};

struct IndexEmptyObjectError : UTTDataViewerError {
  IndexEmptyObjectError(const std::string& object) : UTTDataViewerError("Indexed object is empty: " + object) {}
};

struct IndexOutOfBoundsError : UTTDataViewerError {
  IndexOutOfBoundsError(const std::string& object) : UTTDataViewerError("Index out of bounds for object: " + object) {}
};

struct DataView;

/////////////////////////////////////////////////////////////////////////////////////////////////////
struct UTTDataViewer {
  UTTDataViewer(const UTTClientApp& app);
  ~UTTDataViewer();
  UTTDataViewer(UTTDataViewer&& other);
  UTTDataViewer& operator=(UTTDataViewer&& other);

  void handleCommand(const std::string& cmd);

  std::string getCurrentPath() const;

 private:
  std::unique_ptr<DataView> root_;
  const DataView* current_ = nullptr;
};