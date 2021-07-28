// Copyright 2018 VMware, all rights reserved

/**
 * Status stores the result of an operation.
 */

#pragma once

#include <string>

namespace concordUtils {

class Status {
 public:
  static Status OK() { return Status(ok, ""); }
  static Status NotFound(const std::string& msg) { return Status(notFound, msg); }
  static Status InvalidArgument(const std::string& msg) { return Status(invalidArgument, msg); }
  static Status IllegalOperation(const std::string& msg) { return Status(illegalOperation, msg); }
  static Status GeneralError(const std::string& msg) { return Status(generalError, msg); }
  static Status InterimError(const std::string& msg) { return Status(interimError, msg); }

  bool isOK() const { return type == ok; }
  bool isNotFound() const { return type == notFound; }
  bool isInvalidArgument() const { return type == invalidArgument; }
  bool isIllegalOperation() const { return type == illegalOperation; }
  bool isGeneralError() const { return type == generalError; }

  std::string toString() const { return messagePrefix() + (!isOK() ? message : std::string("")); }
  std::ostream& operator<<(std::ostream& s) const {
    s << toString();
    return s;
  };

  bool operator==(const Status& status) const { return type == status.type; };
  bool operator!=(const Status& status) const { return type != status.type; };

 private:
  enum statusType { ok, notFound, invalidArgument, illegalOperation, generalError, interimError };

  statusType type;
  std::string message;

  Status(statusType t, const std::string& msg) : type(t), message(msg) {}

  std::string messagePrefix() const {
    switch (type) {
      case ok:
        return "OK";
      case notFound:
        return "Not Found: ";
      case invalidArgument:
        return "Invalid Argument: ";
      case illegalOperation:
        return "Illegal Operation: ";
      case generalError:
        return "General Error: ";
      case interimError:
        return "Interim Error: ";
      default:
        return "Unknown Error Type: ";
    }
  }
};

std::ostream& operator<<(std::ostream& s, Status const& status);

}  // namespace concordUtils
