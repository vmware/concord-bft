#pragma once
#include <string>
#include <memory>
namespace libutt {
class Params;
}
namespace libutt::api {
class Details {
 public:
  static Details& instance() {
    static Details d;
    return d;
  }
  void init();
  libutt::Params& getParams();

 private:
  std::unique_ptr<libutt::Params> params;
};
}  // namespace libutt::api
