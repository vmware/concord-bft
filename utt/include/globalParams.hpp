#pragma once
#include <string>
#include <memory>
namespace libutt {
class Params;
}
namespace libutt::api {
class GlobalParams {
 public:
  static GlobalParams& instance() {
    static GlobalParams d;
    return d;
  }
  void init();
  const libutt::Params& getParams() const;
  libutt::Params& getParams();

 private:
  std::unique_ptr<libutt::Params> params;
};
}  // namespace libutt::api
