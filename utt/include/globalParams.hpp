#pragma once
#include <string>
#include <memory>
namespace libutt {
class Params;
}
namespace libutt::api {
class GlobalParams {
  static bool initialized;

 public:
  // static GlobalParams& instance() {
  //   static GlobalParams d;
  //   return d;
  // }
  GlobalParams() = default;
  void init();
  const libutt::Params& getParams() const;
  libutt::Params& getParams();
  GlobalParams(const GlobalParams& other);
  GlobalParams& operator=(const GlobalParams& other);

 private:
  std::unique_ptr<libutt::Params> params;
};
}  // namespace libutt::api
