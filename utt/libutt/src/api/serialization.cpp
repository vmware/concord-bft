#include "serialization.hpp"

#include <utt/Serialization.h>

namespace libutt::api {
std::vector<uint8_t> serializeCurvePoint(const types::CurvePoint& curvePoint) {
  std::stringstream ss;
  serializeVector(ss, curvePoint);
  const auto& str_data = ss.str();
  return std::vector<uint8_t>(str_data.begin(), str_data.end());
}

types::CurvePoint deserializeCurvePoint(const std::vector<uint8_t>& data) {
  std::stringstream ss;
  std::string str_data(data.begin(), data.end());
  ss.str(str_data);
  types::CurvePoint ret;
  deserializeVector(ss, ret);
  return ret;
}
}  // namespace libutt::api