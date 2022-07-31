#include <utt/DataUtils.hpp>
#include <xutils/AutoBuf.h>
#include <utt/Serialization.h>
#include <cstring>
#include <string>
namespace libutt {
IBEEncryptor::IBEEncryptor(const IBE::MPK& mpk) : mpk_{mpk} {}
std::vector<uint8_t> IBEEncryptor::encrypt(const std::string& id, const std::vector<uint8_t>& msg) const {
  AutoBuf<unsigned char> buf(msg.size());
  memcpy(buf.getBuf(), msg.data(), msg.size());
  auto ctxt = mpk_.encrypt(id, buf);
  auto ctxt_str = libutt::serialize<libutt::IBE::Ctxt>(ctxt);
  return std::vector<uint8_t>(ctxt_str.begin(), ctxt_str.end());
}

IBEDecryptor::IBEDecryptor(const IBE::EncSK& esk) : esk_{esk} {}
std::vector<uint8_t> IBEDecryptor::decrypt(const std::vector<uint8_t>& data) const {
  auto ctxt = libutt::deserialize<libutt::IBE::Ctxt>(std::string(data.begin(), data.end()));
  auto [succ, dtxt] = esk_.decrypt(ctxt);
  if (!succ) return {};
  std::vector<uint8_t> ret(dtxt.size());
  memcpy(ret.data(), dtxt.getBuf(), dtxt.size());
  return ret;
}
}  // namespace libutt