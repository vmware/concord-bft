#include "common.hpp"
#include <utt/RandSig.h>
#include <utt/Comm.h>
#include <utt/Serialization.h>
#include <utt/Params.h>

#include <math.h>
#include <ctime>
#include <chrono>
#include <locale>
#include <sstream>
#include <iomanip>

#if defined(_WIN32)
#define timegm _mkgmtime
#endif
#if defined(_WIN64)
#define timegm _mkgmtime
#endif
namespace libutt::api {
types::Signature Utils::aggregateSigShares(uint32_t n, const std::map<uint32_t, types::Signature>& rsigs) {
  std::vector<libutt::RandSigShare> shares;
  std::vector<size_t> signers;
  for (const auto& [sid, rsig] : rsigs) {
    shares.push_back(libutt::deserialize<libutt::RandSigShare>(rsig));
    signers.push_back((size_t)sid);
  }

  auto csig = libutt::RandSigShare::aggregate((size_t)(n), shares, signers);
  auto str_csig = libutt::serialize<libutt::RandSig>(csig);
  return types::Signature(str_csig.begin(), str_csig.end());
}
types::Signature Utils::unblindSignature(const GlobalParams& p,
                                         Commitment::Type t,
                                         const std::vector<types::CurvePoint>& randomness,
                                         const types::Signature& sig) {
  libutt::RandSig rsig = libutt::deserialize<libutt::RandSig>(sig);
  std::vector<Fr> fr_randomness(randomness.size());
  for (size_t i = 0; i < randomness.size(); i++) fr_randomness[i].from_words(randomness[i]);
  libutt::CommKey ck = Commitment::getCommitmentKey(p, t);
  std::vector<G1> g = ck.g;  // g_1, g_2, \dots, g_\ell, g
  g.pop_back();              // g_1, g_2, \dots, g_\ell

  rsig.s2 = rsig.s2 - multiExp(g, fr_randomness);
  auto str_ret = libutt::serialize<libutt::RandSig>(rsig);
  return types::Signature(str_ret.begin(), str_ret.end());
}

uint64_t Utils::getExpirationDateAsUint(const std::string& exp_date) {
  std::tm due_date = {};
  std::istringstream ss(exp_date);
  if (!(ss >> std::get_time(&due_date, "%Y-%m-%d %H:%M:%S"))) return 0;
  auto ret = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::from_time_t(timegm(&due_date)).time_since_epoch())
                 .count();
  return (uint64_t)ret;
}

std::string Utils::getStrExpirationDateFromUint(uint64_t exp_date) {
  auto tp = std::chrono::time_point<std::chrono::system_clock>(std::chrono::milliseconds(exp_date));
  auto tt = std::chrono::system_clock::to_time_t(tp);
  std::tm* due_date;
  due_date = std::gmtime(&tt);
  std::stringstream ss;
  ss << std::put_time(due_date, "%Y-%m-%d %H:%M:%S");
  auto ret = ss.str();
  return ret;
}
}  // namespace libutt::api