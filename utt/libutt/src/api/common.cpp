#include "common.hpp"
#include <utt/RandSig.h>
#include <utt/Comm.h>
#include <utt/Serialization.h>
#include <utt/Params.h>

#include <math.h>
#include <ctime>
#include <time.h>
#include <chrono>
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
  struct tm due_date;
  strptime(exp_date.c_str(), "%Y-%m-%d %T", &due_date);
  auto ulong_exp_date = std::pow(10, 10) * (due_date.tm_year + 1900) + std::pow(10, 8) * (due_date.tm_mon + 1) +
                        std::pow(10, 6) * due_date.tm_mday + std::pow(10, 4) * due_date.tm_hour +
                        std::pow(10, 2) * due_date.tm_min + due_date.tm_sec;
  return (uint64_t)ulong_exp_date;
}

std::string Utils::getStrExpirationDateFromUint(uint64_t exp_date) {
  std::string ret;
  uint16_t year = (uint16_t)(exp_date / (uint64_t)std::pow(10, 10));
  exp_date %= (uint64_t)std::pow(10, 10);
  uint16_t month = (uint16_t)(exp_date / (uint64_t)std::pow(10, 8));
  auto m_str = month >= 10 ? std::to_string(month) : "0" + std::to_string(month);
  exp_date %= (uint64_t)std::pow(10, 8);
  uint16_t day = (uint16_t)(exp_date / (uint64_t)std::pow(10, 6));
  auto d_str = month >= 10 ? std::to_string(day) : "0" + std::to_string(day);
  exp_date %= (uint64_t)std::pow(10, 6);
  uint16_t hours = (uint16_t)(exp_date / (uint64_t)std::pow(10, 4));
  auto h_str = month >= 10 ? std::to_string(hours) : "0" + std::to_string(hours);
  exp_date %= (uint64_t)std::pow(10, 4);
  uint16_t minutes = (uint16_t)(exp_date / (uint64_t)std::pow(10, 2));
  auto min_str = month >= 10 ? std::to_string(minutes) : "0" + std::to_string(minutes);
  exp_date %= (uint64_t)std::pow(10, 2);
  uint16_t seconds = (uint16_t)(exp_date);
  auto sec_str = month >= 10 ? std::to_string(seconds) : "0" + std::to_string(seconds);
  return std::to_string(year) + "-" + m_str + "-" + d_str + " " + h_str + ":" + min_str + ":" + sec_str;
}
}  // namespace libutt::api