#include "burn.hpp"
#include <utt/BurnOp.h>
#include <utt/RandSig.h>
#include <utt/Coin.h>
#include <utt/Address.h>
#include <utt/Params.h>
#include <utt/Serialization.h>
#include <utt/RegAuth.h>

namespace libutt::api::operations {
Burn::Burn(const GlobalParams& d, const Client& cid, const Coin& coin) : c_{coin} {
  Fr fr_pidhash;
  fr_pidhash.from_words(cid.getPidHash());
  Fr prf;
  prf.from_words(cid.getPRFSecretKey());
  const auto rcm = cid.getRcm();
  const auto rcm_str_sig = std::string(rcm.second.begin(), rcm.second.end());
  const auto rcm_sig = libutt::deserialize<libutt::RandSig>(rcm_str_sig);
  auto& rpk = *(cid.rpk_);
  burn_.reset(new libutt::BurnOp(
      d.getParams(), fr_pidhash, cid.getPid(), *(rcm.first.comm_), rcm_sig, prf, *(coin.coin_), *(cid.bpk_), rpk));
}
std::string Burn::getNullifier() const { return burn_->getNullifier(); }
const Coin& Burn::getCoin() const { return c_; }
}  // namespace libutt::api::operations