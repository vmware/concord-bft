#include "burn.hpp"
#include "nullifier.hpp"
#include <utt/BurnOp.h>
#include <utt/RandSig.h>
#include <utt/Coin.h>
#include <utt/Address.h>
#include <utt/Params.h>
#include <utt/Serialization.h>
#include <utt/RegAuth.h>

namespace libutt::api::operations {
Burn::Burn(Details& d, const ClientIdentity& cid, const Coin& c) {
  Fr fr_pidhash;
  fr_pidhash.from_words(cid.getPidHash());
  Fr prf;
  prf.from_words(cid.getPRFSecretKey());
  const auto rcm = cid.getRcm();
  const auto rcm_str_sig = std::string(rcm.second.begin(), rcm.second.end());
  const auto rcm_sig = libutt::deserialize<libutt::RandSig>(rcm_str_sig);
  auto& rpk = *(cid.rpk_);
  burn_.reset(new libutt::BurnOp(
      d.getParams(), fr_pidhash, cid.getPid(), *(rcm.first.comm_), rcm_sig, prf, *(c.coin_), *(cid.bpk_), rpk));
  coin_ = c;
}
std::string Burn::getNullifier() const { return coin_.getNullifier(); }
}  // namespace libutt::api::operations