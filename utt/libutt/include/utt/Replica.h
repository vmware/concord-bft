#pragma once

#include <utt/RandSig.h>>
#include <utt/Params.h>
#include <utt/Tx.h>

namespace libutt::Replica {

std::vector<RandSigShare> signShareOutputCoins(const Tx& tx, const RandSigShareSK& bskShare);

}  // namespace libutt::Replica