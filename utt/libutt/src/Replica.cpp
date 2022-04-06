
#include <utt/Replica.h>

namespace libutt::Replica {

std::vector<RandSigShare> signShareOutputCoins(const Tx& tx, const RandSigShareSK& bskShare) {

    std::vector<RandSigShare> sigShares;

    // replica goes through every TX output and signs it
    for (size_t txoIdx = 0; txoIdx < tx.outs.size(); txoIdx++) {
        auto sigShare = tx.shareSignCoin(txoIdx, bskShare);

        // check this verifies before serialization
        testAssertTrue(tx.verifySigShare(txoIdx, sigShare, bskShare.toPK()));

        sigShares.push_back(sigShare);
    }

    return sigShares;
}

}  // namespace libutt::Replica