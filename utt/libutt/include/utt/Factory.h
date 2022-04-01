#pragma once

#include <utt/Address.h>
#include <utt/Coin.h>
#include <utt/Params.h>
#include <utt/RandSig.h>
#include <utt/RandSigDKG.h>
#include <utt/RegAuth.h>
#include <utt/Wallet.h>

namespace libutt {

/**
 * Useful class for creating a *decentralized* bank, a *centralized* registration authority
 * and a bunch of wallets pre-initialized with coins from them, so that functionality can be
 * tested and benchmarked.
 */
class Factory {
 public:
  RandSigDKG dkg;
  Params p;
  RegAuthSK rsk;

 public:
  /**
   * @param   t   the reconstruction threhsold (you need exactly t shares to reconstruct)
   */
  Factory(size_t t, size_t n)
      : dkg(t, n, Params::NumMessages),
        p(Params::random(dkg.getCK())),
        rsk(RegAuthSK::random(p.getRegCK(), p.getIbeParams())) {}

 public:
  const Params& getParams() const { return p; }

  const RandSigSK& getBankSK() const { return dkg.getSK(); }
  RandSigPK getBankPK() const { return dkg.getSK().toPK(); }

  const RegAuthSK& getRegAuthSK() const { return rsk; }
  RegAuthPK getRegAuthPK() const { return rsk.toPK(); }

  std::vector<RandSigShareSK> getBankShareSKs() const { return dkg.getAllShareSKs(); }

  /**
   * NOTE: Always sets expiration date to Coin SomeExpirationDate()
   */
  Coin mintRandomCoin(const Fr& type, size_t val, const AddrSK& ask) {
    auto sn = Fr::random_element();
    auto exp_date = (type == Coin::BudgetType() ? Coin::SomeExpirationDate() : Coin::DoesNotExpire());
    auto val_fr = Fr(static_cast<long>(val));
    std::string typeStr = Coin::typeToString(type);

    // logdbg << "Minting '" << typeStr << "' coin of value " << val << " for " << ask.pid << endl;
    Coin c(p.getCoinCK(), p.null, sn, val_fr, type, exp_date, ask);

    // sign *full* coin commitment using bank's SK
    c.sig = getBankSK().sign(c.augmentComm());

    return c;
  }

  std::vector<Wallet> randomWallets(size_t numWallets, size_t numCoins, size_t maxDenom, size_t budget) {
    auto bsk = getBankSK();
    std::vector<Wallet> wallets;

    for (size_t i = 0; i < numWallets; i++) {
      // register a randomly-generated user
      AddrSK ask = rsk.registerRandomUser("user" + std::to_string(i + 1));

      Wallet w;
      w.p = p;
      w.ask = ask;
      w.bpk = bsk.toPK();
      w.rpk = rsk.toPK();
      // logdbg << "RPK::vk " << w.rpk.vk << endl;

      // issue some random 'normal' coins to this user
      Coin prevCoin;
      size_t totalVal = 0;
      for (size_t k = 0; k < numCoins; k++) {
        // ...of value less than or equal to the allowed max coin value
        size_t val = static_cast<size_t>(rand()) % maxDenom + 1;
        totalVal += val;

        // ...and everything else random
        Coin c = Factory::mintRandomCoin(Coin::NormalType(), val, ask);
        w.addNormalCoin(c);

        // test serialization
        Coin currCoin;
        std::stringstream ss;
        ss << c;
        ss >> currCoin;
        assertEqual(currCoin, c) assertNotEqual(prevCoin, c);

        prevCoin = c;
      }

      // issue budget coin
      Coin b = Factory::mintRandomCoin(Coin::BudgetType(), budget, ask);

      w.setBudgetCoin(b);

      wallets.push_back(w);
    }

    return wallets;
  }
};

}  // end of namespace libutt
