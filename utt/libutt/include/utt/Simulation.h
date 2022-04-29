
#pragma once

#include <utt/Address.h>
#include <utt/Coin.h>
#include <utt/Factory.h>
#include <utt/Params.h>
#include <utt/RandSig.h>
#include <utt/RandSigDKG.h>
#include <utt/RegAuth.h>
#include <utt/Tx.h>
#include <utt/Utils.h>
#include <utt/Wallet.h>

using namespace libutt;

namespace libutt::Simulation {
struct Context {
  size_t n_ = 0;
  size_t thresh_ = 0;

  Params p_;
  RegAuthSK rsk_;
  RandSigSK bsk_;
  std::vector<RandSigShareSK> bskShares_;

  RegAuthPK rpk_;
  RandSigPK bpk_;
};

void initialize();
Context createContext(size_t n, size_t thresh);

Wallet createWallet(const Context& ctx,
                    const std::string& pid,
                    const std::vector<size_t>& normalCoinVals,
                    size_t budgetCoinVal);

void assertSerialization(Wallet& inOutWallet);
Tx sendValidTxOnNetwork(const Context& ctx, const Tx& inTx);

void addNullifiers(const Tx& tx, std::set<std::string>& nullset);

Coin createNormalCoin(const Context& ctx, size_t val, AddrSK& ask);
Coin createBudgetCoin(const Context& ctx, size_t val, AddrSK& ask);

size_t calcBalance(const Wallet& w);
size_t calcBudget(const Wallet& w);

void replicasSignOutputCoins(Context& ctx, Tx& tx, size_t txoIdx, std::vector<RandSigShare>& sigShares);
std::optional<Coin> tryClaimCoin(Context& ctx,
                                 Tx& tx,
                                 size_t txoIdx,
                                 const std::vector<RandSigShare>& sigShares,
                                 const std::vector<size_t>& signerIds,
                                 const AddrSK& ask);

// Transactions
void doRandomPayment_2t2(
    const Context& ctx, Wallet& w1, Wallet& w2, std::set<std::string>& nullset, std::vector<Wallet>& wallets);
void doRandomPayment_2t1(
    const Context& ctx, Wallet& w1, Wallet& w2, std::set<std::string>& nullset, std::vector<Wallet>& wallets);
void doRandomPayment_1t2(
    const Context& ctx, Wallet& w1, Wallet& w2, std::set<std::string>& nullset, std::vector<Wallet>& wallets);
void doRandomPayment_1t1(
    const Context& ctx, Wallet& w1, Wallet& w2, std::set<std::string>& nullset, std::vector<Wallet>& wallets);
void doRandomCoinSplit(const Context& ctx, Wallet& w, std::set<std::string>& nullset, std::vector<Wallet>& wallets);
void doRandomCoinMerge(const Context& ctx, Wallet& w, std::set<std::string>& nullset, std::vector<Wallet>& wallets);

void doPayment_2t2(const Context& ctx,
                   Wallet& w1,
                   Wallet& w2,
                   size_t c1,
                   size_t c2,
                   size_t amount,
                   std::set<std::string>& nullset,
                   std::vector<Wallet>& wallets);
void doPayment_2t1(const Context& ctx,
                   Wallet& w1,
                   Wallet& w2,
                   size_t c1,
                   size_t c2,
                   std::set<std::string>& nullset,
                   std::vector<Wallet>& wallets);
void doPayment_1t2(const Context& ctx,
                   Wallet& w1,
                   Wallet& w2,
                   size_t c1,
                   size_t amount,
                   std::set<std::string>& nullset,
                   std::vector<Wallet>& wallets);
void doPayment_1t1(const Context& ctx,
                   Wallet& w1,
                   Wallet& w2,
                   size_t c1,
                   std::set<std::string>& nullset,
                   std::vector<Wallet>& wallets);
void doCoinSplit(const Context& ctx,
                 Wallet& w,
                 size_t c1,
                 size_t amount,
                 std::set<std::string>& nullset,
                 std::vector<Wallet>& wallets);
void doCoinMerge(
    const Context& ctx, Wallet& w, size_t c1, size_t c2, std::set<std::string>& nullset, std::vector<Wallet>& wallets);

}  // namespace libutt::Simulation