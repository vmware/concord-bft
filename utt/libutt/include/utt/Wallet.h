#pragma once

#include <vector>

#include <utt/Address.h>
#include <utt/Coin.h>

namespace libutt {
    class RandSig;
    class RegAuthPK;
    class Tx;
    class Wallet;
}

std::ostream& operator<<(std::ostream&, const libutt::Wallet&);
std::istream& operator>>(std::istream&, libutt::Wallet&);

namespace libutt {

    /**
     * A bunch of Coin objects, with useful methods to get coins for creating a TXN!
     */
    class Wallet {
    public:
        Params p;
        AddrSK ask;
        RegAuthPK rpk;
        RandSigPK bpk;
        std::vector<Coin> coins;        // normal coins
        std::optional<Coin> budgetCoin; // single budget coin

    public:
        void addNormalCoin(const Coin& c);
        void setBudgetCoin(const Coin& c);
        void addCoin(const Coin& c) {
            if(c.type == Coin::NormalType()) {
                addNormalCoin(c);
            } else if (c.type == Coin::BudgetType()) {
                setBudgetCoin(c);
            } else {
                throw std::runtime_error("Wrong coin type!");
            }
        }

        size_t numCoins() const { return coins.size(); }

        size_t totalValue() const { return Coin::totalValue(coins); }

        bool hasBudgetCoin() const { return budgetCoin.has_value(); }

        /**
         * Picks the first two coins in the wallet and spends them, sending some to 
         * 'pid' and giving the rest back as change.
         *
         * NOTE: Used for testing and benchmarking.
         *
         * WARNING: Removes the coins from the wallet! Up to the caller to add
         * them back if the TXN fails.
         */
        Tx spendTwoRandomCoins(const std::string& pid, bool removeFromWallet);

        const std::string& getUserPid() const { return ask.pid; }

        void __print() const;

    public:
        bool operator==(const Wallet& o) const {
            return
                p == o.p &&
                ask == o.ask &&
                rpk == o.rpk &&
                bpk == o.bpk &&
                coins == o.coins &&
                budgetCoin == o.budgetCoin &&
                true;
        }

        bool operator!=(const Wallet& o) const {
            return !operator==(o);
        }
    };
}
