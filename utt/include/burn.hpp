#pragma once
#include "coin.hpp"
#include "clientIdentity.hpp"
#include "details.hpp"
#include "bankIdentity.hpp"
#include "nullifier.hpp"
#include <memory>
namespace libutt {
    class BurnOp;
}
namespace libutt::api::operations {
    class Burn {
        public:
        Burn(Details& d, const ClientIdentity& cid, const Coin& c);
        std::string getNullifier() const;
        public:
        friend class libutt::api::BankIdentity;
        friend class libutt::api::ClientIdentity;
        std::shared_ptr<libutt::BurnOp> burn_; 
        Coin coin_;
    };
}