#pragma once

#include <utt/Params.h>
#include <utt/PolyCrypto.h>

#include <xassert/XAssert.h>
#include <xutils/AutoBuf.h>
#include <xutils/Log.h>
#include <xutils/NotImplementedException.h>

namespace libutt {
    class SplitProof;
}

std::ostream& operator<<(std::ostream&, const libutt::SplitProof&);
std::istream& operator>>(std::istream&, libutt::SplitProof&);

namespace libutt {

    class Coin;

    /**
     * Proves that a (partial) ccm, rcm, vcm, nullifier are "consistent"!
     */
    class SplitProof {
    public:
        Fr c;       // the claimed hash
        std::vector<Fr> alpha;

    public:
        size_t getSize() const {
            return 
                _fr_size + 
                sizeof(size_t) + alpha.size()*_fr_size;
        }
        SplitProof() {}

        SplitProof(std::istream& in)
            : SplitProof()
        {
            in >> *this;
        }

        SplitProof(
            const Params& p,
            const Fr& pid_hash, // pid of user
            const Fr& s,        // PRF secret
            const Coin& coin,
            const Fr& a,        // rcm randomness
            const Comm& rcm,
            const std::string& msgToSign);

    public:
        bool verify(
            const Params& p,
            const Nullifier& null,
            const Comm& rcm,
            const Comm& ccm,
            const Comm& vcm,
            const std::string& msgSigned) const;

        Fr hash(
            const Params& p,
            const Nullifier& null, 
            const Comm& rcm,
            const Comm& ccm,
            const Comm& vcm,
            const std::vector<G1>& X,
            const G2& X_tilde,
            const GT& Y_tilde,
            const std::string& msg) const;
    };

} // end of namespace libutt
