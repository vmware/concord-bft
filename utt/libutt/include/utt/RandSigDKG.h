#pragma once

#include <array>
#include <cstdlib>
#include <istream>
#include <optional>
#include <ostream>
#include <random>
#include <tuple>

#include <libff/algebra/scalar_multiplication/multiexp.hpp>

#include <utt/Comm.h>
#include <utt/RandSig.h>
#include <utt/PolyCrypto.h>

#include <xassert/XAssert.h>
#include <xutils/AutoBuf.h>
#include <xutils/Log.h>
#include <xutils/NotImplementedException.h>

namespace libutt {

    class RandSigDKG {
    public:
        CommKey ck;             // the commitment key for the signed commitments
        RandSigSK sk;           // the secret-shared SK (useful for testing)

        std::vector<RandSigShareSK> skShares;
        //std::vector<RandSigSharePK> pkShares;

    public:
        /**
         * t = # of shares needed to reconstruct
         * n = # of total players
         * ell = number of messages that can be committed to and signed
         */
        RandSigDKG(size_t t, size_t n, size_t ell);

    public:
        const RandSigSK& getSK() const { return sk; }
        RandSigPK getPK() const { return sk.toPK(); }

        CommKey getCK() const { return ck; }

        /**
         * Returns the share of the SK/PK for the i'th server, with i \in \{0, 1, 2, \dots, n-1\}
         */
        //std::tuple<RandSigShareSK, RandSigSharePK> getKeyPair(size_t i) const {
        //    return std::make_tuple(skShares.at(i), pkShares.at(i));
        //}

        const RandSigShareSK& getShareSK(size_t i) const {
            return skShares.at(i);
        }
        
        std::vector<RandSigShareSK> getAllShareSKs() const { return skShares; }

        /**
         * Writes the key-pair of each server 'i' in dirPath + '/' + fileNamePrefix + std::to_string(i), for all i \in [0,n)
         */
        void writeToFiles(const std::string& dirPath, const std::string& fileNamePrefix) const;

        /**
         * NOTE: Used only for testing that PK aggregation works, even though we never actually do it in our application.
         */
        static RandSigPK aggregatePK(
            size_t n,
            std::vector<RandSigSharePK> &pkShares,
            std::vector<size_t> &ids
        );
    };
}
