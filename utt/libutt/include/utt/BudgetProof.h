#pragma once

#include <vector>
#include <iostream>

#include <utt/Comm.h>
#include <utt/PolyCrypto.h>

// WARNING: Forward declaration(s), needed for the serialization declarations below
namespace libutt {
    class Comm;
    class CommKey;
    class BudgetProof;
}
 
// WARNING: Declare (de)serialization stream operators in the *global* namespace
std::ostream& operator<<(std::ostream&, const libutt::BudgetProof&);
std::istream& operator>>(std::istream&, libutt::BudgetProof&);

namespace libutt {

    class BudgetProof {
    public:
        std::set<size_t> forMeTxos;
        std::vector<Fr> alpha;
        std::vector<Fr> beta;
        Fr e;

    public:
        size_t getSize() const {
            return 
                // we only need this when measuring general k-to-m txns; for our purposes, we use just 3-to-3 and 2-to-2, so need for this
                //sizeof(size_t) + (sizeof(size_t) * forMeTxos.size()) + 
                sizeof(size_t) + alpha.size() * _fr_size + 
                sizeof(size_t) + beta.size() * _fr_size + 
                _fr_size;
        }

        BudgetProof()
        {}

        BudgetProof(
            const std::set<size_t>& forMeTxos,
            const CommKey& regCK,
            const Comm& rcm,
            const Fr& pid_hash_sender, 
            const Fr& s,    // PRF key
            const Fr& a,    // rcm commitment randomness
            const std::vector<CommKey>& ck,
            const std::vector<Comm>& icm,
            const std::vector<Fr>& ts
        )
            : forMeTxos(forMeTxos)
        {
            size_t numTxos = forMeTxos.size();
            assertEqual(numTxos, ck.size());
            assertEqual(numTxos, icm.size());
            assertEqual(numTxos, ts.size());

#ifndef NDEBUG
            logdbg << "Doing budget proof for TXOs: [ ";
            for(auto txo : forMeTxos) std::cout << txo << ", ";
            std::cout << "]" << endl;
#endif
            
            std::vector<Fr> x = random_field_elems(3);

            assertEqual(rcm.asG1(), Comm::create(regCK, { pid_hash_sender, s, a }, false).asG1());
            G1 X = Comm::create(regCK, x, false).asG1();

            std::vector<G1> Y;
            std::vector<Fr> y;
            for(size_t i = 0; i < numTxos; i++) {
                assertEqual(icm[i].asG1(), Comm::create(ck[i], { pid_hash_sender, ts[i] }, false).asG1());
                y.push_back(Fr::random_element());
                Y.push_back(Comm::create(ck[i], { x[0], y[i] }, false).asG1());
            }

            e = hash(regCK, rcm, ck, icm, X, Y);

            alpha.resize(x.size());
            alpha[0] = x[0] + e * pid_hash_sender; 
            alpha[1] = x[1] + e * s;
            alpha[2] = x[2] + e * a;

            for(size_t i = 0; i < numTxos; i++) {
                beta.push_back(y[i] + e * ts[i]);
            }

            assertTrue(verify(regCK, rcm, ck, icm));
        }

    public:
    
        Fr hash(
            const CommKey& regCK,
            const Comm& rcm,
            const std::vector<CommKey>& ck,
            const std::vector<Comm>& icm,
            const G1& X,
            const std::vector<G1>& Y
        ) const;

        bool verify(
            const CommKey& regCK,
            const Comm& rcm,
            const std::vector<CommKey>& ck,
            const std::vector<Comm>& icm
        ) const {
            if(ck.size() != icm.size() || beta.size() != icm.size()) {
                logerror << "Proof size or verifier input is of wrong size" << endl;
                return false;
            }

            G1 X = Comm::create(regCK, alpha, false).asG1() - (e * rcm.asG1());
            std::vector<G1> Y;

            size_t numTxos = beta.size();
            for(size_t i = 0; i < numTxos; i++) {
                Y.push_back(Comm::create(ck[i], { alpha[0], beta[i] }, false).asG1() - (e * icm[i].asG1()));
            }

            return e == hash(regCK, rcm, ck, icm, X, Y);
        }

    public:
        bool operator==(const BudgetProof& o) const {
            return
                forMeTxos == o.forMeTxos &&
                alpha == o.alpha &&
                beta == o.beta &&
                e == o.e &&
                true;
        }
    };

}
