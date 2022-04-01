#pragma once

#include <optional>
#include <random>
#include <tuple>

#include <utt/Comm.h>
#include <utt/Kzg.h>
#include <utt/PolyCrypto.h>

#include <xassert/XAssert.h>
#include <xutils/AutoBuf.h>
#include <xutils/Log.h>
#include <xutils/NotImplementedException.h>

namespace libutt {
    class KzgPedEqProof;
    class RangeProof;
}

std::ostream& operator<<(std::ostream& out, const libutt::RangeProof& p);
std::istream& operator>>(std::istream& in, libutt::RangeProof& p);

std::ostream& operator<<(std::ostream& out, const libutt::KzgPedEqProof& pi);
std::istream& operator>>(std::istream& in, libutt::KzgPedEqProof& pi);

namespace libutt {

    class Comm;

    class KzgPedEqProof {
    public:
        G1 c_p;     // re-randomized KZG commitment c' = c^r
        G1 w_p;     // re-randomized KZG witness w' = w^r
        G1 cm_p;    // re-randomized Ped commitment cm' = cm^r
        GT Y;       // e(g1, g2)^{f(i) * r}
        Fr e;

        std::vector<Fr> alpha;

    public: 
        size_t getSize() const {
            return 
                _g1_size + 
                _g1_size + 
                _g1_size + 
                _gt_size + 
                _fr_size
                ;
        }
        KzgPedEqProof() {}

        KzgPedEqProof(
            const KZG::Params& kpp, const CommKey& ck_val,
            const G1& kzgComm, const Fr& eval, const G1& kzgWit, 
            const Comm& pedComm, const Fr& z)
        {
            Fr r = Fr::random_element();

            c_p = r * kzgComm;
            w_p = r * kzgWit;
            cm_p = r * pedComm.asG1();
            Y = kpp.eg1g2^(eval * r);

            std::vector<Fr> x = random_field_elems(5);

            std::vector<G1> X;
            X.push_back(x[0] * kzgComm);
            X.push_back(x[0] * pedComm.asG1());
            X.push_back(Comm::create(ck_val, { x[1], x[2] }, false).asG1());
            X.push_back(Comm::create(ck_val, { x[3], x[4] }, false).asG1());

            GT Z = kpp.eg1g2^x[3];

            e = hash(kpp, ck_val, kzgComm, pedComm, X, Z);

            alpha.push_back(x[0] + e * r);
            alpha.push_back(x[1] + e * eval);
            alpha.push_back(x[2] + e * z);
            alpha.push_back(x[3] + e * eval*r);
            alpha.push_back(x[4] + e * z*r);
        }

    public:
        Fr hash(
            const KZG::Params& kpp,
            const CommKey& ck_val,
            const G1& kzgComm,
            const Comm& pedComm,
            const std::vector<G1>& X,
            const GT& Z
        ) const;
        
        bool verify(
            const KZG::Params& kpp,
            const CommKey& ck_val,
            const G1& kzgComm, const Fr& point, 
            const Comm& pedComm)
        const {
            // verify sigma proof first
            assertEqual(alpha.size(), 5);

            std::vector<G1> X;
            X.push_back(alpha[0] * kzgComm - e * c_p);
            X.push_back(alpha[0] * pedComm.asG1() - e * cm_p);
            X.push_back(Comm::create(ck_val, { alpha[1], alpha[2] }, false).asG1() - (e * pedComm.asG1()));
            X.push_back(Comm::create(ck_val, { alpha[3], alpha[4] }, false).asG1() - (e * cm_p));

            GT Z = (kpp.eg1g2^alpha[3]) * (Y^(-e));

            if(e != hash(kpp, ck_val, kzgComm, pedComm, X, Z)) {
                logerror << "Sigma protocol part of range proof failed verification" << endl;
                return false;
            }

            // actually do the blinded KZG proof verification
            return MultiPairing({ c_p, w_p }, { kpp.g2inv, kpp.g2(1) - point * kpp.g2() }) * Y == GT::one();
            //return ReducedPairing(c_p, kpp.g2()) == ReducedPairing(w_p, kpp.g2(1) - point * kpp.g2()) * Y;
        }
    };

    /**
     * A range proof for the TxOut commitment
     * TODO(Crypto) TODO(Perf): Could probably batch compute RangeProof's
     */
    class RangeProof {
    public:
        class Params {
        public:
            constexpr static size_t MAX_BITS = 64;

            static std::vector<Fr> omegas;  // all Nth roots of unity, where N = MAX_BITS
            static Fr two;                  // literally Fr(2)
            static std::vector<Fr> XNminus1;// X^N - 1, used often
            static std::vector<Fr> XminusLastOmega; // X - \omega^{N-1}, used often

        public:
            CommKey ck_val;
            KZG::Params kpp;

        public:
            Params() {}

        public:
            bool operator==(const Params& o) const {
                return 
                    ck_val == o.ck_val &&
                    kpp == o.kpp &&
                    true;
            }

            bool operator!=(const Params& o) const {
                return !operator==(o);
            }

        public:
            static Params random(const CommKey& ck_val);
            static void initializeOmegas();
        };

    public:
        G1 kzgGamma, kzgQ;
        KzgPedEqProof kzgPed_pi;
        // TODO(Crypto) TODO(Perf): could probably cross-aggregate these KZG eval proofs
        Fr gammaOfRho;
        G1 gammaOfRhoWit;
        Fr gammaOfRhoOmega;
        G1 gammaOfRhoOmegaWit;
        Fr qOfRho;
        G1 qOfRhoWit;

    public:
        size_t getSize() const {
            return 
                _g1_size + _g1_size + 
                kzgPed_pi.getSize() + 
                _fr_size +
                _g1_size + 
                _fr_size + 
                _g1_size + 
                _fr_size + 
                _g1_size
                ;
        }
        // WARNING: Used for deserialization only
        RangeProof() {}

        /**
         * Computes a range proof for g_3^val g^z.
         */
        RangeProof(const Params& p, const Comm& vcm, const Fr& val, const Fr& z);

    public:
        /**
         * Verifies this range proof against the specified value commitment.
         */
        bool verify(const Params& p, const Comm& vcm) const;

        std::tuple<Fr, Fr> deriveTauAndRho(const Params& p, const Comm& vcm) const {
            // TODO(Crypto): Incorporate RangeProofs::Params into hash for \tau
            (void)p;

            Fr tau = hashToField("rangeproof|tau|" + vcm.toString());
            Fr rho = hashToField("rangeproof|rho|" + vcm.toString());

            return std::make_tuple(tau, rho);
        }

    public:
        static std::vector<Fr> correlatedRandomness(const std::vector<Fr>& z, size_t m);

        // returns X^N - 1
        static std::vector<Fr> getAccumulator(size_t N) {
            std::vector<Fr> f(N+1, Fr::zero());

            f[0] = -Fr::one();
            f[N] = Fr::one();

            return f;
        }

        // returns X - root
        static std::vector<Fr> getMonomial(const Fr& root) {
            return { - root, Fr::one() }; 
        }
    };

}

std::ostream& operator<<(std::ostream& out, const libutt::RangeProof::Params& pp);
std::istream& operator>>(std::istream& in, libutt::RangeProof::Params& pp);
