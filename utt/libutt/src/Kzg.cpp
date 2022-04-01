#include <utt/Configuration.h>

#include <iostream>

#include <utt/Kzg.h>

#include <libfqfft/polynomial_arithmetic/basic_operations.hpp>

std::ostream& operator<<(std::ostream& out, const libutt::KZG::Params& kpp) {
    out << kpp.s << endl;
    out << kpp.q << endl;
    
    assertEqual(kpp.getMaxDegree(), kpp.q);
    for(size_t i = 0; i <= kpp.q; i++) {
        out << kpp.g1(i) << endl;
        out << kpp.g2(i) << endl;
    }

    return out;
}

std::istream& operator>>(std::istream& in, libutt::KZG::Params& kpp) {
#ifndef NDEBUG
    bool verify = true;
    bool progress = true;
#else
    bool verify = false;
    bool progress = false;
#endif

    in >> kpp.s;
    libff::consume_OUTPUT_NEWLINE(in);

    in >> kpp.q;       // we read the q before so as to preallocate the std::vectors
    libff::consume_OUTPUT_NEWLINE(in);

    kpp.resize(kpp.q);

    in >> kpp.g1si[0];
    libff::consume_OUTPUT_NEWLINE(in);

    in >> kpp.g2si[0];
    libff::consume_OUTPUT_NEWLINE(in);

    libutt::G1 g1 = kpp.g1si[0];
    libutt::G2 g2 = kpp.g2si[0];
    libutt::Fr si = kpp.s;  // will store s^i; initialized as s^1 = s
    int prevPct = -1;
    for(size_t i = 1; i <= kpp.q; i++) {
        // read g1^{s^i}
        in >> kpp.g1si[i];
        libff::consume_OUTPUT_NEWLINE(in);

        // read g2^{s^i}
        in >> kpp.g2si[i];
        libff::consume_OUTPUT_NEWLINE(in);

        // Check what we're reading against trapdoors

        //logtrace << g1si[i] << endl;
        //logtrace << si*g1 << endl << endl;
        //logtrace << g2si[i] << endl;
        //logtrace << si*g2 << endl << endl;

        // Fully verify the parameters, if verify is true
        if(verify) {
            testAssertEqual(kpp.g1si[i], si*g1);
            testAssertEqual(kpp.g2si[i], si*g2);
        }
    
        if(progress) {
            int pct = static_cast<int>(static_cast<double>(i)/static_cast<double>(kpp.q+1) * 100.0);
            if(pct > prevPct) {
                //logdbg << pct << "% ... (i = " << i << " out of " << kpp.q+1 << ")" << endl;
                prevPct = pct;
                
                // Occasionally check the parameters
                testAssertEqual(kpp.g1si[i], si*g1);
                testAssertEqual(kpp.g2si[i], si*g2);
            }
        }

        si = si * kpp.s;
    }
    
    kpp.eg1g2 = libutt::ReducedPairing(g1, g2);
    kpp.g2inv = -g2;

    return in;
}

namespace libutt {

namespace KZG {
    
    Params::Params(size_t q)
        : q(q)
    {
        assertStrictlyGreaterThan(q, 0);

        resize(q);
        G1 g1 = G1::random_element();
        G2 g2 = G2::random_element();

        // pick trapdoor s
        s = Fr::random_element();

        Fr si = s^0;
        int prevPct = -1;
        size_t c = 0;

        loginfo << "Generating random q-SDH params, with q = " << q << endl;
        for (size_t i = 0; i <= q; i++) {
            g1si[i] = si * g1;
            g2si[i] = si * g2;
        
            si *= s;

            c++;

            int pct = static_cast<int>(static_cast<double>(c)/static_cast<double>(q + 1) * 100.0);
            if(pct > prevPct) {
                //logdbg << pct << "% ... (i = " << i << " out of " << q << ")" << endl;
                prevPct = pct;
            }
        }

        eg1g2 = ReducedPairing(g1, g2);
        g2inv = -g2;
    }

    std::tuple<std::vector<Fr>, Fr> naiveEval(const std::vector<Fr>& f, const Fr& point) {
        std::vector<Fr> monom(2), rem(1);

        // set the (x - point) divisor
        monom[0] = -point;
        monom[1] = Fr::one();

        // divides f by (x - point)
        std::vector<Fr> q;
        libfqfft::_polynomial_division(q, rem, f, monom);
        assertEqual(q.size(), f.size() - 1);
        assertTrue(libfqfft::_is_zero(rem) || rem.size() == 1);

        return std::make_tuple(q, rem[0]);
    }

    std::tuple<G1, Fr> naiveProve(const Params& kpp, const std::vector<Fr>& f, const Fr& point) {
        std::vector<Fr> q;
        Fr r;

        std::tie(q, r) = naiveEval(f, point);

        // commit to quotient polynomial
        auto proof = commit<G1>(kpp, q);

        return std::make_tuple(proof, r);
    }

    bool Params::verifyProof(const G1& polyComm, const G1& proof, const Fr& value, const Fr& point) const {
        return verifyProof(polyComm, proof, value * g1(), g2(1) - point * g2());
    }

    bool Params::verifyProof(const G1& polyComm, const G1& proof, const G1& valComm, const G2& acc) const {
        return MultiPairing({polyComm - valComm, proof}, {g2inv, acc}) == GT::one(); 
        //return ReducedPairing(polyComm - valComm, g2()) == ReducedPairing(proof, acc);
    }

} // end of KZG
} // end of libutt
