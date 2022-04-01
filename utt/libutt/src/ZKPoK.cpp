#include <utt/Configuration.h>

#include <utt/Comm.h>
#include <utt/PolyCrypto.h>
#include <utt/ZKPoK.h>

std::ostream& operator<<(std::ostream& out, const libutt::PedEqProof& pi) {
    out << pi.s << endl;
    out << pi.e << endl;
    return out;
}

std::istream& operator>>(std::istream& in, libutt::PedEqProof& pi) {
    in >> pi.s;
    libff::consume_OUTPUT_NEWLINE(in);
    in >> pi.e;
    libff::consume_OUTPUT_NEWLINE(in);
    return in;
}

std::ostream& operator<<(std::ostream& out, const libutt::ZKPoK& zkpok) {
    out << zkpok.s_m << endl;
    out << zkpok.s_t << endl;
    out << zkpok.e << endl;
    return out;
}

std::istream& operator>>(std::istream& in, libutt::ZKPoK& zkpok) {
    in >> zkpok.s_m;
    libff::consume_OUTPUT_NEWLINE(in);
    in >> zkpok.s_t;
    libff::consume_OUTPUT_NEWLINE(in);
    in >> zkpok.e;
    libff::consume_OUTPUT_NEWLINE(in);
    return in;
}

namespace libutt {

    PedEqProof::PedEqProof(
        const CommKey& ck1,
        const Comm& cm1,
        const Fr& r_1,
        const CommKey& ck2,
        const Comm& cm2,
        const Fr& r_2,
        const Fr& m
    ) {
        std::vector<Fr> r = random_field_elems(3);
        std::vector<G1> X(2, G1::zero());

        // X_0 = g_0^r_0 g^r_1
        X[0] = Comm::create(ck1, { r[0], r[1] }, false).asG1();
        // X_1 = h_0^r_0 h^r_2
        X[1] = Comm::create(ck1, { r[0], r[2] }, false).asG1();

        e = hash(ck1, cm1, ck2, cm2, X);

        s.resize(3);
        s[0] = r[0] + e * m;
        s[1] = r[1] + e * r_1;
        s[2] = r[2] + e * r_2;

        assertTrue(verify(ck1, cm1, ck2, cm2));
    }
        
    bool PedEqProof::verify(
        const CommKey& ck1,
        const Comm& cm1,
        const CommKey& ck2,
        const Comm& cm2
    ) const {
        std::vector<G1> X(2, G1::zero());

        // i.e., g_0^s_0 g^s_1 / cm_1^e
        X[0] = Comm::create(ck1, { s[0], s[1] }, false).asG1() - (e * cm1.asG1());
        // i.e., h_0^s_0 h^s_2 / cm_2^e
        X[1] = Comm::create(ck1, { s[0], s[2] }, false).asG1() - (e * cm2.asG1());

        return e == hash(ck1, cm1, ck2, cm2, X);
    }
    
    Fr PedEqProof::hash(
        const CommKey& ck1,
        const Comm& cm1,
        const CommKey& ck2,
        const Comm& cm2,
        const std::vector<G1>& X
    ) const {
        return hashToField(
            std::string("pedeq-same-msg-diff-rand")
             + "|" + ck1.toString() 
             + "|" + cm1.toString() 
             + "|" + ck2.toString() 
             + "|" + cm2.toString() 
             + "|" + hashToHex(X)
             + ""
        );
    }

    ZKPoK::ZKPoK(
        const CommKey& ck,
        const Comm& cm,
        const Fr& m,
        const Fr& t
    ) {
        // NOTE: Could easily generalize to any number \ell of messages, rather than 1
        assertEqual(ck.numMessages(), 1);

		Fr k_m = Fr::random_element();
		Fr k_t = Fr::random_element();

        Comm R = Comm::create(ck, { k_m, k_t }, false);    // just compute this in G1

		e = ZKPoK::hash(ck, cm, R.asG1());

		s_m = k_m - e * m;
		s_t = k_t - e * t;
    }

    bool ZKPoK::verify(const CommKey& ck, const Comm& cm) const {
        assertEqual(ck.numMessages(), 1);

        auto g_1 = ck.g[0];
        auto g   = ck.getGen1();

        std::vector<G1> bases = { g_1,   g, cm.asG1() };
        std::vector<Fr> exps =  { s_m, s_t, e         };

        auto R = multiExp<G1>(bases, exps);
        auto h = ZKPoK::hash(ck, cm, R);

        return e == h;
    }

    Fr ZKPoK::hash(
        const CommKey& ck, 
        const Comm& cm, 
        const G1& R
    ) const {
        return hashToField(
            std::string("zkpok-ck-size-2")
             + "|" + ck.toString() 
             + "|" + cm.toString() 
             + "|" + hashToHex(R)
             + ""
        );
    }

} // end of libutt

