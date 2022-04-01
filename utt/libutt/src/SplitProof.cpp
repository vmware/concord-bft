#include <utt/Configuration.h>

#include <utt/Coin.h>
#include <utt/Params.h>
#include <utt/SplitProof.h>

#include <xutils/NotImplementedException.h>

std::ostream& operator<<(std::ostream& out, const libutt::SplitProof& pi) {
    out << pi.c << endl;
    out << pi.alpha << endl;

    return out;
}

std::istream& operator>>(std::istream& in, libutt::SplitProof& pi) {
    in >> pi.c;
    libff::consume_OUTPUT_NEWLINE(in);

    in >> pi.alpha;
    libff::consume_OUTPUT_NEWLINE(in);

    return in;
}

namespace libutt {

    SplitProof::SplitProof(
        const Params& p,
        const Fr& pid_hash,
        const Fr& s,
        const Coin& coin,
        const Fr& a,
        const Comm& rcm,
        const std::string& msgToSign)
    {
        std::vector<Fr> x = random_field_elems(8);

        // WARNING: C++ arrays use 0-based indexing but comments use 1-based
        std::vector<G1> X(3, G1::zero());
        G2 X_tilde;
        GT Y_tilde;
        
        // X_1 = g_1^x_1 g_2^x_2 g_3^x_3 g^x_4 
        X[0] = Comm::create(p.getCoinCK(), { x[0], x[1], x[2], Fr::zero(), Fr::zero(), x[3] }, false).asG1();

        // X_2 = g_3^x_3 g^x_5
        X[1] = Comm::create(p.getValCK(), { x[2], x[4] }, false).asG1();

        // Let the registration CK be (g_6, g)
        // X_3 = g_1^x_1 g_6^x_6 g^x_7
        X[2] = Comm::create(p.getRegCK(), { x[0], x[5], x[6] }, false).asG1();

        G2 h_tilde = p.null.h_tilde;
        G2 w_tilde = p.null.w_tilde;
        std::vector<G2> bases = { h_tilde, w_tilde };
        // X_tilde[1] = h_tilde^{x_6 + x_2} w_tilde^x_8
        X_tilde = multiExp<G2>(bases, { x[5] + x[1], x[7] } ); 

        // X_tilde[2] = e(nullif, w_tilde)^x_8 = Y^{x_8/t}, since otherwise we'd have to compute a pairing
        Y_tilde = coin.null.y^(x[7] * coin.t.inverse());

        // Fiat-Shamir for the challenge; implicitly signs message
        c = hash(p, coin.null, rcm, coin.commForTxn(), coin.vcm, X, X_tilde, Y_tilde, msgToSign);

        alpha.resize(8);
        alpha[0] = x[0] + c * pid_hash;
        alpha[1] = x[1] + c * coin.sn;
        alpha[2] = x[2] + c * coin.val;
        alpha[3] = x[3] + c * coin.r;
        alpha[4] = x[4] + c * coin.z;
        alpha[5] = x[6] + c * a;
        alpha[6] = x[5] + c * s;
        alpha[7] = x[7] + c * coin.t;
    }

    bool SplitProof::verify(
        const Params& p,
        const Nullifier& null,
        const Comm& rcm,
        const Comm& ccm,
        const Comm& vcm,
        const std::string& msgSigned) const
    {
        assertEqual(alpha.size(), 8);

        std::vector<G1> X(3, G1::zero());
        G2 X_tilde;
        GT Y_tilde;

        X[0] = Comm::create(p.getCoinCK(), { alpha[0], alpha[1], alpha[2], Fr::zero(), Fr::zero(), alpha[3] }, false).asG1() - (c * ccm.asG1());
        X[1] = Comm::create(p.getValCK(), { alpha[2], alpha[4] }, false).asG1() - (c * vcm.asG1());
        X[2] = Comm::create(p.getRegCK(), { alpha[0], alpha[6], alpha[5] }, false).asG1() - (c * rcm.asG1());

        G2 h_tilde = p.null.h_tilde;
        G2 w_tilde = p.null.w_tilde;
        std::vector<G2> bases = { h_tilde, w_tilde };
        X_tilde = multiExp<G2>(bases, { alpha[6] + alpha[1], alpha[7] } ) - (c * null.vk); 
        auto minus_c = -c;
        Y_tilde = (ReducedPairing(null.n, w_tilde)^alpha[7]) * (null.y^minus_c);

        return c == hash(p, null, rcm, ccm, vcm, X, X_tilde, Y_tilde, msgSigned);
    }

    Fr SplitProof::hash(
        const Params& p, 
        const Nullifier& null, 
        const Comm& rcm,
        const Comm& ccm,
        const Comm& vcm,
        const std::vector<G1>& X,
        const G2& X_tilde,
        const GT& Y_tilde,
        const std::string& msg) const 
    {
        std::stringstream ss;

        ss << p.getCoinCK() << endl;
        ss << p.getRegCK() << endl;
        ss << p.null << endl;

        ss << null << endl;
        ss << rcm << endl;
        ss << ccm << endl;
        ss << vcm << endl;

        ss << X << endl;
        ss << X_tilde << endl;
        ss << Y_tilde << endl;

        ss << msg << endl;

        return hashToField(ss.str());
    }

} // end of libutt

