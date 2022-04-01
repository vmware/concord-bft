#include <utt/Configuration.h>

#include <cstdlib>

#include <utt/Address.h>
#include <utt/Comm.h>
#include <utt/RandSig.h>
#include <utt/RegAuth.h>

std::ostream& operator<<(std::ostream& out, const libutt::RegAuthPK& rpk) {
    out << rpk.vk;
    out << rpk.mpk;
    return out;
}

std::istream& operator>>(std::istream& in, libutt::RegAuthPK& rpk) {
    in >> rpk.vk;
    in >> rpk.mpk;
    return in;
}


namespace libutt {

    RegAuthSK RegAuthSK::random(const CommKey& ck_reg, const IBE::Params& p)
    {
        RegAuthSK rsk;

        rsk.ck_reg = ck_reg;

        // pick PS16 SK to sign registration commitments
        rsk.sk = RandSigSK::random(ck_reg);

        // pick Boneh-Franklin MSK
        rsk.p = p;
        rsk.msk = IBE::MSK::random();

        return rsk;
    }

    //AddrSK RegAuthSK::randomUser() const {
    //    // WARNING: Not a secure PRNG, but okay for this.
    //    return registerUser(std::to_string(rand()) + "@rand.com");
    //}
    
    AddrSK RegAuthSK::registerRandomUser(const std::string& pid) const {
        AddrSK ask;

        ask.pid = pid;
        ask.pid_hash = AddrSK::pidHash(pid);

        // pick random PRF key
        ask.s = Fr::random_element();

        // Boneh-Franklin IBE
        ask.e = msk.deriveEncSK(p, ask.pid);

        // NOTE: Randomness zero, since the user will always add it in when transacting
        std::vector<Fr> m = { ask.getPidHash(), ask.s, Fr::zero() };
        // need commitment to be in both G1 and G2 for signature verification to work
        ask.rcm = Comm::create(ck_reg, m, true);
        
        // Sign the registration commitment
        ask.rs = sk.sign(ask.rcm, Fr::random_element());

#ifndef NDEBUG
        auto vk = sk.toPK();
        assertTrue(ask.rs.verify(ask.rcm, vk));
        //logdbg << pid << " w/ RCM: " << ask.rcm << endl;
        //logdbg << pid << " w/ regsig: " << ask.rs << endl;
        //logdbg << "RPK::vk " << vk << endl;
#endif

        return ask;
    }

} // end of namespace libutt
