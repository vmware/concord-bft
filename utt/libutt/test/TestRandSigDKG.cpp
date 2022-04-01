#include <utt/Configuration.h>

#include <utt/Comm.h>
#include <utt/RandSigDKG.h>
#include <utt/Utils.h>

#include <cmath>
#include <ctime>
#include <iostream>
#include <fstream>
#include <random>
#include <stdexcept>
#include <vector>

#include <xassert/XAssert.h>
#include <xutils/Log.h>
#include <xutils/NotImplementedException.h>
#include <xutils/Timer.h>
#include <xutils/Utils.h>

using namespace std;
using namespace libutt;

void testRandSigDKG(size_t t, size_t n, size_t ell) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<long> udi(1, 1024);

    // create a threshold bank
    RandSigDKG dkg(t, n, ell);
    CommKey ck = dkg.getCK();
    //logdbg << "CK: " << endl << ck << endl;

    // pick signature randomness
    Fr u = Fr::random_element();
    G1 h = u * ck.getGen1();
    
    // pick ell messages and their commitment randomness
    std::vector<Fr> m = random_field_elems(ell);
    std::vector<Fr> r = random_field_elems(ell);

    logdbg << "Committing and signing " << ell << " messages" << endl;

    //Fr e = Fr::one();
    //for(size_t k = 0; k < ell; k++) {
    //    r.push_back(Fr::zero());
    //    m.push_back(e);
    //    e = e + Fr::one();
    //}

    // produce commitment to be signed (as a series of individual commitments h^{m_i} g^{r_i})
    CommKey ck_ps;
    ck_ps.g.push_back(h);
    ck_ps.g.push_back(ck.getGen1());

    auto comms = Comm::create(ck_ps, m, r, false);
    for(size_t k = 0; k < ell; k++) {
        logdbg << "comm[" << k << "]: " << endl << comms[k] << endl;
    }

    // produce signature shares
    std::vector<RandSigSharePK> pkShares;
    std::vector<RandSigShare> sigShares;
    for(size_t i = 0; i < n; i++) {
        auto& skShare_tmp = dkg.getShareSK(i);
        //logdbg << "skShare[" << i << "]: " << endl << skShare << endl;

        // test (de)serialization of RandSigShareSK
        std::stringstream ss;
        RandSigShareSK skShare;
        ss << skShare_tmp;
        ss >> skShare;
        testAssertEqual(skShare, skShare_tmp);

        auto pkShare_tmp = skShare.toPK();
        
        // test (de)serialization of RandSigSharePK
        RandSigSharePK pkShare;
        ss << pkShare_tmp;
        ss >> pkShare;
        testAssertEqual(pkShare, pkShare_tmp);

        pkShares.push_back(pkShare);
        sigShares.push_back(skShare.shareSign(comms, h));

        testAssertTrue(sigShares.back().verify(comms, pkShare));
    }

    // TODO: if we're gonna do this often, put it in its own function
    std::vector<size_t> signerIds = random_subset(t, n);
    std::vector<RandSigShare> subsetSigShares;
    std::vector<RandSigSharePK> subsetPkShares;
    for(auto id : signerIds) {
        subsetSigShares.push_back(sigShares[id]);
        subsetPkShares.push_back(pkShares[id]);
    }

    RandSig threshSig1 = RandSigShare::aggregate(n, subsetSigShares, signerIds, ck, r);

    // also check PK interpolates correctly from share PKs using RandSigDKG::aggregate
    RandSigPK pk_temp = RandSigDKG::aggregatePK(n, subsetPkShares, signerIds);
    testAssertEqual(dkg.getPK(), pk_temp);

    // making sure CK and PS16 PK has the same G2 vector
    auto ck_g2 = ck.g_tilde;
    ck_g2.pop_back();
    testAssertEqual(dkg.getPK().Y_tilde, ck_g2);

    // assert threshold signature verifies as normal signature
    // for this, we need a vector commitment to all the messages using randomness zero
    m.push_back(Fr::zero());
    Comm cm = Comm::create(ck, m, true);

    // also sign naively and make sure aggregated signature matches
    RandSig threshSig2 = dkg.sk.sign(cm, u);
    logdbg << "threshSig1: " << endl << threshSig1 << endl;
    logdbg << "threshSig2: " << endl << threshSig2 << endl;

    testAssertTrue(threshSig2.verify(cm, dkg.getPK()));
    testAssertTrue(threshSig1.verify(cm, dkg.getPK()));
    if(threshSig1 != threshSig2) {
        testAssertFail("Threshold signature did not match normal signature");
    }

    // rerandomize and re-verify
    Fr r_delta = Fr::random_element();
    Fr u_delta = Fr::random_element();
    cm.rerandomize(ck, r_delta);
    threshSig1.rerandomize(r_delta, u_delta);
    testAssertTrue(threshSig1.verify(cm, dkg.getPK()));
}

int main(int argc, char *argv[]) {
    libutt::initialize(nullptr, 0);
    //srand(static_cast<unsigned int>(time(NULL)));
    (void)argc;
    (void)argv;

    size_t t = 12;
    size_t n = 20;
    size_t ell = 5;
    testRandSigDKG(t, n, ell);

    loginfo << "All is well." << endl;

    return 0;
}


/*
alias emake='cd libutt && ./scripts/docker/install.sh && cd ..'
alias dmake='make -f Makefile.docker'
*/
