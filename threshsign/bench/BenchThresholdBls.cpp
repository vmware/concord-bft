#include "threshsign/Configuration.h"

#include "Log.h"

#include <iostream>
#include <fstream>
#include <vector>

#include "lib/IThresholdSchemeBenchmark.h"
#include "lib/Benchmark.h"

#include "app/RelicMain.h"
#include "bls/relic/LagrangeInterpolation.h"

#include "threshsign/bls/relic/BlsThresholdScheme.h"

extern "C" {
# include <relic/relic.h>
# include <relic/relic_err.h>
}

using namespace BLS::Relic;
using std::endl;

class ThresholdBlsRelicBenchmark : public IThresholdSchemeBenchmark {
private:
    G1T h, sig, threshSig;
    G2T pk, gen2;
    BNT sk, g1size, g2size;	// the order of the G1 and G2 group
    GTT e1, e2;

    // to store them in an std::vector: neither std::vector<bn_t> nor std::vector<bn_t*> work.
    std::vector<std::unique_ptr<BlsThresholdSigner>> sks;			// secret key shares for all signers
    std::unique_ptr<BlsThresholdVerifier> verifier;
    std::unique_ptr<BlsThresholdAccumulator> accum;

    std::vector<BlsPublicKey> pks;			// public keys (corresp. to SK shares) for all signers
    std::vector<G1T> shares; 		// signature shares for all signers

public:
    ThresholdBlsRelicBenchmark(const BlsPublicParameters& p, int k, int n)
        : IThresholdSchemeBenchmark(p, k, n)
    {
        //numBenchIters = 2;
        assertStrictlyGreaterThan(k, 0);
        assertLessThanOrEqual(k, n);
        // WARNING: Players are numbered from 1 to n (pos [0] is irrelevant in these arrays)
        sks.resize(static_cast<size_t>(numSigners + 1));
        pks.resize(static_cast<size_t>(numSigners + 1));
        shares.resize(static_cast<size_t>(numSigners + 1));

        // See src/bn/relic_bn_utils.c for serializing BN sk and printing on screen in any base!
        g2_get_gen(gen2);
        g1_get_ord(g1size);
        g2_get_ord(g2size);
        gt_rand(e1);
        gt_rand(e2);

        assertTrue(bn_cmp(g1size, g2size) == CMP_EQ);

        generateKeys();
        loginfo << "Created new threshold BLS benchmark: numIters = " << numBenchIters << ", k = " << k << ", n = " << n
                << ", sec = " << p.getSecurityLevel() << " bits" << endl;
    }

    ~ThresholdBlsRelicBenchmark() {}

protected:
    void initSizes() {
        // Compute SK size
        skBits = bn_size_bin(sk);
        logdbg << "SK size: " << skBits << " bytes" << endl;

        // Compute PK size
        pkBits = g2_size_bin(pk, 1);
        logdbg << "PK size (compressed): " << pkBits << " bytes" << endl;
        logdbg << "PK size (uncompressed): " << g2_size_bin(pk, 0) << " bytes" << endl;

        // Compute signature sizes
        g1_rand(sig);
        sigBits = g1_size_bin(sig, 1);
        logdbg << "Signature size (compressed): " << sigBits << " bytes" << endl;
        logdbg << "Signature size (uncompressed): " << g1_size_bin(sig, 0) << " bytes" << endl;

        // For BLS, sigshare size is just |sig| + ceil(\log_2{numSigners}), but we should probably not count that
        // since we can use the IP address in most application as the identifier.
        sigShareBits = sigBits;
    }

    // TODO: reuse key generation code from ViabilityTest
    void generateKeys() {
        const BlsPublicParameters& blsParams = dynamic_cast<const BLS::Relic::BlsPublicParameters&>(params);
        BlsThresholdKeygen keygen(blsParams, reqSigners, numSigners);
        sk = keygen.getSecretKey();
        pk = keygen.getPublicKey();

        for(ShareID signer = 1; signer <= numSigners; signer++) {
            size_t i = static_cast<size_t>(signer);
            // Assign secret key share
            sks[i].reset(new BlsThresholdSigner(blsParams, signer, keygen.getShareSecretKey(signer)));
            // Compute PK
            pks[i] = sks[i]->getPublicKey();
            assertEqual(pks[i].getPoint(), keygen.getShareVerificationKey(signer));

            logtrace << "Share for player " << i << " is p(" << i << ") = " << sks[i]->getShareSecretKey()
                    << " (with vk = " << pks[i].getPoint() << ")"
                    << endl;
        }

        verifier.reset(new BlsThresholdVerifier(blsParams, pk, reqSigners, numSigners, pks));

        initSizes();
    }

public:
    virtual void multiply1() {
        static G1T tmp;
        // NOTE: Group operation on ECs is addition not multiplication
        g1_add(tmp, sig, sig)
    }
    virtual void multiply2() {
        static G2T tmp;
        // NOTE: Group operation on ECs is addition not multiplication
        g2_add(tmp, pk, pk);
    }
    virtual void multiplyT() {
        static GTT tmp;
        gt_mul(tmp, e1, e2);
    }
    virtual void pairing() {
        static GTT tmp;
        pc_map(tmp, h, pk);
    }

    virtual void hash() {
        g1_map(h, msg, msgSize);
    }

    virtual void signSingle() {
        g1_mul(sig, h, sk);
    }

    virtual void verifySingle() {
        pc_map(e1, h, pk);
        pc_map(e2, sig, gen2);

        if (gt_cmp(e1, e2) != CMP_EQ) {
            throw std::logic_error("Your single signing or verification code is wrong.");
        }
    }

    virtual void signShare(ShareID i) {
        size_t idx = static_cast<size_t>(i);
        shares[idx] = sks[idx]->signData(h);
    }

    virtual void verifyShares() {
        assertNotNull(accum);
        accum->setExpectedDigest(msg, msgSize);
    }

    virtual void computeLagrangeCoeffBegin(const VectorOfShares& signers) {
        (void)signers;
        // Deletes the old accumulator pointer
        accum.reset(dynamic_cast<BlsThresholdAccumulator*>(verifier->newAccumulator(false)));

        // "Accumulate" the shares
        for(ShareID id = signers.first(); signers.isEnd(id) == false; id = signers.next(id)) {
            size_t idx = static_cast<size_t>(id);
            accum->addNumById(id, shares[idx]);
        }
    }

    virtual void computeLagrangeCoeff(const VectorOfShares& signers) {
        (void)signers;
        accum->computeLagrangeCoeff();
    }

    virtual void exponentiateLagrangeCoeff(const VectorOfShares& signers) {
        (void)signers;
        accum->exponentiateLagrangeCoeff();
    }

    virtual void aggregateShares(const VectorOfShares& signers) {
        (void)signers;
        accum->aggregateShares();
        threshSig = accum->getThresholdSignature();
    }

    void sanityCheckThresholdSignature(const VectorOfShares& signers) {
        (void)signers;
        logdbg << "Normal signature:    " << sig << endl;
        logdbg << "Threshold signature: " << threshSig << endl;

        pc_map(e1, h, pk);
        pc_map(e2, threshSig, gen2);

        if (gt_cmp(e1, e2) != CMP_EQ) {
            throw std::logic_error("Your threshold signing or verification code is wrong.");
        }
    }
};

int RelicAppMain(const Library& lib, const std::vector<std::string>& args) {
    (void)args;
    lib.getPrecomputedInverses();

    pc_param_print();
    std::ofstream out("bls-relic-numbers.csv");

    BlsPublicParameters params(PublicParametersFactory::getWhatever());

    loginfo << "FP_PRIME: " << FP_PRIME << endl;
    loginfo << "Security level: " << pc_param_level() << endl;


    std::vector<std::pair<int, int>> nk = Benchmark::getThresholdTestCases(501, false, true, false);

//    for(size_t i = 1; i <= 100; i += 25) {
//        nk.push_back(std::pair<int, int>(i+1, i));
//    }

    int j = 0;
    for(auto it = nk.begin(); it != nk.end(); it++) {
        int n = it->first;
        int k = it->second;

        ThresholdBlsRelicBenchmark b(params, k, n);
        b.start();
        if(j++ == 0)
            b.printResults(out);
        else
            b.printNumbers(out);
    }

    return 0;
}
