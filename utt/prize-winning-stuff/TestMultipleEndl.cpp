#include <utt/Configuration.h>

#include <utt/PolyCrypto.h>
#include <utt/PolyOps.h>
#include <utt/Utt.h>

#include <utt/Utils.h>

#include <cmath>
#include <ctime>
#include <iostream>
#include <fstream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <xassert/XAssert.h>
#include <xutils/Log.h>
#include <xutils/NotImplementedException.h>
#include <xutils/Timer.h>
#include <xutils/Utils.h>

using namespace std;
using namespace libfqfft;
using namespace libutt;

void testMintFlow(size_t t, size_t n) {
    (void)t, (void)n;
    // First we generate the config files
    auto p = Params::Random();

    BankThresholdKeygen keys(p, t, n);
    CoinSecrets cs[2] = { 
        CoinSecrets(p), 
        CoinSecrets(p)
    };
    CoinComm cc[2] = {
        CoinComm{p, cs[0]},
        CoinComm{p, cs[1]}
    };
    AddrSK ask = AddrSK::random();
    std::string pid = ask.pid;

    std::vector<std::tuple<CoinSecrets, CoinComm, CoinSig>> my_coins;

    for(auto j=0ul; j<2;j++) {
        auto csign = keys.sk.thresholdSignCoin(p, cc[j], keys.u);
        my_coins.push_back(std::make_tuple(cs[j], cc[j], csign));
    }

    std::vector<std::tuple<std::string, Fr>> recv;
    for(auto j=0ul; j<2ul;j++) {
        auto r = std::get<0>(my_coins[j]).val;
        recv.push_back(std::make_tuple(pid, r));
    }

    auto tx = Tx::create(p, my_coins, recv);
    std::stringstream ss;
    ss << tx;

    Tx tx2(ss);

    testAssertEqual(tx.ins.size(), tx2.ins.size());
    testAssertEqual(tx.outs.size(), tx2.outs.size());
    for(auto i=0ul;i<2;i++) {
        testAssertEqual(tx.ins[i].null.n, tx2.ins[i].null.n);
        testAssertEqual(tx.ins[i].cm_val, tx2.ins[i].cm_val);
        testAssertEqual(tx.ins[i].cc, tx2.ins[i].cc);
        testAssertEqual(tx.ins[i].sig, tx2.ins[i].sig);
        testAssertEqual(tx.ins[i].pi.h, tx2.ins[i].pi.h);
        testAssertEqual(tx.ins[i].pi.a, tx2.ins[i].pi.a);
        testAssertEqual(tx.ins[i].pi.b, tx2.ins[i].pi.b);

        testAssertEqual(tx.outs[i].epk.v1, tx2.outs[i].epk.v1);
        testAssertEqual(tx.outs[i].epk.pok, tx2.outs[i].epk.pok);
        testAssertEqual(tx.outs[i].epk.v2.has_value(), tx2.outs[i].epk.v2.has_value());
        if(tx.outs[i].epk.v2.has_value()) {
            testAssertEqual(tx.outs[i].epk.v2.value(), tx2.outs[i].epk.v2.value());
        }
        testAssertEqual(tx.outs[i].cc_val, tx2.outs[i].cc_val);
        testAssertEqual(tx.outs[i].ctxt.R, tx2.outs[i].ctxt.R);
        for(auto j=0ul; j<KeyPrivateCtxt::IV_SIZE; j++) {
            testAssertEqual(tx.outs[i].ctxt.iv[j], tx2.outs[i].ctxt.iv[j]);
        }
        testAssertEqual(tx.outs[i].ctxt.buf.size(), tx2.outs[i].ctxt.buf.size());
        auto len = tx.outs[i].ctxt.buf.size();
        for(auto j=0;j<len;j++) {
            testAssertEqual(tx.outs[i].ctxt.buf[j], tx2.outs[i].ctxt.buf[j]);
        }

        testAssertEqual(tx.outs[i].epk_dh, tx2.outs[i].epk_dh);
    }   
}

int main(int argc, char *argv[]) {
    initialize(nullptr, 0);
    //srand(static_cast<unsigned int>(time(NULL)));
    (void)argc;
    (void)argv;

    size_t t = 12;
    size_t n = 20;
    testMintFlow(t, n);

    loginfo << "All is well." << endl;

    return 0;
}


/*
alias emake='cd libutt && ./scripts/docker/install.sh && cd ..' && alias dmake='make -f Makefile.docker'
*/
