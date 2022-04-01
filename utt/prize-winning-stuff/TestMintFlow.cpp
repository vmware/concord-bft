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
  auto p = libutt::Params::Random();

  std::string dirname = "/tmp";
  std::string fileNamePrefix = "utt_pvt_replica_";
  std::string clientFileName = "utt_pub_client.data";
  bool debug = false;

  // Since we are deterministic here, delete the files if they already exist to prevent libutt from complaining about
  // and failing other builds
  for (size_t i = 0; i < n; i++) {
    std::string to_delete = dirname + "/" + fileNamePrefix + std::to_string(i);
    remove(to_delete.c_str());
  }

  BankThresholdKeygen keys(p, t, n);
  keys.writeToFiles(dirname, fileNamePrefix);

  std::ofstream cliFile(dirname + "/" + clientFileName);
  cliFile << p;
  if (debug) {
    std::cout << "Wrote Params: " << p << std::endl;
  }

  auto skShares = keys.getAllShareSKs();
  std::vector<BankSharePK> pkShares;
  for (size_t i = 0; i < n; i++) {
    pkShares.push_back(skShares[i].toSharePK(p));
  }

  for (size_t i = 0; i < n; i++) {
    cliFile << pkShares[i];
    if (debug) {
      std::cout << "Wrote PK shares: " << pkShares[i] << std::endl;
    }
  }
  cliFile << keys.getPK(p);
  if (debug) {
    std::cout << "Wrote: " << keys.getPK(p) << std::endl;
  }
  cliFile.close();

  // First the client will read it from the config
  // Load the params file (move it out)
  std::ifstream ifile(dirname + "/" + clientFileName);
  testAssertEqual(false, ifile.fail());

  libutt::Params p2(ifile);
  std::vector<libutt::BankSharePK> bank_pks;
  for (size_t i = 0; i < n; i++) {
    libutt::BankSharePK bspk(ifile);
    //   ifile >> bspk;
    bank_pks.push_back(bspk);
  }
  libutt::BankPK main_pk(ifile);
  // ifile >> main_pk;

  ifile.close();

  // The params are the same before and after serialization
  testAssertEqual(p, p2);
  for (size_t i = 0; i < n; i++) {
    // All the read pks are the same
    testAssertEqual(bank_pks[i].X, pkShares[i].X);
  }
  // The main pk before and after are the same
  testAssertEqual(main_pk.X, keys.getPK(p).X);

  // Testing if the servers are reading the files properly
  for (size_t i = 0; i < n; i++) {
    std::string ifile = dirname + "/" + "utt_pvt_replica_" + std::to_string(i);
    std::ifstream fin(ifile);

    testAssertEqual(fin.fail(), false);
    auto p3 = libutt::Params::Random();
    fin >> p3;

    testAssertEqual(p3, p);

    auto shareSK = libutt::BankShareSK(fin);
    // fin >> *shareSK;

    testAssertEqual(shareSK, skShares[i]);
  }

  // The client creates a new coin
  auto esk = libutt::ESK::random();
  auto epk = esk.toEPK(p);
  long val = 1000;
  // DONE: Test separately that the messages sent are received by the replicas in the same way

  for (size_t i = 0; i < n; i++) {
    testAssertEqual(true, libutt::EpkProof::verify(p, epk));
    libutt::CoinComm cc(p, epk, val);
    libutt::CoinSigShare coin = skShares[i].sign(p, cc);

    libutt::CoinComm cc2(p, esk.toEPK(p), val);
    testAssertEqual(coin.verify(p, cc2, bank_pks[i]), true);
  }

  // Now let the servers sign the message
  std::vector<libutt::CoinSigShare> sigShares;
  std::vector<size_t> ids;

  for (size_t i = 0; i < t; i++) {
    auto id = (i + 10) % n;
    libutt::CoinComm cc(p, epk, val);
    libutt::CoinSigShare coin = skShares[id].sign(p, cc);

    std::stringstream ss;
    ss << coin;

    libutt::CoinSigShare coin2(ss);

    testAssertEqual(coin.s1, coin2.s1);
    testAssertEqual(coin.s2, coin2.s2);
    testAssertEqual(coin.s3, coin2.s3);

    sigShares.push_back(coin);
    ids.push_back(id);
  }

  // Aggregate
  auto combined_coin = CoinSig::aggregate(n, sigShares, ids);
  libutt::CoinComm cc(p, epk, val);

  // Verify
  testAssertEqual(combined_coin.verify(p, cc, main_pk), true);

  // Generate random elements
  libutt::Fr r_delta = libutt::Fr::random_element(), u_delta = libutt::Fr::random_element();
  combined_coin.rerandomize(r_delta, u_delta);

  // Rerandomize the coin commitments
  cc.rerandomize(p, r_delta);

  // Check rerandomization
  testAssertEqual(combined_coin.verify(p, cc, main_pk), true);
}

int main(int argc, char *argv[]) {
  libutt::initialize(nullptr, 0);
  // srand(static_cast<unsigned int>(time(NULL)));
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