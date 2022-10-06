#include <utt/Configuration.h>

#include <utt/PolyCrypto.h>
#include <utt/PolyOps.h>
#include <utt/Utt.h>

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
using namespace libfqfft;
using namespace libutt;

void bench2to2Txn(
    AveragingTimer& tc, AveragingTimer& tv, AveragingTimer& tp, AveragingTimer& tis, AveragingTimer& tma) {
  auto p = libutt::Params::Random();

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<long> udi(1, 1024);

  CoinSecrets c1(p, udi(gen)), c2(p, udi(gen));  // randomly generated coins
  CoinComm cc1(p, c1), cc2(p, c2);               // commit to the coins

  // TODO: make this into a threshold bank
  BankSK bsk = BankSK::random();
  // sign the coins
  // TODO: benchmark times to share sign and aggregate here
  CoinSig scc1 = bsk.thresholdSignCoin(p, cc1);
  CoinSig scc2 = bsk.thresholdSignCoin(p, cc2);

  // together, the coins total this much
  unsigned long totalVal = (c1.val + c2.val).as_ulong();
  std::uniform_int_distribution<unsigned long> udo(0, totalVal);

  // split them up randomly
  unsigned long o1 = udo(gen);
  unsigned long o2 = totalVal - o1;
  Fr out1, out2;
  out1.set_ulong(o1);
  out2.set_ulong(o2);

  AddrSK ask1 = AddrSK::random(), ask2 = AddrSK::random();
  AddrPK apk1 = ask1.toAddrPK(p);
  AddrPK apk2 = ask2.toAddrPK(p);

  tc.startLap();
  Tx tx = Tx::create(p,
                     {
                         std::make_tuple(c1, cc1, scc1),
                         std::make_tuple(c2, cc2, scc2),
                     },
                     {std::make_tuple(apk1, out1), std::make_tuple(apk2, out2)});
  tc.endLap();

  BankPK bpk = bsk.toPK(p);

  tv.startLap();
  tx.verify(p, bpk);
  tv.endLap();

  tp.startLap();
  tx.process(p, bsk);
  tp.endLap();

  // A user will have to check all outputs, so this is meant to measure that time
  // (although here two different users are checking their own outputs, but the time is the same)
  tis.startLap();
  auto opt1 = tx.outs[0].isMine(p, ask1);
  auto opt2 = tx.outs[1].isMine(p, ask2);
  tis.endLap();
  testAssertTrue(opt1.has_value());
  testAssertTrue(opt2.has_value());

  tma.startLap();
  auto walletCoin1 = tx.outs[0].makeMine(p, opt1.value(), bpk);
  tma.endLap();
  (void)walletCoin1;

  tma.startLap();
  auto walletCoin2 = tx.outs[1].makeMine(p, opt2.value(), bpk);
  tma.endLap();
  (void)walletCoin2;
}

int main(int argc, char* argv[]) {
  libutt::initialize(nullptr, 0);
  // srand(static_cast<unsigned int>(time(NULL)));
  (void)argc;
  (void)argv;

  if (argc < 5) {
    cout << "Usage: " << argv[0] << " <num-samples> <out-file> <machine> <descript>" << endl;
    cout << endl;
    cout << "OPTIONS: " << endl;
    // cout << "   <pp-file>       the Kate public parameters file" << endl;
    cout << "   <num-samples>   the # of times to repeat the benchmark" << endl;
    cout << "   <out-file>      output CSV file to write results in" << endl;
    cout << "   <machine>       the machine that the benchmark was run on" << endl;
    cout << "   <descript>      a description of the changes being benchmarked (e.g., 'may24-presentation')" << endl;
    cout << endl;

    return 1;
  }

  size_t numSamples = static_cast<size_t>(std::stoi(argv[1]));
  std::string fileName = argv[2];
  std::string machine = argv[3];
  std::string descr = argv[4];

  bool exists = Utils::fileExists(fileName);
  ofstream fout(fileName, std::ofstream::out | std::ofstream::app);

  if (fout.fail()) {
    throw std::runtime_error("Could not open " + fileName + " for writing");
  }

  // create header if file does not exist, otherwise append
  if (!exists)
    fout << "build,"
         << "num_samples,"
         << "create-2to2-txn-hum,"
         << "verify-2to2-txn-hum,"
         << "process-2to2-txn-hum,"
         << "is-mine-2to2-txn-hum,"
         << "make-mine-2to2-txn-hum,"
         << "create-2to2-txn,"
         << "verify-2to2-txn,"
         << "process-2to2-txn,"
         << "is-mine-2to2-txn,"
         << "make-mine-2to2-txn,"
         << "machine,description,date" << endl;

  AveragingTimer tc("create");
  AveragingTimer tv("verify");
  AveragingTimer tp("process");
  AveragingTimer tis("isMine");
  AveragingTimer tma("makeMine");
  for (size_t i = 0; i < numSamples; i++) {
    bench2to2Txn(tc, tv, tp, tis, tma);
  }

  // write results
  fout
#ifndef NDEBUG
      << "debug"
      << ","
#else
      << "release"
      << ","
#endif
      << numSamples << "," << Utils::humanizeMicroseconds(tc.averageLapTime(), 2) << ","
      << Utils::humanizeMicroseconds(tv.averageLapTime(), 2) << ","
      << Utils::humanizeMicroseconds(tp.averageLapTime(), 2) << ","
      << Utils::humanizeMicroseconds(tis.averageLapTime(), 2) << ","
      << Utils::humanizeMicroseconds(tma.averageLapTime(), 2) << "," << tc.averageLapTime() << ","
      << tv.averageLapTime() << "," << tp.averageLapTime() << "," << tis.averageLapTime() << "," << tma.averageLapTime()
      << "," << machine << "," << descr << "," << timeToString() << endl;

  return 0;
}
