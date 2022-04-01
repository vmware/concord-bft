#include <fstream>

#include <utt/PolyCrypto.h>
#include <utt/Bank.h>

using namespace std;
using namespace libutt;

int main(int argc, char *argv[])
{
    libutt::initialize(nullptr, 0);

    if(argc < 5) {
        cout << "Usage: " << argv[0] << " <dirname> <filename-prefix> <t> <n>" << endl;
        cout << endl;
        cout << "Picks a random Pointcheval-Sanders'16 (PS16) secret key and (t, n) secret shares it amongst n servers so that any subset of t servers can reconstruct it." << endl;
        cout << "Writes the i-th server's secret key share to '<dirname>/<filename-prefix>i" << endl;
        return 1;
    }

    string dirname(argv[1]);
    string filenamePrefix(argv[2]);
    size_t t = static_cast<size_t>(std::stoi(argv[3])),
        n = static_cast<size_t>(std::stoi(argv[4]));

    loginfo << "(" << t << ", " << n << ")-secret sharing a Pointcheval-Sanders'16 (PS16) secret key..." << endl;

    loginfo << "Generating public parameters first..." << endl;
    auto p = libutt::Params::Random();

    BankThresholdKeygen kg(p, t, n);

    kg.writeToFiles(dirname, filenamePrefix);

    loginfo << "All done!" << endl;

    return 0;
}
