#include <fstream>

#include <utt/PolyCrypto.h>
#include <utt/KatePublicParameters.h>

using namespace std;
using namespace libutt;

int main(int argc, char *argv[]) {
  libutt::initialize(nullptr, 0);

  if (argc < 5) {
    cout << "Usage: " << argv[0] << " <trapdoor-file> <out-file> <start-incl> <end-excl>" << endl;
    cout << endl;
    cout << "Reads 's' from <trapdoor-file> and outputs q-SDH parameters (g_1^{s_i}) for i \\in [<start-incl>, "
            "<end-excl>) to <out-file>."
         << endl;
    return 1;
  }

  string inFile(argv[1]);
  string outFile(argv[2]);
  size_t start = static_cast<size_t>(std::stoi(argv[3])), end = static_cast<size_t>(std::stoi(argv[4]));

  ifstream fin(inFile);

  if (fin.fail()) {
    throw std::runtime_error("Could not read trapdoors input file");
  }

  Fr s;
  fin >> s;

  Dkg::KatePublicParameters::generate(start, end, s, outFile, true);

  loginfo << "All done!" << endl;

  return 0;
}
