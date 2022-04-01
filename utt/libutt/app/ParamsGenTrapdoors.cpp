#include <fstream>
#include <utt/PolyCrypto.h>

#include <utt/KatePublicParameters.h>

using namespace std;
using namespace libutt;

int main(int argc, char *argv[]) {
  libutt::initialize(nullptr, 0);

  if (argc < 3) {
    cout << "Usage: " << argv[0] << " <trapdoor-file> <q>" << endl;
    cout << endl;
    cout << "Generates 's' and writes it and 'q' to <trapdoor-file>" << endl;
    return 1;
  }

  string outFile(argv[1]);
  size_t q = static_cast<size_t>(std::stoi(argv[2]));

  ofstream fout(outFile);

  if (fout.fail()) {
    std::cout << "ERROR: Could not open file: " << outFile << endl;
    throw std::runtime_error("Could not open trapdoor output file");
  }

  Fr s = Fr::random_element();
  fout << s << endl;
  fout << q << endl;

  loginfo << "All done!" << endl;

  return 0;
}
