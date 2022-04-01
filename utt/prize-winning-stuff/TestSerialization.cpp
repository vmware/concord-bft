#include <utt/Configuration.h>

#include <fstream>
#include <sstream>

#include <utt/Bank.h>
#include <utt/Params.h>
#include <utt/PolyCrypto.h>

#include <xassert/XAssert.h>
#include <xutils/Log.h>

using namespace libutt;

int main(int argc, char *argv[])
{
    (void)argc; (void)argv;

    libutt::initialize(nullptr, 0);

    // test Fr (de)serialization via to_words/from_words
    Fr r = Fr::random_element();
    testAssertNotEqual(r, Fr::zero());
    auto v = r.to_words();
    Fr rp;
    rp.from_words(v);
    testAssertEqual(r, rp);
    
    // test Fr (de)serialization via Fr_serialize/deserialize
    r = Fr::random_element();
    testAssertNotEqual(r, Fr::zero());
    size_t cap = Fr_num_bytes();
    testAssertEqual(cap, v.size() * sizeof(v[0]));
    unsigned char buffer[cap];
    Fr_serialize(r, buffer, cap);
    rp = Fr_deserialize(buffer, cap);
    testAssertEqual(r, rp);

    // test Params (de)serialization
    auto p1 = libutt::Params::Random(), p2 = libutt::Params::Random();
    // Params p2;
    std::stringstream ss;
    //loginfo << "Random params: " << endl;
    //loginfo << "P1: " << p1 << endl;
    //loginfo << "P2: " << p2 << endl;

    if(p1 == p2) {
        testAssertFail("The two randomly-generated Params were supposed to differ");
    }

    //loginfo << "Serialized params: " << endl;
    //loginfo << p1;
    ss << p1;
    ss >> p2;
    //loginfo << "Deserialized params: " << endl;
    //loginfo << p2;

    if(p1 != p2) {
        testAssertFail("The deserialized Params was supposed to equal the serialized Params");
    }

    for(auto& y_tilde : p1.Y_tilde) {
        testAssertNotEqual(y_tilde, G2::zero());
    }

    // test PS16 (de)serialization
    size_t t = 5, n = 9;

    std::string dirname = "/tmp";
    std::string fileNamePrefix = hashToHex(Fr::random_element()) + "-";

    BankThresholdKeygen kg(p1, t, n);
    kg.writeToFiles(dirname, fileNamePrefix);

    // test reading the written things back
    for(size_t i = 0; i < n; i++) {
        std::ifstream fin(dirname + "/" + fileNamePrefix + std::to_string(i));

        fin >> p2;
        BankShareSK shareSK(fin);
        // fin >> shareSK;

        if(p1 != p2) {
            testAssertFail("Read back wrong params");
        }

        if(shareSK != kg.skShares[i]) {
            logerror << "SK " << i << " deserialization failed" << endl;
            testAssertFail("Read back wrong SK");
        }
    }

    if (n>1) {
        testAssertNotEqual(kg.skShares[0], kg.skShares[1]);
    }

    std::ofstream cliFile(dirname + "/" + "client.dat");
    cliFile << p2;

    for(size_t i=0; i< n;i++) {
        cliFile << kg.getShareSK(i).toSharePK(p1);
    }
    cliFile.close();


    std::cout << "Test '" << argv[0] << "' finished successfully" << std::endl;

    return 0;
}
