#include <utt/Configuration.h>

#include <utt/Coin.h>

#include <cmath>
#include <ctime>
#include <iostream>
#include <fstream>
#include <sstream>
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

void testRandomCoinSecret() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> udi(1, 1024);

    // Create a new random object
    Coin c = Coin::random(udi(gen), Coin::NormalType());    // randomly generated coin

    std::cout << "Random coin value: " << c.val << endl;

    // Test serialization
    std::stringstream ss;

    ss << c;
    Coin samec(ss);
    testAssertEqual(c, samec);

    Coin otherc = Coin::random(udi(gen), Coin::NormalType());    // randomly generated coin
    testAssertNotEqual(c, otherc);
}

int main(int argc, char *argv[]) {
    libutt::initialize(nullptr, 0);
    //srand(static_cast<unsigned int>(time(NULL)));
    (void)argc;
    (void)argv;

    testRandomCoinSecret();

    loginfo << "All is well." << endl;

    return 0;
}
