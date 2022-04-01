#include <utt/PolyOps.h>
#include <utt/PolyCrypto.h>

#include <vector>
#include <cmath>
#include <iostream>
#include <ctime>
#include <fstream>

#include <xutils/Log.h>
#include <xutils/Timer.h>
#include <xassert/XAssert.h>

using namespace std;

using libutt::Fr;
using libutt::G1;
using libutt::random_group_elems;
using libutt::random_field_elems;

void benchAllRootsOfUnity(size_t n, size_t r) {
    if(n < 3) {
        logerror << "Need n bigger than 2" << endl;
        throw std::runtime_error("WTH");
    }
    
    std::vector<G1> bases = random_group_elems<G1>(n);

    //size_t id = static_cast<size_t>(rand()) % n;
    for(size_t id = 0; id < n; id++) {
        //loginfo << "Picking roots of unity exps for player #" << id << endl;
        std::string name = "Multiexp for #" + std::to_string(id);
        Fr wnid = libff::get_root_of_unity<Fr>(1u << Utils::log2ceil(n)) ^ id;
        std::vector<Fr> exp;
        exp.push_back(1);
        exp.push_back(wnid);
        for(size_t i = 0; i < n-2; i++) {
            exp.push_back(exp.back() * wnid);
        }

        //loginfo << " * picking random bases" << endl;

        AveragingTimer tn(name.c_str());
        for(size_t i = 0; i < r; i++) {
            //loginfo << "Round #" << i+1 << endl;
            tn.startLap();
            libutt::multiExp<G1>(bases, exp);
            tn.endLap();
        }

        logperf << tn << endl;
        logperf << "Time per 1 exp: " << static_cast<double>(tn.averageLapTime()) / static_cast<double>(n) << " microseconds" << endl;
    }
}

int main(int argc, char *argv[]) {
    libutt::initialize(nullptr, 0);
    srand(static_cast<unsigned int>(time(nullptr)));

    if(argc < 3) {
        cout << "Usage: " << argv[0] << " <n> <r>" << endl;
        cout << endl;
        cout << "OPTIONS: " << endl;
        cout << "   <n>    the number of exponentiations to do in a single multiexp" << endl;  
        cout << "   <r>    the number of times to repeat the multiexps" << endl;  
        cout << endl;

        return 1;
    }

    size_t n = static_cast<size_t>(std::stoi(argv[1]));
    size_t r = static_cast<size_t>(std::stoi(argv[2]));

    loginfo << "Picking " << n << " random exponents (only once)" << endl;
    std::vector<Fr> exp = random_field_elems(n);

    //loginfo << "Picking all " << n << " " << n << "th roots of unity as the exponents (only once)" << endl;
    //exp = libutt::get_all_roots_of_unity(Utils::log2ceil(n));
    //exp.resize(n);

    //int id = 14;
    //exp.push_back(id);
    //for(size_t i = 0; i < n-1; i++) {
    //    exp.push_back(exp.back() * Fr(id));
    //}
    std::vector<G1> bases = random_group_elems<G1>(n);

    AveragingTimer tn("Multiexp rand base & exp");
    for(size_t i = 0; i < r; i++) {
        //loginfo << "Round #" << i+1 << endl;
        tn.startLap();
        libutt::multiExp<G1>(bases, exp);
        tn.endLap();
    }

    logperf << tn << endl;
    logperf << "Time per 1 exp: " << static_cast<double>(tn.averageLapTime()) / static_cast<double>(n) << " microseconds" << endl;

    return 0;
}
