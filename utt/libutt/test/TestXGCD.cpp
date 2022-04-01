#include <utt/Configuration.h>

#include <utt/PolyCrypto.h>
#include <utt/PolyOps.h>

#include <xassert/XAssert.h>
#include <xutils/Log.h>
#include <xutils/Timer.h>

#include <libfqfft/polynomial_arithmetic/xgcd.hpp>

using namespace libutt;
using namespace std;

typedef std::vector<Fr> poly;

void assert_poly_is_identity(const poly& x) {
    std::vector<Fr> expected;
    expected.push_back(Fr(1));
    testAssertTrue(poly_equal_slow(x, expected));
}

void verifyBezout(const poly& x, const poly& y, const poly& a, const poly& b) {
    poly r1, r2, r3;
    _polynomial_multiplication(r1, x, a);
    _polynomial_multiplication(r2, y, b);
    _polynomial_addition(r3, r1, r2);

    assert_poly_is_identity(r3);
}

int main(int argc, char *argv[])
{
    libutt::initialize(nullptr, 0);

    size_t n = 128;
    if(argc > 1) {
        n = static_cast<size_t>(std::stoi(argv[1]));
    }

    loginfo << "Polynomial degree is " << n << " (change by passing it as argument)" << endl;

    loginfo << "Picking random polynomials ..." << endl;
    vector<Fr> a1, b1, a2, b2, x, y, c, d;
    for (size_t i = 0; i < n; i++) {
        c.push_back(Fr::random_element());
        d.push_back(Fr::random_element());
    }
    loginfo << endl;

    loginfo << "Interpolating polynomial x(.) ..." << endl;
    x = poly_from_roots(c);
    loginfo << "Interpolating polynomial y(.) ..." << endl;
    y = poly_from_roots(d);
    loginfo << endl;

    loginfo << "Computing libfqfft (slow) Bezout coefficients for x(.) and y(.) ..." << endl;
    {
        logperf << std::flush;
        ScopedTimer<std::chrono::seconds> t(std::cout, "Slow EEA took ", " seconds\n");
        std::vector<Fr> gcd;
        libfqfft::_polynomial_xgcd(x, y, gcd, a2, b2);
        assert_poly_is_identity(gcd);
    }
    logperf << endl;
    
    //loginfo << "Verifiying slow EEA Bezout coefficients..." << endl;
    verifyBezout(x, y, a2, b2);


    loginfo << "All is well." << endl;

    return 0;
}
