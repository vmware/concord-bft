#include <utt/Configuration.h>

#include <utt/PolyCrypto.h>
#include <utt/PolyOps.h>

#include <xassert/XAssert.h>
#include <xutils/Log.h>
#include <xutils/Utils.h>

#include <libfqfft/polynomial_arithmetic/basic_operations.hpp>

#include <set>
#include <iomanip>
#include <vector>
#include <queue>

using namespace libutt;

void print_all_accumulators(std::vector<Fr> roots, const std::vector<Fr>& allRoots);
void print_all_accumulators(std::vector<Fr> roots, const std::vector<Fr>& allRoots, const std::vector<Fr>& p, std::string nodeName);

std::vector<Fr> even_or_odd(const std::vector<Fr>& p, bool even);

std::vector<Fr> poly_from_roots(std::vector<Fr>::const_iterator beg, std::vector<Fr>::const_iterator end);

int main(int argc, char *argv[])
{
    (void)argc; (void)argv;

    libutt::initialize(nullptr, 0);

    // test roots of unity
    size_t n = 128;
    //Fr w_n = libff::get_root_of_unity<Fr>(n);
    std::vector<Fr> allRoots = get_all_roots_of_unity(n);
    //for(size_t i = 0; i < n; i++) {
    //    Fr w_ni = w_n^i;
    //
    //    if(std::find(allRoots.begin(), allRoots.end(), w_ni) != allRoots.end()) {
    //        logerror << i << "th" << n << "th root of unity is not unique" << endl;
    //        assertFail("Something's wrong with the roots of unity");
    //    }

    //    allRoots.push_back(w_ni);
    //}

    //testAssertEqual(w_n^n, Fr(1));

    auto checkCoeffs = [&allRoots](const std::vector<Fr>& p, const std::string& poly) {
        size_t d = p.size();
        loginfo << poly << " of degree " << d-1 << " coeffs: ";
        poly_print_wnk(p, allRoots);
        std::cout << endl;
    };

    // test \prod_{i=0}{n-1} (x-w_n^i) = x^n - 1
    std::vector<Fr> xn_minus_1 = poly_from_roots(allRoots.begin(), allRoots.end());
    testAssertEqual(xn_minus_1.size(), n + 1);
    checkCoeffs(xn_minus_1, "x^n - 1");
    testAssertEqual(xn_minus_1[0], Fr(-1));
    testAssertEqual(xn_minus_1[n], Fr::one());

    // test \prod_{i=0}{n/2-1} (x-(w_n^i)^2) = x^{n/2} - 1
    // NOTE: (w_n^i)^2 will be the roots of unity at even indices in 'allRoots'
    // Example: For n = 4, we have the principal 4th root of unity w_4 = i
    // ...and w_4^0 = i^0 = 1, w_4^1 = i^1 = i, w_4^2 = i^2 = -1, w_4^3 = -i
    // The 2nd roots of unity will be (w_4^0)^2 = 1 = w_4^0 and (w_4^1)^2 = -1 = w_4^2
    auto even = even_or_odd(allRoots, true);
    std::vector<Fr> xn2_minus_1 = poly_from_roots(even.begin(), even.end());
    testAssertEqual(xn2_minus_1.size(), n/2 + 1);
    checkCoeffs(xn2_minus_1, "x^{n/2} - 1");
    testAssertEqual(xn2_minus_1[0], Fr(-1));
    testAssertEqual(xn2_minus_1[n/2], Fr::one());
    
    // test \prod_{i=n/2-1}{n-1} (x-w_n^i) = x^{n/2} + 1
    auto odd = even_or_odd(allRoots, false);
    std::vector<Fr> xn2_plus_1 = poly_from_roots(odd.begin(), odd.end());
    testAssertEqual(xn2_plus_1.size(), n/2 + 1);
    checkCoeffs(xn2_plus_1, "x^{n/2} + 1");
    testAssertEqual(xn2_plus_1[0], Fr::one());
    testAssertEqual(xn2_plus_1[n/2], Fr::one());

    logdbg << "All accumulator polys for n = " << n << ": " << endl;
    print_all_accumulators(allRoots, allRoots);

    std::cout << "Test '" << argv[0] << "' finished successfully" << std::endl;
    return 0;
}

void checkMiddleCoeffs(const std::vector<Fr>& p) {
    size_t d = p.size();
    for(size_t i = 1; i < d - 1; i++) {
        testAssertEqual(p[(d-1) - i], Fr::zero());
    }
};

void print_all_accumulators(std::vector<Fr> roots, const std::vector<Fr>& allRoots) {
    std::vector<Fr> p = poly_from_roots(roots.begin(), roots.end());

    print_all_accumulators(roots, allRoots, p, "");
}

void print_all_accumulators(std::vector<Fr> roots, const std::vector<Fr>& allRoots, const std::vector<Fr>& p, std::string nodeName) {
    checkMiddleCoeffs(p);

    std::cout
        << std::endl
        << std::left
        << std::setw(static_cast<int>(Utils::log2ceil(allRoots.size())))
        << (nodeName.empty() ? "root" : nodeName)
        << " -> ";

    poly_print_wnk(p, allRoots);
    std::cout << endl;

    if(roots.size() == 1)
        return;

    std::vector<Fr> even = even_or_odd(roots, true);
    std::vector<Fr> odd = even_or_odd(roots, false);

    // print children below parent (this is just because I'm curious what they look like)
    std::cout << " \\-0->";
    auto p_even = poly_from_roots(even.begin(), even.end());
    poly_print_wnk(p_even, allRoots);
    std::cout << endl;

    std::cout << " \\-1->";
    auto p_odd = poly_from_roots(odd.begin(), odd.end());
    poly_print_wnk(p_odd, allRoots);
    std::cout << endl;

    // make sure left * right = parent
    std::vector<Fr> p_exp;
    libfqfft::_polynomial_multiplication(p_exp, p_even, p_odd);
    testAssertEqual(p, p_exp);

    print_all_accumulators(even, allRoots, p_even, nodeName + "0");
    print_all_accumulators(odd,  allRoots, p_odd, nodeName + "1");
}

std::vector<Fr> poly_from_roots(std::vector<Fr>::const_iterator beg, std::vector<Fr>::const_iterator end)
{
    std::queue<std::vector<Fr>> merge;
    while(beg != end) {
        auto r = *beg;
        merge.push({-r, 1});
        beg++;
    }

    while(merge.size() > 1) {
        std::vector<Fr> res, left, right;

        left = merge.front();
        merge.pop();
        right = merge.front();
        merge.pop();

        libfqfft::_polynomial_multiplication(res, left, right);
        merge.push(res);
    }

    return merge.front();
}

std::vector<Fr> even_or_odd(const std::vector<Fr>& p, bool even) {
    std::vector<Fr> v;

    size_t rem = even ? 0 : 1;
    for(size_t i = 0; i < p.size(); i++) {
        if(i % 2 == rem) {
            v.push_back(p[i]);
        }
    }

    return v;
}
