#include <xassert/XAssert.h>

#include <iostream>
#include <memory>
#include <vector>

int main(int argc, char * argv[]) {
    (void)argc; (void)argv;

    // NOTE: Implicitly tests XAssert's static initializer!

    testAssertTrue(true);
    testAssertFalse(false);

    testAssertInclusiveRange(0, 0, 0);
    testAssertInclusiveRange(0, 0, 1);
    testAssertInclusiveRange(-1, 0, 0);

    testAssertStrictlyGreaterThan(1, 0);
    testAssertStrictlyLessThan(0, 1);

    testAssertGreaterThanOrEqual(1, 1);
    testAssertGreaterThanOrEqual(2, 1);

    testAssertLessThanOrEqual(0, 0);
    testAssertLessThanOrEqual(0, 1);

    testAssertEqual(0, 0);
    testAssertIsZero(0);
    testAssertNotZero(1);

    // This one is a hard one to test!
    //assertFail(shouldNotBeCalled());

    int val = 3;
    testAssertProperty(val, val % 2 == 1 && val > 2);

    testAssertNotNull(std::unique_ptr<int>(new int(3)).get());
    testAssertNotEqual(1, 0);

    testAssertIsPositive(0);
    testAssertIsPositive(1);
    testAssertStrictlyPositive(1);

    testAssertIsPowerOfTwo(1);
    testAssertIsPowerOfTwo(2);
    testAssertIsPowerOfTwo(4);
    testAssertIsPowerOfTwo(128);
    testAssertFalse(XAssert::IsPowerOfTwo(0));
    testAssertFalse(XAssert::IsPowerOfTwo(3));
    testAssertFalse(XAssert::IsPowerOfTwo(5));
    testAssertFalse(XAssert::IsPowerOfTwo(-2));
    testAssertFalse(XAssert::IsPowerOfTwo(-4));
    testAssertFalse(XAssert::IsPowerOfTwo(-5));
    testAssertFalse(XAssert::IsPowerOfTwo(129));
    testAssertFalse(XAssert::IsPowerOfTwo(-256));
    std::vector<int> v(1);
    testAssertValidIndex(0, v);

    testAssertNull(nullptr);

    std::cout << "Test '" << argv[0] << "' passed!" << std::endl;

    return 0;
}
