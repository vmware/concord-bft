//#include <stdio.h>
//
//#include <relic/relic.h>
//#include <relic/relic_err.h>
//#include <relic/relic_test.h>
//
//static void dummy(void);
//static void dummy2(void);
//
//int j;
//
//static void dummy(void) {
//    j++;
//    if (j < 6)
//        dummy2();
//}
//
//static void dummy2(void) {
//    j++;
//    if (j < 5)
//        dummy();
//    else {
//        THROW(ERR_NO_MEMORY);
//    }
//}
//
//int main(void) {
//    err_t e;
//    char *msg = NULL;
//    int code = STS_ERR;
//
//    if (core_init() != STS_OK) {
//        core_clean();
//        return 1;
//    }
//
//    util_banner("Tests for the ERR module:\n", 0);
//
//    TEST_ONCE("not using try-catch is correct") {
//        dummy();
//        if (err_get_code() == STS_ERR) {
//            err_get_msg(&e, &msg);
//            TEST_ASSERT(msg == core_get()->reason[ERR_NO_MEMORY], end);
//            TEST_ASSERT(err_get_code() != STS_ERR, end);
//        }
//    } TEST_END;
//
//    j = 0;
//
//    TEST_ONCE("try-catch is correct and error message is printed");
//    TRY {
//            dummy();
//    }
//    CATCH(e) {
//        switch (e) {
//            case ERR_NO_MEMORY:
//                TEST_END;
//                ERROR(end);
//                break;
//        }
//    }
//
//    util_banner("All tests have passed.\n", 0);
//
//    code = STS_OK;
//    end:
//    core_clean();
//    if (code == STS_ERR)
//        return 0;
//    else {
//        return 1;
//    }
//}

int main() {}