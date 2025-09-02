// @src/test/main.cpp
#include "gtest/gtest.h"

int main(int argc, char **argv) {
    // Initializes the Google Test framework.
    ::testing::InitGoogleTest(&argc, argv);
    
    // Runs all tests. The RUN_ALL_TESTS() macro automatically discovers
    // and runs all tests defined using the TEST() macro.
    return RUN_ALL_TESTS();
}