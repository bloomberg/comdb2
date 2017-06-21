/*
   Copyright 2017 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

/*
 * Tests for signed integer overflow library <int_overflow.h>
 */

#include <limits.h>
#include <stdio.h>
#include <int_overflow.h>

int test_ll_add()
{
    if (overflow_ll_add(1, 1) != 0) {
        return 1;
    }

    if (overflow_ll_add(-1, 1) != 0) {
        return 1;
    }

    if (overflow_ll_add(LLONG_MAX, 1) != 1) {
        return 1;
    }

    if (overflow_ll_add(1, LLONG_MAX) != 1) {
        return 1;
    }

    if (overflow_ll_add(0, LLONG_MAX) != 0) {
        return 1;
    }

    if (overflow_ll_add(LLONG_MAX, 0) != 0) {
        return 1;
    }

    return 0;
}

int test_ll_sub()
{
    if (overflow_ll_sub(1, 1) != 0) {
        return 1;
    }

    if (overflow_ll_sub(LLONG_MIN, 1) != 1) {
        return 1;
    }

    if (overflow_ll_sub(LLONG_MIN, 0) != 0) {
        return 1;
    }

    if (overflow_ll_sub(1, LLONG_MIN) != 1) {
        return 1;
    }

    if (overflow_ll_sub(0, LLONG_MIN) != 1) {
        return 1;
    }

    if (overflow_ll_sub(1, LLONG_MIN + 2) != 0) {
        return 1;
    }

    if (overflow_ll_sub(0, LLONG_MIN + 1) != 0) {
        return 1;
    }

    return 0;
}

int test_ll_mul()
{
    if (overflow_ll_mul(3, 2) != 0) {
        return 1;
    }

    if (overflow_ll_mul(LLONG_MIN, 2) != 1) {
        return 1;
    }

    if (overflow_ll_mul(LLONG_MAX, 2) != 1) {
        return 1;
    }

    return 0;
}

int test_ll_div()
{
    if (overflow_ll_div(2, 3) != 0) {
        return 1;
    }

    if (overflow_ll_div(2, 0) != 1) {
        return 1;
    }

    if (overflow_ll_div(LLONG_MIN, -1) != 1) {
        return 1;
    }

    return 0;
}

int main()
{
    int rc = 0;

    if (test_ll_add()) {
        printf("Failed test in 'add' test suite.");
        rc = 1;
    }

    if (test_ll_sub()) {
        printf("Failed test in 'sub' test suite.");
        rc = 1;
    }

    if (test_ll_mul()) {
        printf("Failed test in 'mul' test suite.");
        rc = 1;
    }

    if (test_ll_div()) {
        printf("Failed test in 'div' test suite.");
        rc = 1;
    }

    return rc;
}
