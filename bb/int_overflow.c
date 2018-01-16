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

/**
 * Signed integer overflow detection utility functions below were derived from
 * CERT Secure Coding standards for operations on signed integers (INT32-C).
 */

#include <limits.h>

/**
 * Checks for overflow will occur when a + b is performed, a and b being long
 * long int.
 * Returns 0 if no overflow will occur.
 *
 * @param long long a
 * @param long long b
 */
int overflow_ll_add(long long a, long long b)
{
    if (((b > 0) && (a > (LLONG_MAX - b))) ||
        ((b < 0) && (a < (LLONG_MIN - b)))) {
        return 1;
    }

    return 0;
}

/**
 * Checks for overflow will occur when a - b is performed, a and b being long
 * long int.
 * Returns 0 if no overflow will occur.
 *
 * @param long long a
 * @param long long b
 */
int overflow_ll_sub(long long a, long long b)
{
    if (((b > 0) && (a < (LLONG_MIN + b))) ||
        ((b < 0) && (a > (LLONG_MAX + b)))) {
        return 1;
    }

    return 0;
}

/**
 * Checks for overflow will occur when a * b is performed, a and b being long
 * long int.
 * Returns 0 if no overflow will occur.
 *
 * @param long long a
 * @param long long b
 */
int overflow_ll_mul(long long a, long long b)
{
    if (a > 0) {     // a is positive
        if (b > 0) { // a and b are positive
            if (a > (LLONG_MAX / b)) {
                return 1;
            }
        } else { // a is positive and b is non-positive
            if (b < (LLONG_MIN / a)) {
                return 1;
            }
        }
    } else {
        if (b > 0) { // a is non-positive and b is positive
            if (a < (LLONG_MIN / b)) {
                return 1;
            }
        } else { // a and b are non-positive
            if ((a != 0) && (b < (LLONG_MAX / a))) {
                return 1;
            }
        }
    }

    return 0;
}

/**
 * Checks for overflow will occur when a / b is performed, a and b being long
 * long int.
 * Returns 0 if no overflow will occur.
 *
 * @param long long a
 * @param long long b
 */
int overflow_ll_div(long long a, long long b)
{
    if ((b == 0) || ((a == LLONG_MIN) && (b == -1))) {
        return 1;
    }

    return 0;
}
