#include <stdio.h>

#ifdef NDEBUG
#undef NDEBUG
#include <assert.h>
#define NDEBUG
#else
#include <assert.h>
#endif

#include "str_util.h"

void test_str_has_ending() {
    // basic cases
    assert (str_has_ending("hello", "lo"));
    assert (!str_has_ending("hello", "he"));
    assert (str_has_ending("world", "world"));

    // case-sensitivity
    assert (!str_has_ending("Hello", "hello"));
    assert (!str_has_ending("hello", "Hello"));

    // ending longer than string
    assert (!str_has_ending("short", "longerending"));

    // empty-string inputs
    assert (str_has_ending("", ""));
    assert (!str_has_ending("", "nonempty"));
    assert (str_has_ending("nonempty", ""));
}

int main(int argc, char **argv)
{
    test_str_has_ending();

    return 0;
}
