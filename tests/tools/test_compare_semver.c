#include "version_util.h"

typedef enum cmp_result {
	LT=-1,
	EQ=0,
	GT=1,
} cmp_result_t;

int test_valid_comparison(const char *lhs, const char *rhs, const cmp_result_t expected_cmp_result) {
	cmp_result_t cmp_result;

	const int rc = compare_semvers(lhs, rhs, &cmp_result);
	if (rc) { 
		fprintf(stderr, "Failed to compare %s and %s\n", lhs, rhs);
		return 1;
	}

	if (cmp_result != expected_cmp_result) {
		fprintf(stderr, "Comparison result %d unexpected for comparison between %s and %s\n", cmp_result, lhs, rhs);
		return 1;
	}

	return 0;
}

int test_invalid_comparison(const char *lhs, const char *rhs) {
	cmp_result_t cmp_result;

	const int rc = compare_semvers(lhs, rhs, &cmp_result);
	if (!rc) { 
		fprintf(stderr, "Expected to fail comparison between %s and %s but got success\n", lhs, rhs);
		return 1;
	}

	return 0;
}


int main(int argc, const char ** argv) {
	int rc = 0;

	// Basic tests
	rc |= test_valid_comparison("0.0.0", "0.0.0", EQ);
	rc |= test_valid_comparison("0.0.1", "0.0.0", GT);
	rc |= test_valid_comparison("0.1.0", "0.0.0", GT);
	rc |= test_valid_comparison("1.0.0", "0.0.0", GT);
	rc |= test_valid_comparison("0.0.0", "0.0.1", LT);
	rc |= test_valid_comparison("0.0.0", "0.1.0", LT);
	rc |= test_valid_comparison("0.0.0", "1.0.0", LT);

	// Test that major version is weighted over 
	// minor version and bugfix version
	rc |= test_valid_comparison("0.1.1", "1.0.0", LT);

	// Test that minor version is weighted over 
	// bugfix version
	rc |= test_valid_comparison("0.0.1", "0.1.0", LT);

	// Test that we correctly compare versions
	// with trailing zeroes.
	rc |= test_valid_comparison("0.0.1", "0.0.100", LT);
	rc |= test_valid_comparison("0.1.0", "0.100.0", LT);
	rc |= test_valid_comparison("1.0.0", "100.0.0", LT);

	// Test that we correctly compare versions
	// with leading zeroes.
	rc |= test_valid_comparison("01.01.01", "1.1.1", EQ);

	// Test some invalid inputs
	rc |= test_invalid_comparison("...", "...");
	rc |= test_invalid_comparison("", "");
	rc |= test_invalid_comparison("a.b.c", "x.y.z");
	rc |= test_invalid_comparison("0,0,0", "0,0,0");

	return rc;
}
