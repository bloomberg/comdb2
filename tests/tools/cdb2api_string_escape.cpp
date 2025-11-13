#include <assert.h>
#include <string.h>

#include <cdb2api.h>

void test_cdb2_string_escape(const char *description, const char *input, const char *expected)
{
    printf("running test case for %s\n", description);
    char *actual = cdb2_string_escape(NULL, input);
    printf("expected:%s\nactual:%s\n", expected, actual);
    assert(strcmp(actual, expected) == 0);
}

int main(int argc, char **argv)
{
    test_cdb2_string_escape("empty text", "", "''");
    test_cdb2_string_escape("short text", "Hello world!", "'Hello world!'");
    test_cdb2_string_escape(
        "long text",
        "'As quirky joke, chefs won't pay devil magic zebra tax.''", 
        "'''As quirky joke, chefs won''t pay devil magic zebra tax.'''''"
    );
    
    return EXIT_SUCCESS;
}
