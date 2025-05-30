#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cson.h>

/* Return a hex string
 * output buffer should be appropriately sized */
char *util_tohex(char *out, const char *in, size_t len)
{
    char *beginning = out;
    char hex[] = "0123456789abcdef";
    const char *end = in + len;
    while (in != end) {
        char i = *(in++);
        *(out++) = hex[(i & 0xf0) >> 4];
        *(out++) = hex[i & 0x0f];
    }
    *out = 0;

    return beginning;
}

static void test_a()
{
    puts(__func__);
    cson_value *arV = cson_value_new_array();
    cson_array *ar = cson_value_get_array(arV);
    for (int i = 0; i < 2; ++i) {
        cson_array_append(ar, cson_value_new_integer(i));
    }
    cson_array_append(ar, cson_value_new_object());

    cson_value *obj0 = cson_value_new_object();
    cson_object *obj = cson_value_get_object(obj0);
    cson_object_set(obj, "myArray", arV);
    cson_object_set(obj, "myInt", cson_value_new_integer(42));
    cson_object_set(obj, "myDouble", cson_value_new_double(42.24, 0));
    cson_object_set(obj, "myString", cson_value_new_string("hi", 2));

    cson_value *obj1 = cson_value_new_object();
    obj = cson_value_get_object(obj1);
    cson_object_set(obj, "obj0", obj0);

    cson_output_FILE(obj1, stdout);
    cson_value_free(obj1);
}

static void test_b()
{
    puts(__func__);
    cson_value *val = cson_value_new_object();
    cson_object *obj = cson_value_get_object(val);
    for (int i = 0; i < 1000; ++i) {
        char key[128];
        sprintf(key, "key_string_number_%d", i);
        cson_object_set(obj, key, cson_value_new_integer(i));
    }
    cson_buffer buf;
    cson_output_buffer(val, &buf);
    cson_value_free(val);
}
static void test_c()
{
    puts(__func__);
    cson_value *arV = cson_value_new_array();
    cson_array *ar = cson_value_get_array(arV);
    for (int i = 0; i < 100000; ++i) {
        cson_array_append(ar, cson_value_new_integer(i));
    }
    cson_buffer buf;
    cson_output_buffer(arV, &buf);
    cson_value_free(arV);
}
static void test_d()
{
    puts(__func__);
    cson_value *v = cson_value_new_array();
    cson_array *a = cson_value_get_array(v);
    cson_array_append(a, cson_value_new_integer(0));
    cson_array_append(a, cson_value_new_integer(1));
    cson_array_append(a, cson_value_new_integer(2));
    cson_array_append(a, cson_value_new_bool(2));
    cson_array_append(a, cson_value_new_bool(0));
    cson_output_FILE(v, stdout);
    cson_value_free(v);
}
static void test_e()
{
    puts(__func__);

    cson_value *v = cson_value_new_integer(5);
    cson_output_FILE(v, stdout);
    cson_value_free(v);

    v = cson_value_new_bool(0);
    cson_output_FILE(v, stdout);
    cson_value_free(v);

    v = cson_value_new_bool(1);
    cson_output_FILE(v, stdout);
    cson_value_free(v);
}
static void test_f()
{
    puts(__func__);
    cson_value *a = cson_value_new_array();
    cson_value *b = cson_value_new_object();
    cson_value *c = cson_value_new_integer(4);
    cson_value *d = cson_value_new_double(4, 0);
    cson_value *e = cson_value_new_string("hi", 2);
    cson_value *f = cson_value_null();
    cson_value *g = cson_value_new_bool(0);
    cson_value *h = cson_value_new_bool(1);

    if (cson_value_is_array(b) || cson_value_is_array(b) ||
        cson_value_is_array(c) || cson_value_is_array(d) ||
        cson_value_is_array(e) || cson_value_is_array(f) ||
        cson_value_is_array(g) || cson_value_is_array(h) ||
        !cson_value_is_array(a)) {
        abort();
    }
    cson_array *ar = cson_value_get_array(a);
    cson_array_append(ar, b);
    cson_array_append(ar, c);
    cson_array_append(ar, d);
    cson_array_append(ar, e);
    cson_array_append(ar, f);
    cson_array_append(ar, g);
    cson_array_append(ar, h);
    if (cson_value_is_array(a))
        puts("pass");
    else
        abort();
    cson_value_free(a);
}
static void test_g()
{
    puts(__func__);
    cson_value *val;
    char src[] = "[\"first\", \"second\"]";
    if (cson_parse_string(&val, src, strlen(src))) {
        puts("failed parse!!!");
        abort();
    }
    if (!cson_value_is_array(val)) {
        puts("huh not array!!!");
        abort();
    }
    cson_array *arr = cson_value_get_array(val);
    if (cson_array_length_get(arr) != 2) {
        puts("huh wrong len");
        abort();
    }
    cson_array_append(arr, cson_value_new_string("third", 5));
    if (cson_array_length_get(arr) != 3) {
        puts("huh wrong len after append");
        abort();
    }
    cson_array_set(arr, 0, cson_new_int(99));
    cson_array_set(arr, 0, cson_new_int(10));
    cson_array_set(arr, 1, cson_new_int(11));
    cson_array_set(arr, 2, cson_new_int(12));
    if (cson_array_length_get(arr) != 3) {
        puts("huh wrong len after set");
        abort();
    }
    cson_output_FILE(val, stdout);
    cson_free_value(val);
    puts("array pass");
}
static void test_h_arr(cson_value *val)
{
    if (!cson_value_is_array(val))
        abort();
    cson_array *arr = cson_value_get_array(val);
    unsigned n = cson_array_length_get(arr);
    if (n != 252)
        abort();
    for (unsigned i = 0; i < 100; ++i) {
        cson_value *got_value = cson_array_get(arr, i);
        if (cson_value_is_object(got_value)) {
            cson_object *obj = cson_value_get_object(got_value);
            char key[128];
            sprintf(key, "%d", i);
            cson_value *obj_get = cson_object_get(obj, key);
            if (!cson_value_is_integer(obj_get))
                abort();
            int ival = cson_value_get_integer(obj_get);
            if (ival != i * 100)
                abort();
        } else {
            abort();
        }
    }
    for (unsigned i = 100; i < 200; ++i) {
        cson_value *got_value = cson_array_get(arr, i);
        if (!cson_value_is_integer(got_value))
            abort();
        if (cson_value_get_integer(got_value) != i - 100)
            abort();
    }
    for (unsigned i = 200; i < 226; ++i) {
        cson_value *got_value = cson_array_get(arr, i);
        if (!cson_value_is_string(got_value))
            abort();
        if (cson_value_get_cstr(got_value)[0] != i - 200 + 'a')
            abort();
    }
    for (unsigned i = 226; i < n; ++i) {
        cson_value *got_value = cson_array_get(arr, i);
        if (!cson_value_is_string(got_value))
            abort();
        if (cson_value_get_cstr(got_value)[0] != i - 226 + 'A')
            abort();
    }
}
static void test_h()
{
    puts(__func__);
    cson_value *val = cson_value_new_array();
    cson_array *arr = cson_value_get_array(val);
    for (int i = 0; i < 100; ++i) {
        cson_value *obj_val = cson_value_new_object();
        cson_object *obj = cson_value_get_object(obj_val);
        char key[128];
        sprintf(key, "%d", i);
        cson_object_set(obj, key, cson_new_int(i * 100));
        cson_array_append(arr, obj_val);
    }
    for (int i = 0; i < 100; ++i) {
        cson_array_append(arr, cson_value_new_integer(i));
    }
    for (char i = 'a'; i <= 'z'; ++i) {
        cson_array_append(arr, cson_value_new_string(&i, 1));
    }
    for (char i = 'A'; i <= 'Z'; ++i) {
        cson_array_append(arr, cson_value_new_string(&i, 1));
    }
    test_h_arr(val);
    cson_buffer buf;
    cson_output_buffer(val, &buf);
    cson_value *newval;
    cson_parse_string(&newval, buf.mem, buf.used);
    cson_value_free(val);
    test_h_arr(newval);
    cson_value_free(newval);
}
static void print_array(cson_array *, int);
static void print_obj(cson_object *, int);
static void print_value(cson_value *val, int indent)
{
    if (cson_value_is_array(val)) {
        print_array(cson_value_get_array(val), indent + 4);
    } else if (cson_value_is_object(val)) {
        print_obj(cson_value_get_object(val), indent + 4);
    } else {
        cson_buffer buf;
        cson_output_buffer(val, &buf);
        printf("_%.*s_\n", buf.used, (char *)buf.mem);
    }
}
static void print_array(cson_array *arr, int indent)
{
    printf("\n%*s[\n", indent, "");
    int n = cson_array_length_get(arr);
    for (int i = 0; i < n; ++i) {
        printf("%*s", indent + 4, "");
        print_value(cson_array_get(arr, i), indent + 4);
    }
    printf("%*s]\n", indent, "");
}
static void print_obj(cson_object *obj, int indent)
{
    printf("%*s{\n", indent, "");
    cson_object_iterator i;
    cson_object_iter_init(obj, &i);
    cson_kvp *kv;
    while ((kv = cson_object_iter_next(&i)) != NULL) {
        const char *key = cson_string_cstr(cson_kvp_key(kv));
        printf("%*s_%s_ -> ", indent + 4, "", key);
        cson_value *val = cson_kvp_value(kv);
        print_value(val, indent + 4);
    }
    printf("%*s}\n", indent, "");
}

static void test_i(void)
{
    puts(__func__);
    char json[] = "{\"a\":1, \"b\":2, \"c\":[10,20,30], \"d\":\"quote here -> "
                  "\\\" <- lets see if it works\"}";
    cson_value *val;
    cson_parse_string(&val, json, sizeof(json) - 1);
    if (!cson_value_is_object(val))
        abort();
    cson_object *obj = cson_value_get_object(val);
    print_obj(obj, 0);
    puts("");
    cson_value *d_val = cson_object_get(obj, "d");
    if (!cson_value_is_string(d_val))
        abort();
    cson_string *d = cson_value_get_string(d_val);
    const char *d_str = cson_string_cstr(d);
    printf("d is %s\n", d_str);
    cson_value_free(val);
}
static void test_j(void)
{
    puts(__func__);
    char json[] = "{\"type\":\"array\",\"value\":[{\"type\":\"number\","
                  "\"value\":3.14159}]}";
    cson_value *val0, *val;
    cson_parse_string(&val0, json, sizeof(json) - 1);
    if (!cson_value_is_object(val0))
        abort();
    val = val0;
    cson_object *obj = cson_value_get_object(val);
    val = cson_object_get(obj, "type");
    if (!cson_value_is_string(val))
        abort();
    const cson_string *string_type = cson_value_get_string(val);
    const char *type = cson_string_cstr(string_type);
    if (strcmp(type, "array"))
        abort();
    val = cson_object_get(obj, "value");
    if (!cson_value_is_array(val))
        abort();
    cson_array *arr = cson_value_get_array(val);
    int n = cson_array_length_get(arr);
    for (int i = 0; i < n; ++i) {
        val = cson_array_get(arr, i);
        if (!cson_value_is_object(val))
            abort();
        obj = cson_value_get_object(val);
        val = cson_object_get(obj, "type");
        if (!cson_value_is_string(val))
            abort();
        const char *type = cson_value_get_cstr(val);
        if (strcmp(type, "number"))
            abort();
        val = cson_object_get(obj, "value");
        if (!cson_value_is_double(val))
            abort();
        const char *dbl_str = cson_value_get_cstr(val);
        if (dbl_str != NULL)
            abort();
    }
    cson_free_value(val0);
}
static void test_k(void)
{
    puts(__func__);
}
static void test_l(void)
{
    puts(__func__);
    cson_value *bad;
    char *bad_json = "{bad}";
    if (cson_parse_string(&bad, bad_json, strlen(bad_json)) == 0)
        abort();
}
static void test_m(void)
{
    puts(__func__);
    char *json = "{\"a\":1,\"b\":2, \"c\":3}";
    cson_value *val;
    if (cson_parse_string(&val, json, strlen(json)) != 0)
        abort();
    if (!cson_value_is_object(val))
        abort();
    cson_object *obj = cson_value_get_object(val);
    cson_object_unset(obj, "bb");
    cson_output_FILE(val, stdout);
    cson_object_unset(obj, "b");
    cson_output_FILE(val, stdout);
    cson_object_set(obj, "b", cson_value_new_integer(1));
    cson_output_FILE(val, stdout);
    cson_object_unset(obj, "b");
    cson_output_FILE(val, stdout);
    cson_object_set(obj, "b", cson_value_new_integer(0));
    cson_output_FILE(val, stdout);
    cson_object_unset(obj, "c");
    cson_output_FILE(val, stdout);
    cson_object_unset(obj, "a");
    cson_output_FILE(val, stdout);
    cson_object_unset(obj, "b");
    cson_output_FILE(val, stdout);
    cson_value_free(val);
}
static void test_n(void)
{
    puts(__func__);
    char bad[] = {0x61, 0x62, 0x63, 0x64, 0x65, 0xff,
                  0x66, 0x67, 0x68, 0x69, 0x6a, 0x00};
    int len = strlen(bad) + 1;
    int hlen = len * 2 + 1;
    char hex[hlen];
    util_tohex(hex, bad, len);
    puts(bad);
    puts(hex);
    cson_value *val = cson_value_new_array();
    cson_array *arr = cson_value_get_array(val);
    //    cson_array_append(arr, cson_value_new_string(bad, strlen(bad)));
    cson_array_append(arr, cson_value_new_string(hex, strlen(hex) + 1));
    cson_output_FILE(val, stderr);
    cson_value_free(val);
}
static void test_o(void)
{
    puts(__func__);
    cson_value *arr_val = cson_value_new_array();
    cson_array *arr = cson_value_get_array(arr_val);
    cson_value *val = cson_value_new_object();
    cson_object *obj = cson_value_get_object(val);
    cson_object_set(obj, "name", cson_value_new_string("hi", 2));
    cson_object_set(obj, "type", cson_value_new_string("hi", 2));
    cson_object_set(obj, "booya", cson_value_new_string("hi", 2));
    cson_array_append(arr, val);
    cson_output_FILE(arr_val, stdout);
    cson_value_free(arr_val);
}
static void test_p(void)
{
    puts(__func__);
    cson_value *inner = cson_value_new_array();
    cson_array *arr = cson_value_get_array(inner);
    cson_array_append(arr, cson_value_new_double(strtod("-InFiNiTy", NULL), 0));
    cson_array_append(arr, cson_value_new_double(strtod("-inf", NULL), 0));
    cson_array_append(arr, cson_value_new_double(strtod("-0.0", NULL), 0));
    cson_array_append(arr, cson_value_new_double(strtod("0.0", NULL), 0));
    cson_array_append(arr, cson_value_new_double(strtod("1.0", NULL), 0));
    cson_array_append(arr, cson_value_new_double(strtod("100.0", NULL), 0));
    cson_array_append(arr, cson_value_new_double(strtod("1000.0", NULL), 0));
    cson_array_append(arr, cson_value_new_double(strtod("1000000.0", NULL), 0));
    cson_array_append(arr, cson_value_new_double(strtod("1000000000.0", NULL), 0));
    cson_array_append(arr, cson_value_new_double(strtod("inf", NULL), 0));
    cson_array_append(arr, cson_value_new_double(strtod("InFiNiTy", NULL), 0));
    cson_array_append(arr, cson_value_new_double(strtod("nan", NULL), 0));
    cson_array_append(arr, cson_value_new_double(strtod("nanQ", NULL), 0));

    cson_value *outer = cson_value_new_array();
    arr = cson_value_get_array(outer);
    cson_array_append(arr, inner);

    cson_output_FILE(outer, stdout);
    cson_value_free(outer);
}
int main()
{
    puts(__func__);
    test_a();
    test_b();
    test_c();
    test_d();
    test_e();
    test_f();
    test_g();
    test_h();
    test_i();
    test_j();
    test_k();
    test_l();
    test_m();
    test_n();
    test_o();
    test_p();
    return 0;
}
