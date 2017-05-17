/*
   Copyright 2015 Bloomberg Finance L.P.

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

#include <errstat.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <str0.h>

void errstat_clr(errstat_t *err)
{
    if (!err) {
        return;
    }

    memset(err, 0, sizeof(errstat_t));
}

void errstat_set_rc(errstat_t *err, unsigned int rc)
{
    if (!err) {
        return;
    }

    err->errval = rc;
}

int errstat_get_rc(errstat_t *err)
{
    if (!err) {
        return ERRSTAT_ERR_PARAM;
    };

    return err->errval;
}

const char *errstat_get_str(errstat_t *err)
{
    if (!err) {
        return ""; /* avoid NULL */
    }

    return err->errstr;
}

void errstat_clr_str(errstat_t *err)
{
    if (!err) {
        return;
    }

    err->errhdrlen = 0;
    memset(err->errstr, 0, sizeof(err->errstr));
}

void errstat_clr_hdr(errstat_t *err)
{
    char tmp[ERRSTAT_STR_SZ];

    if (!err) {
        return;
    }

    /* save current error string */
    strncpy0(tmp, err->errstr + err->errhdrlen, sizeof(tmp));

    /* clear */
    errstat_clr_str(err);

    /* put back error string */
    errstat_set_str(err, tmp);
}

/* *** error string set *** */
void errstat_cat_str(errstat_t *err, const char *errstr)
{
    /*
    if ( !err || !errstr ) { return; }

    strncat0( err->errstr, errstr, sizeof(err->errstr) );
    */
    errstat_cat_strf(err, "%s", errstr);
}

void errstat_set_str(errstat_t *err, const char *errstr)
{
    /*
    if ( !err || !errstr ) { return; }

    strncpy0( err->errstr + err->errhdrlen, errstr,
            sizeof(err->errstr) - err->errhdrlen );
            */
    errstat_set_strf(err, "%s", errstr);
}

void errstat_cat_hdr(errstat_t *err, const char *errhdr)
{
    errstat_cat_hdrf(err, "%s", errhdr);
}

void errstat_set_hdr(errstat_t *err, const char *errhdr)
{
    errstat_set_hdrf(err, "%s", errhdr);
}

/* *** error string format *** */
void errstat_cat_strf(errstat_t *err, const char *fmt, ...)
{
    va_list ap;

    if (!err || !fmt) {
        return;
    }

    va_start(ap, fmt);

    /*
    vsnprintf0( err->errstr + err->errhdrlen + strlen( err->errstr ),
            sizeof(err->errstr) - strlen( err->errstr ),
            fmt, ap );
            */
    errstat_cat_strfap(err, fmt, ap);

    va_end(ap);
}

void errstat_set_strf(errstat_t *err, const char *fmt, ...)
{
    va_list ap;

    if (!err || !fmt) {
        return;
    }

    va_start(ap, fmt);

    /*
    vsnprintf0( err->errstr + err->errhdrlen,
            sizeof(err->errstr) - err->errhdrlen, fmt, ap );
            */
    errstat_set_strfap(err, fmt, ap);

    va_end(ap);
}

void errstat_cat_hdrf(errstat_t *err, const char *fmt, ...)
{
    va_list ap;

    if (!err || !fmt) {
        return;
    }

    va_start(ap, fmt);

    errstat_cat_hdrfap(err, fmt, ap);

    va_end(ap);
}

void errstat_set_hdrf(errstat_t *err, const char *fmt, ...)
{
    va_list ap;

    if (!err || !fmt) {
        return;
    }

    va_start(ap, fmt);

    errstat_set_hdrfap(err, fmt, ap);

    va_end(ap);
}

/* *** error string format with array of arguments *** */
void errstat_cat_strfap(errstat_t *err, const char *fmt, va_list ap)
{
    if (!err || !fmt) {
        return;
    }

    vsnprintf0(err->errstr + strlen(err->errstr),
               sizeof(err->errstr) - strlen(err->errstr), fmt, ap);
}

void errstat_set_strfap(errstat_t *err, const char *fmt, va_list ap)
{
    if (!err || !fmt) {
        return;
    }

    vsnprintf0(err->errstr + err->errhdrlen,
               sizeof(err->errstr) - err->errhdrlen, fmt, ap);
}

void errstat_cat_hdrfap(errstat_t *err, const char *fmt, va_list ap)
{
    char tmp[ERRSTAT_STR_SZ];

    if (!err || !fmt) {
        return;
    }

    /* save current error string */
    strncpy0(tmp, err->errstr + err->errhdrlen, sizeof(tmp));

    /* concatenate header */
    errstat_set_strfap(err, fmt, ap);
    err->errhdrlen = strlen(err->errstr);

    /* reset error string */
    errstat_set_str(err, tmp);
}

void errstat_set_hdrfap(errstat_t *err, const char *fmt, va_list ap)
{
    char tmp[ERRSTAT_STR_SZ];

    if (!err || !fmt) {
        return;
    }

    /* save current error string */
    strncpy0(tmp, err->errstr + err->errhdrlen, sizeof(tmp));

    /* set header */
    err->errhdrlen = 0;
    errstat_set_strfap(err, fmt, ap);
    err->errhdrlen = strlen(err->errstr);

    /* reset error string */
    errstat_set_str(err, tmp);
}

void errstat_set_rcstrf(errstat_t *err, unsigned int rc, const char *fmt, ...)
{
    va_list ap;
    if (!err || !fmt) {
        return;
    }

    errstat_set_rc(err, rc);

    va_start(ap, fmt);
    errstat_set_strfap(err, fmt, ap);
    va_end(ap);
}

#ifdef ERRSTAT_TESTSUITE
int main(int ac, char **av)
{
    errstat_t err;

    printf("sizeof(errstat_t) == %d\n", sizeof(errstat_t));

    /* test error value */
    errstat_clr(&err);
    errstat_set_op(&err, 1);
    errstat_set_rc(&err, 42);
    printf("op( %d ) rc( %d ) val( %d )\n", errstat_get_op(&err),
           errstat_get_rc(&err), errstat_has_int(&err));

    /* test error string */
    printf("empty? str( '%s' )\n", errstat_get_str(&err));
    errstat_set_str(&err, "foo");
    errstat_cat_str(&err, "bar");
    printf("str( '%s' )\n", errstat_get_str(&err));

    /* test format error string */
    errstat_clr_str(&err);
    errstat_set_strf(&err, "%d ", 42);
    errstat_cat_strf(&err, "%s", "forty-two");
    printf("val( %d ) str( '%s' )\n", errstat_has_int(&err),
           errstat_get_str(&err));

    /* test header error string */
    errstat_set_hdr(&err, "test");
    printf("str/hdr( '%s' )\n", errstat_get_str(&err));
    errstat_cat_hdr(&err, "> ");
    printf("str/hdr( '%s' )\n", errstat_get_str(&err));

    /* test change error string but keeping header */
    errstat_set_strf(&err, "%s", "forty-two ");
    errstat_cat_strf(&err, "%d ", 42);
    printf("val( %d ) str/hdr( '%s' )\n", errstat_has_int(&err),
           errstat_get_str(&err));

    /* test clearing everything and start again */
    errstat_clr_hdr(&err);
    printf("val( %d ) str( '%s' )\n", errstat_has_int(&err),
           errstat_get_str(&err));
    errstat_clr_str(&err);
    printf("empty? str( '%s' )\n", errstat_get_str(&err));
    errstat_set_strf(&err, "%s", "forty-two ");
    errstat_cat_strf(&err, "%d ", 42);
    printf("val( %d ) str( '%s' )\n", errstat_has_int(&err),
           errstat_get_str(&err));

    return 0;
}
#endif
