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

#ifndef INCLUDED_ERRSTAT_H
#define INCLUDED_ERRSTAT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>

#include <compile_time_assert.h>

/**
 * @brief  Set of size for errstat definition
 */
enum { ERRSTAT_STR_SZ = 240 };

/**
 * @brief  Error codes which could be returned when using errstat.
 */
enum {
    ERRSTAT_ERR_PARAM = -1 /**< invalid errstat_t parameter */
};

/**
 * @brief  errstat structure it is public to avoid malloc. Callers
 * should use appropriate function to access member of structure.
 */
struct errstat {
    int errval;                  /**< Error code value */
    int errhdrlen;               /**< internal offset in errstr */
    int reserved[2];             /**< for later use */
    char errstr[ERRSTAT_STR_SZ]; /**< Error string */
};

typedef struct errstat errstat_t;

enum { ERRSTAT_LEN = 4 + 4 + 8 + ERRSTAT_STR_SZ };

BB_COMPILE_TIME_ASSERT(errstat_len, sizeof(errstat_t) == ERRSTAT_LEN);

/**
 * @brief  Clear an errstat_t setting its value to 0 and string to "".
 *
 * @param[in/out]   err     errstat_t to be cleared.
 */
void errstat_clr(errstat_t *err);

/**
 * @brief  Sets code associated with given errstat operation.
 *
 * @param[in/out]   err     errstat_t to be set.
 * @param[in]       rc      error value (rc should be >= 0)
 */
void errstat_set_rc(errstat_t *err, unsigned int rc);

/**
 * @brief  Returns operation associated with given errstat.
 *
 * @param[in]       err     errstat_t to be queried.
 *
 * @return Error value specific for operation or
 * ERRSTAT_ERR_PARAM if err is invalid.
 */
int errstat_get_rc(errstat_t *err);

/**
 * @brief  Returns error string associated with errstat_t.
 *
 * @param[in]       err     errstat_t to be queried
 *
 * @return error string or empty string if err is invalid.
 */
const char *errstat_get_str(errstat_t *err);

/**
 * @brief  Clears error string of given errstat_t.
 *
 * @param[in/out]   err     errstat_t to be cleared.
 */
void errstat_clr_str(errstat_t *err);

/**
 * @brief  Clears error header of given errstat_t.
 *
 * @param[in/out]   err     errstat_t to be cleared.
 */
void errstat_clr_hdr(errstat_t *err);

/**
 * @brief  Appends given error string to current string.
 *
 * @param[in/out]   err     errstat_t to be set.
 * @param[in]       errstr  string to append.
 */
void errstat_cat_str(errstat_t *err, const char *errstr);

/**
 * @brief  Sets given error string to errstat_t.
 *
 * @param[in/out]   err     errstat_t to be set.
 * @param[in]       errstr  string to set.
 */
void errstat_set_str(errstat_t *err, const char *errstr);

/**
 * @brief  Appends given error header to current header.
 *
 * @param[in/out]   err     errstat_t to be set.
 * @param[in]       errhdr  header to append.
 */
void errstat_cat_hdr(errstat_t *err, const char *errhdr);

/**
 * @brief  Sets given error header to errstat_t.
 *
 * @param[in/out]   err     errstat_t to be set.
 * @param[in]       errhdr  header to set.
 */
void errstat_set_hdr(errstat_t *err, const char *errhdr);

/**
 * @brief  Appends given arguments in a formatted manner to current string.
 *
 * @param[in/out]   err     errstat_t to be set.
 * @param[in]       fmt     format (@see snprintf)
 * @param[in]       ...     arguments (@see snprintf)
 */
void errstat_cat_strf(errstat_t *err, const char *fmt, ...);

/**
 * @brief  Sets given arguments in a formatted manner to current string.
 *
 * @param[in/out]   err     errstat_t to be set.
 * @param[in]       fmt     format (@see snprintf)
 * @param[in]       ...     arguments (@see snprintf)
 */
void errstat_set_strf(errstat_t *err, const char *fmt, ...);

/**
 * @brief  Appends given arguments in a formatted manner to current header.
 *
 * @param[in/out]   err     errstat_t to be set.
 * @param[in]       fmt     format (@see snprintf)
 * @param[in]       ...     arguments (@see snprintf)
 */
void errstat_cat_hdrf(errstat_t *err, const char *fmt, ...);

/**
 * @brief  Sets given arguments in a formatted manner to current header.
 *
 * @param[in/out]   err     errstat_t to be set.
 * @param[in]       fmt     format (@see snprintf)
 * @param[in]       ...     arguments (@see snprintf)
 */
void errstat_set_hdrf(errstat_t *err, const char *fmt, ...);

/**
 * @brief  Appends given arguments in a formatted manner to current string.
 *
 * @param[in/out]   err     errstat_t to be set.
 * @param[in]       fmt     format (@see vsnprintf)
 * @param[in]       ap      arguments (@see vsnprintf)
 */
void errstat_cat_strfap(errstat_t *err, const char *fmt, va_list ap);

/**
 * @brief  Sets given arguments in a formatted manner to current string.
 *
 * @param[in/out]   err     errstat_t to be set.
 * @param[in]       fmt     format (@see vsnprintf)
 * @param[in]       ap      arguments (@see vsnprintf)
 */
void errstat_set_strfap(errstat_t *err, const char *fmt, va_list ap);

/**
 * @brief  Appends given arguments in a formatted manner to current header.
 *
 * @param[in/out]   err     errstat_t to be set.
 * @param[in]       fmt     format (@see vsnprintf)
 * @param[in]       ap      arguments (@see vsnprintf)
 */
void errstat_cat_hdrfap(errstat_t *err, const char *fmt, va_list ap);

/**
 * @brief  Sets given arguments in a formatted manner to current header.
 *
 * @param[in/out]   err     errstat_t to be set.
 * @param[in]       fmt     format (@see vsnprintf)
 * @param[in]       ap      arguments (@see vsnprintf)
 */
void errstat_set_hdrfap(errstat_t *err, const char *fmt, va_list ap);

/** 
 * @brief Sets both error code and error string 
 *
 * @param[in/out]   err     errstat_t to be set.
 * @param[in]       rc      error value (rc should be >= 0)
 * @param[in]       fmt     format (@see snprintf)
 * @param[in]       ...     arguments (@see snprintf)
 */
void errstat_set_rcstrf(errstat_t *err, unsigned int rc, const char *fmt, ...);

#ifdef __cplusplus
}
#endif

#endif
