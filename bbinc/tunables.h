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

#ifndef _TUNABLES_H
#define _TUNABLES_H

#include "plhash.h"

typedef enum {
    /*
      The tunable is not required to have an argument with set through
      lrl file during startup. This bit is mostly to preserve backward
      compatibility and to keep the setting of boolean tunables simple.
      Note: This bit is ignored for PUT TUNABLE command, where a value
      is always needed.
    */
    NOARG = 1 << 0,

    /* The tunable can't be changed at runtime. */
    READONLY = 1 << 1,

    /* The tunable does not accept 0 as value (>0). */
    NOZERO = 1 << 2,

    /*
      The tunable can accept positive as well as negative values.
      By default, only natural numbers (>= 0) are allowed.
    */
    SIGNED = 1 << 3,

    /*
      Consider the tunable's actual value as inverse of its current value.
      This bit has been added to handle the tunables which share same variable
      to store their value. (e.g. early/noearly)
    */
    INVERSE_VALUE = 1 << 4,

    /* The tunable has been deprecated. */
    DEPRECATED = 1 << 5,

    /* The tunable has been marked experimental. */
    EXPERIMENTAL = 1 << 6,

    /* The tunable is for internal use only. */
    INTERNAL = 1 << 7,

    /* Set this flag if no argument has been specified. */
    EMPTY = 1 << 8,
} comdb2_tunable_flag;

/*
  Tunable types

  * TUNABLE_INTEGER
    The tunables of this type, by default, allow all numbers >= 0.
    NOZERO bit can be set to allow all values > 0. The range can be
    extended by setting SIGNED bit to allow positive as well as
    negative values.

  * TUNABLE_DOUBLE
    The tunable, by default, accepts all real numbers >= 0. Like,
    TUNABLE_INTEGER zeros can be disallowed by setting NOZERO bit
    and, similarly, range can be extended by setting SIGNED bit.

  * TUNABLE_BOOLEAN
    The tunable can accept boolean values (0, 1, off, on).

  * TUNABLE_STRING
    The tunable can accept string as value.

  * TUNABLE_ENUM
    The tunable can accept only a limited set of strings. This can be
    done by defining the 'update' function pointer for the tunable.
*/
typedef enum {
    TUNABLE_INTEGER,
    TUNABLE_DOUBLE,
    TUNABLE_BOOLEAN,
    TUNABLE_STRING,
    TUNABLE_ENUM,

    /* Must always be the last. */
    TUNABLE_INVALID,
} comdb2_tunable_type;

struct comdb2_tunable {
    /* Name of the tunable. (Mandatory) */
    char *name;

    /*
      Description of the tunable. (Optional)
      Nirbhay TODO: Make it mandatory.
    */
    char *descr;

    /* Type of the tunable. (Mandatory) */
    comdb2_tunable_type type;

    /* Variable to store tunable's value. (Mandatory) */
    void *var;

    /* Flags. */
    unsigned long int flags;

    /* Returns the value of the tunable in cstring. (Optional) */
    const char *(*value)(void *);

    /*
      Verify the value of the tunable. (Optional)
      Verify whether the new value is valid.
    */
    int (*verify)(void *, void *);

    /*
      Update the value of the tunable. (Optional)
      Note: read-only tunables are not updatable.
    */
    int (*update)(void *, void *);

    /* Frees the memory used by tunable. (Optional) */
    void (*destroy)(void *);
};
typedef struct comdb2_tunable comdb2_tunable;

typedef struct {
    /* Number of tunables registered. */
    int count;

    /* Do not register tunables any more. */
    int freeze;

    /* Array of all the registered tunables. */
    comdb2_tunable **array;

    /* HASH of all the registered tunables (for quick lookup by name). */
    hash_t *hash;

    /* Mutex */
    pthread_mutex_t mu;
} comdb2_tunables;

#define REGISTER_TUNABLE(NAME, DESCR, TYPE, VAR_PTR, FLAGS, VALUE_FN,          \
                         VERIFY_FN, UPDATE_FN, DESTROY_FN)                     \
    do {                                                                       \
        comdb2_tunable t = {NAME,     DESCR,     TYPE,      VAR_PTR,   FLAGS,  \
                            VALUE_FN, VERIFY_FN, UPDATE_FN, DESTROY_FN};       \
        register_tunable(t);                                                   \
    } while (0)

/* Resigter the tunable to the array of tunables. */
int register_tunable(comdb2_tunable tunable);

/* Returns name of the specified tunable type. */
const char *tunable_type(comdb2_tunable_type type);

/* Verify whether the given value is in [0-100] range. */
int percent_verify(void *context, void *percent);

#endif /* _TUNABLES_H */
