/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Internal interface definitions for SQLite.
**
*/
#ifndef _SQLITEINT_H_
#define _SQLITEINT_H_

/* COMDB2 MODIFICATION */
#include <cheapstack.h>
#undef debug_raw

/*
** Include the header file used to customize the compiler options for MSVC.
** This should be done first so that it can successfully prevent spurious
** compiler warnings due to subsequent content in this file and other files
** that are included by this file.
*/
#include "msvc.h"

/*
** Special setup for VxWorks
*/
#include "vxworks.h"

/*
** These #defines should enable >2GB file support on POSIX if the
** underlying operating system supports it.  If the OS lacks
** large file support, or if the OS is windows, these should be no-ops.
**
** Ticket #2739:  The _LARGEFILE_SOURCE macro must appear before any
** system #includes.  Hence, this block of code must be the very first
** code in all source files.
**
** Large file support can be disabled using the -DSQLITE_DISABLE_LFS switch
** on the compiler command line.  This is necessary if you are compiling
** on a recent machine (ex: Red Hat 7.2) but you want your code to work
** on an older machine (ex: Red Hat 6.0).  If you compile on Red Hat 7.2
** without this option, LFS is enable.  But LFS does not exist in the kernel
** in Red Hat 6.0, so the code won't work.  Hence, for maximum binary
** portability you should omit LFS.
**
** The previous paragraph was written in 2005.  (This paragraph is written
** on 2008-11-28.) These days, all Linux kernels support large files, so
** you should probably leave LFS enabled.  But some embedded platforms might
** lack LFS in which case the SQLITE_DISABLE_LFS macro might still be useful.
**
** Similar is true for Mac OS X.  LFS is only supported on Mac OS X 9 and later.
*/
#ifndef SQLITE_DISABLE_LFS
# define _LARGE_FILE       1
# ifndef _FILE_OFFSET_BITS
#   define _FILE_OFFSET_BITS 64
# endif
# define _LARGEFILE_SOURCE 1
#endif

/* Needed for various definitions... */
#if defined(__GNUC__) && !defined(_GNU_SOURCE)
# define _GNU_SOURCE
#endif

#if defined(__OpenBSD__) && !defined(_BSD_SOURCE)
# define _BSD_SOURCE
#endif

/*
** For MinGW, check to see if we can include the header file containing its
** version information, among other things.  Normally, this internal MinGW
** header file would [only] be included automatically by other MinGW header
** files; however, the contained version information is now required by this
** header file to work around binary compatibility issues (see below) and
** this is the only known way to reliably obtain it.  This entire #if block
** would be completely unnecessary if there was any other way of detecting
** MinGW via their preprocessor (e.g. if they customized their GCC to define
** some MinGW-specific macros).  When compiling for MinGW, either the
** _HAVE_MINGW_H or _HAVE__MINGW_H (note the extra underscore) macro must be
** defined; otherwise, detection of conditions specific to MinGW will be
** disabled.
*/
#if defined(_HAVE_MINGW_H)
# include "mingw.h"
#elif defined(_HAVE__MINGW_H)
# include "_mingw.h"
#endif

/*
** For MinGW version 4.x (and higher), check to see if the _USE_32BIT_TIME_T
** define is required to maintain binary compatibility with the MSVC runtime
** library in use (e.g. for Windows XP).
*/
#if !defined(_USE_32BIT_TIME_T) && !defined(_USE_64BIT_TIME_T) && \
    defined(_WIN32) && !defined(_WIN64) && \
    defined(__MINGW_MAJOR_VERSION) && __MINGW_MAJOR_VERSION >= 4 && \
    defined(__MSVCRT__)
# define _USE_32BIT_TIME_T
#endif

/* The public SQLite interface.  The _FILE_OFFSET_BITS macro must appear
** first in QNX.  Also, the _USE_32BIT_TIME_T macro must appear first for
** MinGW.
*/
#include "sqlitex.h"

/*
** Include the configuration header output by 'configure' if we're using the
** autoconf-based build
*/
#ifdef _HAVE_SQLITE_CONFIG_H
#include "config.h"
#endif

#include "sqliteLimit.h"

/* Disable nuisance warnings on Borland compilers */
#if defined(__BORLANDC__)
#pragma warn -rch /* unreachable code */
#pragma warn -ccc /* Condition is always true or false */
#pragma warn -aus /* Assigned value is never used */
#pragma warn -csu /* Comparing signed and unsigned */
#pragma warn -spa /* Suspicious pointer arithmetic */
#endif

/*
** Include standard header files as necessary
*/
#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif
#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif

/* COMDB2 MODIFICATION */
#define SQLITE_INDEX_SAMPLES 10

/*
** The following macros are used to cast pointers to integers and
** integers to pointers.  The way you do this varies from one compiler
** to the next, so we have developed the following set of #if statements
** to generate appropriate macros for a wide range of compilers.
**
** The correct "ANSI" way to do this is to use the intptr_t type. 
** Unfortunately, that typedef is not available on all compilers, or
** if it is available, it requires an #include of specific headers
** that vary from one machine to the next.
**
** Ticket #3860:  The llvm-gcc-4.2 compiler from Apple chokes on
** the ((void*)&((char*)0)[X]) construct.  But MSVC chokes on ((void*)(X)).
** So we have to define the macros in different ways depending on the
** compiler.
*/
#if defined(__PTRDIFF_TYPE__)  /* This case should work for GCC */
# define SQLITE_INT_TO_PTR(X)  ((void*)(__PTRDIFF_TYPE__)(X))
# define SQLITE_PTR_TO_INT(X)  ((int)(__PTRDIFF_TYPE__)(X))
#elif !defined(__GNUC__)       /* Works for compilers other than LLVM */
# define SQLITE_INT_TO_PTR(X)  ((void*)&((char*)0)[X])
# define SQLITE_PTR_TO_INT(X)  ((int)(((char*)X)-(char*)0))
#elif defined(HAVE_STDINT_H)   /* Use this case if we have ANSI headers */
# define SQLITE_INT_TO_PTR(X)  ((void*)(intptr_t)(X))
# define SQLITE_PTR_TO_INT(X)  ((int)(intptr_t)(X))
#else                          /* Generates a warning - but it always works */
# define SQLITE_INT_TO_PTR(X)  ((void*)(X))
# define SQLITE_PTR_TO_INT(X)  ((int)(X))
#endif

/*
** A macro to hint to the compiler that a function should not be
** inlined.
*/
#if defined(__GNUC__)
#  define SQLITE_NOINLINE  __attribute__((noinline))
#elif defined(_MSC_VER) && _MSC_VER>=1310
#  define SQLITE_NOINLINE  __declspec(noinline)
#else
#  define SQLITE_NOINLINE
#endif

/*
** The SQLITE_THREADSAFE macro must be defined as 0, 1, or 2.
** 0 means mutexes are permanently disable and the library is never
** threadsafe.  1 means the library is serialized which is the highest
** level of threadsafety.  2 means the library is multithreaded - multiple
** threads can use SQLite as long as no two threads try to use the same
** database connection at the same time.
**
** Older versions of SQLite used an optional THREADSAFE macro.
** We support that for legacy.
*/
#if !defined(SQLITE_THREADSAFE)
# if defined(THREADSAFE)
#   define SQLITE_THREADSAFE THREADSAFE
# else
#   define SQLITE_THREADSAFE 1 /* IMP: R-07272-22309 */
# endif
#endif

/*
** Powersafe overwrite is on by default.  But can be turned off using
** the -DSQLITE_POWERSAFE_OVERWRITE=0 command-line option.
*/
#ifndef SQLITE_POWERSAFE_OVERWRITE
# define SQLITE_POWERSAFE_OVERWRITE 1
#endif

/*
** EVIDENCE-OF: R-25715-37072 Memory allocation statistics are enabled by
** default unless SQLite is compiled with SQLITE_DEFAULT_MEMSTATUS=0 in
** which case memory allocation statistics are disabled by default.
*/
#if !defined(SQLITE_DEFAULT_MEMSTATUS)
# define SQLITE_DEFAULT_MEMSTATUS 1
#endif

/*
** Exactly one of the following macros must be defined in order to
** specify which memory allocation subsystem to use.
**
**     SQLITE_SYSTEM_MALLOC          // Use normal system malloc()
**     SQLITE_WIN32_MALLOC           // Use Win32 native heap API
**     SQLITE_ZERO_MALLOC            // Use a stub allocator that always fails
**     SQLITE_MEMDEBUG               // Debugging version of system malloc()
**
** On Windows, if the SQLITE_WIN32_MALLOC_VALIDATE macro is defined and the
** assert() macro is enabled, each call into the Win32 native heap subsystem
** will cause HeapValidate to be called.  If heap validation should fail, an
** assertion will be triggered.
**
** If none of the above are defined, then set SQLITE_SYSTEM_MALLOC as
** the default.
*/
#if defined(SQLITE_SYSTEM_MALLOC) \
  + defined(SQLITE_WIN32_MALLOC) \
  + defined(SQLITE_ZERO_MALLOC) \
  + defined(SQLITE_MEMDEBUG)>1
# error "Two or more of the following compile-time configuration options\
 are defined but at most one is allowed:\
 SQLITE_SYSTEM_MALLOC, SQLITE_WIN32_MALLOC, SQLITE_MEMDEBUG,\
 SQLITE_ZERO_MALLOC"
#endif
#if defined(SQLITE_SYSTEM_MALLOC) \
  + defined(SQLITE_WIN32_MALLOC) \
  + defined(SQLITE_ZERO_MALLOC) \
  + defined(SQLITE_MEMDEBUG)==0
# define SQLITE_SYSTEM_MALLOC 1
#endif

/*
** If SQLITE_MALLOC_SOFT_LIMIT is not zero, then try to keep the
** sizes of memory allocations below this value where possible.
*/
#if !defined(SQLITE_MALLOC_SOFT_LIMIT)
# define SQLITE_MALLOC_SOFT_LIMIT 1024
#endif

/*
** We need to define _XOPEN_SOURCE as follows in order to enable
** recursive mutexes on most Unix systems and fchmod() on OpenBSD.
** But _XOPEN_SOURCE define causes problems for Mac OS X, so omit
** it.
*/
/* COMDB2 MODIFICATION */
/* Not compiling on sun with the following */
#if 0
#if !defined(_XOPEN_SOURCE) && !defined(__DARWIN__) && !defined(__APPLE__)
#  define _XOPEN_SOURCE 600
#endif
#endif

/*
** NDEBUG and SQLITE_DEBUG are opposites.  It should always be true that
** defined(NDEBUG)==!defined(SQLITE_DEBUG).  If this is not currently true,
** make it true by defining or undefining NDEBUG.
**
** Setting NDEBUG makes the code smaller and faster by disabling the
** assert() statements in the code.  So we want the default action
** to be for NDEBUG to be set and NDEBUG to be undefined only if SQLITE_DEBUG
** is set.  Thus NDEBUG becomes an opt-in rather than an opt-out
** feature.
*/
#if !defined(NDEBUG) && !defined(SQLITE_DEBUG) 
# define NDEBUG 1
#endif
#if defined(NDEBUG) && defined(SQLITE_DEBUG)
# undef NDEBUG
#endif

/*
** Enable SQLITE_ENABLE_EXPLAIN_COMMENTS if SQLITE_DEBUG is turned on.
*/
#if !defined(SQLITE_ENABLE_EXPLAIN_COMMENTS) && defined(SQLITE_DEBUG)
# define SQLITE_ENABLE_EXPLAIN_COMMENTS 1
#endif

/*
** The testcase() macro is used to aid in coverage testing.  When 
** doing coverage testing, the condition inside the argument to
** testcase() must be evaluated both true and false in order to
** get full branch coverage.  The testcase() macro is inserted
** to help ensure adequate test coverage in places where simple
** condition/decision coverage is inadequate.  For example, testcase()
** can be used to make sure boundary values are tested.  For
** bitmask tests, testcase() can be used to make sure each bit
** is significant and used at least once.  On switch statements
** where multiple cases go to the same block of code, testcase()
** can insure that all cases are evaluated.
**
*/
#ifdef SQLITE_COVERAGE_TEST
  void sqlitexCoverage(int);
# define testcase(X)  if( X ){ sqlitexCoverage(__LINE__); }
#else
# define testcase(X)
#endif

/*
** The TESTONLY macro is used to enclose variable declarations or
** other bits of code that are needed to support the arguments
** within testcase() and assert() macros.
*/
#if !defined(NDEBUG) || defined(SQLITE_COVERAGE_TEST)
# define TESTONLY(X)  X
#else
# define TESTONLY(X)
#endif

/*
** Sometimes we need a small amount of code such as a variable initialization
** to setup for a later assert() statement.  We do not want this code to
** appear when assert() is disabled.  The following macro is therefore
** used to contain that setup code.  The "VVA" acronym stands for
** "Verification, Validation, and Accreditation".  In other words, the
** code within VVA_ONLY() will only run during verification processes.
*/
#ifndef NDEBUG
# define VVA_ONLY(X)  X
#else
# define VVA_ONLY(X)
#endif

/*
** The ALWAYS and NEVER macros surround boolean expressions which 
** are intended to always be true or false, respectively.  Such
** expressions could be omitted from the code completely.  But they
** are included in a few cases in order to enhance the resilience
** of SQLite to unexpected behavior - to make the code "self-healing"
** or "ductile" rather than being "brittle" and crashing at the first
** hint of unplanned behavior.
**
** In other words, ALWAYS and NEVER are added for defensive code.
**
** When doing coverage testing ALWAYS and NEVER are hard-coded to
** be true and false so that the unreachable code they specify will
** not be counted as untested code.
*/
#if defined(SQLITE_COVERAGE_TEST)
# define ALWAYS(X)      (1)
# define NEVER(X)       (0)
#elif !defined(NDEBUG)
# define ALWAYS(X)      ((X)?1:(assert(0),0))
# define NEVER(X)       ((X)?(assert(0),1):0)
#else
# define ALWAYS(X)      (X)
# define NEVER(X)       (X)
#endif

/*
** Return true (non-zero) if the input is an integer that is too large
** to fit in 32-bits.  This macro is used inside of various testcase()
** macros to verify that we have tested SQLite for large-file support.
*/
#define IS_BIG_INT(X)  (((X)&~(i64)0xffffffff)!=0)

/*
** The macro unlikely() is a hint that surrounds a boolean
** expression that is usually false.  Macro likely() surrounds
** a boolean expression that is usually true.  These hints could,
** in theory, be used by the compiler to generate better code, but
** currently they are just comments for human readers.
*/
/* COMDB2 MODIFICATION */
#ifndef likely /* not already defined in comdb2.h */
#if defined(__GNUC__) || defined(__IBMC__)
# define likely(x)      __builtin_expect(!!(x), 1)
# define unlikely(x)    __builtin_expect(!!(x), 0)
#else
# define likely(X)    (X)
# define unlikely(X)  (X)
#endif
#endif

#include "hash.h"
#include "parse.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stddef.h>

/*
** If compiling for a processor that lacks floating point support,
** substitute integer for floating-point
*/
#ifdef SQLITE_OMIT_FLOATING_POINT
# define double sqlite_int64
# define float sqlite_int64
# define LONGDOUBLE_TYPE sqlite_int64
# ifndef SQLITE_BIG_DBL
#   define SQLITE_BIG_DBL (((sqlitex_int64)1)<<50)
# endif
# define SQLITE_OMIT_DATETIME_FUNCS 1
# define SQLITE_OMIT_TRACE 1
# undef SQLITE_MIXED_ENDIAN_64BIT_FLOAT
# undef SQLITE_HAVE_ISNAN
#endif
#ifndef SQLITE_BIG_DBL
# define SQLITE_BIG_DBL (1e99)
#endif

/*
** OMIT_TEMPDB is set to 1 if SQLITE_OMIT_TEMPDB is defined, or 0
** afterward. Having this macro allows us to cause the C compiler 
** to omit code used by TEMP tables without messy #ifndef statements.
*/
#ifdef SQLITE_OMIT_TEMPDB
#define OMIT_TEMPDB 1
#else
#define OMIT_TEMPDB 0
#endif

/*
** The "file format" number is an integer that is incremented whenever
** the VDBE-level file format changes.  The following macros define the
** the default file format for new databases and the maximum file format
** that the library can read.
*/
#define SQLITE_MAX_FILE_FORMAT 4
#ifndef SQLITE_DEFAULT_FILE_FORMAT
# define SQLITE_DEFAULT_FILE_FORMAT 4
#endif

/*
** Determine whether triggers are recursive by default.  This can be
** changed at run-time using a pragma.
*/
#ifndef SQLITE_DEFAULT_RECURSIVE_TRIGGERS
# define SQLITE_DEFAULT_RECURSIVE_TRIGGERS 0
#endif

/*
** Provide a default value for SQLITE_TEMP_STORE in case it is not specified
** on the command-line
*/
#ifndef SQLITE_TEMP_STORE
# define SQLITE_TEMP_STORE 1
# define SQLITE_TEMP_STORE_xc 1  /* Exclude from ctime.c */
#endif

/*
** If no value has been provided for SQLITE_MAX_WORKER_THREADS, or if
** SQLITE_TEMP_STORE is set to 3 (never use temporary files), set it 
** to zero.
*/
#if SQLITE_TEMP_STORE==3 || SQLITE_THREADSAFE==0
# undef SQLITE_MAX_WORKER_THREADS
# define SQLITE_MAX_WORKER_THREADS 0
#endif
#ifndef SQLITE_MAX_WORKER_THREADS
# define SQLITE_MAX_WORKER_THREADS 8
#endif
#ifndef SQLITE_DEFAULT_WORKER_THREADS
# define SQLITE_DEFAULT_WORKER_THREADS 0
#endif
#if SQLITE_DEFAULT_WORKER_THREADS>SQLITE_MAX_WORKER_THREADS
# undef SQLITE_MAX_WORKER_THREADS
# define SQLITE_MAX_WORKER_THREADS SQLITE_DEFAULT_WORKER_THREADS
#endif


/*
** GCC does not define the offsetof() macro so we'll have to do it
** ourselves.
*/
#ifndef offsetof
#define offsetof(STRUCTURE,FIELD) ((int)((char*)&((STRUCTURE*)0)->FIELD))
#endif

/*
** Macros to compute minimum and maximum of two numbers.
*/
#define MIN(A,B) ((A)<(B)?(A):(B))
#define MAX(A,B) ((A)>(B)?(A):(B))

/*
** Swap two objects of type TYPE.
*/
#define SWAP(TYPE,A,B) {TYPE t=A; A=B; B=t;}

/*
** Check to see if this machine uses EBCDIC.  (Yes, believe it or
** not, there are still machines out there that use EBCDIC.)
*/
#if 'A' == '\301'
# define SQLITE_EBCDIC 1
#else
# define SQLITE_ASCII 1
#endif

/*
** Integers of known sizes.  These typedefs might change for architectures
** where the sizes very.  Preprocessor macros are available so that the
** types can be conveniently redefined at compile-type.  Like this:
**
**         cc '-DUINTPTR_TYPE=long long int' ...
*/
#ifndef UINT32_TYPE
# ifdef HAVE_UINT32_T
#  define UINT32_TYPE uint32_t
# else
#  define UINT32_TYPE unsigned int
# endif
#endif
#ifndef UINT16_TYPE
# ifdef HAVE_UINT16_T
#  define UINT16_TYPE uint16_t
# else
#  define UINT16_TYPE unsigned short int
# endif
#endif
#ifndef INT16_TYPE
# ifdef HAVE_INT16_T
#  define INT16_TYPE int16_t
# else
#  define INT16_TYPE short int
# endif
#endif
#ifndef UINT8_TYPE
# ifdef HAVE_UINT8_T
#  define UINT8_TYPE uint8_t
# else
#  define UINT8_TYPE unsigned char
# endif
#endif
#ifndef INT8_TYPE
# ifdef HAVE_INT8_T
#  define INT8_TYPE int8_t
# else
#  define INT8_TYPE signed char
# endif
#endif
#ifndef LONGDOUBLE_TYPE
# define LONGDOUBLE_TYPE long double
#endif
typedef sqlite_int64 i64;          /* 8-byte signed integer */
typedef sqlite_uint64 u64;         /* 8-byte unsigned integer */
typedef UINT32_TYPE u32;           /* 4-byte unsigned integer */
typedef UINT16_TYPE u16;           /* 2-byte unsigned integer */
typedef INT16_TYPE i16;            /* 2-byte signed integer */
typedef UINT8_TYPE u8;             /* 1-byte unsigned integer */
typedef INT8_TYPE i8;              /* 1-byte signed integer */

/*
** SQLITE_MAX_U32 is a u64 constant that is the maximum u64 value
** that can be stored in a u32 without loss of data.  The value
** is 0x00000000ffffffff.  But because of quirks of some compilers, we
** have to specify the value in the less intuitive manner shown:
*/
#define SQLITE_MAX_U32  ((((u64)1)<<32)-1)

/*
** The datatype used to store estimates of the number of rows in a
** table or index.  This is an unsigned integer type.  For 99.9% of
** the world, a 32-bit integer is sufficient.  But a 64-bit integer
** can be used at compile-time if desired.
*/
#ifdef SQLITE_64BIT_STATS
 typedef u64 tRowcnt;    /* 64-bit only if requested at compile-time */
#else
 typedef u32 tRowcnt;    /* 32-bit is the default */
#endif

/*
** Estimated quantities used for query planning are stored as 16-bit
** logarithms.  For quantity X, the value stored is 10*log2(X).  This
** gives a possible range of values of approximately 1.0e986 to 1e-986.
** But the allowed values are "grainy".  Not every value is representable.
** For example, quantities 16 and 17 are both represented by a LogEst
** of 40.  However, since LogEst quantities are suppose to be estimates,
** not exact values, this imprecision is not a problem.
**
** "LogEst" is short for "Logarithmic Estimate".
**
** Examples:
**      1 -> 0              20 -> 43          10000 -> 132
**      2 -> 10             25 -> 46          25000 -> 146
**      3 -> 16            100 -> 66        1000000 -> 199
**      4 -> 20           1000 -> 99        1048576 -> 200
**     10 -> 33           1024 -> 100    4294967296 -> 320
**
** The LogEst can be negative to indicate fractional values. 
** Examples:
**
**    0.5 -> -10           0.1 -> -33        0.0625 -> -40
*/
typedef INT16_TYPE LogEst;

/*
** Macros to determine whether the machine is big or little endian,
** and whether or not that determination is run-time or compile-time.
**
** For best performance, an attempt is made to guess at the byte-order
** using C-preprocessor macros.  If that is unsuccessful, or if
** -DSQLITE_RUNTIME_BYTEORDER=1 is set, then byte-order is determined
** at run-time.
*/
#ifdef SQLITE_AMALGAMATION
const int sqlitexone = 1;
#else
extern const int sqlitexone;
#endif
#if (defined(i386)     || defined(__i386__)   || defined(_M_IX86) ||    \
     defined(__x86_64) || defined(__x86_64__) || defined(_M_X64)  ||    \
     defined(_M_AMD64) || defined(_M_ARM)     || defined(__x86)   ||    \
     defined(__arm__)) && !defined(SQLITE_RUNTIME_BYTEORDER)
# define SQLITE_BYTEORDER    1234
# define SQLITE_BIGENDIAN    0
# define SQLITE_LITTLEENDIAN 1
# define SQLITE_UTF16NATIVE  SQLITE_UTF16LE
#endif
#if (defined(sparc)    || defined(__ppc__))  \
    && !defined(SQLITE_RUNTIME_BYTEORDER)
# define SQLITE_BYTEORDER    4321
# define SQLITE_BIGENDIAN    1
# define SQLITE_LITTLEENDIAN 0
# define SQLITE_UTF16NATIVE  SQLITE_UTF16BE
#endif
#if !defined(SQLITE_BYTEORDER)
# define SQLITE_BYTEORDER    0     /* 0 means "unknown at compile-time" */
# define SQLITE_BIGENDIAN    (*(char *)(&sqlitexone)==0)
# define SQLITE_LITTLEENDIAN (*(char *)(&sqlitexone)==1)
# define SQLITE_UTF16NATIVE  (SQLITE_BIGENDIAN?SQLITE_UTF16BE:SQLITE_UTF16LE)
#endif

/*
** Constants for the largest and smallest possible 64-bit signed integers.
** These macros are designed to work correctly on both 32-bit and 64-bit
** compilers.
*/
#define LARGEST_INT64  (0xffffffff|(((i64)0x7fffffff)<<32))
#define SMALLEST_INT64 (((i64)-1) - LARGEST_INT64)

/* 
** Round up a number to the next larger multiple of 8.  This is used
** to force 8-byte alignment on 64-bit architectures.
*/
#define ROUND8(x)     (((x)+7)&~7)

/*
** Round down to the nearest multiple of 8
*/
#define ROUNDDOWN8(x) ((x)&~7)

/*
** Assert that the pointer X is aligned to an 8-byte boundary.  This
** macro is used only within assert() to verify that the code gets
** all alignment restrictions correct.
**
** Except, if SQLITE_4_BYTE_ALIGNED_MALLOC is defined, then the
** underlying malloc() implementation might return us 4-byte aligned
** pointers.  In that case, only verify 4-byte alignment.
*/
#ifdef SQLITE_4_BYTE_ALIGNED_MALLOC
# define EIGHT_BYTE_ALIGNMENT(X)   ((((char*)(X) - (char*)0)&3)==0)
#else
# define EIGHT_BYTE_ALIGNMENT(X)   ((((char*)(X) - (char*)0)&7)==0)
#endif

/*
** Disable MMAP on platforms where it is known to not work
*/
#if defined(__OpenBSD__) || defined(__QNXNTO__)
# undef SQLITE_MAX_MMAP_SIZE
# define SQLITE_MAX_MMAP_SIZE 0
#endif

/*
** Default maximum size of memory used by memory-mapped I/O in the VFS
*/
#ifdef __APPLE__
# include <TargetConditionals.h>
# if TARGET_OS_IPHONE
#   undef SQLITE_MAX_MMAP_SIZE
#   define SQLITE_MAX_MMAP_SIZE 0
# endif
#endif
#ifndef SQLITE_MAX_MMAP_SIZE
# if defined(__linux__) \
  || defined(_WIN32) \
  || (defined(__APPLE__) && defined(__MACH__)) \
  || defined(__sun)
#   define SQLITE_MAX_MMAP_SIZE 0x7fff0000  /* 2147418112 */
# else
#   define SQLITE_MAX_MMAP_SIZE 0
# endif
# define SQLITE_MAX_MMAP_SIZE_xc 1 /* exclude from ctime.c */
#endif

/*
** The default MMAP_SIZE is zero on all platforms.  Or, even if a larger
** default MMAP_SIZE is specified at compile-time, make sure that it does
** not exceed the maximum mmap size.
*/
#ifndef SQLITE_DEFAULT_MMAP_SIZE
# define SQLITE_DEFAULT_MMAP_SIZE 0
# define SQLITE_DEFAULT_MMAP_SIZE_xc 1  /* Exclude from ctime.c */
#endif
#if SQLITE_DEFAULT_MMAP_SIZE>SQLITE_MAX_MMAP_SIZE
# undef SQLITE_DEFAULT_MMAP_SIZE
# define SQLITE_DEFAULT_MMAP_SIZE SQLITE_MAX_MMAP_SIZE
#endif

/*
** Only one of SQLITE_ENABLE_STAT3 or SQLITE_ENABLE_STAT4 can be defined.
** Priority is given to SQLITE_ENABLE_STAT4.  If either are defined, also
** define SQLITE_ENABLE_STAT3_OR_STAT4
*/
#ifdef SQLITE_ENABLE_STAT4
# undef SQLITE_ENABLE_STAT3
# define SQLITE_ENABLE_STAT3_OR_STAT4 1
#elif SQLITE_ENABLE_STAT3
# define SQLITE_ENABLE_STAT3_OR_STAT4 1
#elif SQLITE_ENABLE_STAT3_OR_STAT4
# undef SQLITE_ENABLE_STAT3_OR_STAT4
#endif

/*
** SELECTTRACE_ENABLED will be either 1 or 0 depending on whether or not
** the Select query generator tracing logic is turned on.
*/
#if defined(SQLITE_DEBUG) || defined(SQLITE_ENABLE_SELECTTRACE)
# define SELECTTRACE_ENABLED 1
#else
# define SELECTTRACE_ENABLED 0
#endif

/*
** An instance of the following structure is used to store the busy-handler
** callback for a given sqlite handle. 
**
** The sqlite.busyHandler member of the sqlite struct contains the busy
** callback for the database handle. Each pager opened via the sqlite
** handle is passed a pointer to sqlite.busyHandler. The busy-handler
** callback is currently invoked only from within pager.c.
*/
typedef struct BusyHandler BusyHandler;
struct BusyHandler {
  int (*xFunc)(void *,int);  /* The busy callback */
  void *pArg;                /* First arg to busy callback */
  int nBusy;                 /* Incremented with each busy call */
};

/*
** Name of the master database table.  The master database table
** is a special table that holds the names and attributes of all
** user tables and indices.
*/
#define MASTER_NAME       "sqlite_master"
#define TEMP_MASTER_NAME  "sqlite_temp_master"

/*
** The root-page of the master database table.
*/
#define MASTER_ROOT       1

/*
** The name of the schema table.
*/
#define SCHEMA_TABLE(x)  ((!OMIT_TEMPDB)&&(x==1)?TEMP_MASTER_NAME:MASTER_NAME)

/*
** A convenience macro that returns the number of elements in
** an array.
*/
#define ArraySize(X)    ((int)(sizeof(X)/sizeof(X[0])))

/*
** Determine if the argument is a power of two
*/
#define IsPowerOfTwo(X) (((X)&((X)-1))==0)

/*
** The following value as a destructor means to use sqlitexDbFree().
** The sqlitexDbFree() routine requires two parameters instead of the 
** one parameter that destructors normally want.  So we have to introduce 
** this magic value that the code knows to handle differently.  Any 
** pointer will work here as long as it is distinct from SQLITEX_STATIC
** and SQLITEX_TRANSIENT.
*/
#define SQLITE_DYNAMIC   ((sqlitex_destructor_type)sqlitexMallocSize)

/*
** When SQLITE_OMIT_WSD is defined, it means that the target platform does
** not support Writable Static Data (WSD) such as global and static variables.
** All variables must either be on the stack or dynamically allocated from
** the heap.  When WSD is unsupported, the variable declarations scattered
** throughout the SQLite code must become constants instead.  The SQLITE_WSD
** macro is used for this purpose.  And instead of referencing the variable
** directly, we use its constant as a key to lookup the run-time allocated
** buffer that holds real variable.  The constant is also the initializer
** for the run-time allocated buffer.
**
** In the usual case where WSD is supported, the SQLITE_WSD and GLOBAL
** macros become no-ops and have zero performance impact.
*/
#ifdef SQLITE_OMIT_WSD
  #define SQLITE_WSD const
  #define GLOBAL(t,v) (*(t*)sqlitex_wsd_find((void*)&(v), sizeof(v)))
  #define sqlitexGlobalConfig GLOBAL(struct SqlitexConfig, sqlitexConfig)
  int sqlitex_wsd_init(int N, int J);
  void *sqlitex_wsd_find(void *K, int L);
#else
  #define SQLITE_WSD 
  #define GLOBAL(t,v) v
  #define sqlitexGlobalConfig sqlitexConfig
#endif

/*
** The following macros are used to suppress compiler warnings and to
** make it clear to human readers when a function parameter is deliberately 
** left unused within the body of a function. This usually happens when
** a function is called via a function pointer. For example the 
** implementation of an SQL aggregate step callback may not use the
** parameter indicating the number of arguments passed to the aggregate,
** if it knows that this is enforced elsewhere.
**
** When a function parameter is not used at all within the body of a function,
** it is generally named "NotUsed" or "NotUsed2" to make things even clearer.
** However, these macros may also be used to suppress warnings related to
** parameters that may or may not be used depending on compilation options.
** For example those parameters only used in assert() statements. In these
** cases the parameters are named as per the usual conventions.
*/
#define UNUSED_PARAMETER(x) (void)(x)
#define UNUSED_PARAMETER2(x,y) UNUSED_PARAMETER(x),UNUSED_PARAMETER(y)

/*
** Forward references to structures
*/
typedef struct AggInfo AggInfo;
typedef struct AuthContext AuthContext;
typedef struct AutoincInfo AutoincInfo;
typedef struct Bitvec Bitvec;
typedef struct CollSeq CollSeq;
typedef struct Column Column;
typedef struct Db Db;
typedef struct Schema Schema;
typedef struct Expr Expr;
typedef struct ExprList ExprList;
typedef struct ExprSpan ExprSpan;
typedef struct FKey FKey;
typedef struct FuncDestructor FuncDestructor;
typedef struct FuncDef FuncDef;
typedef struct FuncDefHash FuncDefHash;
typedef struct IdList IdList;
typedef struct Index Index;
typedef struct IndexSample IndexSample;
typedef struct KeyClass KeyClass;
typedef struct KeyInfo KeyInfo;
typedef struct Lookaside Lookaside;
typedef struct LookasideSlot LookasideSlot;
typedef struct Module Module;
typedef struct NameContext NameContext;
typedef struct Parse Parse;
typedef struct PrintfArguments PrintfArguments;
typedef struct RowSet RowSet;
typedef struct Savepoint Savepoint;
typedef struct Select Select;
typedef struct SQLiteThread SQLiteThread;
typedef struct SelectDest SelectDest;
typedef struct SrcList SrcList;
typedef struct StrAccum StrAccum;
typedef struct Table Table;
typedef struct TableLock TableLock;
typedef struct Token Token;
typedef struct TreeView TreeView;
typedef struct Trigger Trigger;
typedef struct TriggerPrg TriggerPrg;
typedef struct TriggerStep TriggerStep;
typedef struct UnpackedRecord UnpackedRecord;
typedef struct VTable VTable;
typedef struct VtabCtx VtabCtx;
typedef struct Walker Walker;
typedef struct WhereInfo WhereInfo;
typedef struct With With;
typedef struct Cdb2TrigEvent Cdb2TrigEvent;
typedef struct Cdb2TrigEvents Cdb2TrigEvents;
typedef struct Cdb2TrigTables Cdb2TrigTables;

/*
** Defer sourcing vdbe.h and btree.h until after the "u8" and 
** "BusyHandler" typedefs. vdbe.h also requires a few of the opaque
** pointer types (i.e. FuncDef) defined above.
*/
#include "sqlite_btree.h"
#include "vdbe.h"
#include "pager.h"

#include "os.h"
#include "mutex.h"


/*
** Each database file to be accessed by the system is an instance
** of the following structure.  There are normally two of these structures
** in the sqlite.aDb[] array.  aDb[0] is the main database file and
** aDb[1] is the database file used to hold temporary tables.  Additional
** databases may be attached.
*/
struct Db {
  char *zName;         /* Name of this database */
  Btree *pBt;          /* The B*Tree structure for this database file */
  u8 safety_level;     /* How aggressive at syncing data to disk */
  Schema *pSchema;     /* Pointer to database schema (possibly shared) */
};

/*
** An instance of the following structure stores a database schema.
**
** Most Schema objects are associated with a Btree.  The exception is
** the Schema for the TEMP databaes (sqlitex.aDb[1]) which is free-standing.
** In shared cache mode, a single Schema object can be shared by multiple
** Btrees that refer to the same underlying BtShared object.
** 
** Schema objects are automatically deallocated when the last Btree that
** references them is destroyed.   The TEMP Schema is manually freed by
** sqlitex_close().
*
** A thread must be holding a mutex on the corresponding Btree in order
** to access Schema content.  This implies that the thread must also be
** holding a mutex on the sqlitex connection pointer that owns the Btree.
** For a TEMP Schema, only the connection mutex is required.
*/
struct Schema {
  int schema_cookie;   /* Database schema version number for this file */
  int iGeneration;     /* Generation counter.  Incremented with each change */
  Hash tblHash;        /* All tables indexed by name */
  Hash idxHash;        /* All (named) indices indexed by name */
  Hash trigHash;       /* All triggers indexed by name */
  Hash fkeyHash;       /* All foreign keys by referenced table name */
  Table *pSeqTab;      /* The sqlite_sequence table used by AUTOINCREMENT */
  u8 file_format;      /* Schema format version for this file */
  u8 enc;              /* Text encoding used by this database */
  u16 schemaFlags;     /* Flags associated with this schema */
  int cache_size;      /* Number of pages to use in the cache */
};

/*
** These macros can be used to test, set, or clear bits in the 
** Db.pSchema->flags field.
*/
#define DbHasProperty(D,I,P)     (((D)->aDb[I].pSchema->schemaFlags&(P))==(P))
#define DbHasAnyProperty(D,I,P)  (((D)->aDb[I].pSchema->schemaFlags&(P))!=0)
#define DbSetProperty(D,I,P)     (D)->aDb[I].pSchema->schemaFlags|=(P)
#define DbClearProperty(D,I,P)   (D)->aDb[I].pSchema->schemaFlags&=~(P)

/*
** Allowed values for the DB.pSchema->flags field.
**
** The DB_SchemaLoaded flag is set after the database schema has been
** read into internal hash tables.
**
** DB_UnresetViews means that one or more views have column names that
** have been filled out.  If the schema changes, these column names might
** changes and so the view will need to be reset.
*/
#define DB_SchemaLoaded    0x0001  /* The schema has been loaded */
#define DB_UnresetViews    0x0002  /* Some views have defined column names */
#define DB_Empty           0x0004  /* The file is empty (length 0 bytes) */

/*
** The number of different kinds of things that can be limited
** using the sqlitex_limit() interface.
*/
#define SQLITE_N_LIMIT (SQLITE_LIMIT_WORKER_THREADS+1)

/*
** Lookaside malloc is a set of fixed-size buffers that can be used
** to satisfy small transient memory allocation requests for objects
** associated with a particular database connection.  The use of
** lookaside malloc provides a significant performance enhancement
** (approx 10%) by avoiding numerous malloc/free requests while parsing
** SQL statements.
**
** The Lookaside structure holds configuration information about the
** lookaside malloc subsystem.  Each available memory allocation in
** the lookaside subsystem is stored on a linked list of LookasideSlot
** objects.
**
** Lookaside allocations are only allowed for objects that are associated
** with a particular database connection.  Hence, schema information cannot
** be stored in lookaside because in shared cache mode the schema information
** is shared by multiple database connections.  Therefore, while parsing
** schema information, the Lookaside.bEnabled flag is cleared so that
** lookaside allocations are not used to construct the schema objects.
*/
struct Lookaside {
  u16 sz;                 /* Size of each buffer in bytes */
  u8 bEnabled;            /* False to disable new lookaside allocations */
  u8 bMalloced;           /* True if pStart obtained from sqlitex_malloc() */
  int nOut;               /* Number of buffers currently checked out */
  int mxOut;              /* Highwater mark for nOut */
  int anStat[3];          /* 0: hits.  1: size misses.  2: full misses */
  LookasideSlot *pFree;   /* List of available buffers */
  void *pStart;           /* First byte of available memory space */
  void *pEnd;             /* First byte past end of available space */
};
struct LookasideSlot {
  LookasideSlot *pNext;    /* Next buffer in the list of free buffers */
};

/*
** A hash table for function definitions.
**
** Hash each FuncDef structure into one of the FuncDefHash.a[] slots.
** Collisions are on the FuncDef.pHash chain.
*/
struct FuncDefHash {
  FuncDef *a[23];       /* Hash table for functions */
};

#ifdef SQLITE_USER_AUTHENTICATION
/*
** Information held in the "sqlitex" database connection object and used
** to manage user authentication.
*/
typedef struct sqlitex_userauth sqlitex_userauth;
struct sqlitex_userauth {
  u8 authLevel;                 /* Current authentication level */
  int nAuthPW;                  /* Size of the zAuthPW in bytes */
  char *zAuthPW;                /* Password used to authenticate */
  char *zAuthUser;              /* User name used to authenticate */
};

/* Allowed values for sqlitex_userauth.authLevel */
#define UAUTH_Unknown     0     /* Authentication not yet checked */
#define UAUTH_Fail        1     /* User authentication failed */
#define UAUTH_User        2     /* Authenticated as a normal user */
#define UAUTH_Admin       3     /* Authenticated as an administrator */

/* Functions used only by user authorization logic */
int sqlitexUserAuthTable(const char*);
int sqlitexUserAuthCheckLogin(sqlitex*,const char*,u8*);
void sqlitexUserAuthInit(sqlitex*);
void sqlitexCryptFunc(sqlitex_context*,int,sqlitex_value**);

#endif /* SQLITE_USER_AUTHENTICATION */

/*
** typedef for the authorization callback function.
*/
#ifdef SQLITE_USER_AUTHENTICATION
  typedef int (*sqlitex_xauth)(void*,int,const char*,const char*,const char*,
                               const char*, const char*);
#else
  typedef int (*sqlitex_xauth)(void*,int,const char*,const char*,const char*,
                               const char*);
#endif


/*
** Each database connection is an instance of the following structure.
*/
struct sqlitex {
  sqlitex_vfs *pVfs;            /* OS Interface */
  struct Vdbe *pVdbe;           /* List of active virtual machines */
  CollSeq *pDfltColl;           /* The default collating sequence (BINARY) */
  sqlitex_mutex *mutex;         /* Connection mutex */
  Db *aDb;                      /* All backends */
  int nDb;                      /* Number of backends currently in use */
  int flags;                    /* Miscellaneous flags. See below */
  i64 lastRowid;                /* ROWID of most recent insert (see above) */
  i64 szMmap;                   /* Default mmap_size setting */
  unsigned int openFlags;       /* Flags passed to sqlitex_vfs.xOpen() */
  int errCode;                  /* Most recent error code (SQLITE_*) */
  int errMask;                  /* & result codes with this before returning */
  u16 dbOptFlags;               /* Flags to enable/disable optimizations */
  u8 enc;                       /* Text encoding */
  u8 autoCommit;                /* The auto-commit flag. */
  u8 temp_store;                /* 1: file 2: memory 0: default */
  u8 mallocFailed;              /* True if we have seen a malloc failure */
  u8 dfltLockMode;              /* Default locking-mode for attached dbs */
  signed char nextAutovac;      /* Autovac setting after VACUUM if >=0 */
  u8 suppressErr;               /* Do not issue error messages if true */
  u8 vtabOnConflict;            /* Value to return for s3_vtab_on_conflict() */
  u8 isTransactionSavepoint;    /* True if the outermost savepoint is a TS */
  int nextPagesize;             /* Pagesize after VACUUM if >0 */
  u32 magic;                    /* Magic number for detect library misuse */
  int nChange;                  /* Value returned by sqlitex_changes() */
  int nTotalChange;             /* Value returned by sqlitex_total_changes() */
  int aLimit[SQLITE_N_LIMIT];   /* Limits */
  int nMaxSorterMmap;           /* Maximum size of regions mapped by sorter */
  struct sqlitexInitInfo {      /* Information used during initialization */
    char *zTblName;             /* Optional table name for attachments */
    int newTnum;                /* Rootpage of table being initialized */
    u8 iDb;                     /* Which db file is being initialized */
    u8 busy;                    /* TRUE if currently initializing */
    u8 orphanTrigger;           /* Last statement is orphaned TEMP trigger */
    u8 imposterTable;           /* Building an imposter table */
  } init;
  int nVdbeActive;              /* Number of VDBEs currently running */
  int nVdbeRead;                /* Number of active VDBEs that read or write */
  int nVdbeWrite;               /* Number of active VDBEs that read and write */
  int nVdbeExec;                /* Number of nested calls to VdbeExec() */
  int nExtension;               /* Number of loaded extensions */
  void **aExtension;            /* Array of shared library handles */
  void (*xTrace)(void*,const char*);        /* Trace function */
  void *pTraceArg;                          /* Argument to the trace function */
  void (*xProfile)(void*,const char*,u64);  /* Profiling function */
  void *pProfileArg;                        /* Argument to profile function */
  void *pCommitArg;                 /* Argument to xCommitCallback() */   
  int (*xCommitCallback)(void*);    /* Invoked at every commit. */
  void *pRollbackArg;               /* Argument to xRollbackCallback() */   
  void (*xRollbackCallback)(void*); /* Invoked at every commit. */
  void *pUpdateArg;
  void (*xUpdateCallback)(void*,int, const char*,const char*,sqlite_int64);
#ifndef SQLITE_OMIT_WAL
  int (*xWalCallback)(void *, sqlitex *, const char *, int);
  void *pWalArg;
#endif
  void(*xCollNeeded)(void*,sqlitex*,int eTextRep,const char*);
  void(*xCollNeeded16)(void*,sqlitex*,int eTextRep,const void*);
  void *pCollNeededArg;
  sqlitex_value *pErr;          /* Most recent error message */
  union {
    volatile int isInterrupted; /* True if sqlitex_interrupt has been called */
    double notUsed1;            /* Spacer */
  } u1;
  Lookaside lookaside;          /* Lookaside malloc configuration */
#ifndef SQLITE_OMIT_AUTHORIZATION
  sqlitex_xauth xAuth;          /* Access authorization function */
  void *pAuthArg;               /* 1st argument to the access auth function */
#endif
#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
  int (*xProgress)(void *);     /* The progress callback */
  void *pProgressArg;           /* Argument to the progress callback */
  unsigned nProgressOps;        /* Number of opcodes for progress callback */
#endif
#ifndef SQLITE_OMIT_VIRTUALTABLE
  int nVTrans;                  /* Allocated size of aVTrans */
  Hash aModule;                 /* populated by sqlitex_create_module() */
  VtabCtx *pVtabCtx;            /* Context for active vtab connect/create */
  VTable **aVTrans;             /* Virtual tables with open transactions */
  VTable *pDisconnect;    /* Disconnect these in next sqlitex_prepare() */
#endif
  FuncDefHash aFunc;            /* Hash table of connection functions */
  Hash aCollSeq;                /* All collating sequences */
  BusyHandler busyHandler;      /* Busy callback */
  Db aDbStatic[2];              /* Static space for the 2 default backends */
  Savepoint *pSavepoint;        /* List of active savepoints */
  int busyTimeout;              /* Busy handler timeout, in msec */
  int nSavepoint;               /* Number of non-transaction savepoints */
  int nStatement;               /* Number of nested statement-transactions  */
  i64 nDeferredCons;            /* Net deferred constraints this transaction. */
  i64 nDeferredImmCons;         /* Net deferred immediate constraints */
  int *pnBytesFreed;            /* If not NULL, increment this in DbFree() */
#ifdef SQLITE_ENABLE_UNLOCK_NOTIFY
  /* The following variables are all protected by the STATIC_MASTER 
  ** mutex, not by sqlitex.mutex. They are used by code in notify.c. 
  **
  ** When X.pUnlockConnection==Y, that means that X is waiting for Y to
  ** unlock so that it can proceed.
  **
  ** When X.pBlockingConnection==Y, that means that something that X tried
  ** tried to do recently failed with an SQLITE_LOCKED error due to locks
  ** held by Y.
  */
  sqlitex *pBlockingConnection; /* Connection that caused SQLITE_LOCKED */
  sqlitex *pUnlockConnection;           /* Connection to watch for unlock */
  void *pUnlockArg;                     /* Argument to xUnlockNotify */
  void (*xUnlockNotify)(void **, int);  /* Unlock notify callback */
  sqlitex *pNextBlocked;        /* Next in list of all blocked connections */
#endif
#ifdef SQLITE_USER_AUTHENTICATION
  sqlitex_userauth auth;        /* User authentication information */
#endif
  /* COMDB2 MODIFICATION */
  u8 should_fingerprint;
};

/*
** A macro to discover the encoding of a database.
*/
#define SCHEMA_ENC(db) ((db)->aDb[0].pSchema->enc)
#define ENC(db)        ((db)->enc)

/*
** Possible values for the sqlitex.flags.
*/
#define SQLITE_VdbeTrace      0x00000001  /* True to trace VDBE execution */
#define SQLITE_InternChanges  0x00000002  /* Uncommitted Hash table changes */
#define SQLITE_FullFSync      0x00000004  /* Use full fsync on the backend */
#define SQLITE_CkptFullFSync  0x00000008  /* Use full fsync for checkpoint */
#define SQLITE_CacheSpill     0x00000010  /* OK to spill pager cache */
#define SQLITE_FullColNames   0x00000020  /* Show full column names on SELECT */
#define SQLITE_ShortColNames  0x00000040  /* Show short columns names */
#define SQLITE_CountRows      0x00000080  /* Count rows changed by INSERT, */
                                          /*   DELETE, or UPDATE and return */
                                          /*   the count using a callback. */
#define SQLITE_NullCallback   0x00000100  /* Invoke the callback once if the */
                                          /*   result set is empty */
#define SQLITE_SqlTrace       0x00000200  /* Debug print SQL as it executes */
#define SQLITE_VdbeListing    0x00000400  /* Debug listings of VDBE programs */
#define SQLITE_WriteSchema    0x00000800  /* OK to update SQLITE_MASTER */
#define SQLITE_VdbeAddopTrace 0x00001000  /* Trace sqlitexVdbeAddOp() calls */
#define SQLITE_IgnoreChecks   0x00002000  /* Do not enforce check constraints */
#define SQLITE_ReadUncommitted 0x0004000  /* For shared-cache mode */
#define SQLITE_LegacyFileFmt  0x00008000  /* Create new databases in format 1 */
#define SQLITE_RecoveryMode   0x00010000  /* Ignore schema errors */
#define SQLITE_ReverseOrder   0x00020000  /* Reverse unordered SELECTs */
#define SQLITE_RecTriggers    0x00040000  /* Enable recursive triggers */
#define SQLITE_ForeignKeys    0x00080000  /* Enforce foreign key constraints  */
#define SQLITE_AutoIndex      0x00100000  /* Enable automatic indexes */
#define SQLITE_PreferBuiltin  0x00200000  /* Preference to built-in funcs */
#define SQLITE_LoadExtension  0x00400000  /* Enable load_extension */
#define SQLITE_EnableTrigger  0x00800000  /* True to enable triggers */
#define SQLITE_DeferFKs       0x01000000  /* Defer all FK constraints */
#define SQLITE_QueryOnly      0x02000000  /* Disable database changes */
#define SQLITE_VdbeEQP        0x04000000  /* Debug EXPLAIN QUERY PLAN */


/*
** Bits of the sqlitex.dbOptFlags field that are used by the
** sqlitex_test_control(SQLITE_TESTCTRL_OPTIMIZATIONS,...) interface to
** selectively disable various optimizations.
*/
#define SQLITE_QueryFlattener 0x0001   /* Query flattening */
#define SQLITE_ColumnCache    0x0002   /* Column cache */
#define SQLITE_GroupByOrder   0x0004   /* GROUPBY cover of ORDERBY */
#define SQLITE_FactorOutConst 0x0008   /* Constant factoring */
/*                not used    0x0010   // Was: SQLITE_IdxRealAsInt */
#define SQLITE_DistinctOpt    0x0020   /* DISTINCT using indexes */
#define SQLITE_CoverIdxScan   0x0040   /* Covering index scans */
#define SQLITE_OrderByIdxJoin 0x0080   /* ORDER BY of joins via index */
#define SQLITE_SubqCoroutine  0x0100   /* Evaluate subqueries as coroutines */
#define SQLITE_Transitive     0x0200   /* Transitive constraints */
#define SQLITE_OmitNoopJoin   0x0400   /* Omit unused tables in joins */
#define SQLITE_Stat34         0x0800   /* Use STAT3 or STAT4 data */
#define SQLITE_CursorHints    0x1000   /* Add OP_CursorHint opcodes */
#define SQLITE_CountOfView    0x2000   /* The count-of-view optimization */
#define SQLITE_AllOpts        0xffff   /* All optimizations */

/*
** Macros for testing whether or not optimizations are enabled or disabled.
*/
#ifndef SQLITE_OMIT_BUILTIN_TEST
#define OptimizationDisabled(db, mask)  (((db)->dbOptFlags&(mask))!=0)
#define OptimizationEnabled(db, mask)   (((db)->dbOptFlags&(mask))==0)
#else
#define OptimizationDisabled(db, mask)  0
#define OptimizationEnabled(db, mask)   1
#endif

/*
** Return true if it OK to factor constant expressions into the initialization
** code. The argument is a Parse object for the code generator.
*/
#define ConstFactorOk(P) ((P)->okConstFactor)

/*
** Possible values for the sqlite.magic field.
** The numbers are obtained at random and have no special meaning, other
** than being distinct from one another.
*/
#define SQLITE_MAGIC_OPEN     0xa029a697  /* Database is open */
#define SQLITE_MAGIC_CLOSED   0x9f3c2d33  /* Database is closed */
#define SQLITE_MAGIC_SICK     0x4b771290  /* Error and awaiting close */
#define SQLITE_MAGIC_BUSY     0xf03b7906  /* Database currently in use */
#define SQLITE_MAGIC_ERROR    0xb5357930  /* An SQLITE_MISUSE error occurred */
#define SQLITE_MAGIC_ZOMBIE   0x64cffc7f  /* Close with last statement close */

/*
** Each SQL function is defined by an instance of the following
** structure.  A pointer to this structure is stored in the sqlite.aFunc
** hash table.  When multiple functions have the same name, the hash table
** points to a linked list of these structures.
*/
struct FuncDef {
  i16 nArg;            /* Number of arguments.  -1 means unlimited */
  u16 funcFlags;       /* Some combination of SQLITE_FUNC_* */
  void *pUserData;     /* User data parameter */
  FuncDef *pNext;      /* Next function with same name */
  void (*xFunc)(sqlitex_context*,int,sqlitex_value**); /* Regular function */
  void (*xStep)(sqlitex_context*,int,sqlitex_value**); /* Aggregate step */
  void (*xFinalize)(sqlitex_context*);                /* Aggregate finalizer */
  char *zName;         /* SQL name of the function. */
  FuncDef *pHash;      /* Next with a different name but the same hash */
  FuncDestructor *pDestructor;   /* Reference counted destructor function */
};

/*
** This structure encapsulates a user-function destructor callback (as
** configured using create_function_v2()) and a reference counter. When
** create_function_v2() is called to create a function with a destructor,
** a single object of this type is allocated. FuncDestructor.nRef is set to 
** the number of FuncDef objects created (either 1 or 3, depending on whether
** or not the specified encoding is SQLITE_ANY). The FuncDef.pDestructor
** member of each of the new FuncDef objects is set to point to the allocated
** FuncDestructor.
**
** Thereafter, when one of the FuncDef objects is deleted, the reference
** count on this object is decremented. When it reaches 0, the destructor
** is invoked and the FuncDestructor structure freed.
*/
struct FuncDestructor {
  int nRef;
  void (*xDestroy)(void *);
  void *pUserData;
};

/*
** Possible values for FuncDef.flags.  Note that the _LENGTH and _TYPEOF
** values must correspond to OPFLAG_LENGTHARG and OPFLAG_TYPEOFARG.  There
** are assert() statements in the code to verify this.
*/
#define SQLITE_FUNC_ENCMASK  0x003 /* SQLITE_UTF8, SQLITE_UTF16BE or UTF16LE */
#define SQLITE_FUNC_LIKE     0x004 /* Candidate for the LIKE optimization */
#define SQLITE_FUNC_CASE     0x008 /* Case-sensitive LIKE-type function */
#define SQLITE_FUNC_EPHEM    0x010 /* Ephemeral.  Delete with VDBE */
#define SQLITE_FUNC_NEEDCOLL 0x020 /* sqlitexGetFuncCollSeq() might be called */
#define SQLITE_FUNC_LENGTH   0x040 /* Built-in length() function */
#define SQLITE_FUNC_TYPEOF   0x080 /* Built-in typeof() function */
#define SQLITE_FUNC_COUNT    0x100 /* Built-in count(*) aggregate */
#define SQLITE_FUNC_COALESCE 0x200 /* Built-in coalesce() or ifnull() */
#define SQLITE_FUNC_UNLIKELY 0x400 /* Built-in unlikely() function */
#define SQLITE_FUNC_CONSTANT 0x800 /* Constant inputs give a constant output */
#define SQLITE_FUNC_MINMAX  0x1000 /* True for min() and max() aggregates */

/*
** The following three macros, FUNCTION(), LIKEFUNC() and AGGREGATE() are
** used to create the initializers for the FuncDef structures.
**
**   FUNCTION(zName, nArg, iArg, bNC, xFunc)
**     Used to create a scalar function definition of a function zName 
**     implemented by C function xFunc that accepts nArg arguments. The
**     value passed as iArg is cast to a (void*) and made available
**     as the user-data (sqlitex_user_data()) for the function. If 
**     argument bNC is true, then the SQLITE_FUNC_NEEDCOLL flag is set.
**
**   VFUNCTION(zName, nArg, iArg, bNC, xFunc)
**     Like FUNCTION except it omits the SQLITE_FUNC_CONSTANT flag.
**
**   AGGREGATE(zName, nArg, iArg, bNC, xStep, xFinal)
**     Used to create an aggregate function definition implemented by
**     the C functions xStep and xFinal. The first four parameters
**     are interpreted in the same way as the first 4 parameters to
**     FUNCTION().
**
**   LIKEFUNC(zName, nArg, pArg, flags)
**     Used to create a scalar function definition of a function zName 
**     that accepts nArg arguments and is implemented by a call to C 
**     function likeFunc. Argument pArg is cast to a (void *) and made
**     available as the function user-data (sqlitex_user_data()). The
**     FuncDef.flags variable is set to the value passed as the flags
**     parameter.
*/
#define FUNCTION(zName, nArg, iArg, bNC, xFunc) \
  {nArg, SQLITE_FUNC_CONSTANT|SQLITE_UTF8|(bNC*SQLITE_FUNC_NEEDCOLL), \
   SQLITE_INT_TO_PTR(iArg), 0, xFunc, 0, 0, #zName, 0, 0}
#define VFUNCTION(zName, nArg, iArg, bNC, xFunc) \
  {nArg, SQLITE_UTF8|(bNC*SQLITE_FUNC_NEEDCOLL), \
   SQLITE_INT_TO_PTR(iArg), 0, xFunc, 0, 0, #zName, 0, 0}
#define FUNCTION2(zName, nArg, iArg, bNC, xFunc, extraFlags) \
  {nArg,SQLITE_FUNC_CONSTANT|SQLITE_UTF8|(bNC*SQLITE_FUNC_NEEDCOLL)|extraFlags,\
   SQLITE_INT_TO_PTR(iArg), 0, xFunc, 0, 0, #zName, 0, 0}
#define STR_FUNCTION(zName, nArg, pArg, bNC, xFunc) \
  {nArg, SQLITE_FUNC_CONSTANT|SQLITE_UTF8|(bNC*SQLITE_FUNC_NEEDCOLL), \
   pArg, 0, xFunc, 0, 0, #zName, 0, 0}
#define LIKEFUNC(zName, nArg, arg, flags) \
  {nArg, SQLITE_FUNC_CONSTANT|SQLITE_UTF8|flags, \
   (void *)arg, 0, likeFunc, 0, 0, #zName, 0, 0}
#define AGGREGATE(zName, nArg, arg, nc, xStep, xFinal) \
  {nArg, SQLITE_UTF8|(nc*SQLITE_FUNC_NEEDCOLL), \
   SQLITE_INT_TO_PTR(arg), 0, 0, xStep,xFinal,#zName,0,0}
#define AGGREGATE2(zName, nArg, arg, nc, xStep, xFinal, extraFlags) \
  {nArg, SQLITE_UTF8|(nc*SQLITE_FUNC_NEEDCOLL)|extraFlags, \
   SQLITE_INT_TO_PTR(arg), 0, 0, xStep,xFinal,#zName,0,0}

/*
** All current savepoints are stored in a linked list starting at
** sqlitex.pSavepoint. The first element in the list is the most recently
** opened savepoint. Savepoints are added to the list by the vdbe
** OP_Savepoint instruction.
*/
struct Savepoint {
  char *zName;                        /* Savepoint name (nul-terminated) */
  i64 nDeferredCons;                  /* Number of deferred fk violations */
  i64 nDeferredImmCons;               /* Number of deferred imm fk. */
  Savepoint *pNext;                   /* Parent savepoint (if any) */
};

/*
** The following are used as the second parameter to sqlitexSavepoint(),
** and as the P1 argument to the OP_Savepoint instruction.
*/
#define SAVEPOINT_BEGIN      0
#define SAVEPOINT_RELEASE    1
#define SAVEPOINT_ROLLBACK   2


/*
** Each SQLite module (virtual table definition) is defined by an
** instance of the following structure, stored in the sqlitex.aModule
** hash table.
*/
struct Module {
  const sqlitex_module *pModule;       /* Callback pointers */
  const char *zName;                   /* Name passed to create_module() */
  void *pAux;                          /* pAux passed to create_module() */
  void (*xDestroy)(void *);            /* Module destructor function */
  Table *pEpoTab;                      /* Eponymous table for this module */
};

/*
** information about each column of an SQL table is held in an instance
** of this structure.
*/
struct Column {
  char *zName;     /* Name of this column */
  Expr *pDflt;     /* Default value of this column */
  char *zDflt;     /* Original text of the default value */
  char *zType;     /* Data type for this column */
  char *zColl;     /* Collating sequence.  If NULL, use the default */
  u8 notNull;      /* An OE_ code for handling a NOT NULL constraint */
  char affinity;   /* One of the SQLITE_AFF_... values */
  u8 szEst;        /* Estimated size of this column.  INT==1 */
  u8 colFlags;     /* Boolean properties.  See COLFLAG_ defines below */
};

/* Allowed values for Column.colFlags:
*/
#define COLFLAG_PRIMKEY  0x0001    /* Column is part of the primary key */
#define COLFLAG_HIDDEN   0x0002    /* A hidden column in a virtual table */

/*
** A "Collating Sequence" is defined by an instance of the following
** structure. Conceptually, a collating sequence consists of a name and
** a comparison routine that defines the order of that sequence.
**
** If CollSeq.xCmp is NULL, it means that the
** collating sequence is undefined.  Indices built on an undefined
** collating sequence may not be read or written.
*/
struct CollSeq {
  char *zName;          /* Name of the collating sequence, UTF-8 encoded */
  u8 enc;               /* Text encoding handled by xCmp() */
  void *pUser;          /* First argument to xCmp() */
  int (*xCmp)(void*,int, const void*, int, const void*);
  void (*xDel)(void*);  /* Destructor for pUser */
};

/*
** A sort order can be either ASC or DESC.
*/
#define SQLITE_SO_ASC       0  /* Sort in ascending order */
#define SQLITE_SO_DESC      1  /* Sort in ascending order */

/*
** Column affinity types.
**
** These used to have mnemonic name like 'i' for SQLITE_AFF_INTEGER and
** 't' for SQLITE_AFF_TEXT.  But we can save a little space and improve
** the speed a little by numbering the values consecutively.  
**
** But rather than start with 0 or 1, we begin with 'A'.  That way,
** when multiple affinity types are concatenated into a string and
** used as the P4 operand, they will be more readable.
**
** Note also that the numeric types are grouped together so that testing
** for a numeric type is a single comparison.  And the NONE type is first.
*/
/* COMDB2 MODIFICATION
 * Changed to upper case so we don't break SQLITE_JUMPIFNULL, etc. 
 * (!) If you change the types for NONE and TEXT, please update the hardcoded
 * affinity type strings in analyzeOneTable().
 */
#define SQLITE_AFF_NONE     'A'
#define SQLITE_AFF_TEXT     'B'
/* COMDB2 MODIFICATION */
#define SQLITE_AFF_DATETIME 'C'
#define SQLITE_AFF_INTV_YE  'D'
#define SQLITE_AFF_INTV_MO  'E'
#define SQLITE_AFF_INTV_DY  'F'
#define SQLITE_AFF_INTV_HO  'G'
#define SQLITE_AFF_INTV_MI  'H'
#define SQLITE_AFF_INTV_SE  'I'
/* COMDB2 MODIFICATION */
#define SQLITE_AFF_NUMERIC  'J'  /* originally 'C'... */
#define SQLITE_AFF_INTEGER  'K'  /* originally 'D'... */
#define SQLITE_AFF_REAL     'L'  /* originally 'E'... */
/* COMDB2 MODIFICATION */
#define SQLITE_AFF_DECIMAL  'M'  
#define SQLITE_AFF_SMALL    'N'  /* for float */

/*
** The SQLITE_AFF_MASK values masks off the significant bits of an
** affinity value. 
*/
/* COMDB2 MODIFICATION */
#define SQLITE_AFF_MASK     0x4F

/* COMDB2 MODIFICATION 
#define sqlitexIsNumericAffinity(X)  ((X)>=SQLITE_AFF_NUMERIC) */
#define sqlitexIsNumericAffinity(X)  (((X)&SQLITE_AFF_MASK)>=SQLITE_AFF_NUMERIC)

/*
** Additional bit values that can be ORed with an affinity without
** changing the affinity.
**
** The SQLITE_NOTNULL flag is a combination of NULLEQ and JUMPIFNULL.
** It causes an assert() to fire if either operand to a comparison
** operator is NULL.  It is added to certain comparison operators to
** prove that the operands are always NOT NULL.
*/
#define SQLITE_JUMPIFNULL   0x10  /* jumps if either operand is NULL */
#define SQLITE_STOREP2      0x20  /* Store result in reg[P2] rather than jump */
#define SQLITE_NULLEQ       0x80  /* NULL=NULL */
#define SQLITE_NOTNULL      0x90  /* Assert that operands are never NULL */

/*
** An object of this type is created for each virtual table present in
** the database schema. 
**
** If the database schema is shared, then there is one instance of this
** structure for each database connection (sqlitex*) that uses the shared
** schema. This is because each database connection requires its own unique
** instance of the sqlitex_vtab* handle used to access the virtual table 
** implementation. sqlitex_vtab* handles can not be shared between 
** database connections, even when the rest of the in-memory database 
** schema is shared, as the implementation often stores the database
** connection handle passed to it via the xConnect() or xCreate() method
** during initialization internally. This database connection handle may
** then be used by the virtual table implementation to access real tables 
** within the database. So that they appear as part of the callers 
** transaction, these accesses need to be made via the same database 
** connection as that used to execute SQL operations on the virtual table.
**
** All VTable objects that correspond to a single table in a shared
** database schema are initially stored in a linked-list pointed to by
** the Table.pVTable member variable of the corresponding Table object.
** When an sqlitex_prepare() operation is required to access the virtual
** table, it searches the list for the VTable that corresponds to the
** database connection doing the preparing so as to use the correct
** sqlitex_vtab* handle in the compiled query.
**
** When an in-memory Table object is deleted (for example when the
** schema is being reloaded for some reason), the VTable objects are not 
** deleted and the sqlitex_vtab* handles are not xDisconnect()ed 
** immediately. Instead, they are moved from the Table.pVTable list to
** another linked list headed by the sqlitex.pDisconnect member of the
** corresponding sqlitex structure. They are then deleted/xDisconnected 
** next time a statement is prepared using said sqlitex*. This is done
** to avoid deadlock issues involving multiple sqlitex.mutex mutexes.
** Refer to comments above function sqlitexVtabUnlockList() for an
** explanation as to why it is safe to add an entry to an sqlitex.pDisconnect
** list without holding the corresponding sqlitex.mutex mutex.
**
** The memory for objects of this type is always allocated by 
** sqlitexDbMalloc(), using the connection handle stored in VTable.db as 
** the first argument.
*/
struct VTable {
  sqlitex *db;              /* Database connection associated with this table */
  Module *pMod;             /* Pointer to module implementation */
  sqlitex_vtab *pVtab;      /* Pointer to vtab instance */
  int nRef;                 /* Number of pointers to this structure */
  u8 bConstraint;           /* True if constraints are supported */
  int iSavepoint;           /* Depth of the SAVEPOINT stack */
  VTable *pNext;            /* Next in linked list (see above) */
};

/*
** Each SQL table is represented in memory by an instance of the
** following structure.
**
** Table.zName is the name of the table.  The case of the original
** CREATE TABLE statement is stored, but case is not significant for
** comparisons.
**
** Table.nCol is the number of columns in this table.  Table.aCol is a
** pointer to an array of Column structures, one for each column.
**
** If the table has an INTEGER PRIMARY KEY, then Table.iPKey is the index of
** the column that is that key.   Otherwise Table.iPKey is negative.  Note
** that the datatype of the PRIMARY KEY must be INTEGER for this field to
** be set.  An INTEGER PRIMARY KEY is used as the rowid for each row of
** the table.  If a table has no INTEGER PRIMARY KEY, then a random rowid
** is generated for each row of the table.  TF_HasPrimaryKey is set if
** the table has any PRIMARY KEY, INTEGER or otherwise.
**
** Table.tnum is the page number for the root BTree page of the table in the
** database file.  If Table.iDb is the index of the database table backend
** in sqlite.aDb[].  0 is for the main database and 1 is for the file that
** holds temporary tables and indices.  If TF_Ephemeral is set
** then the table is stored in a file that is automatically deleted
** when the VDBE cursor to the table is closed.  In this case Table.tnum 
** refers VDBE cursor number that holds the table open, not to the root
** page number.  Transient tables are used to hold the results of a
** sub-query that appears instead of a real table name in the FROM clause 
** of a SELECT statement.
*/
struct Table {
  char *zName;         /* Name of the table or view */
  Column *aCol;        /* Information about each column */
  Index *pIndex;       /* List of SQL indexes on this table. */
  Select *pSelect;     /* NULL for tables.  Points to definition if a view. */
  FKey *pFKey;         /* Linked list of all foreign keys in this table */
  char *zColAff;       /* String defining the affinity of each column */
#ifndef SQLITE_OMIT_CHECK
  ExprList *pCheck;    /* All CHECK constraints */
#endif
  LogEst nRowLogEst;   /* Estimated rows in table - from sqlite_stat1 table */
  int tnum;            /* Root BTree node for this table (see note above) */
  i16 iPKey;           /* If not negative, use aCol[iPKey] as the primary key */
  i16 nCol;            /* Number of columns in this table */
  u16 nRef;            /* Number of pointers to this Table */
  LogEst szTabRow;     /* Estimated size of each table row in bytes */
#ifdef SQLITE_ENABLE_COSTMULT
  LogEst costMult;     /* Cost multiplier for using this table */
#endif
  u8 tabFlags;         /* Mask of TF_* values */
  u8 keyConf;          /* What to do in case of uniqueness conflict on iPKey */
#ifndef SQLITE_OMIT_ALTERTABLE
  int addColOffset;    /* Offset in CREATE TABLE stmt to add a new column */
#endif
#ifndef SQLITE_OMIT_VIRTUALTABLE
  int nModuleArg;      /* Number of arguments to the module */
  char **azModuleArg;  /* 0: module 1: schema 2: vtab name 3...: args */
  VTable *pVTable;     /* List of VTable objects. */
#endif
  Trigger *pTrigger;   /* List of triggers stored in pSchema */
  Schema *pSchema;     /* Schema that contains this table */
  Table *pNextZombie;  /* Next on the Parse.pZombieTab list */
  int    version;      /* used for cached table schema, from remote */
  int    iDb;          /* iDb version */
};

/*
** Allowed values for Table.tabFlags.
*/
#define TF_Readonly        0x01    /* Read-only system table */
#define TF_Ephemeral       0x02    /* An ephemeral table */
#define TF_HasPrimaryKey   0x04    /* Table has a primary key */
#define TF_Autoincrement   0x08    /* Integer primary key is autoincrement */
#define TF_Virtual         0x10    /* Is a virtual table */
#define TF_WithoutRowid    0x20    /* No rowid used. PRIMARY KEY is the key */


/*
** Test to see whether or not a table is a virtual table.  This is
** done as a macro so that it will be optimized out when virtual
** table support is omitted from the build.
*/
#ifndef SQLITE_OMIT_VIRTUALTABLE
#  define IsVirtual(X)      (((X)->tabFlags & TF_Virtual)!=0)
#else
#  define IsVirtual(X)      0
#endif

/*
** Macros to determine if a column is hidden.  IsOrdinaryHiddenColumn()
** only works for non-virtual tables (ordinary tables and views) and is
** always false unless SQLITE_ENABLE_HIDDEN_COLUMNS is defined.  The
** IsHiddenColumn() macro is general purpose.
*/
#if defined(SQLITE_ENABLE_HIDDEN_COLUMNS)
#  define IsHiddenColumn(X)         (((X)->colFlags & COLFLAG_HIDDEN)!=0)
#  define IsOrdinaryHiddenColumn(X) (((X)->colFlags & COLFLAG_HIDDEN)!=0)
#elif !defined(SQLITE_OMIT_VIRTUALTABLE)
#  define IsHiddenColumn(X)         (((X)->colFlags & COLFLAG_HIDDEN)!=0)
#  define IsOrdinaryHiddenColumn(X) 0
#else
#  define IsHiddenColumn(X)         0
#  define IsOrdinaryHiddenColumn(X) 0
#endif


/* Does the table have a rowid */
#define HasRowid(X)     (((X)->tabFlags & TF_WithoutRowid)==0)

/*
** Each foreign key constraint is an instance of the following structure.
**
** A foreign key is associated with two tables.  The "from" table is
** the table that contains the REFERENCES clause that creates the foreign
** key.  The "to" table is the table that is named in the REFERENCES clause.
** Consider this example:
**
**     CREATE TABLE ex1(
**       a INTEGER PRIMARY KEY,
**       b INTEGER CONSTRAINT fk1 REFERENCES ex2(x)
**     );
**
** For foreign key "fk1", the from-table is "ex1" and the to-table is "ex2".
** Equivalent names:
**
**     from-table == child-table
**       to-table == parent-table
**
** Each REFERENCES clause generates an instance of the following structure
** which is attached to the from-table.  The to-table need not exist when
** the from-table is created.  The existence of the to-table is not checked.
**
** The list of all parents for child Table X is held at X.pFKey.
**
** A list of all children for a table named Z (which might not even exist)
** is held in Schema.fkeyHash with a hash key of Z.
*/
struct FKey {
  Table *pFrom;     /* Table containing the REFERENCES clause (aka: Child) */
  FKey *pNextFrom;  /* Next FKey with the same in pFrom. Next parent of pFrom */
  char *zTo;        /* Name of table that the key points to (aka: Parent) */
  FKey *pNextTo;    /* Next with the same zTo. Next child of zTo. */
  FKey *pPrevTo;    /* Previous with the same zTo */
  int nCol;         /* Number of columns in this key */
  /* EV: R-30323-21917 */
  u8 isDeferred;       /* True if constraint checking is deferred till COMMIT */
  u8 aAction[2];        /* ON DELETE and ON UPDATE actions, respectively */
  Trigger *apTrigger[2];/* Triggers for aAction[] actions */
  struct sColMap {      /* Mapping of columns in pFrom to columns in zTo */
    int iFrom;            /* Index of column in pFrom */
    char *zCol;           /* Name of column in zTo.  If NULL use PRIMARY KEY */
  } aCol[1];            /* One entry for each of nCol columns */
};

/*
** SQLite supports many different ways to resolve a constraint
** error.  ROLLBACK processing means that a constraint violation
** causes the operation in process to fail and for the current transaction
** to be rolled back.  ABORT processing means the operation in process
** fails and any prior changes from that one operation are backed out,
** but the transaction is not rolled back.  FAIL processing means that
** the operation in progress stops and returns an error code.  But prior
** changes due to the same operation are not backed out and no rollback
** occurs.  IGNORE means that the particular row that caused the constraint
** error is not inserted or updated.  Processing continues and no error
** is returned.  REPLACE means that preexisting database rows that caused
** a UNIQUE constraint violation are removed so that the new insert or
** update can proceed.  Processing continues and no error is reported.
**
** RESTRICT, SETNULL, and CASCADE actions apply only to foreign keys.
** RESTRICT is the same as ABORT for IMMEDIATE foreign keys and the
** same as ROLLBACK for DEFERRED keys.  SETNULL means that the foreign
** key is set to NULL.  CASCADE means that a DELETE or UPDATE of the
** referenced table row is propagated into the row that holds the
** foreign key.
** 
** The following symbolic values are used to record which type
** of action to take.
*/
#define OE_None     0   /* There is no constraint to check */
#define OE_Rollback 1   /* Fail the operation and rollback the transaction */
#define OE_Abort    2   /* Back out changes but do no rollback transaction */
#define OE_Fail     3   /* Stop the operation but leave all prior changes */
#define OE_Ignore   4   /* Ignore the error. Do not do the INSERT or UPDATE */
#define OE_Replace  5   /* Delete existing record, then do INSERT or UPDATE */

#define OE_Restrict 6   /* OE_Abort for IMMEDIATE, OE_Rollback for DEFERRED */
#define OE_SetNull  7   /* Set the foreign key value to NULL */
#define OE_SetDflt  8   /* Set the foreign key value to its default */
#define OE_Cascade  9   /* Cascade the changes */

#define OE_Default  10  /* Do whatever the default action is */


/*
** An instance of the following structure is passed as the first
** argument to sqlitexVdbeKeyCompare and is used to control the 
** comparison of the two index keys.
**
** Note that aSortOrder[] and aColl[] have nField+1 slots.  There
** are nField slots for the columns of an index then one extra slot
** for the rowid at the end.
*/
struct KeyInfo {
  u32 nRef;           /* Number of references to this KeyInfo object */
  u8 enc;             /* Text encoding - one of the SQLITE_UTF* values */
  u16 nField;         /* Number of key columns in the index */
  u16 nXField;        /* Number of columns beyond the key columns */
  sqlitex *db;        /* The database connection */
  u8 *aSortOrder;     /* Sort order for each column. */
  CollSeq *aColl[1];  /* Collating sequence for each term of the key */
};

/*
** An instance of the following structure holds information about a
** single index record that has already been parsed out into individual
** values.
**
** A record is an object that contains one or more fields of data.
** Records are used to store the content of a table row and to store
** the key of an index.  A blob encoding of a record is created by
** the OP_MakeRecord opcode of the VDBE and is disassembled by the
** OP_Column opcode.
**
** This structure holds a record that has already been disassembled
** into its constituent fields.
**
** The r1 and r2 member variables are only used by the optimized comparison
** functions vdbeRecordCompareInt() and vdbeRecordCompareString().
*/
struct UnpackedRecord {
  KeyInfo *pKeyInfo;  /* Collation and sort-order information */
  u16 nField;         /* Number of entries in apMem[] */
  i8 default_rc;      /* Comparison result if keys are equal */
  u8 errCode;         /* Error detected by xRecordCompare (CORRUPT or NOMEM) */
  Mem *aMem;          /* Values */
  int r1;             /* Value to return if (lhs > rhs) */
  int r2;             /* Value to return if (rhs < lhs) */
};


/*
** Each SQL index is represented in memory by an
** instance of the following structure.
**
** The columns of the table that are to be indexed are described
** by the aiColumn[] field of this structure.  For example, suppose
** we have the following table and index:
**
**     CREATE TABLE Ex1(c1 int, c2 int, c3 text);
**     CREATE INDEX Ex2 ON Ex1(c3,c1);
**
** In the Table structure describing Ex1, nCol==3 because there are
** three columns in the table.  In the Index structure describing
** Ex2, nColumn==2 since 2 of the 3 columns of Ex1 are indexed.
** The value of aiColumn is {2, 0}.  aiColumn[0]==2 because the 
** first column to be indexed (c3) has an index of 2 in Ex1.aCol[].
** The second column to be indexed (c1) has an index of 0 in
** Ex1.aCol[], hence Ex2.aiColumn[1]==0.
**
** The Index.onError field determines whether or not the indexed columns
** must be unique and what to do if they are not.  When Index.onError=OE_None,
** it means this is not a unique index.  Otherwise it is a unique index
** and the value of Index.onError indicate the which conflict resolution 
** algorithm to employ whenever an attempt is made to insert a non-unique
** element.
*/
struct Index {
  char *zName;             /* Name of this index */
  i16 *aiColumn;           /* Which columns are used by this index.  1st is 0 */
  LogEst *aiRowLogEst;     /* From ANALYZE: Est. rows selected by each column */
  Table *pTable;           /* The SQL table being indexed */
  char *zColAff;           /* String defining the affinity of each column */
  Index *pNext;            /* The next index associated with the same table */
  Schema *pSchema;         /* Schema containing this index */
  u8 *aSortOrder;          /* for each column: True==DESC, False==ASC */
  char **azColl;           /* Array of collation sequence names for index */
  Expr *pPartIdxWhere;     /* WHERE clause for partial indices */
  KeyInfo *pKeyInfo;       /* A KeyInfo object suitable for this index */
  int tnum;                /* DB Page containing root of this index */
  LogEst szIdxRow;         /* Estimated average row size in bytes */
  u16 nKeyCol;             /* Number of columns forming the key */
  u16 nColumn;             /* Number of columns stored in the index */
  u8 onError;              /* OE_Abort, OE_Ignore, OE_Replace, or OE_None */
  unsigned idxType:2;      /* 1==UNIQUE, 2==PRIMARY KEY, 0==CREATE INDEX */
  unsigned bUnordered:1;   /* Use this index for == or IN queries only */
  unsigned uniqNotNull:1;  /* True if UNIQUE and NOT NULL for all columns */
  unsigned isResized:1;    /* True if resizeIndexObject() has been called */
  unsigned isCovering:1;   /* True if this is a covering index */
  unsigned noSkipScan:1;   /* Do not try to use skip-scan if true */
#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
  int nSample;             /* Number of elements in aSample[] */
  int nAlloc;              /* Number of elements allocated in aSample[] (will be >= nSample) */
  int nSampleCol;          /* Size of IndexSample.anEq[] and so on */
  tRowcnt *aAvgEq;         /* Average nEq values for keys not in aSample */
  IndexSample *aSample;    /* Samples of the left-most key */
  tRowcnt *aiRowEst;       /* Non-logarithmic stat1 data for this index */
  tRowcnt nRowEst0;        /* Non-logarithmic number of rows in the index */
#endif
};

/*
** Allowed values for Index.idxType
*/
#define SQLITE_IDXTYPE_APPDEF      0   /* Created using CREATE INDEX */
#define SQLITE_IDXTYPE_UNIQUE      1   /* Implements a UNIQUE constraint */
#define SQLITE_IDXTYPE_PRIMARYKEY  2   /* Is the PRIMARY KEY for the table */

/* Return true if index X is a PRIMARY KEY index */
#define IsPrimaryKeyIndex(X)  ((X)->idxType==SQLITE_IDXTYPE_PRIMARYKEY)

/* Return true if index X is a UNIQUE index */
#define IsUniqueIndex(X)      ((X)->onError!=OE_None)

/*
** Each sample stored in the sqlite_stat3 table is represented in memory 
** using a structure of this type.  See documentation at the top of the
** analyze.c source file for additional information.
*/
struct IndexSample {
  void *p;          /* Pointer to sampled record */
  int n;            /* Size of record in bytes */
  tRowcnt *anEq;    /* Est. number of rows where the key equals this sample */
  tRowcnt *anLt;    /* Est. number of rows where key is less than this sample */
  tRowcnt *anDLt;   /* Est. number of distinct keys less than this sample */
};

/*
** Each token coming out of the lexer is an instance of
** this structure.  Tokens are also used as part of an expression.
**
** Note if Token.z==0 then Token.dyn and Token.n are undefined and
** may contain random values.  Do not make any assumptions about Token.dyn
** and Token.n when Token.z==0.
*/
struct Token {
  const char *z;     /* Text of the token.  Not NULL-terminated! */
  unsigned int n;    /* Number of characters in this token */
};

/*
** An instance of this structure contains information needed to generate
** code for a SELECT that contains aggregate functions.
**
** If Expr.op==TK_AGG_COLUMN or TK_AGG_FUNCTION then Expr.pAggInfo is a
** pointer to this structure.  The Expr.iColumn field is the index in
** AggInfo.aCol[] or AggInfo.aFunc[] of information needed to generate
** code for that node.
**
** AggInfo.pGroupBy and AggInfo.aFunc.pExpr point to fields within the
** original Select structure that describes the SELECT statement.  These
** fields do not need to be freed when deallocating the AggInfo structure.
*/
struct AggInfo {
  u8 directMode;          /* Direct rendering mode means take data directly
                          ** from source tables rather than from accumulators */
  u8 useSortingIdx;       /* In direct mode, reference the sorting index rather
                          ** than the source table */
  int sortingIdx;         /* Cursor number of the sorting index */
  int sortingIdxPTab;     /* Cursor number of pseudo-table */
  int nSortingColumn;     /* Number of columns in the sorting index */
  int mnReg, mxReg;       /* Range of registers allocated for aCol and aFunc */
  ExprList *pGroupBy;     /* The group by clause */
  struct AggInfo_col {    /* For each column used in source tables */
    Table *pTab;             /* Source table */
    int iTable;              /* Cursor number of the source table */
    int iColumn;             /* Column number within the source table */
    int iSorterColumn;       /* Column number in the sorting index */
    int iMem;                /* Memory location that acts as accumulator */
    Expr *pExpr;             /* The original expression */
  } *aCol;
  int nColumn;            /* Number of used entries in aCol[] */
  int nAccumulator;       /* Number of columns that show through to the output.
                          ** Additional columns are used only as parameters to
                          ** aggregate functions */
  struct AggInfo_func {   /* For each aggregate function */
    Expr *pExpr;             /* Expression encoding the function */
    FuncDef *pFunc;          /* The aggregate function implementation */
    int iMem;                /* Memory location that acts as accumulator */
    int iDistinct;           /* Ephemeral table used to enforce DISTINCT */
  } *aFunc;
  int nFunc;              /* Number of entries in aFunc[] */
};

/*
** The datatype ynVar is a signed integer, either 16-bit or 32-bit.
** Usually it is 16-bits.  But if SQLITE_MAX_VARIABLE_NUMBER is greater
** than 32767 we have to make it 32-bit.  16-bit is preferred because
** it uses less memory in the Expr object, which is a big memory user
** in systems with lots of prepared statements.  And few applications
** need more than about 10 or 20 variables.  But some extreme users want
** to have prepared statements with over 32767 variables, and for them
** the option is available (at compile-time).
*/
#if SQLITE_MAX_VARIABLE_NUMBER<=32767
typedef i16 ynVar;
#else
typedef int ynVar;
#endif

/*
** Each node of an expression in the parse tree is an instance
** of this structure.
**
** Expr.op is the opcode. The integer parser token codes are reused
** as opcodes here. For example, the parser defines TK_GE to be an integer
** code representing the ">=" operator. This same integer code is reused
** to represent the greater-than-or-equal-to operator in the expression
** tree.
**
** If the expression is an SQL literal (TK_INTEGER, TK_FLOAT, TK_BLOB, 
** or TK_STRING), then Expr.token contains the text of the SQL literal. If
** the expression is a variable (TK_VARIABLE), then Expr.token contains the 
** variable name. Finally, if the expression is an SQL function (TK_FUNCTION),
** then Expr.token contains the name of the function.
**
** Expr.pRight and Expr.pLeft are the left and right subexpressions of a
** binary operator. Either or both may be NULL.
**
** Expr.x.pList is a list of arguments if the expression is an SQL function,
** a CASE expression or an IN expression of the form "<lhs> IN (<y>, <z>...)".
** Expr.x.pSelect is used if the expression is a sub-select or an expression of
** the form "<lhs> IN (SELECT ...)". If the EP_xIsSelect bit is set in the
** Expr.flags mask, then Expr.x.pSelect is valid. Otherwise, Expr.x.pList is 
** valid.
**
** An expression of the form ID or ID.ID refers to a column in a table.
** For such expressions, Expr.op is set to TK_COLUMN and Expr.iTable is
** the integer cursor number of a VDBE cursor pointing to that table and
** Expr.iColumn is the column number for the specific column.  If the
** expression is used as a result in an aggregate SELECT, then the
** value is also stored in the Expr.iAgg column in the aggregate so that
** it can be accessed after all aggregates are computed.
**
** If the expression is an unbound variable marker (a question mark 
** character '?' in the original SQL) then the Expr.iTable holds the index 
** number for that variable.
**
** If the expression is a subquery then Expr.iColumn holds an integer
** register number containing the result of the subquery.  If the
** subquery gives a constant result, then iTable is -1.  If the subquery
** gives a different answer at different times during statement processing
** then iTable is the address of a subroutine that computes the subquery.
**
** If the Expr is of type OP_Column, and the table it is selecting from
** is a disk table or the "old.*" pseudo-table, then pTab points to the
** corresponding table definition.
**
** ALLOCATION NOTES:
**
** Expr objects can use a lot of memory space in database schema.  To
** help reduce memory requirements, sometimes an Expr object will be
** truncated.  And to reduce the number of memory allocations, sometimes
** two or more Expr objects will be stored in a single memory allocation,
** together with Expr.zToken strings.
**
** If the EP_Reduced and EP_TokenOnly flags are set when
** an Expr object is truncated.  When EP_Reduced is set, then all
** the child Expr objects in the Expr.pLeft and Expr.pRight subtrees
** are contained within the same memory allocation.  Note, however, that
** the subtrees in Expr.x.pList or Expr.x.pSelect are always separately
** allocated, regardless of whether or not EP_Reduced is set.
*/
struct Expr {
  u8 op;                 /* Operation performed by this node */
  char affinity;         /* The affinity of the column or 0 if not a column */
  u32 flags;             /* Various flags.  EP_* See below */
  union {
    char *zToken;          /* Token value. Zero terminated and dequoted */
    int iValue;            /* Non-negative integer value if EP_IntValue */
  } u;

  /* If the EP_TokenOnly flag is set in the Expr.flags mask, then no
  ** space is allocated for the fields below this point. An attempt to
  ** access them will result in a segfault or malfunction. 
  *********************************************************************/

  Expr *pLeft;           /* Left subnode */
  Expr *pRight;          /* Right subnode */
  union {
    ExprList *pList;     /* op = IN, EXISTS, SELECT, CASE, FUNCTION, BETWEEN */
    Select *pSelect;     /* EP_xIsSelect and op = IN, EXISTS, SELECT */
  } x;

  /* If the EP_Reduced flag is set in the Expr.flags mask, then no
  ** space is allocated for the fields below this point. An attempt to
  ** access them will result in a segfault or malfunction.
  *********************************************************************/

#if SQLITE_MAX_EXPR_DEPTH>0
  int nHeight;           /* Height of the tree headed by this node */
#endif
  int iTable;            /* TK_COLUMN: cursor number of table holding column
                         ** TK_REGISTER: register number
                         ** TK_TRIGGER: 1 -> new, 0 -> old
                         ** EP_Unlikely:  134217728 times likelihood */
  ynVar iColumn;         /* TK_COLUMN: column index.  -1 for rowid.
                         ** TK_VARIABLE: variable number (always >= 1). */
  i16 iAgg;              /* Which entry in pAggInfo->aCol[] or ->aFunc[] */
  i16 iRightJoinTable;   /* If EP_FromJoin, the right table of the join */
  u8 op2;                /* TK_REGISTER: original value of Expr.op
                         ** TK_COLUMN: the value of p5 for OP_Column
                         ** TK_AGG_FUNCTION: nesting depth */
  AggInfo *pAggInfo;     /* Used by TK_AGG_COLUMN and TK_AGG_FUNCTION */
  Table *pTab;           /* Table for TK_COLUMN expressions. */
  /* COMDB2 MODIFICATION */
  u8 visited;            /* 1 if visited by fingerprinter. */
};

/*
** The following are the meanings of bits in the Expr.flags field.
*/
#define EP_FromJoin  0x000001 /* Originates in ON/USING clause of outer join */
#define EP_Agg       0x000002 /* Contains one or more aggregate functions */
#define EP_Resolved  0x000004 /* IDs have been resolved to COLUMNs */
#define EP_Error     0x000008 /* Expression contains one or more errors */
#define EP_Distinct  0x000010 /* Aggregate function with DISTINCT keyword */
#define EP_VarSelect 0x000020 /* pSelect is correlated, not constant */
#define EP_DblQuoted 0x000040 /* token.z was originally in "..." */
#define EP_InfixFunc 0x000080 /* True for an infix function: LIKE, GLOB, etc */
#define EP_Collate   0x000100 /* Tree contains a TK_COLLATE operator */
#define EP_Generic   0x000200 /* Ignore COLLATE or affinity on this tree */
#define EP_IntValue  0x000400 /* Integer value contained in u.iValue */
#define EP_xIsSelect 0x000800 /* x.pSelect is valid (otherwise x.pList is) */
#define EP_Skip      0x001000 /* COLLATE, AS, or UNLIKELY */
#define EP_Reduced   0x002000 /* Expr struct EXPR_REDUCEDSIZE bytes only */
#define EP_TokenOnly 0x004000 /* Expr struct EXPR_TOKENONLYSIZE bytes only */
#define EP_Static    0x008000 /* Held in memory not obtained from malloc() */
#define EP_MemToken  0x010000 /* Need to sqlitexDbFree() Expr.zToken */
#define EP_NoReduce  0x020000 /* Cannot EXPRDUP_REDUCE this Expr */
#define EP_Unlikely  0x040000 /* unlikely() or likelihood() function */
#define EP_ConstFunc 0x080000 /* Node is a SQLITE_FUNC_CONSTANT function */
#define EP_CanBeNull 0x100000 /* Can be null despite NOT NULL constraint */
#define EP_Subquery  0x200000 /* Tree contains a TK_SELECT operator */

/*
** Combinations of two or more EP_* flags
*/
#define EP_Propagate (EP_Collate|EP_Subquery) /* Propagate these bits up tree */

/*
** These macros can be used to test, set, or clear bits in the 
** Expr.flags field.
*/
#define ExprHasProperty(E,P)     (((E)->flags&(P))!=0)
#define ExprHasAllProperty(E,P)  (((E)->flags&(P))==(P))
#define ExprSetProperty(E,P)     (E)->flags|=(P)
#define ExprClearProperty(E,P)   (E)->flags&=~(P)

/* The ExprSetVVAProperty() macro is used for Verification, Validation,
** and Accreditation only.  It works like ExprSetProperty() during VVA
** processes but is a no-op for delivery.
*/
#ifdef SQLITE_DEBUG
# define ExprSetVVAProperty(E,P)  (E)->flags|=(P)
#else
# define ExprSetVVAProperty(E,P)
#endif

/*
** Macros to determine the number of bytes required by a normal Expr 
** struct, an Expr struct with the EP_Reduced flag set in Expr.flags 
** and an Expr struct with the EP_TokenOnly flag set.
*/
#define EXPR_FULLSIZE           sizeof(Expr)           /* Full size */
#define EXPR_REDUCEDSIZE        offsetof(Expr,iTable)  /* Common features */
#define EXPR_TOKENONLYSIZE      offsetof(Expr,pLeft)   /* Fewer features */

/*
** Flags passed to the sqlitexExprDup() function. See the header comment 
** above sqlitexExprDup() for details.
*/
#define EXPRDUP_REDUCE         0x0001  /* Used reduced-size Expr nodes */

/*
** A list of expressions.  Each expression may optionally have a
** name.  An expr/name combination can be used in several ways, such
** as the list of "expr AS ID" fields following a "SELECT" or in the
** list of "ID = expr" items in an UPDATE.  A list of expressions can
** also be used as the argument to a function, in which case the a.zName
** field is not used.
**
** By default the Expr.zSpan field holds a human-readable description of
** the expression that is used in the generation of error messages and
** column labels.  In this case, Expr.zSpan is typically the text of a
** column expression as it exists in a SELECT statement.  However, if
** the bSpanIsTab flag is set, then zSpan is overloaded to mean the name
** of the result column in the form: DATABASE.TABLE.COLUMN.  This later
** form is used for name resolution with nested FROM clauses.
*/
struct ExprList {
  int nExpr;             /* Number of expressions on the list */
  struct ExprList_item { /* For each expression in the list */
    Expr *pExpr;            /* The list of expressions */
    char *zName;            /* Token associated with this expression */
    char *zSpan;            /* Original text of the expression */
    u8 sortOrder;           /* 1 for DESC or 0 for ASC */
    unsigned done :1;       /* A flag to indicate when processing is finished */
    unsigned bSpanIsTab :1; /* zSpan holds DB.TABLE.COLUMN */
    unsigned reusable :1;   /* Constant expression is reusable */
    union {
      struct {
        u16 iOrderByCol;      /* For ORDER BY, column number in result set */
        u16 iAlias;           /* Index into Parse.aAlias[] for zName */
      } x;
      int iConstExprReg;      /* Register in which Expr value is cached */
    } u;
  } *a;                  /* Alloc a power of two greater or equal to nExpr */
};

/*
** An instance of this structure is used by the parser to record both
** the parse tree for an expression and the span of input text for an
** expression.
*/
struct ExprSpan {
  Expr *pExpr;          /* The expression parse tree */
  const char *zStart;   /* First character of input text */
  const char *zEnd;     /* One character past the end of input text */
};

/*
** An instance of this structure can hold a simple list of identifiers,
** such as the list "a,b,c" in the following statements:
**
**      INSERT INTO t(a,b,c) VALUES ...;
**      CREATE INDEX idx ON t(a,b,c);
**      CREATE TRIGGER trig BEFORE UPDATE ON t(a,b,c) ...;
**
** The IdList.a.idx field is used when the IdList represents the list of
** column names after a table name in an INSERT statement.  In the statement
**
**     INSERT INTO t(a,b,c) ...
**
** If "a" is the k-th column of table "t", then IdList.a[0].idx==k.
*/
struct IdList {
  struct IdList_item {
    char *zName;      /* Name of the identifier */
    int idx;          /* Index in some Table.aCol[] of a column named zName */
  } *a;
  int nId;         /* Number of identifiers on the list */
};

/*
** The bitmask datatype defined below is used for various optimizations.
**
** Changing this from a 64-bit to a 32-bit type limits the number of
** tables in a join to 32 instead of 64.  But it also reduces the size
** of the library by 738 bytes on ix86.
*/
typedef u64 Bitmask;

/*
** The number of bits in a Bitmask.  "BMS" means "BitMask Size".
*/
#define BMS  ((int)(sizeof(Bitmask)*8))

/*
** A bit in a Bitmask
*/
#define MASKBIT(n)   (((Bitmask)1)<<(n))
#define MASKBIT32(n) (((unsigned int)1)<<(n))

/*
** The following structure describes the FROM clause of a SELECT statement.
** Each table or subquery in the FROM clause is a separate element of
** the SrcList.a[] array.
**
** With the addition of multiple database support, the following structure
** can also be used to describe a particular table such as the table that
** is modified by an INSERT, DELETE, or UPDATE statement.  In standard SQL,
** such a table must be a simple name: ID.  But in SQLite, the table can
** now be identified by a database name, a dot, then the table name: ID.ID.
**
** The jointype starts out showing the join type between the current table
** and the next table on the list.  The parser builds the list this way.
** But sqlitexSrcListShiftJoinType() later shifts the jointypes so that each
** jointype expresses the join between the table and the previous table.
**
** In the colUsed field, the high-order bit (bit 63) is set if the table
** contains more than 63 columns and the 64-th or later column is used.
*/
struct SrcList {
  int nSrc;        /* Number of tables or subqueries in the FROM clause */
  u32 nAlloc;      /* Number of entries allocated in a[] below */
  Index *pPrevIdx; /* COMDB2 MODIFICATION */
  struct SrcList_item {
    Schema *pSchema;  /* Schema to which this item is fixed */
    char *zDatabase;  /* Name of database holding this table */
    char *zName;      /* Name of the table */
    char *zAlias;     /* The "B" part of a "A AS B" phrase.  zName is the "A" */
    Table *pTab;      /* An SQL table corresponding to zName */
    Select *pSelect;  /* A SELECT statement used in place of a table name */
    int addrFillSub;  /* Address of subroutine to manifest a subquery */
    int regReturn;    /* Register holding return address of addrFillSub */
    int regResult;    /* Registers holding results of a co-routine */
    struct {
      u8 jointype;      /* Type of join between this able and the previous */
      unsigned notIndexed :1;    /* True if there is a NOT INDEXED clause */
      unsigned isIndexedBy :1;   /* True if there is an INDEXED BY clause */
      unsigned isTabFunc :1;     /* True if table-valued-function syntax */
      unsigned isCorrelated :1;  /* True if sub-query is correlated */
      unsigned viaCoroutine :1;  /* Implemented as a co-routine */
      unsigned isRecursive :1;   /* True for recursive reference in WITH */
    } fg;
#ifndef SQLITE_OMIT_EXPLAIN
    u8 iSelectId;     /* If pSelect!=0, the id of the sub-select in EQP */
#endif
    int iCursor;      /* The VDBE cursor number used to access this table */
    Expr *pOn;        /* The ON clause of a join */
    IdList *pUsing;   /* The USING clause of a join */
    Bitmask colUsed;  /* Bit N (1<<N) set if column N of pTab is used */
    union {
      char *zIndexedBy;    /* Identifier from "INDEXED BY <zIndex>" clause */
      ExprList *pFuncArg;  /* Arguments to table-valued-function */
    } u1;
    Index *pIBIndex;  /* Index structure corresponding to u1.zIndexedBy */
  } a[1];             /* One entry for each identifier on the list */
};

/*
** Permitted values of the SrcList.a.jointype field
*/
#define JT_INNER     0x0001    /* Any kind of inner or cross join */
#define JT_CROSS     0x0002    /* Explicit use of the CROSS keyword */
#define JT_NATURAL   0x0004    /* True for a "natural" join */
#define JT_LEFT      0x0008    /* Left outer join */
#define JT_RIGHT     0x0010    /* Right outer join */
#define JT_OUTER     0x0020    /* The "OUTER" keyword is present */
#define JT_ERROR     0x0040    /* unknown or unsupported join type */


/*
** Flags appropriate for the wctrlFlags parameter of sqlitexWhereBegin()
** and the WhereInfo.wctrlFlags member.
*/
#define WHERE_ORDERBY_NORMAL   0x0000 /* No-op */
#define WHERE_ORDERBY_MIN      0x0001 /* ORDER BY processing for min() func */
#define WHERE_ORDERBY_MAX      0x0002 /* ORDER BY processing for max() func */
#define WHERE_ONEPASS_DESIRED  0x0004 /* Want to do one-pass UPDATE/DELETE */
#define WHERE_DUPLICATES_OK    0x0008 /* Ok to return a row more than once */
#define WHERE_OMIT_OPEN_CLOSE  0x0010 /* Table cursors are already open */
#define WHERE_FORCE_TABLE      0x0020 /* Do not use an index-only search */
#define WHERE_ONETABLE_ONLY    0x0040 /* Only code the 1st table in pTabList */
#define WHERE_NO_AUTOINDEX     0x0080 /* Disallow automatic indexes */
#define WHERE_GROUPBY          0x0100 /* pOrderBy is really a GROUP BY */
#define WHERE_DISTINCTBY       0x0200 /* pOrderby is really a DISTINCT clause */
#define WHERE_WANT_DISTINCT    0x0400 /* All output needs to be distinct */
#define WHERE_SORTBYGROUP      0x0800 /* Support sqlitexWhereIsSorted() */
#define WHERE_REOPEN_IDX       0x1000 /* Try to use OP_ReopenIdx */
#define WHERE_ONEPASS_MULTIROW 0x2000 /* ONEPASS is ok with multiple rows */

/* Allowed return values from sqlitexWhereIsDistinct()
*/
#define WHERE_DISTINCT_NOOP      0  /* DISTINCT keyword not used */
#define WHERE_DISTINCT_UNIQUE    1  /* No duplicates */
#define WHERE_DISTINCT_ORDERED   2  /* All duplicates are adjacent */
#define WHERE_DISTINCT_UNORDERED 3  /* Duplicates are scattered */

/*
** A NameContext defines a context in which to resolve table and column
** names.  The context consists of a list of tables (the pSrcList) field and
** a list of named expression (pEList).  The named expression list may
** be NULL.  The pSrc corresponds to the FROM clause of a SELECT or
** to the table being operated on by INSERT, UPDATE, or DELETE.  The
** pEList corresponds to the result set of a SELECT and is NULL for
** other statements.
**
** NameContexts can be nested.  When resolving names, the inner-most 
** context is searched first.  If no match is found, the next outer
** context is checked.  If there is still no match, the next context
** is checked.  This process continues until either a match is found
** or all contexts are check.  When a match is found, the nRef member of
** the context containing the match is incremented. 
**
** Each subquery gets a new NameContext.  The pNext field points to the
** NameContext in the parent query.  Thus the process of scanning the
** NameContext list corresponds to searching through successively outer
** subqueries looking for a match.
*/
struct NameContext {
  Parse *pParse;       /* The parser */
  SrcList *pSrcList;   /* One or more tables used to resolve names */
  ExprList *pEList;    /* Optional list of result-set columns */
  AggInfo *pAggInfo;   /* Information about aggregates at this level */
  NameContext *pNext;  /* Next outer name context.  NULL for outermost */
  int nRef;            /* Number of names resolved by this context */
  int nErr;            /* Number of errors encountered while resolving names */
  u16 ncFlags;         /* Zero or more NC_* flags defined below */
};

/*
** Allowed values for the NameContext, ncFlags field.
**
** Note:  NC_MinMaxAgg must have the same value as SF_MinMaxAgg and
** SQLITE_FUNC_MINMAX.
** 
*/
#define NC_AllowAgg  0x0001  /* Aggregate functions are allowed here */
#define NC_HasAgg    0x0002  /* One or more aggregate functions seen */
#define NC_IsCheck   0x0004  /* True if resolving names in a CHECK constraint */
#define NC_InAggFunc 0x0008  /* True if analyzing arguments to an agg func */
#define NC_PartIdx   0x0010  /* True if resolving a partial index WHERE */
#define NC_MinMaxAgg 0x1000  /* min/max aggregates seen.  See note above */

/*
** An instance of the following structure contains all information
** needed to generate code for a single SELECT statement.
**
** nLimit is set to -1 if there is no LIMIT clause.  nOffset is set to 0.
** If there is a LIMIT clause, the parser sets nLimit to the value of the
** limit and nOffset to the value of the offset (or 0 if there is not
** offset).  But later on, nLimit and nOffset become the memory locations
** in the VDBE that record the limit and offset counters.
**
** addrOpenEphm[] entries contain the address of OP_OpenEphemeral opcodes.
** These addresses must be stored so that we can go back and fill in
** the P4_KEYINFO and P2 parameters later.  Neither the KeyInfo nor
** the number of columns in P2 can be computed at the same time
** as the OP_OpenEphm instruction is coded because not
** enough information about the compound query is known at that point.
** The KeyInfo for addrOpenTran[0] and [1] contains collating sequences
** for the result set.  The KeyInfo for addrOpenEphm[2] contains collating
** sequences for the ORDER BY clause.
*/
struct Select {
  ExprList *pEList;      /* The fields of the result */
  u8 op;                 /* One of: TK_UNION TK_ALL TK_INTERSECT TK_EXCEPT */
  u16 selFlags;          /* Various SF_* values */
  int iLimit, iOffset;   /* Memory registers holding LIMIT & OFFSET counters */
#if SELECTTRACE_ENABLED
  char zSelName[12];     /* Symbolic name of this SELECT use for debugging */
#endif
  int addrOpenEphm[2];   /* OP_OpenEphem opcodes related to this select */
  u64 nSelectRow;        /* Estimated number of result rows */
  SrcList *pSrc;         /* The FROM clause */
  Expr *pWhere;          /* The WHERE clause */
  ExprList *pGroupBy;    /* The GROUP BY clause */
  Expr *pHaving;         /* The HAVING clause */
  ExprList *pOrderBy;    /* The ORDER BY clause */
  Select *pPrior;        /* Prior select in a compound select statement */
  Select *pNext;         /* Next select to the left in a compound */
  Expr *pLimit;          /* LIMIT expression. NULL means not used. */
  Expr *pOffset;         /* OFFSET expression. NULL means not used. */
  With *pWith;           /* WITH clause attached to this select. Or NULL. */
  /* COMDB2 MODIFICATION */
  u8 recording;          /* set this to 1 from SELECTV */
};

/*
** Allowed values for Select.selFlags.  The "SF" prefix stands for
** "Select Flag".
*/
#define SF_Distinct        0x0001  /* Output should be DISTINCT */
#define SF_Resolved        0x0002  /* Identifiers have been resolved */
#define SF_Aggregate       0x0004  /* Contains aggregate functions */
#define SF_UsesEphemeral   0x0008  /* Uses the OpenEphemeral opcode */
#define SF_Expanded        0x0010  /* sqlitexSelectExpand() called on this */
#define SF_HasTypeInfo     0x0020  /* FROM subqueries have Table metadata */
#define SF_Compound        0x0040  /* Part of a compound query */
#define SF_Values          0x0080  /* Synthesized from VALUES clause */
#define SF_AllValues       0x0100  /* All terms of compound are VALUES */
#define SF_NestedFrom      0x0200  /* Part of a parenthesized FROM clause */
#define SF_MaybeConvert    0x0400  /* Need convertCompoundSelectToSubquery() */
#define SF_Recursive       0x0800  /* The recursive part of a recursive CTE */
#define SF_MinMaxAgg       0x1000  /* Aggregate containing min() or max() */
#define SF_IncludeHidden   0x8000  /* Include hidden columns in output */

/*
** The results of a SELECT can be distributed in several ways, as defined
** by one of the following macros.  The "SRT" prefix means "SELECT Result
** Type".
**
**     SRT_Union       Store results as a key in a temporary index 
**                     identified by pDest->iSDParm.
**
**     SRT_Except      Remove results from the temporary index pDest->iSDParm.
**
**     SRT_Exists      Store a 1 in memory cell pDest->iSDParm if the result
**                     set is not empty.
**
**     SRT_Discard     Throw the results away.  This is used by SELECT
**                     statements within triggers whose only purpose is
**                     the side-effects of functions.
**
** All of the above are free to ignore their ORDER BY clause. Those that
** follow must honor the ORDER BY clause.
**
**     SRT_Output      Generate a row of output (using the OP_ResultRow
**                     opcode) for each row in the result set.
**
**     SRT_Mem         Only valid if the result is a single column.
**                     Store the first column of the first result row
**                     in register pDest->iSDParm then abandon the rest
**                     of the query.  This destination implies "LIMIT 1".
**
**     SRT_Set         The result must be a single column.  Store each
**                     row of result as the key in table pDest->iSDParm. 
**                     Apply the affinity pDest->affSdst before storing
**                     results.  Used to implement "IN (SELECT ...)".
**
**     SRT_EphemTab    Create an temporary table pDest->iSDParm and store
**                     the result there. The cursor is left open after
**                     returning.  This is like SRT_Table except that
**                     this destination uses OP_OpenEphemeral to create
**                     the table first.
**
**     SRT_Coroutine   Generate a co-routine that returns a new row of
**                     results each time it is invoked.  The entry point
**                     of the co-routine is stored in register pDest->iSDParm
**                     and the result row is stored in pDest->nDest registers
**                     starting with pDest->iSdst.
**
**     SRT_Table       Store results in temporary table pDest->iSDParm.
**     SRT_Fifo        This is like SRT_EphemTab except that the table
**                     is assumed to already be open.  SRT_Fifo has
**                     the additional property of being able to ignore
**                     the ORDER BY clause.
**
**     SRT_DistFifo    Store results in a temporary table pDest->iSDParm.
**                     But also use temporary table pDest->iSDParm+1 as
**                     a record of all prior results and ignore any duplicate
**                     rows.  Name means:  "Distinct Fifo".
**
**     SRT_Queue       Store results in priority queue pDest->iSDParm (really
**                     an index).  Append a sequence number so that all entries
**                     are distinct.
**
**     SRT_DistQueue   Store results in priority queue pDest->iSDParm only if
**                     the same record has never been stored before.  The
**                     index at pDest->iSDParm+1 hold all prior stores.
*/
#define SRT_Union        1  /* Store result as keys in an index */
#define SRT_Except       2  /* Remove result from a UNION index */
#define SRT_Exists       3  /* Store 1 if the result is not empty */
#define SRT_Discard      4  /* Do not save the results anywhere */
#define SRT_Fifo         5  /* Store result as data with an automatic rowid */
#define SRT_DistFifo     6  /* Like SRT_Fifo, but unique results only */
#define SRT_Queue        7  /* Store result in an queue */
#define SRT_DistQueue    8  /* Like SRT_Queue, but unique results only */

/* The ORDER BY clause is ignored for all of the above */
#define IgnorableOrderby(X) ((X->eDest)<=SRT_DistQueue)

#define SRT_Output       9  /* Output each row of result */
#define SRT_Mem         10  /* Store result in a memory cell */
#define SRT_Set         11  /* Store results as keys in an index */
#define SRT_EphemTab    12  /* Create transient tab and store like SRT_Table */
#define SRT_Coroutine   13  /* Generate a single row of result */
#define SRT_Table       14  /* Store result as data with an automatic rowid */

/*
** An instance of this object describes where to put of the results of
** a SELECT statement.
*/
struct SelectDest {
  u8 eDest;            /* How to dispose of the results.  On of SRT_* above. */
  char affSdst;        /* Affinity used when eDest==SRT_Set */
  int iSDParm;         /* A parameter used by the eDest disposal method */
  int iSdst;           /* Base register where results are written */
  int nSdst;           /* Number of registers allocated */
  ExprList *pOrderBy;  /* Key columns for SRT_Queue and SRT_DistQueue */
};

/*
** During code generation of statements that do inserts into AUTOINCREMENT 
** tables, the following information is attached to the Table.u.autoInc.p
** pointer of each autoincrement table to record some side information that
** the code generator needs.  We have to keep per-table autoincrement
** information in case inserts are down within triggers.  Triggers do not
** normally coordinate their activities, but we do need to coordinate the
** loading and saving of autoincrement information.
*/
struct AutoincInfo {
  AutoincInfo *pNext;   /* Next info block in a list of them all */
  Table *pTab;          /* Table this info block refers to */
  int iDb;              /* Index in sqlitex.aDb[] of database holding pTab */
  int regCtr;           /* Memory register holding the rowid counter */
};

/*
** Size of the column cache
*/
#ifndef SQLITE_N_COLCACHE
# define SQLITE_N_COLCACHE 10
#endif

/*
** At least one instance of the following structure is created for each 
** trigger that may be fired while parsing an INSERT, UPDATE or DELETE
** statement. All such objects are stored in the linked list headed at
** Parse.pTriggerPrg and deleted once statement compilation has been
** completed.
**
** A Vdbe sub-program that implements the body and WHEN clause of trigger
** TriggerPrg.pTrigger, assuming a default ON CONFLICT clause of
** TriggerPrg.orconf, is stored in the TriggerPrg.pProgram variable.
** The Parse.pTriggerPrg list never contains two entries with the same
** values for both pTrigger and orconf.
**
** The TriggerPrg.aColmask[0] variable is set to a mask of old.* columns
** accessed (or set to 0 for triggers fired as a result of INSERT 
** statements). Similarly, the TriggerPrg.aColmask[1] variable is set to
** a mask of new.* columns used by the program.
*/
struct TriggerPrg {
  Trigger *pTrigger;      /* Trigger this program was coded from */
  TriggerPrg *pNext;      /* Next entry in Parse.pTriggerPrg list */
  SubProgram *pProgram;   /* Program implementing pTrigger/orconf */
  int orconf;             /* Default ON CONFLICT policy */
  u32 aColmask[2];        /* Masks of old.*, new.* columns accessed */
};

/*
** The yDbMask datatype for the bitmask of all attached databases.
*/
#if SQLITE_MAX_ATTACHED>30
  typedef unsigned char yDbMask[(SQLITE_MAX_ATTACHED+9)/8];
# define DbMaskTest(M,I)    (((M)[(I)/8]&(1<<((I)&7)))!=0)
# define DbMaskZero(M)      memset((M),0,sizeof(M))
# define DbMaskSet(M,I)     (M)[(I)/8]|=(1<<((I)&7))
# define DbMaskAllZero(M, S)   sqlitexDbMaskAllZero(M, S)
# define DbMaskNonZero(M, S)   (sqlitexDbMaskAllZero(M, S)==0)
#else
  typedef unsigned int yDbMask;
# define DbMaskTest(M,I)    (((M)&(((yDbMask)1)<<(I)))!=0)
# define DbMaskZero(M)      (M)=0
# define DbMaskSet(M,I)     (M)|=(((yDbMask)1)<<(I))
# define DbMaskAllZero(M, S)   (M)==0
# define DbMaskNonZero(M, S)   (M)!=0
#endif

/*
** An SQL parser context.  A copy of this structure is passed through
** the parser and down into all the parser action routine in order to
** carry around information that is global to the entire parse.
**
** The structure is divided into two parts.  When the parser and code
** generate call themselves recursively, the first part of the structure
** is constant but the second part is reset at the beginning and end of
** each recursion.
**
** The nTableLock and aTableLock variables are only used if the shared-cache 
** feature is enabled (if sqlitexTsd()->useSharedData is true). They are
** used to store the set of table-locks required by the statement being
** compiled. Function sqlitexTableLock() is used to add entries to the
** list.
*/
/** COMDB2 MODIFICATION
  mark which cursor is recording and which not 
 */
#define MAX_CURSOR_IDS  10000
struct Parse {
  sqlitex *db;         /* The main database structure */
  char *zErrMsg;       /* An error message */
  Vdbe *pVdbe;         /* An engine for executing database bytecode */
  int rc;              /* Return code from execution */
  u8 colNamesSet;      /* TRUE after OP_ColumnName has been issued to pVdbe */
  u8 checkSchema;      /* Causes schema cookie check after an error */
  u8 nested;           /* Number of nested calls to the parser/code generator */
  u8 nTempReg;         /* Number of temporary registers in aTempReg[] */
  u8 isMultiWrite;     /* True if statement may modify/insert multiple rows */
  u8 mayAbort;         /* True if statement may throw an ABORT exception */
  u8 hasCompound;      /* Need to invoke convertCompoundSelectToSubquery() */
  u8 okConstFactor;    /* OK to factor out constants */
  int aTempReg[8];     /* Holding area for temporary registers */
  int nRangeReg;       /* Size of the temporary register block */
  int iRangeReg;       /* First register in temporary register block */
  int nErr;            /* Number of errors seen */
  int nTab;            /* Number of previously allocated VDBE cursors */
  int nMem;            /* Number of memory cells used so far */
  int nSet;            /* Number of sets used so far */
  int nOnce;           /* Number of OP_Once instructions so far */
  int nOpAlloc;        /* Number of slots allocated for Vdbe.aOp[] */
  int iFixedOp;        /* Never back out opcodes iFixedOp-1 or earlier */
  int ckBase;          /* Base register of data during check constraints */
  int iPartIdxTab;     /* Table corresponding to a partial index */
  int iCacheLevel;     /* ColCache valid when aColCache[].iLevel<=iCacheLevel */
  int iCacheCnt;       /* Counter used to generate aColCache[].lru values */
  int nLabel;          /* Number of labels used */
  int *aLabel;         /* Space to hold the labels */
  struct yColCache {
    int iTable;           /* Table cursor number */
    i16 iColumn;          /* Table column number */
    u8 tempReg;           /* iReg is a temp register that needs to be freed */
    int iLevel;           /* Nesting level */
    int iReg;             /* Reg with value of this column. 0 means none. */
    int lru;              /* Least recently used entry has the smallest value */
  } aColCache[SQLITE_N_COLCACHE];  /* One for each column cache entry */
  ExprList *pConstExpr;/* Constant expressions */
  Token constraintName;/* Name of the constraint currently being parsed */
  yDbMask writeMask;   /* Start a write transaction on these databases */
  yDbMask cookieMask;  /* Bitmask of schema verified databases */
  int cookieValue[SQLITE_MAX_ATTACHED+2];  /* Values of cookies to verify */
  int regRowid;        /* Register holding rowid of CREATE TABLE entry */
  int regRoot;         /* Register holding root page number for new objects */
  int nMaxArg;         /* Max args passed to user function by sub-program */
#if SELECTTRACE_ENABLED
  int nSelect;         /* Number of SELECT statements seen */
  int nSelectIndent;   /* How far to indent SELECTTRACE() output */
#endif
#ifndef SQLITE_OMIT_SHARED_CACHE
  int nTableLock;        /* Number of locks in aTableLock */
  TableLock *aTableLock; /* Required table locks for shared-cache mode */
#endif
  AutoincInfo *pAinc;  /* Information about AUTOINCREMENT counters */

  /* Information used while coding trigger programs. */
  Parse *pToplevel;    /* Parse structure for main program (or NULL) */
  Table *pTriggerTab;  /* Table triggers are being coded for */
  int addrCrTab;       /* Address of OP_CreateTable opcode on CREATE TABLE */
  int addrSkipPK;      /* Address of instruction to skip PRIMARY KEY index */
  u32 nQueryLoop;      /* Est number of iterations of a query (10*log2(N)) */
  u32 oldmask;         /* Mask of old.* columns referenced */
  u32 newmask;         /* Mask of new.* columns referenced */
  u8 eTriggerOp;       /* TK_UPDATE, TK_INSERT or TK_DELETE */
  u8 eOrconf;          /* Default ON CONFLICT policy for trigger steps */
  u8 disableTriggers;  /* True to disable triggers */

  /************************************************************************
  ** Above is constant between recursions.  Below is reset before and after
  ** each recursion.  The boundary between these two regions is determined
  ** using offsetof(Parse,nVar) so the nVar field must be the first field
  ** in the recursive region.
  ************************************************************************/

  int nVar;                 /* Number of '?' variables seen in the SQL so far */
  int nzVar;                /* Number of available slots in azVar[] */
  u8 iPkSortOrder;          /* ASC or DESC for INTEGER PRIMARY KEY */
  u8 bFreeWith;             /* True if pWith should be freed with parser */
  u8 explain;               /* True if the EXPLAIN flag is found on the query */
#ifndef SQLITE_OMIT_VIRTUALTABLE
  u8 declareVtab;           /* True if inside sqlitex_declare_vtab() */
  int nVtabLock;            /* Number of virtual tables to lock */
#endif
  int nAlias;               /* Number of aliased result set columns */
  int nHeight;              /* Expression tree height of current sub-select */
#ifndef SQLITE_OMIT_EXPLAIN
  int iSelectId;            /* ID of current select for EXPLAIN output */
  int iNextSelectId;        /* Next available select ID for EXPLAIN output */
#endif
  char **azVar;             /* Pointers to names of parameters */
  Vdbe *pReprepare;         /* VM being reprepared (sqlitexReprepare()) */
  const char *zTail;        /* All SQL text past the last semicolon parsed */
  Table *pNewTable;         /* A table being constructed by CREATE TABLE */
  Trigger *pNewTrigger;     /* Trigger under construct by a CREATE TRIGGER */
  const char *zAuthContext; /* The 6th parameter to db->xAuth callbacks */
  Token sNameToken;         /* Token with unqualified schema object name */
  Token sLastToken;         /* The last token parsed */
#ifndef SQLITE_OMIT_VIRTUALTABLE
  Token sArg;               /* Complete text of a module argument */
  Table **apVtabLock;       /* Pointer to virtual tables needing locking */
#endif
  Table *pZombieTab;        /* List of Table objects to delete after code gen */
  TriggerPrg *pTriggerPrg;  /* Linked list of coded triggers */
  With *pWith;              /* Current WITH clause, or NULL */
  /* COMDB2 MODIFICATION */
  int enableExplainTrace;
  int trackIdentifiers;
  Hash identifiers;
  int recording[MAX_CURSOR_IDS/sizeof(int)];  /* register which cursors are recording and which not */
};

/* COMDB2 MODIFICATION */
#if NODEBUG 
#define SET_CURSOR_RECORDING(p,i)      \
   (p)->recording[(i)/sizeof(int)] |= (1<<(i)%sizeof(int))   
#define CLR_CURSOR_RECORDING(p,i)      \
   (p)->recording[(i)/sizeof(int)] &= ~(1<<(i)%sizeof(int))
#define GET_CURSOR_RECORDING(p,i)      \
   ((p)->recording[(i)/sizeof(int)] & (1<<(i)%sizeof(int)))

#else

static void SET_CURSOR_RECORDING( Parse *p, int i)
{
   (p)->recording[(i)/sizeof(int)] |= (1<<(i)%sizeof(int));   
}
static void CLR_CURSOR_RECORDING( Parse *p, int i)
{
   (p)->recording[(i)/sizeof(int)] &= ~(1<<(i)%sizeof(int));
}
static int GET_CURSOR_RECORDING( Parse *p, int i)
{ 
   return ((p)->recording[(i)/sizeof(int)] & (1<<(i)%sizeof(int)));
}
#endif

/*
** Return true if currently inside an sqlitex_declare_vtab() call.
*/
#ifdef SQLITE_OMIT_VIRTUALTABLE
  #define IN_DECLARE_VTAB 0
#else
  #define IN_DECLARE_VTAB (pParse->declareVtab)
#endif

/*
** An instance of the following structure can be declared on a stack and used
** to save the Parse.zAuthContext value so that it can be restored later.
*/
struct AuthContext {
  const char *zAuthContext;   /* Put saved Parse.zAuthContext here */
  Parse *pParse;              /* The Parse structure */
};

/*
** Bitfield flags for P5 value in various opcodes.
*/
#define OPFLAG_NCHANGE       0x01    /* OP_Insert: Set to update db->nChange */
                                     /* Also used in P2 (not P5) of OP_Delete */
#define OPFLAG_EPHEM         0x01    /* OP_Column: Ephemeral output is ok */
#define OPFLAG_LASTROWID     0x02    /* Set to update db->lastRowid */
#define OPFLAG_ISUPDATE      0x04    /* This OP_Insert is an sql UPDATE */
#define OPFLAG_APPEND        0x08    /* This is likely to be an append */
#define OPFLAG_USESEEKRESULT 0x10    /* Try to avoid a seek in BtreeInsert() */
#define OPFLAG_LENGTHARG     0x40    /* OP_Column only used for length() */
#define OPFLAG_TYPEOFARG     0x80    /* OP_Column only used for typeof() */
#define OPFLAG_BULKCSR       0x01    /* OP_Open** used to open bulk cursor */
#define OPFLAG_SEEKEQ        0x02    /* OP_Open** cursor uses EQ seek only */
#define OPFLAG_FORDELETE     0x08    /* OP_Open is opening for-delete csr */
#define OPFLAG_P2ISREG       0x10    /* P2 to OP_Open** is a register number */
#define OPFLAG_PERMUTE       0x01    /* OP_Compare: use the permutation */
#define OPFLAG_SAVEPOSITION  0x02    /* OP_Delete: keep cursor position */
#define OPFLAG_AUXDELETE     0x04    /* OP_Delete: index in a DELETE op */

/*
 * Each trigger present in the database schema is stored as an instance of
 * struct Trigger. 
 *
 * Pointers to instances of struct Trigger are stored in two ways.
 * 1. In the "trigHash" hash table (part of the sqlitex* that represents the 
 *    database). This allows Trigger structures to be retrieved by name.
 * 2. All triggers associated with a single table form a linked list, using the
 *    pNext member of struct Trigger. A pointer to the first element of the
 *    linked list is stored as the "pTrigger" member of the associated
 *    struct Table.
 *
 * The "step_list" member points to the first element of a linked list
 * containing the SQL statements specified as the trigger program.
 */
struct Trigger {
  char *zName;            /* The name of the trigger                        */
  char *table;            /* The table or view to which the trigger applies */
  u8 op;                  /* One of TK_DELETE, TK_UPDATE, TK_INSERT         */
  u8 tr_tm;               /* One of TRIGGER_BEFORE, TRIGGER_AFTER */
  Expr *pWhen;            /* The WHEN clause of the expression (may be NULL) */
  IdList *pColumns;       /* If this is an UPDATE OF <column-list> trigger,
                             the <column-list> is stored here */
  Schema *pSchema;        /* Schema containing the trigger */
  Schema *pTabSchema;     /* Schema containing the table */
  TriggerStep *step_list; /* Link list of trigger program steps             */
  Trigger *pNext;         /* Next trigger associated with the table */
};

/*
** A trigger is either a BEFORE or an AFTER trigger.  The following constants
** determine which. 
**
** If there are multiple triggers, you might of some BEFORE and some AFTER.
** In that cases, the constants below can be ORed together.
*/
#define TRIGGER_BEFORE  1
#define TRIGGER_AFTER   2

/*
 * An instance of struct TriggerStep is used to store a single SQL statement
 * that is a part of a trigger-program. 
 *
 * Instances of struct TriggerStep are stored in a singly linked list (linked
 * using the "pNext" member) referenced by the "step_list" member of the 
 * associated struct Trigger instance. The first element of the linked list is
 * the first step of the trigger-program.
 * 
 * The "op" member indicates whether this is a "DELETE", "INSERT", "UPDATE" or
 * "SELECT" statement. The meanings of the other members is determined by the 
 * value of "op" as follows:
 *
 * (op == TK_INSERT)
 * orconf    -> stores the ON CONFLICT algorithm
 * pSelect   -> If this is an INSERT INTO ... SELECT ... statement, then
 *              this stores a pointer to the SELECT statement. Otherwise NULL.
 * target    -> A token holding the quoted name of the table to insert into.
 * pExprList -> If this is an INSERT INTO ... VALUES ... statement, then
 *              this stores values to be inserted. Otherwise NULL.
 * pIdList   -> If this is an INSERT INTO ... (<column-names>) VALUES ... 
 *              statement, then this stores the column-names to be
 *              inserted into.
 *
 * (op == TK_DELETE)
 * target    -> A token holding the quoted name of the table to delete from.
 * pWhere    -> The WHERE clause of the DELETE statement if one is specified.
 *              Otherwise NULL.
 * 
 * (op == TK_UPDATE)
 * target    -> A token holding the quoted name of the table to update rows of.
 * pWhere    -> The WHERE clause of the UPDATE statement if one is specified.
 *              Otherwise NULL.
 * pExprList -> A list of the columns to update and the expressions to update
 *              them to. See sqlitexUpdate() documentation of "pChanges"
 *              argument.
 * 
 */
struct TriggerStep {
  u8 op;               /* One of TK_DELETE, TK_UPDATE, TK_INSERT, TK_SELECT */
  u8 orconf;           /* OE_Rollback etc. */
  Trigger *pTrig;      /* The trigger that this step is a part of */
  Select *pSelect;     /* SELECT statment or RHS of INSERT INTO .. SELECT ... */
  Token target;        /* Target table for DELETE, UPDATE, INSERT */
  Expr *pWhere;        /* The WHERE clause for DELETE or UPDATE steps */
  ExprList *pExprList; /* SET clause for UPDATE. */
  IdList *pIdList;     /* Column names for INSERT */
  TriggerStep *pNext;  /* Next in the link-list */
  TriggerStep *pLast;  /* Last element in link-list. Valid for 1st elem only */
};

/*
** The following structure contains information used by the sqliteFix...
** routines as they walk the parse tree to make database references
** explicit.  
*/
typedef struct DbFixer DbFixer;
struct DbFixer {
  Parse *pParse;      /* The parsing context.  Error messages written here */
  Schema *pSchema;    /* Fix items to this schema */
  int bVarOnly;       /* Check for variable references only */
  const char *zDb;    /* Make sure all objects are contained in this database */
  const char *zType;  /* Type of the container - used for error messages */
  const Token *pName; /* Name of the container - used for error messages */
};

/*
** An objected used to accumulate the text of a string where we
** do not necessarily know how big the string will be in the end.
*/
struct StrAccum {
  sqlitex *db;         /* Optional database for lookaside.  Can be NULL */
  char *zBase;         /* A base allocation.  Not from malloc. */
  char *zText;         /* The string collected so far */
  int  nChar;          /* Length of the string so far */
  int  nAlloc;         /* Amount of space allocated in zText */
  int  mxAlloc;        /* Maximum allowed string length */
  u8   useMalloc;      /* 0: none,  1: sqlitexDbMalloc,  2: sqlitex_malloc */
  u8   accError;       /* STRACCUM_NOMEM or STRACCUM_TOOBIG */
};
#define STRACCUM_NOMEM   1
#define STRACCUM_TOOBIG  2

/*
** A pointer to this structure is used to communicate information
** from sqlitexInit and OP_ParseSchema into the sqlitexInitCallback.
*/
typedef struct {
  sqlitex *db;        /* The database being initialized */
  char **pzErrMsg;    /* Error message stored here */
  int iDb;            /* 0 for main database.  1 for TEMP, 2.. for ATTACHed */
  int rc;             /* Result code stored here */
} InitData;

/*
** Structure containing global configuration data for the SQLite library.
**
** This structure also contains some state information.
*/
struct SqlitexConfig {
  int bMemstat;                     /* True to enable memory status */
  int bCoreMutex;                   /* True to enable core mutexing */
  int bFullMutex;                   /* True to enable full mutexing */
  int bOpenUri;                     /* True to interpret filenames as URIs */
  int bUseCis;                      /* Use covering indices for full-scans */
  int mxStrlen;                     /* Maximum string length */
  int neverCorrupt;                 /* Database is always well-formed */
  int szLookaside;                  /* Default lookaside buffer size */
  int nLookaside;                   /* Default lookaside buffer count */
  sqlitex_mem_methods m;            /* Low-level memory allocation interface */
  sqlitex_mutex_methods mutex;      /* Low-level mutex interface */
  sqlitex_pcache_methods2 pcache2;  /* Low-level page-cache interface */
  void *pHeap;                      /* Heap storage space */
  int nHeap;                        /* Size of pHeap[] */
  int mnReq, mxReq;                 /* Min and max heap requests sizes */
  sqlitex_int64 szMmap;             /* mmap() space per open file */
  sqlitex_int64 mxMmap;             /* Maximum value for szMmap */
  void *pScratch;                   /* Scratch memory */
  int szScratch;                    /* Size of each scratch buffer */
  int nScratch;                     /* Number of scratch buffers */
  void *pPage;                      /* Page cache memory */
  int szPage;                       /* Size of each page in pPage[] */
  int nPage;                        /* Number of pages in pPage[] */
  int mxParserStack;                /* maximum depth of the parser stack */
  int sharedCacheEnabled;           /* true if shared-cache mode enabled */
  u32 szPma;                        /* Maximum Sorter PMA size */
  /* The above might be initialized to non-zero.  The following need to always
  ** initially be zero, however. */
  int isInit;                       /* True after initialization has finished */
  int inProgress;                   /* True while initialization in progress */
  int isMutexInit;                  /* True after mutexes are initialized */
  int isMallocInit;                 /* True after malloc is initialized */
  int isPCacheInit;                 /* True after malloc is initialized */
  int nRefInitMutex;                /* Number of users of pInitMutex */
  sqlitex_mutex *pInitMutex;        /* Mutex used by sqlitex_initialize() */
  void (*xLog)(void*,int,const char*); /* Function for logging */
  void *pLogArg;                       /* First argument to xLog() */
#ifdef SQLITE_ENABLE_SQLLOG
  void(*xSqllog)(void*,sqlitex*,const char*, int);
  void *pSqllogArg;
#endif
#ifdef SQLITE_VDBE_COVERAGE
  /* The following callback (if not NULL) is invoked on every VDBE branch
  ** operation.  Set the callback using SQLITE_TESTCTRL_VDBE_COVERAGE.
  */
  void (*xVdbeBranch)(void*,int iSrcLine,u8 eThis,u8 eMx);  /* Callback */
  void *pVdbeBranchArg;                                     /* 1st argument */
#endif
#ifndef SQLITE_OMIT_BUILTIN_TEST
  int (*xTestCallback)(int);        /* Invoked by sqlitexFaultSim() */
#endif
  int bLocaltimeFault;              /* True to fail localtime() calls */
};

/*
** This macro is used inside of assert() statements to indicate that
** the assert is only valid on a well-formed database.  Instead of:
**
**     assert( X );
**
** One writes:
**
**     assert( X || CORRUPT_DB );
**
** CORRUPT_DB is true during normal operation.  CORRUPT_DB does not indicate
** that the database is definitely corrupt, only that it might be corrupt.
** For most test cases, CORRUPT_DB is set to false using a special
** sqlitex_test_control().  This enables assert() statements to prove
** things that are always true for well-formed databases.
*/
#define CORRUPT_DB  (sqlitexConfig.neverCorrupt==0)

/*
** Context pointer passed down through the tree-walk.
*/
struct Walker {
  int (*xExprCallback)(Walker*, Expr*);     /* Callback for expressions */
  int (*xSelectCallback)(Walker*,Select*);  /* Callback for SELECTs */
  void (*xSelectCallback2)(Walker*,Select*);/* Second callback for SELECTs */
  Parse *pParse;                            /* Parser context.  */
  int walkerDepth;                          /* Number of subqueries */
  u8 eCode;                                 /* A small processing code */
  union {                                   /* Extra data for callback */
    NameContext *pNC;                          /* Naming context */
    int n;                                     /* A counter */
    int iCur;                                  /* A cursor number */
    SrcList *pSrcList;                         /* FROM clause */
    struct SrcCount *pSrcCount;                /* Counting column references */
    struct CCurHint *pCCurHint;                /* Used by codeCursorHint() */
  } u;
};

/* Forward declarations */
int sqlitexWalkExpr(Walker*, Expr*);
int sqlitexWalkExprList(Walker*, ExprList*);
int sqlitexWalkSelect(Walker*, Select*);
int sqlitexWalkSelectExpr(Walker*, Select*);
int sqlitexWalkSelectFrom(Walker*, Select*);
int sqlitexExprWalkNoop(Walker*, Expr*);

/*
** Return code from the parse-tree walking primitives and their
** callbacks.
*/
#define WRC_Continue    0   /* Continue down into children */
#define WRC_Prune       1   /* Omit children but continue walking siblings */
#define WRC_Abort       2   /* Abandon the tree walk */

/*
** An instance of this structure represents a set of one or more CTEs
** (common table expressions) created by a single WITH clause.
*/
struct With {
  int nCte;                       /* Number of CTEs in the WITH clause */
  With *pOuter;                   /* Containing WITH clause, or NULL */
  struct Cte {                    /* For each CTE in the WITH clause.... */
    char *zName;                    /* Name of this CTE */
    ExprList *pCols;                /* List of explicit column names, or NULL */
    Select *pSelect;                /* The definition of this CTE */
    const char *zErr;               /* Error message for circular references */
  } a[1];
};

#ifdef SQLITE_BUILDING_FOR_COMDB2
/*
** An instance of the TreeView object is used for printing the content of
** data structures on sqlite3DebugPrintf() using a tree-like view.
*/
struct TreeView {
  int iLevel;             /* Which level of the tree we are on */
  u8  bLine[100];         /* Draw vertical in column i if bLine[i] is true */
};
#endif /* SQLITE_DEBUG */

/*
** Assuming zIn points to the first byte of a UTF-8 character,
** advance zIn to point to the first byte of the next UTF-8 character.
*/
#define SQLITE_SKIP_UTF8(zIn) {                        \
  if( (*(zIn++))>=0xc0 ){                              \
    while( (*zIn & 0xc0)==0x80 ){ zIn++; }             \
  }                                                    \
}

/*
** The SQLITE_*_BKPT macros are substitutes for the error codes with
** the same name but without the _BKPT suffix.  These macros invoke
** routines that report the line-number on which the error originated
** using sqlitex_log().  The routines also provide a convenient place
** to set a debugger breakpoint.
*/
int sqlitexCorruptError(int);
int sqlitexMisuseError(int);
int sqlitexCantopenError(int);
#define SQLITE_CORRUPT_BKPT sqlitexCorruptError(__LINE__)
#define SQLITE_MISUSE_BKPT sqlitexMisuseError(__LINE__)
#define SQLITE_CANTOPEN_BKPT sqlitexCantopenError(__LINE__)


/*
** FTS4 is really an extension for FTS3.  It is enabled using the
** SQLITE_ENABLE_FTS3 macro.  But to avoid confusion we also call
** the SQLITE_ENABLE_FTS4 macro to serve as an alias for SQLITE_ENABLE_FTS3.
*/
#if defined(SQLITE_ENABLE_FTS4) && !defined(SQLITE_ENABLE_FTS3)
# define SQLITE_ENABLE_FTS3 1
#endif

/*
** The ctype.h header is needed for non-ASCII systems.  It is also
** needed by FTS3 when FTS3 is included in the amalgamation.
*/
#if !defined(SQLITE_ASCII) || \
    (defined(SQLITE_ENABLE_FTS3) && defined(SQLITE_AMALGAMATION))
# include <ctype.h>
#endif

/*
** The following macros mimic the standard library functions toupper(),
** isspace(), isalnum(), isdigit() and isxdigit(), respectively. The
** sqlite versions only work for ASCII characters, regardless of locale.
*/
#ifdef SQLITE_ASCII
# define sqlitexToupper(x)  ((x)&~(sqlitexCtypeMap[(unsigned char)(x)]&0x20))
# define sqlitexIsspace(x)   (sqlitexCtypeMap[(unsigned char)(x)]&0x01)
# define sqlitexIsalnum(x)   (sqlitexCtypeMap[(unsigned char)(x)]&0x06)
# define sqlitexIsalpha(x)   (sqlitexCtypeMap[(unsigned char)(x)]&0x02)
# define sqlitexIsdigit(x)   (sqlitexCtypeMap[(unsigned char)(x)]&0x04)
# define sqlitexIsxdigit(x)  (sqlitexCtypeMap[(unsigned char)(x)]&0x08)
# define sqlitexTolower(x)   (sqlitexUpperToLower[(unsigned char)(x)])
# define sqlitexIsquote(x)   (sqlitexCtypeMap[(unsigned char)(x)]&0x80)
#else
# define sqlitexToupper(x)   toupper((unsigned char)(x))
# define sqlitexIsspace(x)   isspace((unsigned char)(x))
# define sqlitexIsalnum(x)   isalnum((unsigned char)(x))
# define sqlitexIsalpha(x)   isalpha((unsigned char)(x))
# define sqlitexIsdigit(x)   isdigit((unsigned char)(x))
# define sqlitexIsxdigit(x)  isxdigit((unsigned char)(x))
# define sqlitexTolower(x)   tolower((unsigned char)(x))
# define sqlitexIsquote(x)   ((x)=='"'||(x)=='\''||(x)=='['||(x)=='`')
#endif
int sqlitexIsIdChar(u8);

/*
** Internal function prototypes
*/
#define sqlitexStrICmp sqlitex_stricmp
int sqlitexStrlen30(const char*);
#define sqlitexStrNICmp sqlitex_strnicmp

int sqlitexMallocInit(void);
void sqlitexMallocEnd(void);
void *sqlitexMalloc(u64);
void *sqlitexMallocZero(u64);
void *sqlitexDbMallocZero(sqlitex*, u64);
void *sqlitexDbMallocRaw(sqlitex*, u64);
char *sqlitexDbStrDup(sqlitex*,const char*);
char *sqlitexDbStrNDup(sqlitex*,const char*, u64);
void *sqlitexRealloc(void*, u64);
void *sqlitexDbReallocOrFree(sqlitex *, void *, u64);
void *sqlitexDbRealloc(sqlitex *, void *, u64);
void sqlitexDbFree(sqlitex*, void*);
int sqlitexMallocSize(void*);
int sqlitexDbMallocSize(sqlitex*, void*);
void *sqlitexScratchMalloc(int);
void sqlitexScratchFree(void*);
void *sqlitexPageMalloc(int);
void sqlitexPageFree(void*);
void sqlitexMemSetDefault(void);
void sqlitexBenignMallocHooks(void (*)(void), void (*)(void));
int sqlitexHeapNearlyFull(void);

/*
** On systems with ample stack space and that support alloca(), make
** use of alloca() to obtain space for large automatic objects.  By default,
** obtain space from malloc().
**
** The alloca() routine never returns NULL.  This will cause code paths
** that deal with sqlitexStackAlloc() failures to be unreachable.
*/
#ifdef SQLITE_USE_ALLOCA
# define sqlitexStackAllocRaw(D,N)   alloca(N)
# define sqlitexStackAllocZero(D,N)  memset(alloca(N), 0, N)
# define sqlitexStackFree(D,P)       
#else
# define sqlitexStackAllocRaw(D,N)   sqlitexDbMallocRaw(D,N)
# define sqlitexStackAllocZero(D,N)  sqlitexDbMallocZero(D,N)
# define sqlitexStackFree(D,P)       sqlitexDbFree(D,P)
#endif

#ifdef SQLITE_ENABLE_MEMSYS3
const sqlitex_mem_methods *sqlitexMemGetMemsys3(void);
#endif
#ifdef SQLITE_ENABLE_MEMSYS5
const sqlitex_mem_methods *sqlitexMemGetMemsys5(void);
#endif


#ifndef SQLITE_MUTEX_OMIT
  sqlitex_mutex_methods const *sqlitexDefaultMutex(void);
  sqlitex_mutex_methods const *sqlitexNoopMutex(void);
  sqlitex_mutex *sqlitexMutexAlloc(int);
  int sqlitexMutexInit(void);
  int sqlitexMutexEnd(void);
#endif

int sqlitexStatusValue(int);
void sqlitexStatusAdd(int, int);
void sqlitexStatusSet(int, int);

#ifndef SQLITE_OMIT_FLOATING_POINT
  int sqlitexIsNaN(double);
#else
# define sqlitexIsNaN(X)  0
#endif

/*
** An instance of the following structure holds information about SQL
** functions arguments that are the parameters to the printf() function.
*/
struct PrintfArguments {
  int nArg;                /* Total number of arguments */
  int nUsed;               /* Number of arguments used so far */
  sqlitex_value **apArg;   /* The argument values */
};

#define SQLITE_PRINTF_INTERNAL 0x01
#define SQLITE_PRINTF_SQLFUNC  0x02
void sqlitexVXPrintf(StrAccum*, u32, const char*, va_list);
void sqlitexXPrintf(StrAccum*, u32, const char*, ...);
char *sqlitexMPrintf(sqlitex*,const char*, ...);
char *sqlitexVMPrintf(sqlitex*,const char*, va_list);
char *sqlitexMAppendf(sqlitex*,char*,const char*,...);
/* COMDB2 MODIFICATION: required for comdb2sqlexplain */
#if defined(SQLITE_TEST) || defined(SQLITE_BUILDING_FOR_COMDB2)
  void sqlite3DebugPrintf(const char*, ...);
#endif
#if defined(SQLITE_TEST)
  void *sqlitexTestTextToPtr(const char*);
#endif

#if defined(SQLITE_BUILDING_FOR_COMDB2)
  TreeView *sqlitexTreeViewPush(TreeView*,u8);
  void sqlitexTreeViewPop(TreeView*);
  void sqlitexTreeViewLine(TreeView*, const char*, ...);
  void sqlitexTreeViewItem(TreeView*, const char*, u8);
  void sqlitexTreeViewExpr(TreeView*, const Expr*, u8);
  void sqlitexTreeViewExprList(TreeView*, const ExprList*, u8, const char*);
  void sqlitexTreeViewSelect(TreeView*, const Select*, u8);
#endif


void sqlitexSetString(char **, sqlitex*, const char*, ...);
void sqlitexErrorMsg(Parse*, const char*, ...);
void sqlitexDequote(char*);
int sqlitexKeywordCode(const unsigned char*, int);
int sqlitexRunParser(Parse*, const char*, char **);
void sqlitexFinishCoding(Parse*);
int sqlitexGetTempReg(Parse*);
void sqlitexReleaseTempReg(Parse*,int);
int sqlitexGetTempRange(Parse*,int);
void sqlitexReleaseTempRange(Parse*,int,int);
void sqlitexClearTempRegCache(Parse*);
Expr *sqlitexExprAlloc(sqlitex*,int,const Token*,int);
Expr *sqlitexExpr(sqlitex*,int,const char*);
void sqlitexExprAttachSubtrees(sqlitex*,Expr*,Expr*,Expr*);
Expr *sqlitexPExpr(Parse*, int, Expr*, Expr*, const Token*);
Expr *sqlitexExprAnd(sqlitex*,Expr*, Expr*);
Expr *sqlitexExprFunction(Parse*,ExprList*, Token*);
void sqlitexExprAssignVarNumber(Parse*, Expr*);
void sqlitexExprDelete(sqlitex*, Expr*);
ExprList *sqlitexExprListAppend(Parse*,ExprList*,Expr*);
void sqlitexExprListSetName(Parse*,ExprList*,Token*,int);
void sqlitexExprListSetSpan(Parse*,ExprList*,ExprSpan*);
void sqlitexExprListDelete(sqlitex*, ExprList*);
u32 sqlitexExprListFlags(const ExprList*);
int sqlitexInit(sqlitex*, char**);
int sqlitexInitCallback(void*, int, char**, char**);
void sqlitexPragma(Parse*,Token*,Token*,Token*,int);
void sqlitexResetAllSchemasOfConnection(sqlitex*);
void sqlitexResetOneSchema(sqlitex*,int);
void sqlitexResetOneSchemaByName(sqlitex*,const char*,const char*);
void sqlitexCollapseDatabaseArray(sqlitex*);
void sqlitexBeginParse(Parse*,int);
void sqlitexCommitInternalChanges(sqlitex*);
void sqlitexDeleteColumnNames(sqlitex*,Table*);
Table *sqlitexResultSetOfSelect(Parse*,Select*);
void sqlitexOpenMasterTable(Parse *, int);
Index *sqlitexPrimaryKeyIndex(Table*);
i16 sqlitexColumnOfIndex(Index*, i16);
void sqlitexStartTable(Parse*,Token*,Token*,int,int,int,int);
void sqlitexColumnPropertiesFromName(Table*, Column*);
void sqlitexAddColumn(Parse*,Token*);
void sqlitexAddNotNull(Parse*, int);
void sqlitexAddPrimaryKey(Parse*, ExprList*, int, int, int);
void sqlitexAddCheckConstraint(Parse*, Expr*);
void sqlitexAddColumnType(Parse*,Token*);
void sqlitexAddDefaultValue(Parse*,ExprSpan*);
void sqlitexAddCollateType(Parse*, Token*);
void sqlitexEndTable(Parse*,Token*,Token*,u8,Select*);
int sqlitexParseUri(const char*,const char*,unsigned int*,
                    sqlitex_vfs**,char**,char **);
Btree *sqlitexDbNameToBtree(sqlitex*,const char*);
int sqlitexCodeOnce(Parse *);

#ifdef SQLITE_OMIT_BUILTIN_TEST
# define sqlitexFaultSim(X) SQLITE_OK
#else
  int sqlitexFaultSim(int);
#endif

Bitvec *sqlitexBitvecCreate(u32);
int sqlitexBitvecTest(Bitvec*, u32);
int sqlitexBitvecSet(Bitvec*, u32);
void sqlitexBitvecClear(Bitvec*, u32, void*);
void sqlitexBitvecDestroy(Bitvec*);
u32 sqlitexBitvecSize(Bitvec*);
int sqlitexBitvecBuiltinTest(int,int*);

RowSet *sqlitexRowSetInit(sqlitex*, void*, unsigned int);
void sqlitexRowSetClear(RowSet*);
void sqlitexRowSetInsert(RowSet*, i64);
int sqlitexRowSetTest(RowSet*, int iBatch, i64);
int sqlitexRowSetNext(RowSet*, i64*);

void sqlitexCreateView(Parse*,Token*,Token*,Token*,Select*,int,int);

#if !defined(SQLITE_OMIT_VIEW) || !defined(SQLITE_OMIT_VIRTUALTABLE)
  int sqlitexViewGetColumnNames(Parse*,Table*);
#else
# define sqlitexViewGetColumnNames(A,B) 0
#endif

#if SQLITE_MAX_ATTACHED>30
  int sqlitexDbMaskAllZero(yDbMask, int);
#endif
void sqlitexDropTable(Parse*, SrcList*, int, int);
void sqlitexCodeDropTable(Parse*, Table*, int, int);
void sqlitexDeleteTable(sqlitex*, Table*);
#ifndef SQLITE_OMIT_AUTOINCREMENT
  void sqlitexAutoincrementBegin(Parse *pParse);
  void sqlitexAutoincrementEnd(Parse *pParse);
#else
# define sqlitexAutoincrementBegin(X)
# define sqlitexAutoincrementEnd(X)
#endif
void sqlitexInsert(Parse*, SrcList*, Select*, IdList*, int);
void *sqlitexArrayAllocate(sqlitex*,void*,int,int*,int*);
IdList *sqlitexIdListAppend(sqlitex*, IdList*, Token*);
int sqlitexIdListIndex(IdList*,const char*);
SrcList *sqlitexSrcListEnlarge(sqlitex*, SrcList*, int, int);
SrcList *sqlitexSrcListAppend(sqlitex*, SrcList*, Token*, Token*);
SrcList *sqlitexSrcListAppendFromTerm(Parse*, SrcList*, Token*, Token*,
                                      Token*, Select*, Expr*, IdList*);
void sqlitexSrcListIndexedBy(Parse *, SrcList *, Token *);
void sqlitexSrcListFuncArgs(Parse*, SrcList*, ExprList*);
int sqlitexIndexedByLookup(Parse *, struct SrcList_item *);
void sqlitexSrcListShiftJoinType(SrcList*);
/* COMDB2 MODIFICATION */
void sqlitexSrcListAssignCursors(Parse*, SrcList*, int);
void sqlitexIdListDelete(sqlitex*, IdList*);
void sqlitexSrcListDelete(sqlitex*, SrcList*);
Index *sqlitexAllocateIndexObject(sqlitex*,i16,int,char**);
Index *sqlitexCreateIndex(Parse*,Token*,Token*,SrcList*,ExprList*,int,Token*,
                          Expr*, int, int);
void sqlitexDropIndex(Parse*, SrcList*, int);
int sqlitexSelect(Parse*, Select*, SelectDest*);
Select *sqlitexSelectNew(Parse*,ExprList*,SrcList*,Expr*,ExprList*,
                         Expr*,ExprList*,u16,Expr*,Expr*);
/* COMDB2 MODIFICATION */
Select *sqlitexSelectNewv(Parse*,ExprList*,SrcList*,Expr*,ExprList*,
                         Expr*,ExprList*,u16,Expr*,Expr*);
void sqlitexSelectDelete(sqlitex*, Select*);
Table *sqlitexSrcListLookup(Parse*, SrcList*);
int sqlitexIsReadOnly(Parse*, Table*, int);
void sqlitexOpenTable(Parse*, int iCur, int iDb, Table*, int);
#if defined(SQLITE_ENABLE_UPDATE_DELETE_LIMIT) && !defined(SQLITE_OMIT_SUBQUERY)
Expr *sqlitexLimitWhere(Parse*,SrcList*,Expr*,ExprList*,Expr*,Expr*,char*);
#endif
void sqlitexDeleteFrom(Parse*, SrcList*, Expr*, ExprList*, Expr*, Expr*);
void sqlitexUpdate(Parse*, SrcList*, ExprList*, Expr*,int,ExprList*,Expr*,Expr*);
WhereInfo *sqlitexWhereBegin(Parse*,SrcList*,Expr*,ExprList*,ExprList*,u16,int);
void sqlitexWhereEnd(WhereInfo*);
u64 sqlitexWhereOutputRowCount(WhereInfo*);
int sqlitexWhereIsDistinct(WhereInfo*);
int sqlitexWhereIsOrdered(WhereInfo*);
int sqlitexWhereIsSorted(WhereInfo*);
int sqlitexWhereContinueLabel(WhereInfo*);
int sqlitexWhereBreakLabel(WhereInfo*);
int sqlitexWhereOkOnePass(WhereInfo*, int*);
#define ONEPASS_OFF      0        /* Use of ONEPASS not allowed */
#define ONEPASS_SINGLE   1        /* ONEPASS valid for a single row update */
#define ONEPASS_MULTI    2        /* ONEPASS is valid for multiple rows */
int sqlitexExprCodeGetColumn(Parse*, Table*, int, int, int, u8);
void sqlitexExprCodeGetColumnOfTable(Vdbe*, Table*, int, int, int);
void sqlitexExprCodeMove(Parse*, int, int, int);
void sqlitexExprCacheStore(Parse*, int, int, int);
void sqlitexExprCachePush(Parse*);
void sqlitexExprCachePop(Parse*);
void sqlitexExprCacheRemove(Parse*, int, int);
void sqlitexExprCacheClear(Parse*);
void sqlitexExprCacheAffinityChange(Parse*, int, int);
void sqlitexExprCode(Parse*, Expr*, int);
void sqlitexExprCodeFactorable(Parse*, Expr*, int);
void sqlitexExprCodeAtInit(Parse*, Expr*, int, u8);
int sqlitexExprCodeTemp(Parse*, Expr*, int*);
int sqlitexExprCodeTarget(Parse*, Expr*, int);
void sqlitexExprCodeAndCache(Parse*, Expr*, int);
int sqlitexExprCodeExprList(Parse*, ExprList*, int, u8);
#define SQLITE_ECEL_DUP      0x01  /* Deep, not shallow copies */
#define SQLITE_ECEL_FACTOR   0x02  /* Factor out constant terms */
void sqlitexExprIfTrue(Parse*, Expr*, int, int);
void sqlitexExprIfFalse(Parse*, Expr*, int, int);
Table *sqlitexFindTable(sqlitex*,const char*, const char*);
Table *sqlitexFindTableCheckOnly(sqlitex*,const char*, const char*);
Table *sqlitexFindTableByAnalysisLoad(sqlitex*,const char*, const char*);
Table *sqlitexLocateTable(Parse*,int isView,const char*, const char*);
Table *sqlitexLocateTableItem(Parse*,int isView,struct SrcList_item *);
Index *sqlitexFindIndex(sqlitex*,const char*, const char*);
void sqlitexUnlinkAndDeleteTable(sqlitex*,int,const char*);
void sqlitexUnlinkAndDeleteIndex(sqlitex*,int,const char*);
void sqlitexVacuum(Parse*);
int sqlitexRunVacuum(char**, sqlitex*);
char *sqlitexNameFromToken(sqlitex*, Token*);
int sqlitexExprCompare(Expr*, Expr*, int);
int sqlitexExprListCompare(ExprList*, ExprList*, int);
int sqlitexExprImpliesExpr(Expr*, Expr*, int);
void sqlitexExprAnalyzeAggregates(NameContext*, Expr*);
void sqlitexExprAnalyzeAggList(NameContext*,ExprList*);
int sqlitexFunctionUsesThisSrc(Expr*, SrcList*);
Vdbe *sqlitexGetVdbe(Parse*);
/* COMDB2 MODIFICATION */
void sqlitexCreateUpdCols(Vdbe *,sqlitex *db, int, int *);
int sqlitexPredicatedClearViews(sqlitex *db, 
      int (*predicated_delete)(const char *name, sqlitex *db, void *arg), void *arg);
void sqlitexPrngSaveState(void);
void sqlitexPrngRestoreState(void);
void sqlitexRollbackAll(sqlitex*,int);
void sqlitexCodeVerifySchema(Parse*, int);
void sqlitexCodeVerifyNamedSchema(Parse*, const char *zDb);
void sqlitexBeginTransaction(Parse*, int);
void sqlitexCommitTransaction(Parse*);
void sqlitexRollbackTransaction(Parse*);
void sqlitexSavepoint(Parse*, int, Token*);
void sqlitexCloseSavepoints(sqlitex *);
void sqlitexLeaveMutexAndCloseZombie(sqlitex*);
int sqlitexExprIsConstant(Expr*);
int sqlitexExprIsConstantNotJoin(Expr*);
int sqlitexExprIsConstantOrFunction(Expr*, u8);
int sqlitexExprIsTableConstant(Expr*,int);
#ifdef SQLITE_ENABLE_CURSOR_HINTS
int sqlitexExprContainsSubquery(Expr*);
#endif
int sqlitexExprIsInteger(Expr*, int*);
int sqlitexExprCanBeNull(const Expr*);
int sqlitexExprNeedsNoAffinityChange(const Expr*, char);
int sqlitexIsRowid(const char*);
void sqlitexGenerateRowDelete(
      Parse*,Table*,Trigger*,int,int,int,i16,u8,u8,u8, int);
void sqlitexGenerateRowIndexDelete(Parse*, Table*, int, int, int*, int);
int sqlitexGenerateIndexKey(Parse*, Index*, int, int, int, int*,Index*,int);
void sqlitexResolvePartIdxLabel(Parse*,int);
void sqlitexGenerateConstraintChecks(Parse*,Table*,int*,int,int,int,int,
                                     u8,u8,int,int*);
void sqlitexCompleteInsertion(Parse*,Table*,int,int,int,int*,int,int,int);
int sqlitexOpenTableAndIndices(Parse*, Table*, int, u8, int, u8*, int*, int*);
void sqlitexBeginWriteOperation(Parse*, int, int);
void sqlitexMultiWrite(Parse*);
void sqlitexMayAbort(Parse*);
void sqlitexHaltConstraint(Parse*, int, int, char*, i8, u8);
void sqlitexUniqueConstraint(Parse*, int, Index*);
void sqlitexRowidConstraint(Parse*, int, Table*);
Expr *sqlitexExprDup(sqlitex*,Expr*,int);
ExprList *sqlitexExprListDup(sqlitex*,ExprList*,int);
SrcList *sqlitexSrcListDup(sqlitex*,SrcList*,int);
IdList *sqlitexIdListDup(sqlitex*,IdList*);
Select *sqlitexSelectDup(sqlitex*,Select*,int);
#if SELECTTRACE_ENABLED
void sqlitexSelectSetName(Select*,const char*);
#else
# define sqlitexSelectSetName(A,B)
#endif
void sqlitexFuncDefInsert(FuncDefHash*, FuncDef*);
FuncDef *sqlitexFindFunction(sqlitex*,const char*,int,int,u8,u8);
void sqlitexRegisterBuiltinFunctions(sqlitex*);
void sqlite3RegisterDateTimeFunctions(void);
void sqlitexRegisterGlobalFunctions(void);
int sqlitexSafetyCheckOk(sqlitex*);
int sqlitexSafetyCheckSickOrOk(sqlitex*);
void sqlitexChangeCookie(Parse*, int);

#if !defined(SQLITE_OMIT_VIEW) && !defined(SQLITE_OMIT_TRIGGER)
void sqlitexMaterializeView(Parse*, Table*, Expr*, ExprList*,Expr*,Expr*,int);
#endif

#ifndef SQLITE_OMIT_TRIGGER
  void sqlitexBeginTrigger(Parse*, Token*,Token*,int,int,IdList*,SrcList*,
                           Expr*,int, int);
  void sqlitexFinishTrigger(Parse*, TriggerStep*, Token*);
  void sqlitexDropTrigger(Parse*, SrcList*, int);
  void sqlitexDropTriggerPtr(Parse*, Trigger*);
  Trigger *sqlitexTriggersExist(Parse *, Table*, int, ExprList*, int *pMask);
  Trigger *sqlitexTriggerList(Parse *, Table *);
  void sqlitexCodeRowTrigger(Parse*, Trigger *, int, ExprList*, int, Table *,
                            int, int, int);
  void sqlitexCodeRowTriggerDirect(Parse *, Trigger *, Table *, int, int, int);
  void sqliteViewTriggers(Parse*, Table*, Expr*, int, ExprList*);
  void sqlitexDeleteTriggerStep(sqlitex*, TriggerStep*);
  TriggerStep *sqlitexTriggerSelectStep(sqlitex*,Select*);
  TriggerStep *sqlitexTriggerInsertStep(sqlitex*,Token*, IdList*,
                                        Select*,u8);
  TriggerStep *sqlitexTriggerUpdateStep(sqlitex*,Token*,ExprList*, Expr*, u8);
  TriggerStep *sqlitexTriggerDeleteStep(sqlitex*,Token*, Expr*);
  void sqlitexDeleteTrigger(sqlitex*, Trigger*);
  void sqlitexUnlinkAndDeleteTrigger(sqlitex*,int,const char*);
  u32 sqlitexTriggerColmask(Parse*,Trigger*,ExprList*,int,int,Table*,int);
# define sqlitexParseToplevel(p) ((p)->pToplevel ? (p)->pToplevel : (p))
#else
# define sqlitexTriggersExist(B,C,D,E,F) 0
# define sqlitexDeleteTrigger(A,B)
# define sqlitexDropTriggerPtr(A,B)
# define sqlitexUnlinkAndDeleteTrigger(A,B,C)
# define sqlitexCodeRowTrigger(A,B,C,D,E,F,G,H,I)
# define sqlitexCodeRowTriggerDirect(A,B,C,D,E,F)
# define sqlitexTriggerList(X, Y) 0
# define sqlitexParseToplevel(p) p
# define sqlitexTriggerColmask(A,B,C,D,E,F,G) 0
#endif

int sqlitexJoinType(Parse*, Token*, Token*, Token*);
void sqlitexCreateForeignKey(Parse*, ExprList*, Token*, ExprList*, int);
void sqlitexDeferForeignKey(Parse*, int);
#ifndef SQLITE_OMIT_AUTHORIZATION
  void sqlitexAuthRead(Parse*,Expr*,Schema*,SrcList*);
  int sqlitexAuthCheck(Parse*,int, const char*, const char*, const char*);
  void sqlitexAuthContextPush(Parse*, AuthContext*, const char*);
  void sqlitexAuthContextPop(AuthContext*);
  int sqlitexAuthReadCol(Parse*, const char *, const char *, int);
#else
# define sqlitexAuthRead(a,b,c,d)
# define sqlitexAuthCheck(a,b,c,d,e)    SQLITE_OK
# define sqlitexAuthContextPush(a,b,c)
# define sqlitexAuthContextPop(a)  ((void)(a))
#endif
void sqlitexAttach(Parse*, Expr*, Expr*, Expr*);
void sqlitexDetach(Parse*, Expr*);
void sqlitexFixInit(DbFixer*, Parse*, int, const char*, const Token*);
int sqlitexFixSrcList(DbFixer*, SrcList*);
int sqlitexFixSelect(DbFixer*, Select*);
int sqlitexFixExpr(DbFixer*, Expr*);
int sqlitexFixExprList(DbFixer*, ExprList*);
int sqlitexFixTriggerStep(DbFixer*, TriggerStep*);
int sqlitexAtoF(const char *z, double*, int, u8);
int sqlitexGetInt32(const char *, int*);
int sqlitexAtoi(const char*);
int sqlitexUtf16ByteLen(const void *pData, int nChar);
int sqlitexUtf8CharLen(const char *pData, int nByte);
u32 sqlitexUtf8Read(const u8**);
LogEst sqlitexLogEst(u64);
LogEst sqlitexLogEstAdd(LogEst,LogEst);
//TODO: AZ: #ifndef SQLITE_OMIT_VIRTUALTABLE
LogEst sqlitexLogEstFromDouble(double);
//TODO: AZ: #endif
u64 sqlitexLogEstToInt(LogEst);

/*
** Routines to read and write variable-length integers.  These used to
** be defined locally, but now we use the varint routines in the util.c
** file.
*/
int sqlitexPutVarint(unsigned char*, u64);
u8 sqlitexGetVarint(const unsigned char *, u64 *);
u8 sqlitexGetVarint32(const unsigned char *, u32 *);
int sqlitexVarintLen(u64 v);

/*
** The common case is for a varint to be a single byte.  They following
** macros handle the common case without a procedure call, but then call
** the procedure for larger varints.
*/
#define getVarint32(A,B)  \
  (u8)((*(A)<(u8)0x80)?((B)=(u32)*(A)),1:sqlitexGetVarint32((A),(u32 *)&(B)))
#define putVarint32(A,B)  \
  (u8)(((u32)(B)<(u32)0x80)?(*(A)=(unsigned char)(B)),1:\
  sqlitexPutVarint((A),(B)))
#define getVarint    sqlitexGetVarint
#define putVarint    sqlitexPutVarint


const char *sqlitexIndexAffinityStr(Vdbe *, Index *);
void sqlitexTableAffinity(Vdbe*, Table*, int);
char sqlitexCompareAffinity(Expr *pExpr, char aff2);
int sqlitexIndexAffinityOk(Expr *pExpr, char idx_affinity);
char sqlitexExprAffinity(Expr *pExpr);
int sqlitexAtoi64(const char*, i64*, int, u8);
int sqlitexDecOrHexToI64(const char*, i64*);
void sqlitexErrorWithMsg(sqlitex*, int, const char*,...);
void sqlitexError(sqlitex*,int);
void *sqlitexHexToBlob(sqlitex*, const char *z, int n);
u8 sqlitexHexToInt(int h);
int sqlitexTwoPartName(Parse *, Token *, Token *, Token **);

#if defined(SQLITE_TEST) 
const char *sqlitexErrName(int);
#endif

const char *sqlitexErrStr(int);
int sqlitexReadSchema(Parse *pParse);
CollSeq *sqlitexFindCollSeq(sqlitex*,u8 enc, const char*,int);
CollSeq *sqlitexLocateCollSeq(Parse *pParse, const char*zName);
CollSeq *sqlitexExprCollSeq(Parse *pParse, Expr *pExpr);
Expr *sqlitexExprAddCollateToken(Parse *pParse, Expr*, const Token*, int);
Expr *sqlitexExprAddCollateString(Parse*,Expr*,const char*);
Expr *sqlitexExprSkipCollate(Expr*);
int sqlitexCheckCollSeq(Parse *, CollSeq *);
int sqlitexCheckObjectName(Parse *, const char *);
void sqlitexVdbeSetChanges(sqlitex *, int);
int sqlitexAddInt64(i64*,i64);
int sqlitexSubInt64(i64*,i64);
int sqlitexMulInt64(i64*,i64);
int sqlitexAbsInt32(int);
#ifdef SQLITE_ENABLE_8_3_NAMES
void sqlitexFileSuffix3(const char*, char*);
#else
# define sqlitexFileSuffix3(X,Y)
#endif
u8 sqlitexGetBoolean(const char *z,u8);

const void *sqlitexValueText(sqlitex_value*, u8);
int sqlitexValueBytes(sqlitex_value*, u8);
void sqlitexValueSetStr(sqlitex_value*, int, const void *,u8, 
                        void(*)(void*));
void sqlitexValueSetNull(sqlitex_value*);
void sqlitexValueFree(sqlitex_value*);
sqlitex_value *sqlitexValueNew(sqlitex *);
char *sqlitexUtf16to8(sqlitex *, const void*, int, u8);
int sqlitexValueFromExpr(sqlitex *, Expr *, u8, u8, sqlitex_value **);
void sqlitexValueApplyAffinity(sqlitex_value *, u8, u8);
#ifndef SQLITE_AMALGAMATION
extern const unsigned char sqlitexOpcodeProperty[];
extern const unsigned char sqlitexUpperToLower[];
extern const unsigned char sqlitexCtypeMap[];
extern const Token sqlitexIntTokens[];
extern SQLITE_WSD struct SqlitexConfig sqlitexConfig;
extern SQLITE_WSD FuncDefHash sqlitexGlobalFunctions;
#ifndef SQLITE_OMIT_WSD
extern int sqlitexPendingByte;
#endif
#endif
void sqlitexRootPageMoved(sqlitex*, int, int, int);
void sqlitexReindex(Parse*, Token*, Token*);
void sqlitexAlterFunctions(void);
void sqlitexAlterRenameTable(Parse*, SrcList*, Token*);
int sqlitexGetToken(const unsigned char *, int *);
void sqlitexNestedParse(Parse*, const char*, ...);
void sqlitexExpirePreparedStatements(sqlitex*);
int sqlitexCodeSubselect(Parse *, Expr *, int, int);
void sqlitexSelectPrep(Parse*, Select*, NameContext*);
int sqlitexMatchSpanName(const char*, const char*, const char*, const char*);
int sqlitexResolveExprNames(NameContext*, Expr*);
int sqlitexResolveExprListNames(NameContext*, ExprList*);
void sqlitexResolveSelectNames(Parse*, Select*, NameContext*);
void sqlitexResolveSelfReference(Parse*,Table*,int,Expr*,ExprList*);
int sqlitexResolveOrderGroupBy(Parse*, Select*, ExprList*, const char*);
void sqlitexColumnDefault(Vdbe *, Table *, int, int);
void sqlitexAlterFinishAddColumn(Parse *, Token *);
void sqlitexAlterBeginAddColumn(Parse *, SrcList *);
CollSeq *sqlitexGetCollSeq(Parse*, u8, CollSeq *, const char*);
char sqlitexAffinityType(const char*, u8*);
void sqlitexAnalyze(Parse*, Token*, Token*);
int sqlitexInvokeBusyHandler(BusyHandler*);
int sqlitexFindDb(sqlitex*, Token*);
int sqlitexFindDbName(sqlitex *, const char *);
int sqlitexAnalysisLoad(sqlitex*,int iDB);
void sqlitexDeleteIndexSamples(sqlitex*,Index*);
void sqlitexDefaultRowEst(Index*);
void sqlitexRegisterLikeFunctions(sqlitex*, int);
int sqlitexIsLikeFunction(sqlitex*,Expr*,int*,char*);
void sqlitexMinimumFileFormat(Parse*, int, int);
void sqlitexSchemaClear(void *);
void sqlitexSchemaClearByName(void *,const char *);
Schema *sqlitexSchemaGet(sqlitex *, Btree *);
int sqlitexSchemaToIndex(sqlitex *db, Schema *);
KeyInfo *sqlitexKeyInfoAlloc(sqlitex*,int,int);
void sqlitexKeyInfoUnref(KeyInfo*);
KeyInfo *sqlitexKeyInfoRef(KeyInfo*);
KeyInfo *sqlitexKeyInfoOfIndex(Parse*, Index*);
#ifdef SQLITE_DEBUG
int sqlitexKeyInfoIsWriteable(KeyInfo*);
#endif
int sqlitexCreateFunc(sqlitex *, const char *, int, int, void *, 
  void (*)(sqlitex_context*,int,sqlitex_value **),
  void (*)(sqlitex_context*,int,sqlitex_value **), void (*)(sqlitex_context*),
  FuncDestructor *pDestructor
);
int sqlitexApiExit(sqlitex *db, int);
int sqlitexOpenTempDatabase(Parse *);

/* COMDB2 MODIFICATION */
void register_date_functionsX(sqlitex * db);

void sqlitexStrAccumInit(StrAccum*, char*, int, int);
void sqlitexStrAccumAppend(StrAccum*,const char*,int);
void sqlitexStrAccumAppendAll(StrAccum*,const char*);
void sqlitexAppendChar(StrAccum*,int,char);
char *sqlitexStrAccumFinish(StrAccum*);
void sqlitexStrAccumReset(StrAccum*);
void sqlitexSelectDestInit(SelectDest*,int,int);
Expr *sqlitexCreateColumnExpr(sqlitex *, SrcList *, int, int);

void sqlitexBackupRestart(sqlitex_backup *);
void sqlitexBackupUpdate(sqlitex_backup *, Pgno, const u8 *);

#ifdef SQLITE_ENABLE_STAT3_OR_STAT4
void sqlitexAnalyzeFunctions(void);
int sqlitexStat4ProbeSetValue(Parse*,Index*,UnpackedRecord**,Expr*,u8,int,int*);
int sqlitexStat4ValueFromExpr(Parse*, Expr*, u8, sqlitex_value**);
void sqlitexStat4ProbeFree(UnpackedRecord*);
int sqlitexStat4Column(sqlitex*, const void*, int, int, sqlitex_value**);
#endif

/*
** The interface to the LEMON-generated parser
*/
void *sqlitexParserAlloc(void*(*)(u64));
void sqlitexParserFree(void*, void(*)(void*));
void sqlitexParser(void*, int, Token, Parse*);
#ifdef YYTRACKMAXSTACKDEPTH
  int sqlitexParserStackPeak(void*);
#endif

void sqlitexAutoLoadExtensions(sqlitex*);
#ifndef SQLITE_OMIT_LOAD_EXTENSION
  void sqlitexCloseExtensions(sqlitex*);
#else
# define sqlitexCloseExtensions(X)
#endif

#ifndef SQLITE_OMIT_SHARED_CACHE
  void sqlitexTableLock(Parse *, int, int, u8, const char *);
#else
  #define sqlitexTableLock(v,w,x,y,z)
#endif

#ifdef SQLITE_TEST
  int sqlitexUtf8To8(unsigned char*);
#endif

#ifdef SQLITE_OMIT_VIRTUALTABLE
#  define sqlitexVtabClear(Y)
#  define sqlitexVtabSync(X,Y) SQLITE_OK
#  define sqlitexVtabRollback(X)
#  define sqlitexVtabCommit(X)
#  define sqlitexVtabInSync(db) 0
#  define sqlitexVtabLock(X) 
#  define sqlitexVtabUnlock(X)
#  define sqlitexVtabUnlockList(X)
#  define sqlitexVtabSavepoint(X, Y, Z) SQLITE_OK
#  define sqlitexGetVTable(X,Y)  ((VTable*)0)
#else
   void sqlitexVtabClear(sqlitex *db, Table*);
   void sqlitexVtabDisconnect(sqlitex *db, Table *p);
   int sqlitexVtabSync(sqlitex *db, Vdbe*);
   int sqlitexVtabRollback(sqlitex *db);
   int sqlitexVtabCommit(sqlitex *db);
   void sqlitexVtabLock(VTable *);
   void sqlitexVtabUnlock(VTable *);
   void sqlitexVtabUnlockList(sqlitex*);
   int sqlitexVtabSavepoint(sqlitex *, int, int);
   void sqlitexVtabImportErrmsg(Vdbe*, sqlitex_vtab*);
   VTable *sqlitexGetVTable(sqlitex*, Table*);
#  define sqlitexVtabInSync(db) ((db)->nVTrans>0 && (db)->aVTrans==0)
#endif
int sqlitexVtabEponymousTableInit(Parse*,Module*);
void sqlitexVtabEponymousTableClear(sqlitex*,Module*);
void sqlitexVtabMakeWritable(Parse*,Table*);
void sqlitexVtabBeginParse(Parse*, Token*, Token*, Token*, int);
void sqlitexVtabFinishParse(Parse*, Token*);
void sqlitexVtabArgInit(Parse*);
void sqlitexVtabArgExtend(Parse*, Token*);
int sqlitexVtabCallCreate(sqlitex*, int, const char *, char **);
int sqlitexVtabCallConnect(Parse*, Table*);
int sqlitexVtabCallDestroy(sqlitex*, int, const char *);
int sqlitexVtabBegin(sqlitex *, VTable *);
FuncDef *sqlitexVtabOverloadFunction(sqlitex *,FuncDef*, int nArg, Expr*);
void sqlitexInvalidFunction(sqlitex_context*,int,sqlitex_value**);
sqlitex_int64 sqlitexStmtCurrentTime(sqlitex_context*);
int sqlitexVdbeParameterIndex(Vdbe*, const char*, int);
int sqlitexTransferBindings(sqlitex_stmt *, sqlitex_stmt *);
void sqlitexParserReset(Parse*);
#ifdef SQLITE_ENABLE_NORMALIZE
char *sqlitexNormalize(Vdbe*, const char*);
#endif
int sqlitexReprepare(Vdbe*);
void sqlitexExprListCheckLength(Parse*, ExprList*, const char*);
CollSeq *sqlitexBinaryCompareCollSeq(Parse *, Expr *, Expr *);
int sqlitexTempInMemory(const sqlitex*);
const char *sqlitexJournalModename(int);
#ifndef SQLITE_OMIT_WAL
  int sqlitexCheckpoint(sqlitex*, int, int, int*, int*);
  int sqlitexWalDefaultHook(void*,sqlitex*,const char*,int);
#endif
#ifndef SQLITE_OMIT_CTE
  With *sqlitexWithAdd(Parse*,With*,Token*,ExprList*,Select*);
  void sqlitexWithDelete(sqlitex*,With*);
  void sqlitexWithPush(Parse*, With*, u8);
#else
#define sqlitexWithPush(x,y,z)
#define sqlitexWithDelete(x,y)
#endif

/* Declarations for functions in fkey.c. All of these are replaced by
** no-op macros if OMIT_FOREIGN_KEY is defined. In this case no foreign
** key functionality is available. If OMIT_TRIGGER is defined but
** OMIT_FOREIGN_KEY is not, only some of the functions are no-oped. In
** this case foreign keys are parsed, but no other functionality is 
** provided (enforcement of FK constraints requires the triggers sub-system).
*/
#if !defined(SQLITE_OMIT_FOREIGN_KEY) && !defined(SQLITE_OMIT_TRIGGER)
  void sqlitexFkCheck(Parse*, Table*, int, int, int*, int);
  void sqlitexFkDropTable(Parse*, SrcList *, Table*);
  void sqlitexFkActions(Parse*, Table*, ExprList*, int, int*, int);
  int sqlitexFkRequired(Parse*, Table*, int*, int);
  u32 sqlitexFkOldmask(Parse*, Table*);
  FKey *sqlitexFkReferences(Table *);
#else
  #define sqlitexFkActions(a,b,c,d,e,f)
  #define sqlitexFkCheck(a,b,c,d,e,f)
  #define sqlitexFkDropTable(a,b,c)
  #define sqlitexFkOldmask(a,b)         0
  #define sqlitexFkRequired(a,b,c,d)    0
#endif
#ifndef SQLITE_OMIT_FOREIGN_KEY
  void sqlitexFkDelete(sqlitex *, Table*);
  int sqlitexFkLocateIndex(Parse*,Table*,FKey*,Index**,int**);
#else
  #define sqlitexFkDelete(a,b)
  #define sqlitexFkLocateIndex(a,b,c,d,e)
#endif


/*
** Available fault injectors.  Should be numbered beginning with 0.
*/
#define SQLITE_FAULTINJECTOR_MALLOC     0
#define SQLITE_FAULTINJECTOR_COUNT      1

/*
** The interface to the code in fault.c used for identifying "benign"
** malloc failures. This is only present if SQLITE_OMIT_BUILTIN_TEST
** is not defined.
*/
#ifndef SQLITE_OMIT_BUILTIN_TEST
  void sqlitexBeginBenignMalloc(void);
  void sqlitexEndBenignMalloc(void);
#else
  #define sqlitexBeginBenignMalloc()
  #define sqlitexEndBenignMalloc()
#endif

/*
** Allowed return values from sqlitexFindInIndex()
*/
#define IN_INDEX_ROWID        1   /* Search the rowid of the table */
#define IN_INDEX_EPH          2   /* Search an ephemeral b-tree */
#define IN_INDEX_INDEX_ASC    3   /* Existing index ASCENDING */
#define IN_INDEX_INDEX_DESC   4   /* Existing index DESCENDING */
#define IN_INDEX_NOOP         5   /* No table available. Use comparisons */
/*
** Allowed flags for the 3rd parameter to sqlitexFindInIndex().
*/
#define IN_INDEX_NOOP_OK     0x0001  /* OK to return IN_INDEX_NOOP */
#define IN_INDEX_MEMBERSHIP  0x0002  /* IN operator used for membership test */
#define IN_INDEX_LOOP        0x0004  /* IN operator used as a loop */
int sqlitexFindInIndex(Parse *, Expr *, u32, int*);

#ifdef SQLITE_ENABLE_ATOMIC_WRITE
  int sqlitexJournalOpen(sqlitex_vfs *, const char *, sqlitex_file *, int, int);
  int sqlitexJournalSize(sqlitex_vfs *);
  int sqlitexJournalCreate(sqlitex_file *);
  int sqlitexJournalExists(sqlitex_file *p);
#else
  #define sqlitexJournalSize(pVfs) ((pVfs)->szOsFile)
  #define sqlitexJournalExists(p) 1
#endif

void sqlitexMemJournalOpen(sqlitex_file *);
int sqlitexMemJournalSize(void);
int sqlitexIsMemJournal(sqlitex_file *);

void sqlitexExprSetHeightAndFlags(Parse *pParse, Expr *p);
#if SQLITE_MAX_EXPR_DEPTH>0
  int sqlitexSelectExprHeight(Select *);
  int sqlitexExprCheckHeight(Parse*, int);
#else
  #define sqlitexSelectExprHeight(x) 0
  #define sqlitexExprCheckHeight(x,y)
#endif

u32 sqlitexGet4byte(const u8*);
void sqlitexPut4byte(u8*, u32);

#ifdef SQLITE_ENABLE_UNLOCK_NOTIFY
  void sqlitexConnectionBlocked(sqlitex *, sqlitex *);
  void sqlitexConnectionUnlocked(sqlitex *db);
  void sqlitexConnectionClosed(sqlitex *db);
#else
  #define sqlitexConnectionBlocked(x,y)
  #define sqlitexConnectionUnlocked(x)
  #define sqlitexConnectionClosed(x)
#endif

#ifdef SQLITE_DEBUG
  void sqlitexParserTrace(FILE*, char *);
#endif

/*
** If the SQLITE_ENABLE IOTRACE exists then the global variable
** sqlitexIoTrace is a pointer to a printf-like routine used to
** print I/O tracing messages. 
*/
#ifdef SQLITE_ENABLE_IOTRACE
# define IOTRACE(A)  if( sqlitexIoTrace ){ sqlitexIoTrace A; }
  void sqlitexVdbeIOTraceSql(Vdbe*);
SQLITE_EXTERN void (*sqlitexIoTrace)(const char*,...);
#else
# define IOTRACE(A)
# define sqlitexVdbeIOTraceSql(X)
#endif

/*
** These routines are available for the mem2.c debugging memory allocator
** only.  They are used to verify that different "types" of memory
** allocations are properly tracked by the system.
**
** sqlitexMemdebugSetType() sets the "type" of an allocation to one of
** the MEMTYPE_* macros defined below.  The type must be a bitmask with
** a single bit set.
**
** sqlitexMemdebugHasType() returns true if any of the bits in its second
** argument match the type set by the previous sqlitexMemdebugSetType().
** sqlitexMemdebugHasType() is intended for use inside assert() statements.
**
** sqlitexMemdebugNoType() returns true if none of the bits in its second
** argument match the type set by the previous sqlitexMemdebugSetType().
**
** Perhaps the most important point is the difference between MEMTYPE_HEAP
** and MEMTYPE_LOOKASIDE.  If an allocation is MEMTYPE_LOOKASIDE, that means
** it might have been allocated by lookaside, except the allocation was
** too large or lookaside was already full.  It is important to verify
** that allocations that might have been satisfied by lookaside are not
** passed back to non-lookaside free() routines.  Asserts such as the
** example above are placed on the non-lookaside free() routines to verify
** this constraint. 
**
** All of this is no-op for a production build.  It only comes into
** play when the SQLITE_MEMDEBUG compile-time option is used.
*/
#ifdef SQLITE_MEMDEBUG
  void sqlitexMemdebugSetType(void*,u8);
  int sqlitexMemdebugHasType(void*,u8);
  int sqlitexMemdebugNoType(void*,u8);
#else
# define sqlitexMemdebugSetType(X,Y)  /* no-op */
# define sqlitexMemdebugHasType(X,Y)  1
# define sqlitexMemdebugNoType(X,Y)   1
#endif
#define MEMTYPE_HEAP       0x01  /* General heap allocations */
#define MEMTYPE_LOOKASIDE  0x02  /* Heap that might have been lookaside */
#define MEMTYPE_SCRATCH    0x04  /* Scratch allocations */
#define MEMTYPE_PCACHE     0x08  /* Page cache allocations */

/*
** Threading interface
*/
#if SQLITE_MAX_WORKER_THREADS>0
int sqlitexThreadCreate(SQLiteThread**,void*(*)(void*),void*);
int sqlitexThreadJoin(SQLiteThread*, void**);
#endif

/* COMDB2 MODIFICATION */
enum {
    SQLITE_ATTR_QUANTITY   = 1,
    SQLITE_ATTR_BOOLEAN    = 2
};

enum {
#define DEF_ATTR(NAME, name, type, dflt) NAME,
#include "sqlite_tunables.h"
    SQLITE_ATTR_MAX
};
#undef DEF_ATTR

struct sqlitex_tunables_type {
#define DEF_ATTR(NAME, name, type, dflt) int name;
#include "sqlite_tunables.h"
};
#undef DEF_ATTR

extern struct sqlitex_tunables_type sqlitex_gbl_tunables;
void sqlitex_tunables_init(void);
void sqlitex_dump_tunables(void);
void sqlitex_set_tunable_by_name(char *tname, char *val);

/* COMDB2 MODIFICATION */
extern int sqlite3AddAndLockTable(sqlitex *db, const char *dbname, const char *table,
      int *version, int in_analysis_load);
extern int sqlite3UnlockTable(const char *dbname, const char *table);
extern int comdb2_dynamic_attachXX(sqlitex *db, sqlitex_context *context, sqlitex_value **argv,
      const char *zName, const char *zFile, char **pzErrDyn, int version);
extern int comdb2_fdb_check_class(const char *dbname);
int sqlitexInitTable(sqlitex *db, char **pzErrMsg, const char *zName);
extern int sqlite3UpdateMemCollAttr(BtCursor *pCur, int idx, Mem *mem);
char* sqlitexExprDescribe(Vdbe *v, const Expr *pExpr);
char* sqlitexExprDescribeAtRuntime(Vdbe *v, const Expr *pExpr);
char *sqlitexDescribeIndexOrder(sqlitex *db, 
      const char *zName, const char *zDb, 
      Mem *m, int nfields, int *hasCondition,
      char **columns,
      int op,
      int is_equality,
      unsigned long long colMask);

#if defined(SQLITE_ENABLE_DBSTAT_VTAB) || defined(SQLITE_TEST)
int sqlitexDbstatRegister(sqlitex*);
#endif

#ifndef _SQL_H_
#include <mem_sqlite.h>
#include <mem_override.h>
#endif

struct Cdb2TrigEvent {
  int op;
  IdList *cols;
};
struct Cdb2TrigEvents {
  Cdb2TrigEvent del;
  Cdb2TrigEvent ins;
  Cdb2TrigEvent upd;
};
struct Cdb2TrigTables {
  Table *table;
  Cdb2TrigEvents *events;
  Cdb2TrigTables *next;
};
Cdb2TrigEvents *comdb2AddTriggerEventX(Parse*,Cdb2TrigEvents*,Cdb2TrigEvent*);
void comdb2DropTriggerX(Parse*,Token*);
Cdb2TrigTables *comdb2AddTriggerTableX(Parse*,Cdb2TrigTables*,SrcList*,Cdb2TrigEvents*);
void comdb2CreateTriggerX(Parse*,int dynamic,Token*,Cdb2TrigTables*);

void comdb2CreateScalarFuncX(Parse *, Token *);
void comdb2DropScalarFuncX(Parse *, Token *);
void comdb2CreateAggFuncX(Parse *, Token *);
void comdb2DropAggFuncX(Parse *, Token *);

/* Query fingerprints. */ 
void sqlitexFingerprintSelect(sqlitex_stmt *stmt, Select *p);
void sqlitexFingerprintDelete(sqlitex_stmt *stmt, SrcList *pTabList, Expr *pWhere);
void sqlitexFingerprintInsert(sqlitex_stmt *stmt, SrcList *, Select *, IdList *, With *, ExprList *pList);
void sqlitexFingerprintUpdate(sqlitex_stmt *stmt, SrcList *pTabList, ExprList *pChanges, Expr *pWhere, int onError);

#endif /* _SQLITEINT_H_ */
