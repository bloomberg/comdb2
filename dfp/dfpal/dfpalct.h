/* ------------------------------------------------------------------ */
/* Decimal Floating Point Abstraction Layer (DFPAL)                   */
/* dfpalct.h                                                          */
/* ------------------------------------------------------------------ */
/* (c) Copyright IBM Corporation, 2007.  All rights reserved.         */
/*                                                                    */
/* This software is made available under the terms of the             */
/* ICU License -- ICU 1.8.1 and later.                                */
/*                                                                    */
/* The description and User's Guide ("The DFPAL C Library") for       */
/* this software is called dfpalugaio.html.  This document is         */
/* included in this package and also available at:                    */
/* http://www2.hursley.ibm.com/decimal                                */
/*                                                                    */
/* Author:                                                            */
/*   Punit Shah (punit@us.ibm.com)                                    */
/*                                                                    */
/* Please send comments, suggestions, and corrections to the          */
/* the following email address:                                       */
/*   dfpal-l@austin.ibm.com                                           */
/*                                                                    */
/* Major contribution:                                                */
/*   Ian McIntosh (ianm@ca.ibm.com)                                   */
/*   Calvin Sze (calvins@us.ibm.com)                                  */
/* ------------------------------------------------------------------ */
#if !defined(__DFPALCT_H__)
  #define __DFPALCT_H__

  #if defined(DFPAL_OS_LOP)
    #if defined(__xlc__) || defined(__xlC__)
      #define DFPAL_OS_LOP_XLC 1
    #elif defined(__GNUC__)
      #define DFPAL_OS_LOP_GCC 1
    #else
      #error Compiler not supported to enable hardware DFP. Use -DDFPAL_NO_HW_DFP flag
    #endif
  #endif

  /* system includes */
  #if !defined(DFPAL_OS_WINDOWS)
  #include <pthread.h>
  #endif
  #include <stdio.h>

  /* decNumber package includes */
  #include "decimal128.h"
  #include "decimal64.h"
  #include "decimal32.h"
  #include "decContext.h"
  #include "decPacked.h"
  #if defined (DFPAL_USE_DECFLOAT)
    #include "decQuad.h"
    #include "decDouble.h"
    #include "decSingle.h"
  #endif

  #include "dfpaltypes.h"
  #include "dfpal.h"
  #include "ppcdfp.h"

  #if !defined (DECNUMBERLOC)
    #define Flag   uint8_t
    #define uByte  uint8_t
    #define Short  int16_t
    #define uShort uint16_t
    #define Int    int32_t
    #define uInt   uint32_t
    #define Long   int64_t
    #define uLong  uint64_t
  #endif /* #if !defined (DECNUMBERLOC) */

  #define DFPAL_VERSION "2.20"

  #define DEBUG_FNAME_LEN 255
  #define DEBUG_ENV "DFPAL_DEBUG"

  #define EXE_MODE_ENV "DFPAL_EXE_MODE"
  #define EXE_MODE_HW "PPCHW"
  #define EXE_MODE_SW "DNSW"

  #if defined(DFPAL_INTEGER64_LITERAL_LL)
    #define DFPAL_S64(a) a##LL
    #define DFPAL_U64(a) a##ULL
  #elif defined(DFPAL_INTEGER64_LITERAL_L)
    #define DFPAL_S64(a) a##L
    #define DFPAL_U64(a) a##UL
  #elif defined(DFPAL_INTEGER64_LITERAL_i64)
    #define DFPAL_S64(a) a##i64
    #define DFPAL_U64(a) a##ui64
  #else  /* default literal specifier */
    #define DFPAL_S64(a) a##LL
    #define DFPAL_U64(a) a##ULL
  #endif 
   
  #define DFPAL_INT32_MAX (2147483647)
  #define DFPAL_INT32_MIN (-DFPAL_INT32_MAX - 1)

  #define DFPAL_UINT32_MAX (4294967295U)
  #define DFPAL_UINT32_MIN (0U)

  #define DFPAL_INT64_MAX DFPAL_S64(9223372036854775807)
  #define DFPAL_INT64_MIN (-DFPAL_INT64_MAX - DFPAL_S64(1))

  #define DFPAL_UINT64_MAX DFPAL_U64(18446744073709551615)
  #define DFPAL_UINT64_MIN DFPAL_U64(0)

  /* Process context. To be initialized using pthread_once() or equivalent */
  typedef struct {
    Int initDfpalErrNum;              /* used for DFPAL library init error */
    Int initOsErrNum;                 /* used for DFPAL library init error */
    #if !defined(DFPAL_OS_WINDOWS)
      pthread_key_t dfpalThreadMemKey;/* for thread local storage */
    #endif
    Flag dfpInHw;                     /* ture, if DFP hw acceleration */
                                      /* is available */
    enum dfpalExeMode dfpUserMode;    /* user preference, default is AUTO */
    enum dfpalExeMode dfpRealMode;    /* best possible execution mode */
    Flag dfpDebug;                    /* debug mode, default is off */
    char dbgFile[DEBUG_FNAME_LEN+1];  /* debug output file name */
    FILE *dbgOut;                     /* debug file stream */
    } dfpalCmnContext;

  /* Thread context */
  #define ERR_MSG_SIZE 255
  typedef struct {
    char *dfpalErrorText;          /* DFPAL descriptive error text */
    Int dfpalErrNum;               /* DFPAL error number, def: depalerr.h */
    Int osErrorNum;                /* operating system error number */
    const dfpalCmnContext *dfpalProcContext;
                                   /* common context, read-only */
    decContext dn64Context;        /* decNumber context, if DNSW is used */
    decContext dn128Context;       /* decNumber context, if DNSW is used */
  } dfpalThreadContext;

  /* Conversion (decimalXX <-> dfp_XXXX) unions */
  typedef union {
    decimal32 dsw;
    dfp_single dhw;
    } dec32DataXchg;
  
  typedef union {
    decimal64 dsw;
    dfp_double dhw;
    uByte bcd[8];
    } dec64DataXchg;
  
  typedef union {
    decimal128 dsw;
    dfp_quad dhw;
    uByte bcd[16];
    } dec128DataXchg;

  extern dfpalCmnContext globalContext;

  #if defined(DFPAL_THREAD_SAFE)
    #if defined(DFPAL_OS_WINDOWS)
      #define Run_Once(gateKeeper,funcPtr)
      #define Tls_Store(data,ret) {specificContext=(data); (ret)=0;}
      #define Tls_Load()  (specificContext)
    #else /* } { #if defined(DFPAL_OS_WINDOWS) */
      #define Run_Once(gateKeeper,funcPtr) pthread_once((gateKeeper), funcPtr)
  
      #define Errorcheck_Mutex_Init(lock,ret)                                  \
      {                                                                        \
        pthread_mutexattr_t mattr;                                             \
        ret=DFPAL_ERR_NO_ERROR;                                                \
                                                                               \
        if (ret=pthread_mutexattr_init(&mattr)) {                              \
          Set_Init_Error(DFPAL_ERR_MUTEX_ATTR_INIT, ret);                      \
          ret=DFPAL_ERR_MUTEX_ATTR_INIT;                                       \
        }                                                                      \
        else { /* successfully initialized attr */                             \
          if (ret=pthread_mutexattr_settype (&mattr,PTHREAD_MUTEX_ERRORCHECK)){\
            Set_Init_Error(DFPAL_ERR_MUTEX_TYPE, ret);                         \
            ret=DFPAL_ERR_MUTEX_TYPE;                                          \
          }                                                                    \
          else {  /* successfully set desired mutex type */                    \
            if (ret=pthread_mutex_init((lock), &mattr)) {                      \
              Set_Init_Error(DFPAL_ERR_MUTEX_INIT, ret);                       \
              ret=DFPAL_ERR_MUTEX_INIT;                                        \
            }                                                                  \
          }                                                                    \
        }                                                                      \
      }
        
      #define Mutex_Lock(lock) 
      #define Mutex_Unlock(lock)
  
      #define Tls_Store(data,ret) \
        (ret)=pthread_setspecific(globalContext.dfpalThreadMemKey,(data))
      #define Tls_Load() \
        ((dfpalThreadContext *) \
        pthread_getspecific(globalContext.dfpalThreadMemKey))
      #endif /* } #if defined(DFPAL_OS_WINDOWS) */
  #else /* } { #if defined(DFPAL_THREAD_SAFE) */
    #define Run_Once(gateKeeper,funcPtr) funcPtr()
    #define Errorcheck_Mutex_Init(lock,ret)
    #define Mutex_Lock(lock) 
    #define Mutex_Unlock(lock)
    
    #define Tls_Store(data,ret) {specificContext=(data); (ret)=0;}
    #define Tls_Load() (specificContext)
  #endif /* #if defined(DFPAL_THREAD_SAFE) */
  
  #if defined(DFPAL_THREAD_SAFE)
    #if defined(DFPAL_OS_WINDOWS) 
      extern __declspec( thread ) dfpalThreadContext *specificContext;
    #else
      extern pthread_once_t onceGlobalContext;
    #endif 
  #else /* #if defined(DFPAL_THREAD_SAFE) */
    extern dfpalThreadContext *specificContext;
  #endif /* #if defined(DFPAL_THREAD_SAFE) */
#endif /* #if !defined(__DFPALCT_H__) */
