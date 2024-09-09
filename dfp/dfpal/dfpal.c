/* ------------------------------------------------------------------ */
/* Decimal Floating Point Abstraction Layer (DFPAL)                   */
/* dfpal.c                                                            */
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
/* The pthread.h file header file must be the first included file */
/* of each source file using the threads library. Otherwise, the  */
/* -D_THREAD_SAFE compilation flag should be used, or the cc_r    */
/* compiler used. In this case, the flag is automatically set.    */

#if defined(DFPAL_OS_WINDOWS) /* { */
  #define _MT              /* must be defined before any headers */
  #include <windows.h>
  #define DFPAL_SNPRINTF _snprintf
#else /* } { */
  #include <pthread.h>     /* must be first file for PTHREAD_ONCE_INIT */
  #define DFPAL_SNPRINTF snprintf
#endif /* } #if defined(DFPAL_OS_WINDOWS) */

#include <math.h>              /* HUGE_VAL */                        
#include <string.h>            /* strncpy() */
#include <stdlib.h>            /* getenv() */
#include <errno.h>             /* errno */

#define __DFPAL_C__

#include "dfpalct.h"
#include "dfpal.h"
#include "ppcdfp.h"

#if defined(DFPAL_OS_AIX5L) || defined(DFPAL_OS_I5OS)
  #include <dlfcn.h>           /* dlopen(), dlsym(), dlclose() */
  #include <sys/systemcfg.h>   /* _system_configuration */
  #define DFPAL_FP_INVALID_RC  ((dfpalflag_t)(FP_INV_SNAN|FP_INV_ISI|      \
                               FP_INV_IDI|FP_INV_ZDZ|FP_INV_IMZ|FP_INV_CMP|\
                               FP_INV_SQRT|FP_INV_CVI|FP_INV_VXSOFT))
  #define DFPAL_FP_INVALID_VXSOFT  ((dfpalflag_t)FP_INV_VXSOFT)
  #define DFPAL_FP_PPC_CLEAR_ALL   ((dfpalflag_t)(~((DFPAL_FP_INVALID_RC)| \
                                   FP_INVALID|FP_OVERFLOW|FP_UNDERFLOW|    \
                                   FP_DIV_BY_ZERO|FP_INEXACT)))
#elif defined(DFPAL_OS_LOP)
  #include <stdio.h>
  #include <unistd.h>
  #include <elf.h>
  #include <link.h>
  #include <asm/elf.h>
  #include <bits/dlfcn.h>
  #define DFPAL_FP_INVALID_RC  ((dfpalflag_t)(FP_INV_SNAN|FP_INV_ISI|      \
                               FP_INV_IDI|FP_INV_ZDZ|FP_INV_IMZ|FP_INV_CMP|\
                               FP_INV_SQRT|FP_INV_CVI|FP_INV_VXSOFT))
  #define DFPAL_FP_INVALID_VXSOFT  ((dfpalflag_t)FP_INV_VXSOFT)
  #define DFPAL_FP_PPC_CLEAR_ALL   ((dfpalflag_t)(~((DFPAL_FP_INVALID_RC)| \
                                   FP_INVALID|FP_OVERFLOW|FP_UNDERFLOW|    \
                                   FP_DIV_BY_ZERO|FP_INEXACT)))
  #define PPC_FEATURE_HAS_DFP      0x00000400 /* Decimal FP Unit */
#else
  #define DFPAL_FP_INVALID_RC      ((dfpalflag_t)0)
  #define DFPAL_FP_INVALID_VXSOFT  ((dfpalflag_t)0)
  #define DFPAL_FP_PPC_CLEAR_ALL   ((dfpalflag_t)0)
#endif

static const char * const dfpalVerStr=DFPAL_VERSION;

dfpalCmnContext globalContext;
dfpalCSFlag controlFlags;

#if defined(DFPAL_THREAD_SAFE)
  #if defined(DFPAL_OS_WINDOWS)
    __declspec( thread ) dfpalThreadContext *specificContext=NULL;
  #else
    pthread_once_t onceGlobalContext = PTHREAD_ONCE_INIT;
  #endif
#else /* #if defined(DFPAL_THREAD_SAFE) */
  dfpalThreadContext *specificContext=NULL;
#endif /* #if defined(DFPAL_THREAD_SAFE) */

static const uInt power10[]={1, 10, 100, 1000, 10000, 100000,
                     1000000, 10000000, 100000000, 1000000000 };
static const Int byteset=1;
static const Flag *bytearray=(Flag *)(&byteset);
/* ----------------------------------------------------------------------- */
/* Local defines                                                           */
/* ----------------------------------------------------------------------- */
#define DFPAL_LITTLE_ENDIAN  (bytearray[0])
#define DFPAL_MAX_DOUBLE_PREC     17    /* 2^52=16+1 digits: maximum */
                                        /* precision for double */
#define DFPAL_DOUBLE_STRLEN       25    /* including NULL */
#define DFPAL_DECIMAL64_STRLEN    25    /* including NULL */
#define DFPAL_DECIMAL128_STRLEN   43    /* including NULL */

#define DFPAL_DECIMAL64_BCDLEN     9    /* not including NULL */
#define DFPAL_DECIMAL128_BCDLEN   18    /* not including NULL */

/* Do not use following rounding mode directly, use ones */
/* starting with DFPAL_XXXX, defined in dfpal.h          */
#if !defined(DFP_ROUND_TO_NEAREST_WITH_TIES_TO_EVEN)
  #define DFP_ROUND_TO_NEAREST_WITH_TIES_TO_EVEN          0
  #define DFP_ROUND_TOWARD_ZERO                           1
  #define DFP_ROUND_TOWARD_POSITIVE_INFINITY              2
  #define DFP_ROUND_TOWARD_NEGATIVE_INFINITY              3
  #define DFP_ROUND_TO_NEAREST_WITH_TIES_AWAY_FROM_ZERO   4
  #define DFP_ROUND_TO_NEAREST_WITH_TIES_TOWARD_ZERO      5
  #define DFP_ROUND_AWAY_FROM_ZERO                        6
  #define DFP_ROUND_TO_PREPARE_FOR_SHORTER_PRECISION      7
#endif


/* ----------------------------------------------------------------------- */
/* Special value constants. Do not use until initialized.                  */
/* These constants must be used within if(globalContext.dfpRealMode==PPCHW)*/
/* ----------------------------------------------------------------------- */
static dfp_double dfp_double_zero=0;
static decimal64  dec64_zero={0};
static dfp_quad   dfp_quad_zero=0;
static decimal128 dec128_zero={0};
#if defined(DFPAL_USE_DECFLOAT)
  static decDouble decDouble_zero={0};
  static decQuad decQuad_zero={0};
#endif

static dfp_double dfp_double_positive_one=0;
static decimal64  dec64_positive_one={0};
static dfp_quad   dfp_quad_positive_one=0;
static decimal128 dec128_positive_one={0};

static dfp_double dfp_double_negative_one=0;
static decimal64  dec64_negative_one={0};
static dfp_quad   dfp_quad_negative_one=0;
static decimal128 dec128_negative_one={0};

static dfp_double dfp_double_quiet_NaN=0;
static decimal64  dec64_quiet_NaN={0};
static dfp_quad   dfp_quad_quiet_NaN=0;
static decimal128 dec128_quiet_NaN={0};
#if defined(DFPAL_USE_DECFLOAT)
  static decDouble decDouble_quiet_NaN={0};
  static decQuad decQuad_quiet_NaN={0};
#endif

static dfp_double dfp_double_signalling_NaN=0;
static dfp_quad   dfp_quad_signalling_NaN=0;

static dfp_double dfp_double_positive_infinity=0;
static dfp_quad   dfp_quad_positive_infinity=0;

static dfp_double dfp_double_negative_infinity=0;
static dfp_quad   dfp_quad_negative_infinity=0;

/* ----------------------------------------------------------------------- */
/* Local macros                                                            */
/* ----------------------------------------------------------------------- */

#define Set_Init_Error(de,oe)                                               \
  {                                                                         \
    globalContext.initDfpalErrNum=(de);                                     \
    globalContext.initOsErrNum=(oe);                                        \
  }

#define Clear_Init_Error()                                                  \
  {                                                                         \
    globalContext.initDfpalErrNum=DFPAL_ERR_NO_ERROR;                       \
    globalContext.initOsErrNum=0;                                           \
  }


#define PRINT_DFP(size, msg, n)                                      \
{                                                                    \
  dec##size##DataXchg xchg##size##;                                  \
  char res[128];                                                     \
  xchg##size##.dhw=(n);                                              \
  printf(#msg "=%s\n", dec##size##ToString(xchg##size##.dsw, res));  \
}

#if !defined(DFPAL_NO_HW_DFP)
  #if defined(DFPAL_OS_AIX5L) || defined(DFPAL_OS_I5OS) || \
    defined(DFPAL_OS_LOP)
    #define Change_FPSCR_Status(status, trapname, newval)             \
      {                                                               \
        ppc_fpscr curFPSCR;                                           \
        curFPSCR.all.d=ppc_mffs();                                    \
        curFPSCR.fpscr_attrib.status=newval;                          \
        ppc_mtfs(curFPSCR.all.d);                                     \
        if (newval && curFPSCR.fpscr_attrib.trapname) raise(SIGFPE);  \
      }
  #endif
#else
  #define Change_FPSCR_Status(status, trapname, newval)    {(void)0;}
#endif

#define Set_decNumber_Status(context, trapname)                     \
  {                                                                 \
    (context)->status |= (trapname);                                \
    if ((context)->status & (context)->traps) raise(SIGFPE);        \
  }

  
/* ----------------------------------------------------------------------- */
/* Local function declerations                                             */
/* ----------------------------------------------------------------------- */
static Flag dfpalIsHwDfpAvailable(enum dfpalExeMode);
static void dfpalSetError(Int dfpalError, Int osError);
static void dfpalInitConstant(void);
static Long dfpalSignedInt64FromNumber(decNumber *dnIn, decContext *set);
decNumber *dfpalSignedInt64ToNumber(decNumber *dnOut, 
  const Long rhs, decContext *set);
static uLong dfpalUnsignedInt64FromNumber(decNumber *dnIn, decContext *set);
static decNumber *dfpalUnsignedInt64ToNumber(decNumber *dnOut, 
  const uLong rhs, decContext *set);


/* ----------------------------------------------------------------------- */
/* Local functions                                                         */
/* ----------------------------------------------------------------------- */
/* To be called under if(globalContext.dfpRealMode==PPCHW) */
static void dfpalInitConstant(void)
{
  union {
    uLong hex;
    dfp_double dfp;
  } double_value;
  union {
    uLong hex[2];
    dfp_quad  dfp;
  } quad_value;

  dfp_integer i; 
  dec64DataXchg x64;
  dec128DataXchg x128;

  /* zero */
  double_value.hex=0x2238000000000000ull;
  dfp_double_zero=double_value.dfp;
  x64.dhw=dfp_double_zero;
  dec64_zero=x64.dsw;

  quad_value.hex[0]=0x2208000000000000ull;
  quad_value.hex[1]=0x0000000000000000ull;
  dfp_quad_zero=quad_value.dfp;
  x128.dhw=dfp_quad_zero;
  dec128_zero=x128.dsw;

  /* +1 */
  i.sll=+1;
  dfp_double_positive_one=ppc_dcffix_via_dcffixq(i.d);
  x64.dhw=dfp_double_positive_one;
  dec64_positive_one=x64.dsw;

  dfp_quad_positive_one=ppc_dcffixq(i.d);
  x128.dhw=dfp_quad_positive_one;
  dec128_positive_one=x128.dsw;

  /* -1 */
  i.sll=-1;
  dfp_double_negative_one=ppc_dcffix_via_dcffixq(i.d);
  x64.dhw=dfp_double_negative_one;
  dec64_negative_one=x64.dsw;

  dfp_quad_negative_one=ppc_dcffixq(i.d);
  x128.dhw=dfp_quad_negative_one;
  dec128_negative_one=x128.dsw;

  /* quiet NaN */
  double_value.hex=0x7C00000000000000ull;
  dfp_double_quiet_NaN=double_value.dfp;
  x64.dhw=dfp_double_quiet_NaN;
  dec64_quiet_NaN=x64.dsw;

  quad_value.hex[0]=0x7C00000000000000ull;
  quad_value.hex[1]=0x0000000000000000ull;
  dfp_quad_quiet_NaN=quad_value.dfp;
  x128.dhw=dfp_quad_quiet_NaN;
  dec128_quiet_NaN=x128.dsw;

  /* signalling NaN */
  double_value.hex=0x7E00000000000000ull;
  dfp_double_signalling_NaN=double_value.dfp;

  quad_value.hex[0]=0x7E00000000000000ull;
  quad_value.hex[1]=0x0000000000000000ull;
  dfp_quad_signalling_NaN=quad_value.dfp;

  /* +ve infinity */
  double_value.hex=0x7800000000000000ull;
  dfp_double_positive_infinity=double_value.dfp;

  quad_value.hex[0]=0x7800000000000000ull;
  quad_value.hex[1]=0x0000000000000000ull;
  dfp_quad_positive_infinity=quad_value.dfp;

  /* -ve infinity */
  double_value.hex=0xF800000000000000ull;
  dfp_double_negative_infinity=double_value.dfp;

  quad_value.hex[0]=0xF800000000000000ull;
  quad_value.hex[1]=0x0000000000000000ull;
  dfp_quad_negative_infinity= quad_value.dfp;

  return;
}

/* To be called under if(globalContext.dfpRealMode==DNSW) */
static void dfpalInitDecFloatConstant(void)
{
  #if defined(DFPAL_USE_DECFLOAT)
  union {
    uLong hex;
    decDouble dfp;
  } double_value;
  union {
    uLong hex[2];
    decQuad  dfp;
  } quad_value;

  /* quiet NaN */
  double_value.hex=0x7C00000000000000ull;
  decDouble_quiet_NaN=double_value.dfp;

  quad_value.hex[0]=0x7C00000000000000ull;
  quad_value.hex[1]=0x0000000000000000ull;
  decQuad_quiet_NaN=quad_value.dfp;

  /* zero */
  double_value.hex=0x2238000000000000ull;
  decDouble_zero=double_value.dfp;

  quad_value.hex[0]=0x2208000000000000ull;
  quad_value.hex[1]=0x0000000000000000ull;
  decQuad_zero=quad_value.dfp;
  #endif

  return;
}


Int dfpalErrorFlag() {
  dfpalThreadContext *thc;

  if((thc=Tls_Load())!=NULL) {
    return(thc->dfpalErrNum);
  }

  return(globalContext.initDfpalErrNum);
}

static Flag dfpalIsHwDfpAvailable(enum dfpalExeMode usrmode) {
  #if defined(DFPAL_NO_HW_DFP)
    return((Flag)0);
  #elif defined(DFPAL_OS_AIX5L)
    void *libcHandle;
    Int (*fptr__get_dfp_version)(void);
    Int dfpStatus=0;

    switch(usrmode) {
      case AUTO:
      case PPCHW:
        if (_system_configuration.implementation>POWER_5) {
          /* runnning on POWER6 processor, check OS support */
          errno=0;
          if ((libcHandle=dlopen("libc.a(shr_64.o)", RTLD_NOW|RTLD_MEMBER))==
           (void *)NULL) {
            if (ENOEXEC==errno) {
              if ((libcHandle=dlopen("libc.a(shr.o)", RTLD_NOW|RTLD_MEMBER))==
               (void *)NULL) {
                return((Flag)0); /* be safe! */
              }
            }
            else {
              return((Flag)0); /* no hope */
            }
          }

          if ((fptr__get_dfp_version=
           (Int(*)(void))dlsym(libcHandle, "__get_dfp_version"))!=
           (Int(*)(void))NULL) {
            /* __get_dfp_version found, check OS compatibility */
            dfpStatus=fptr__get_dfp_version();
          }
          dlclose(libcHandle);
          if (dfpStatus!=0) return((Flag)1); /* all ok */
        }

        /* not running on POWER6 processor or there is no OS support */
        return((Flag)0);
        break;
      case DNSW:
        return((Flag)0);
        break;
    }
  #elif defined(DFPAL_OS_LOP)
  #define DFPAL_AV_SIZE 40
    switch(usrmode) {
      case AUTO:
      case PPCHW:
        errno=0;
        ElfW(auxv_t) av[DFPAL_AV_SIZE];
        FILE * procfs;
        Int i;
        Int items;

        procfs = fopen("/proc/self/auxv","rb");
        if (!procfs) return((Flag)0);

        for (; !feof(procfs) && !ferror(procfs) ;)
        {
          items=fread(av, sizeof(ElfW(auxv_t)), DFPAL_AV_SIZE, procfs);
          for (i = 0; i < items ; i++) {
            if (av[i].a_type == AT_HWCAP) {
              if (av[i].a_un.a_val & PPC_FEATURE_HAS_DFP) {
                fclose(procfs);
                return((Flag)1);
              }
            }

            if (av[i].a_type == AT_NULL) {
              fclose(procfs);
              return((Flag)0);
            }
          }
        }
        fclose(procfs);
        return((Flag)0);
        break;
      case DNSW:
        return((Flag)0);
        break;
    }
  #elif defined(DFPAL_OS_I5OS)
    switch(usrmode) {
      case AUTO:
      case PPCHW:
        if ((_system_configuration.implementation>POWER_5) && 
         (__power_dfp()!=0)) {
          return((Flag)1);
        }
        return((Flag)0);
        break;
      case DNSW:
        return((Flag)0);
        break;
    }
  #endif
#if 0 
    /* not reached */
    return((Flag)0);
#endif
}

static void dfpalSetError(Int dfpalError, Int osError) {
  dfpalThreadContext *thc;

  if ((thc=Tls_Load())!=NULL) {
    thc->dfpalErrNum = dfpalError;
    thc->osErrorNum = osError;
    thc->dfpalErrorText=(char *)dfpalErrMessageCtlog[dfpalError];
  }
  else {
   Set_Init_Error(DFPAL_ERR_NO_INIT,0);
  }
}


decNumber *dfpalSignedInt64ToNumber (
  decNumber *dnOut,
  const Long rhs,
  decContext *set)
{
  Int i, md;
  Long sl64=rhs;
  uLong ul64;      /* place-holder for "rhs" when abs(rhs)>LLONG_MAX  */ 
                   /* abs(LLONG_MIN) > what sl64 can hold             */

  dnOut->exponent=0;
  dnOut->bits=0;
  if (sl64 == 0 ) {
    dnOut->digits=1;
    dnOut->lsu[0]=0;
    return(dnOut);
  }

  if (sl64 < 0 ) {
    dnOut->bits|=DECNEG;
    ul64 = (uLong)(-sl64);
  }
  else {
    ul64=(uLong)sl64;
  }

  /* DECNUMDIGITS=34; we have enough space to hold 2^63=19 digits */
  dnOut->digits=0;
  for(i=0;ul64;i++) {
    dnOut->lsu[i]=ul64%power10[DECDPUN];
    ul64/=power10[DECDPUN];
    dnOut->digits+=DECDPUN;
  }

  for(md=1;;md++) {
    if(dnOut->lsu[i-1]<power10[md]) {
      dnOut->digits-=(DECDPUN-md);
      break;
    }
  }

  return(dnOut);
}


static Long dfpalSignedInt64FromNumber(decNumber *dnIn, decContext *set)
{
  decNumber dnZero, dnRescaled, dnMinMax, cmpRes;
  decContext cmpSet, workSet;

  workSet=*set;
  workSet.round=DEC_ROUND_DOWN;  /* truncate */
  workSet.traps=0;
  workSet.digits=34; /* 2^63 needs 19 digits */
  decNumberZero(&dnZero);
  /* ignore inexact raised by Quantize */
  decNumberQuantize(&dnRescaled, dnIn, &dnZero, &workSet);

  /* 2^63 needs 19 digits */
  if ((dnRescaled.digits > 19) || ((dnRescaled.bits&DECSPECIAL)!=0)) {
    /* input has too many digits or is a special value,                 */
    /* no special treatment for sNaN, we're setting invalid in any case */
    Set_decNumber_Status(set, DEC_Invalid_operation)
    if (decNumberIsNegative(dnIn) || decNumberIsNaN(dnIn)) 
      return(DFPAL_INT64_MIN);
    else
      return(DFPAL_INT64_MAX);
  }

  /* Use 128-bits. 64-bit only has 16 digits and 2^63 needs 19 digits */
  decContextDefault(&cmpSet, DEC_INIT_DECIMAL128);

  dfpalSignedInt64ToNumber(&dnMinMax, DFPAL_INT64_MIN, &cmpSet);
  decNumberCompare(&cmpRes, &dnRescaled, &dnMinMax, &cmpSet);

  /* if (input value < DFPAL_INT64_MIN) */
  if (decNumberIsNegative(&cmpRes)) {
    Set_decNumber_Status(set, DEC_Invalid_operation)
    return(DFPAL_INT64_MIN);
  } else {
    dfpalSignedInt64ToNumber(&dnMinMax, DFPAL_INT64_MAX, &cmpSet);
    decNumberCompare(&cmpRes, &dnMinMax, &dnRescaled, &cmpSet);

    /* if (DFPAL_INT64_MAX < input value) */
    if (decNumberIsNegative(&cmpRes)) {
      Set_decNumber_Status(set, DEC_Invalid_operation)
      return(DFPAL_INT64_MAX);
    } else {
      Int l;
      Long sl64, digitExp;
      /* dnRescaled is in range so perform the conversion */
      sl64=dnRescaled.lsu[0]; /* maximum DECDPUN<=9; we're ok */
      digitExp=power10[DECDPUN];
      for (l=1; l < (dnRescaled.digits+DECDPUN-1)/DECDPUN; l++) {
        sl64 += (digitExp * dnRescaled.lsu[l]);
        digitExp *= power10[DECDPUN];
      }
      if (dnRescaled.bits & DECNEG) sl64=-sl64;

      return(sl64);
    }
  }
}


static decNumber *dfpalUnsignedInt64ToNumber (
  decNumber *dnOut,
  const uLong rhs,
  decContext *set)
{
  Int i, md;
  uLong ul64=rhs;

  dnOut->exponent=0;
  dnOut->bits=0;
  if (ul64 == 0 ) {
    dnOut->digits=1;
    dnOut->lsu[0]=0;
    return(dnOut);
  }

  /* DECNUMDIGITS=34; we have enough space to hold 2^64=20 digits */
  dnOut->digits=0;
  for(i=0;ul64;i++) {
    dnOut->lsu[i]=ul64%power10[DECDPUN];
    ul64/=power10[DECDPUN];
    dnOut->digits+=DECDPUN;
  }

  for(md=1;;md++) {
    if(dnOut->lsu[i-1]<power10[md]) {
      dnOut->digits-=(DECDPUN-md);
      break;
    }
  }

  return(dnOut);
}


static uLong dfpalUnsignedInt64FromNumber(decNumber *dnIn, decContext *set)
{
  decNumber dnZero, dnRescaled, dnMax, cmpRes;
  decContext cmpSet, workSet;

  if (decNumberIsZero(dnIn)) {
    /* -0 should not raise INVALID */
    return(DFPAL_UINT64_MIN);
  }

  workSet=*set;
  workSet.round=DEC_ROUND_DOWN;  /* truncate */
  workSet.traps=0;
  workSet.digits=34;  /* 2^64 needs 20 digits */
  decNumberZero(&dnZero);
  /* ignore inexact raised by Quantize */
  decNumberQuantize(&dnRescaled, dnIn, &dnZero, &workSet);

  /* 2^64 needs 20 digits */
  if ((dnRescaled.digits > 20) || 
    ((dnRescaled.bits&DECSPECIAL)!=0) ||
    (decNumberIsNegative(&dnRescaled) && (!decNumberIsZero(&dnRescaled)))) {
    /* input has too many digits, is negative, or is a special value.   */
    /* no special treatment for sNaN, we're setting invalid in any case */
    Set_decNumber_Status(set, DEC_Invalid_operation)
    if (decNumberIsNegative(dnIn) || decNumberIsNaN(dnIn))
      return(DFPAL_UINT64_MIN);
    else
      return(DFPAL_UINT64_MAX);
  }

  /* Use 128-bits. 64-bit only has 16 digits and 2^64 needs 20 digits */
  decContextDefault(&cmpSet, DEC_INIT_DECIMAL128);

  dfpalUnsignedInt64ToNumber(&dnMax, DFPAL_UINT64_MAX, &cmpSet);
  decNumberCompare(&cmpRes, &dnMax, &dnRescaled, &cmpSet);

  /* if (DFPAL_UINT64_MAX < input value) */
  if (decNumberIsNegative(&cmpRes)) {
    Set_decNumber_Status(set, DEC_Invalid_operation)
    return(DFPAL_UINT64_MAX);
  } 
  else {
    Int l;
    uLong ul64, digitExp;
    /* dnRescaled is in range so perform the conversion */
    ul64=dnRescaled.lsu[0]; /* maximum DECDPUN<=9; we're ok */
    digitExp=power10[DECDPUN];
    for (l=1; l < (dnRescaled.digits+DECDPUN-1)/DECDPUN; l++) {
      ul64 += (digitExp * dnRescaled.lsu[l]);
      digitExp *= power10[DECDPUN];
    }
    return(ul64);
  }
#if 0 
  /* not reached, some compilers issue warning without this */
  return(DFPAL_UINT64_MAX);
#endif
}


/* ----------------------------------------------------------------------- */
/* Public API                                                              */
/* ----------------------------------------------------------------------- */
const char * dfpalVersion()
{
  return(dfpalVerStr);
}

void dfpalClearError() {
  dfpalThreadContext *thc;

  if((thc=Tls_Load())!=NULL) {
    thc->dfpalErrNum = 0;
    thc->osErrorNum = 0;
  }
  else {
    Clear_Init_Error();
  }
}

Int dfpalGetError(Int *dfpalErr, Int *osErr, char **dfpalErrTxt) {
  dfpalThreadContext *thc;

  if ((thc=Tls_Load())!=NULL) {
    *dfpalErr=thc->dfpalErrNum;
    *osErr=thc->osErrorNum;
    *dfpalErrTxt=thc->dfpalErrorText;
    return(thc->dfpalErrNum);
  }
  else {
    *dfpalErr=globalContext.initDfpalErrNum;
    *osErr=globalContext.initOsErrNum;
    *dfpalErrTxt=(char *)dfpalErrMessageCtlog[globalContext.initDfpalErrNum];
    return(globalContext.initDfpalErrNum);
  }
}

uInt dfpalMemSize()
{
  return(sizeof(dfpalThreadContext));
}


#if defined(DFPAL_OS_WINDOWS) && defined(DFPAL_STANDALONE_DLL)
BOOL APIENTRY DllMain(HANDLE hModule, 
  DWORD  ul_reason_for_call, 
  LPVOID lpReserved)
{
  switch( ul_reason_for_call ) {
    case DLL_PROCESS_ATTACH:
      dfpalInitProcessContext();
      break;
    case DLL_THREAD_ATTACH:
      break;
    case DLL_THREAD_DETACH:
      break;
    case DLL_PROCESS_DETACH:
      break;

  }
  return TRUE;
}
#endif

void dfpalInitProcessContext(void) {
  Int rc=DFPAL_ERR_NO_ERROR;
  char *dbgeval=(char *)NULL;
  char *exeeval=(char *)NULL;

  Clear_Init_Error();

  if (decContextTestEndian(1)) {
    Set_Init_Error(DFPAL_ERR_WRONG_ENDIAN, 0);
    return;
  }

  globalContext.dfpInHw=dfpalIsHwDfpAvailable(AUTO); 
  globalContext.dfpDebug=FALSE;

  /* user preference about execution mode */
  globalContext.dfpUserMode=AUTO;
  if ((exeeval=getenv(EXE_MODE_ENV))!=(char *)NULL) {
    if (strcmp(exeeval, EXE_MODE_SW)==0) {
      globalContext.dfpUserMode=DNSW;
      globalContext.dfpRealMode=DNSW;
    }
    else if (strcmp(exeeval,EXE_MODE_HW)==0) {
      globalContext.dfpUserMode=PPCHW;
      globalContext.dfpRealMode=PPCHW;
      if (!globalContext.dfpInHw) {
        globalContext.dfpRealMode=DNSW;
        Set_Init_Error(DFPAL_ERR_NO_HW_SUPPORT, 0);
        return;
      }
    }
  }
  /* unset or unknown value for EXE_MODE_ENV will result in AUTO mode */
  if (globalContext.dfpUserMode==AUTO) {
      globalContext.dfpRealMode=DNSW;
      if (globalContext.dfpInHw)
        globalContext.dfpRealMode=PPCHW;
  }


  /* set exception bit mask  and rounding mode */
  if(globalContext.dfpRealMode==PPCHW) {
    #if !defined(DFPAL_NO_HW_DFP)

    /* initialize constants */
    dfpalInitConstant();

    DFPAL_FP_INVALID=FP_INVALID;
    DFPAL_FP_OVERFLOW=FP_OVERFLOW;
    DFPAL_FP_UNDERFLOW=FP_UNDERFLOW;
    DFPAL_FP_DIV_BY_ZERO=FP_DIV_BY_ZERO;
    DFPAL_FP_INEXACT=FP_INEXACT;

    DFPAL_TRP_INVALID=TRP_INVALID;
    DFPAL_TRP_OVERFLOW=TRP_OVERFLOW;
    DFPAL_TRP_UNDERFLOW=TRP_UNDERFLOW;
    DFPAL_TRP_DIV_BY_ZERO=TRP_DIV_BY_ZERO;
    DFPAL_TRP_INEXACT=TRP_INEXACT;

    DFPAL_ROUND_TO_NEAREST_WITH_TIES_TO_EVEN=
      DFP_ROUND_TO_NEAREST_WITH_TIES_TO_EVEN;
    DFPAL_ROUND_TOWARD_ZERO=DFP_ROUND_TOWARD_ZERO;
    DFPAL_ROUND_TOWARD_POSITIVE_INFINITY=DFP_ROUND_TOWARD_POSITIVE_INFINITY;
    DFPAL_ROUND_TOWARD_NEGATIVE_INFINITY=DFP_ROUND_TOWARD_NEGATIVE_INFINITY;
    DFPAL_ROUND_TO_NEAREST_WITH_TIES_AWAY_FROM_ZERO=
      DFP_ROUND_TO_NEAREST_WITH_TIES_AWAY_FROM_ZERO;
    DFPAL_ROUND_TO_NEAREST_WITH_TIES_TOWARD_ZERO=
      DFP_ROUND_TO_NEAREST_WITH_TIES_TOWARD_ZERO;
    DFPAL_ROUND_AWAY_FROM_ZERO=DFP_ROUND_AWAY_FROM_ZERO;
    #endif
  }
  else {
    /* initialize constants */
    dfpalInitDecFloatConstant();

    DFPAL_FP_INVALID=(dfpalflag_t)DEC_IEEE_854_Invalid_operation;
    DFPAL_FP_OVERFLOW=(dfpalflag_t)DEC_IEEE_854_Overflow;
    DFPAL_FP_UNDERFLOW=(dfpalflag_t)DEC_IEEE_854_Underflow;
    DFPAL_FP_DIV_BY_ZERO=(dfpalflag_t)DEC_IEEE_854_Division_by_zero;
    DFPAL_FP_INEXACT=(dfpalflag_t)DEC_IEEE_854_Inexact;

    DFPAL_TRP_INVALID=(dfpaltrap_t)DEC_IEEE_854_Invalid_operation;
    DFPAL_TRP_OVERFLOW=(dfpaltrap_t)DEC_IEEE_854_Overflow;
    DFPAL_TRP_UNDERFLOW=(dfpaltrap_t)DEC_IEEE_854_Underflow;
    DFPAL_TRP_DIV_BY_ZERO=(dfpaltrap_t)DEC_IEEE_854_Division_by_zero;
    DFPAL_TRP_INEXACT=(dfpaltrap_t)DEC_IEEE_854_Inexact;

    /* default rounding mode in decNumber is DEC_ROUND_HALF_EVEN */
    DFPAL_ROUND_TO_NEAREST_WITH_TIES_TO_EVEN=(dfpalrnd_t)DEC_ROUND_HALF_EVEN;
    DFPAL_ROUND_TOWARD_ZERO=(dfpalrnd_t)DEC_ROUND_DOWN;
    DFPAL_ROUND_TOWARD_POSITIVE_INFINITY=(dfpalrnd_t)DEC_ROUND_CEILING;
    DFPAL_ROUND_TOWARD_NEGATIVE_INFINITY=(dfpalrnd_t)DEC_ROUND_FLOOR;
    DFPAL_ROUND_TO_NEAREST_WITH_TIES_AWAY_FROM_ZERO=(dfpalrnd_t)DEC_ROUND_HALF_UP;
    DFPAL_ROUND_TO_NEAREST_WITH_TIES_TOWARD_ZERO=(dfpalrnd_t)DEC_ROUND_HALF_DOWN;
    DFPAL_ROUND_AWAY_FROM_ZERO=(dfpalrnd_t)DEC_ROUND_UP;
  }

  /* read debug option and initialize mutex */
  if ((dbgeval=getenv(DEBUG_ENV))!=(char *)NULL) {
    strncpy(globalContext.dbgFile, dbgeval, DEBUG_FNAME_LEN);

    if (globalContext.dbgFile[DEBUG_FNAME_LEN]!='\0') {
      /* file name can not fit into allocated storage */
      Set_Init_Error(DFPAL_ERR_DEBUG_FNAME_LONG, 0);
      return;
    }
  }

  #if !defined(DFPAL_OS_WINDOWS)
  if((rc=pthread_key_create(&(globalContext.dfpalThreadMemKey), NULL))
    != 0) {
    Set_Init_Error(DFPAL_ERR_CREATE_TLS_KEY, rc);
    /* fclose(globalContext.dbgOut); */
    return;
  }
  #endif

  return;
}

Int dfpalInit(void *context) {
  Int rc=DFPAL_ERR_UNKNOWN;
  dfpalThreadContext *thContext=(dfpalThreadContext*)context;

  if (thContext==(dfpalThreadContext *)NULL) {
    Set_Init_Error(DFPAL_ERR_NO_MEM,rc);
    return (DFPAL_ERR_NO_MEM);
  }

  Run_Once(&onceGlobalContext, dfpalInitProcessContext);
  if(globalContext.initDfpalErrNum!=DFPAL_ERR_NO_ERROR) {
    return(globalContext.initDfpalErrNum);
  }

  Tls_Store(thContext,rc);
  if (rc!=0) {
    Set_Init_Error(DFPAL_ERR_TLS_STORE,rc);
    return (DFPAL_ERR_TLS_STORE);
  }

  thContext->dfpalErrNum=DFPAL_ERR_NO_ERROR;
  thContext->osErrorNum=0;
  thContext->dfpalProcContext=&globalContext;
  /* 754r requires that programs shall not inherit status, it            */
  /* must be cleared when thread/process it created. decContextDefault() */
  /* is clearing status. HW status needs to be cleared explicitly.       */
  decContextDefault(&(thContext->dn64Context), DEC_INIT_DECIMAL64);
  decContextDefault(&(thContext->dn128Context), DEC_INIT_DECIMAL128);

  /* clear hardware status flags, see above */
  if(globalContext.dfpRealMode==PPCHW) {
    dfpalClearStatusFlag(DFPAL_FP_ALL);
    dfpalDisableAllTrap();
  }
  
  /* match decNumber rounding mode with hardware default */
  thContext->dn64Context.round=
    (enum rounding)DFPAL_ROUND_TO_NEAREST_WITH_TIES_TO_EVEN;
  thContext->dn128Context.round=
    (enum rounding)DFPAL_ROUND_TO_NEAREST_WITH_TIES_TO_EVEN;

  return(DFPAL_ERR_NO_ERROR);
}

dfpalCSFlag *dfpalGetFlagHandle()
{
  return(&controlFlags);
}

enum dfpalExeMode dfpalGetExeMode()
{
  return(globalContext.dfpRealMode);
}

void dfpalEnd(void (*fptr_memfree)(void *))
{
  dfpalThreadContext *thContext=Tls_Load();
  if (thContext != (dfpalThreadContext *)NULL) fptr_memfree(thContext);
}

void dfpalResetContext(void)
{
  dfpalThreadContext *thc=Tls_Load();  
  thc->dfpalErrNum=DFPAL_ERR_NO_ERROR;
  thc->osErrorNum=0;
  thc->dfpalProcContext=&globalContext;
  decContextDefault(&(thc->dn64Context), DEC_INIT_DECIMAL64);
  decContextDefault(&(thc->dn128Context), DEC_INIT_DECIMAL128);
}

/* ----------------------------------------------------------------------- */
/* macros for arithmetic functions                                         */
/* ----------------------------------------------------------------------- */
#define decNumber_Arithmetic1(size,oper,oprnd)                    \
  {                                                               \
    decimal##size result##size;                                   \
    decNumber lhsdn, resultdn;                                    \
    decContext * dnctx = &(Tls_Load()->dn##size##Context);        \
    decimal##size##ToNumber(&(oprnd),&lhsdn);                     \
    decNumber##oper(&resultdn, &lhsdn, dnctx);                    \
    decimal##size##FromNumber(&result##size, &resultdn, dnctx);   \
    return(result##size);                                         \
  }

#define decFloat_Arithmetic1(size,df,oper,oprnd)                  \
  {                                                               \
    dec##df result##df;                                           \
    decContext * dnctx = &(Tls_Load()->dn##size##Context);        \
    dec##df##oper(&result##df, (dec##df *)&oprnd, dnctx);         \
    return(*((decimal##size *)&result##df));                      \
  }

#define decNumber_For_PPC_Arithmetic1(size,oper,oprnd)            \
  {                                                               \
    decimal##size result##size;                                   \
    decNumber lhsdn, resultdn;                                    \
    decContext * dnctx = &(Tls_Load()->dn##size##Context);        \
    Set_decNumber_Context(dnctx)                                  \
    decimal##size##ToNumber(&(oprnd),&lhsdn);                     \
    decNumber##oper(&resultdn, &lhsdn, dnctx);                    \
    decimal##size##FromNumber(&result##size, &resultdn, dnctx);   \
    Update_FPSCR_Status(dnctx);                                   \
    return(result##size);                                         \
  }


#define decNumber_Arithmetic2(size,oper,lhsoprnd,rhsoprnd)        \
  {                                                               \
    decimal##size result##size;                                   \
    decNumber lhsdn, rhsdn, resultdn;                             \
    decContext * dnctx = &(Tls_Load()->dn##size##Context);        \
    decimal##size##ToNumber(&(lhsoprnd),&lhsdn);                  \
    decimal##size##ToNumber(&(rhsoprnd),&rhsdn);                  \
    decNumber##oper(&resultdn, &lhsdn, &rhsdn, dnctx);            \
    decimal##size##FromNumber(&result##size, &resultdn, dnctx);   \
    return(result##size);                                         \
  }


#define decFloat_Arithmetic2(size,df,oper,lhsoprnd,rhsoprnd)      \
  {                                                               \
    dec##df result##df;                                           \
    decContext * dnctx = &(Tls_Load()->dn##size##Context);        \
    dec##df##oper(&result##df, (dec##df *)&lhsoprnd,              \
      (dec##df *)&rhsoprnd, dnctx);                               \
    return(*((decimal##size *)&result##df));                      \
  }


#define decNumber_For_PPC_Arithmetic2(size,oper,lhsoprnd,rhsoprnd)      \
  {                                                                     \
    decimal##size result##size;                                         \
    decNumber lhsdn, rhsdn, resultdn;                                   \
    decContext * dnctx = &(Tls_Load()->dn##size##Context);              \
    Set_decNumber_Context(dnctx)                                        \
    decimal##size##ToNumber(&(lhsoprnd),&lhsdn);                        \
    decimal##size##ToNumber(&(rhsoprnd),&rhsdn);                        \
    decNumber##oper(&resultdn, &lhsdn, &rhsdn, dnctx);                  \
    decimal##size##FromNumber(&result##size, &resultdn, dnctx);         \
    Update_FPSCR_Status(dnctx);                                         \
    return(result##size);                                               \
  }


#define decNumber_To_Integer_RM(size, rndmode, rhs)                   \
  {                                                                   \
    decNumber rhsdn, resultdn;                                        \
    decimal##size result##size;                                       \
    enum rounding crm;                                                \
    decContext * dnctx = &(Tls_Load()->dn##size##Context);            \
    crm=dnctx->round;                                                 \
    decimal##size##ToNumber(&rhs, &rhsdn);                            \
    dnctx->round=(enum rounding)rndmode;                              \
    decNumberToIntegralValue(&resultdn, &rhsdn, dnctx);               \
    dnctx->round=crm;  /* restore rounding mode */                    \
    decimal##size##FromNumber(&result##size, &resultdn, dnctx);       \
    return(result##size);                                             \
  }

#define decFloat_To_Integer_RM(size, df, rndmode, rhs)                \
  {                                                                   \
    dec##df result##df;                                               \
    decContext * dnctx = &(Tls_Load()->dn##size##Context);            \
    dec##df##ToIntegralValue(&result##df, (dec##df *)&rhs, dnctx,     \
      (enum rounding)rndmode);                                        \
    return(*((decimal##size *)&result##df));                          \
  }


#define Operation_Not_Implemented1(size,oper,rhsoprnd)            \
decimal##size decimal##size##oper(const decimal##size rhsoprnd)   \
{                                                                 \
  decimal##size result={0};                                       \
  return (result);                                                \
}

/* lookup table for decNumber only arithmetic. Input HW -> output decNumber */
dfpalrnd_t dfpalRoundLookup[]={
  DEC_ROUND_HALF_EVEN,
  DEC_ROUND_DOWN,
  DEC_ROUND_CEILING,
  DEC_ROUND_FLOOR,
  DEC_ROUND_HALF_UP,
  DEC_ROUND_HALF_DOWN,
  DEC_ROUND_UP
  };

#define Set_decNumber_Context(set)                                        \
  if (globalContext.dfpRealMode==PPCHW) {                                 \
    ppc_fpscr curFPSCR;                                                   \
    curFPSCR.all.d=ppc_mffs();                                            \
                                                                          \
    /* read/set FPSCR rounding mode */                                    \
    (set)->round=(enum rounding)dfpalRoundLookup[curFPSCR.all.fpscr.drm]; \
    (set)->traps=0;                                                       \
    (set)->status=0;                                                      \
                                                                          \
    /* do not set decNumber trap enable here in FPSCR. */                 \
    /* Otherwise, decNumber will raise SIGFPE before   */                 \
    /* allowing DFPAL to copy status bits              */                 \
  }

/* clears ALL status bits */
#define Read_Clear_FPSCR(out)                                             \
  {                                                                       \
    ppc_fpscr cFPSCR;                                                     \
    cFPSCR.all.d=ppc_mffs();                                              \
    (out)=cFPSCR;                                                         \
    cFPSCR.all.fpscr.lower32 &= (DFPAL_FP_INVALID_RC|DFPAL_FP_INEXACT|    \
      DFPAL_FP_OVERFLOW|DFPAL_FP_UNDERFLOW|DFPAL_FP_DIV_BY_ZERO);         \
    ppc_mtfs(cFPSCR.all.d);                                               \
  }

/* clears ALL status bits */
#define Read_Clear_FPSCR_Set_RM(out, newRM)                               \
  {                                                                       \
    ppc_fpscr cFPSCR;                                                     \
    cFPSCR.all.d=ppc_mffs();                                              \
    (out)=cFPSCR;                                                         \
    cFPSCR.all.fpscr.lower32 &= (DFPAL_FP_INVALID_RC|DFPAL_FP_INEXACT|    \
      DFPAL_FP_OVERFLOW|DFPAL_FP_UNDERFLOW|DFPAL_FP_DIV_BY_ZERO);         \
    cFPSCR.fpscr_attrib.drn=(newRM);                                      \
    ppc_mtfs(cFPSCR.all.d);                                               \
  }


#define Read_FPSCR(in) ((in).all.d=ppc_mffs())
#define Restore_FPSCR(saved) (ppc_mtfs((saved).all.d))

#define Update_FPSCR_Status(set)                                   \
  if (globalContext.dfpRealMode==PPCHW) {                          \
    Flag rfpe=0;                                                   \
    ppc_fpscr curFPSCR;                                            \
    curFPSCR.all.d=ppc_mffs();   /* read FPSCR */                  \
                                                                   \
    /* read/set trap control */                                    \
    /* Software can not set vx bit directly. Setting vxsoft */     \
    /* will automatically set vx bit.                       */     \
    if((set)->status&DEC_IEEE_854_Invalid_operation) {             \
      curFPSCR.fpscr_attrib.vxsoft=1;                              \
    }                                                              \
    if((set)->status&DEC_IEEE_854_Overflow) {                      \
      curFPSCR.fpscr_attrib.ox=1;                                  \
      if (curFPSCR.fpscr_attrib.oe) rfpe=1;                        \
    }                                                              \
    if((set)->status&DEC_IEEE_854_Underflow) {                     \
       curFPSCR.fpscr_attrib.ux=1;                                 \
      if (curFPSCR.fpscr_attrib.ue) rfpe=1;                        \
    }                                                              \
    if((set)->status&DEC_IEEE_854_Division_by_zero) {              \
      curFPSCR.fpscr_attrib.zx=1;                                  \
      if (curFPSCR.fpscr_attrib.ze) rfpe=1;                        \
    }                                                              \
    if((set)->status&DEC_IEEE_854_Inexact) {                       \
      curFPSCR.fpscr_attrib.xx=1;                                  \
      if (curFPSCR.fpscr_attrib.xe) rfpe=1;                        \
    }                                                              \
    ppc_mtfs(curFPSCR.all.d);   /* set FPSCR */                    \
    if (rfpe) raise(SIGFPE);                                       \
  }

Operation_Not_Implemented1(64, Acos, rhs)
Operation_Not_Implemented1(64, Asin, rhs) 
Operation_Not_Implemented1(64, Atan, rhs) 
Operation_Not_Implemented1(64, Cos, rhs) 
Operation_Not_Implemented1(64, Sin, rhs)
Operation_Not_Implemented1(64, Tan, rhs)
Operation_Not_Implemented1(64, Cosh, rhs)
Operation_Not_Implemented1(64, Sinh, rhs)
Operation_Not_Implemented1(64, Tanh, rhs)

Operation_Not_Implemented1(128, Acos, rhs)
Operation_Not_Implemented1(128, Asin, rhs) 
Operation_Not_Implemented1(128, Atan, rhs) 
Operation_Not_Implemented1(128, Cos, rhs) 
Operation_Not_Implemented1(128, Sin, rhs)
Operation_Not_Implemented1(128, Tan, rhs)
Operation_Not_Implemented1(128, Cosh, rhs)
Operation_Not_Implemented1(128, Sinh, rhs)
Operation_Not_Implemented1(128, Tanh, rhs)


#define Decimal64_Function1(oper,rhsoprnd,hwf)                           \
  decimal64 decimal64##oper(const decimal64 rhsoprnd)                    \
  {                                                                      \
    if(globalContext.dfpRealMode==PPCHW) {                               \
      dfp_double xres;                                                   \
      xres=hwf(*((dfp_double *)&rhsoprnd));                              \
      return(*((decimal64 *)&xres));                                     \
    }                                                                    \
    else {                                                               \
      decimal64 result64;                                                \
      decNumber rhsdn, resultdn;                                         \
      decContext * dnctx = &(Tls_Load()->dn64Context);                   \
      decimal64ToNumber(&(rhsoprnd),&rhsdn);                             \
      decNumber##oper(&resultdn, &rhsdn, dnctx);                         \
      decimal64FromNumber(&result64, &resultdn, dnctx);                  \
      return(result64);                                                  \
    }                                                                    \
  }

#define Decimal128_Function1(oper,rhsoprnd,hwf)                          \
  decimal128 decimal128##oper(const decimal128 rhsoprnd)                 \
  {                                                                      \
    if(globalContext.dfpRealMode==PPCHW) {                               \
      dfp_quad xres;                                                     \
      dfp_pad qwpad=0.0;                                                 \
      xres.dhw=hwf(qwpad,*((dfp_quad *)&rhsoprnd));                      \
      return(*((decimal128 *)&xres));                                    \
    }                                                                    \
    else {                                                               \
      decimal128 result128;                                              \
      decNumber rhsdn, resultdn;                                         \
      decContext * dnctx = &(Tls_Load()->dn128Context);                  \
      decimal128ToNumber(&(rhsoprnd),&rhsdn);                            \
      decNumber##oper(&resultdn, &rhsdn, dnctx);                         \
      decimal128FromNumber(&result128, &resultdn, dnctx);                \
      return(result128);                                                 \
    }                                                                    \
  }


#define Decimal64_Function2(oper,lhsoprnd,rhsoprnd,hwf)                  \
  decimal64 decimal64##oper(const decimal64 lhsoprnd,                    \
    const decimal64 rhsoprnd)                                            \
  {                                                                      \
    if(globalContext.dfpRealMode==PPCHW) {                               \
      dfp_double xres;                                                   \
      xres=hwf(*((dfp_double *)&lhsoprnd),*((dfp_double *)&rhsoprnd));   \
      return(*((decimal64 *)&xres));                                     \
    }                                                                    \
    else {                                                               \
      decimal64 result64;                                                \
      decNumber lhsdn, rhsdn, resultdn;                                  \
      decContext * dnctx = &(Tls_Load()->dn64Context);                   \
      decimal64ToNumber(&(lhsoprnd),&lhsdn);                             \
      decimal64ToNumber(&(rhsoprnd),&rhsdn);                             \
      decNumber##oper(&resultdn, &lhsdn, &rhsdn, dnctx);                 \
      decimal64FromNumber(&result64, &resultdn, dnctx);                  \
      return(result64);                                                  \
    }                                                                    \
  }

#define Decimal64_decDouble_Function2(oper,lhsoprnd,rhsoprnd,hwf)        \
  decimal64 decimal64##oper(const decimal64 lhsoprnd,                    \
    const decimal64 rhsoprnd)                                            \
  {                                                                      \
    if(globalContext.dfpRealMode==PPCHW) {                               \
      dfp_double xres;                                                   \
      xres=hwf(*((dfp_double *)&lhsoprnd),*((dfp_double *)&rhsoprnd));   \
      return(*((decimal64 *)&xres));                                     \
    }                                                                    \
    else {                                                               \
      decDouble result64;                                                \
      decContext * dnctx = &(Tls_Load()->dn64Context);                   \
      decDouble##oper(&result64, (decDouble *)&lhsoprnd,                 \
        (decDouble *)&rhsoprnd, dnctx);                                  \
      return(*((decimal64 *)&result64));                                 \
    }                                                                    \
  }


#define Decimal128_Function2(oper,lhsoprnd,rhsoprnd,hwf)                 \
  decimal128 decimal128##oper(const decimal128 lhsoprnd,                 \
    const decimal128 rhsoprnd)                                           \
  {                                                                      \
    if(globalContext.dfpRealMode==PPCHW) {                               \
      dfp_quad xres;                                                     \
      dfp_pad qwpad=0.0;                                                 \
      xres=hwf(qwpad,*((dfp_quad *)&lhsoprnd),*((dfp_quad *)&rhsoprnd)); \
      return(*((decimal128 *)&xres));                                    \
    }                                                                    \
    else {                                                               \
      decimal128 result128;                                              \
      decNumber lhsdn, rhsdn, resultdn;                                  \
      decContext * dnctx = &(Tls_Load()->dn128Context);                  \
      decimal128ToNumber(&(lhsoprnd),&lhsdn);                            \
      decimal128ToNumber(&(rhsoprnd),&rhsdn);                            \
      decNumber##oper(&resultdn, &lhsdn, &rhsdn, dnctx);                 \
      decimal128FromNumber(&result128, &resultdn, dnctx);                \
      return(result128);                                                 \
    }                                                                    \
  }


#define Decimal128_decQuad_Function2(oper,lhsoprnd,rhsoprnd,hwf)         \
  decimal128 decimal128##oper(const decimal128 lhsoprnd,                 \
    const decimal128 rhsoprnd)                                           \
  {                                                                      \
    if(globalContext.dfpRealMode==PPCHW) {                               \
      dfp_quad xres;                                                     \
      dfp_pad qwpad=0.0;                                                 \
      xres=hwf(qwpad,*((dfp_quad *)&lhsoprnd),*((dfp_quad *)&rhsoprnd)); \
      return(*((decimal128 *)&xres));                                    \
    }                                                                    \
    else {                                                               \
      decQuad result128;                                                 \
      decContext * dnctx = &(Tls_Load()->dn128Context);                  \
      decQuad##oper(&result128, (decQuad *)&lhsoprnd,                    \
        (decQuad *)&rhsoprnd, dnctx);                                    \
      return(*((decimal128 *)&result128));                               \
    }                                                                    \
  }

#if defined(DFPAL_USE_DECFLOAT)
  #define Double_Word_Function2(oper,lhsoprnd,rhsoprnd,hwf) \
    Decimal64_decDouble_Function2(oper,lhsoprnd,rhsoprnd,hwf)
  #define Quad_Word_Function2(oper,lhsoprnd,rhsoprnd,hwf) \
    Decimal128_decQuad_Function2(oper,lhsoprnd,rhsoprnd,hwf)

  #define dec_Arithmetic1(size,df,oper,oprnd) \
    decFloat_Arithmetic1(size,df,oper,oprnd)
  #define dec_Arithmetic2(size,df,oper,lhsoprnd,rhsoprnd)\
    decFloat_Arithmetic2(size,df,oper,lhsoprnd,rhsoprnd)

  #define dec_To_Integer_RM(size, df, rndmode, rhs) \
    decFloat_To_Integer_RM(size, df, rndmode, rhs)
#else /* #if defined(DFPAL_USE_DECFLOAT) */
  #define Double_Word_Function2(oper,lhsoprnd,rhsoprnd,hwf) \
    Decimal64_Function2(oper,lhsoprnd,rhsoprnd,hwf)
  #define Quad_Word_Function2(oper,lhsoprnd,rhsoprnd,hwf) \
    Decimal128_Function2(oper,lhsoprnd,rhsoprnd,hwf)

  #define dec_Arithmetic1(size,df,oper,oprnd) \
    decNumber_Arithmetic1(size,oper,oprnd)
  #define dec_Arithmetic2(size,df,oper,lhsoprnd,rhsoprnd)\
    decNumber_Arithmetic2(size,oper,lhsoprnd,rhsoprnd)

  #define dec_To_Integer_RM(size, df, rndmode, rhs) \
    decNumber_To_Integer_RM(size, rndmode, rhs)
#endif /* #if defined(DFPAL_USE_DECFLOAT) */

/* ----------------------------------------------------------------------- */
/* macros for decimal<->{U|S}int{32|64} conversion                         */
/* ----------------------------------------------------------------------- */

#define Read_FPSCR_Set_RM(beforeFPSCR, newRM)                            \
{                                                                        \
  dfp_integer tzRM;                                                      \
  (beforeFPSCR).all.d=ppc_mffs();                                        \
  tzRM.fpscr.drm=(newRM);                                                \
  ppc_mtfsf_drm(tzRM.d);                                                 \
}

#define Restore_FPSCR_XX_RM(restoreFPSCR)                                \
{                                                                        \
  ppc_fpscr afterFPSCR;                                                  \
  afterFPSCR.all.d=ppc_mffs();                                           \
  afterFPSCR.fpscr_attrib.xx=(restoreFPSCR).fpscr_attrib.xx;             \
  afterFPSCR.fpscr_attrib.drn=(restoreFPSCR).fpscr_attrib.drn;           \
  ppc_mtfs(afterFPSCR.all.d);                                            \
}

#define Decimal64_To_Aint64(fnrtn, fnname, ppcfn, asgtype, dnfn)         \
  fnrtn fnname(const decimal64 rhs)                                      \
  {                                                                      \
    if(globalContext.dfpRealMode==PPCHW) {                               \
      dfp_integer ires;                                                  \
      ppc_fpscr beforeFPSCR;                                             \
                                                                         \
      Read_FPSCR_Set_RM(beforeFPSCR, DFP_ROUND_TOWARD_ZERO)              \
      ires.d=ppcfn(*((dfp_double *)&rhs));                               \
      Restore_FPSCR_XX_RM(beforeFPSCR)                                   \
      return((fnrtn)ires.asgtype);                                       \
    }                                                                    \
    else {                                                               \
      decNumber rhsdn;                                                   \
      fnrtn result;                                                      \
      decContext * dnctx = &(Tls_Load()->dn64Context);                   \
      decimal64ToNumber(&rhs, &rhsdn);                                   \
      return(dnfn(&rhsdn,dnctx));                                        \
    }                                                                    \
  }


#define Decimal_To_Uint64(size)                                          \
  uLong decimal##size##ToUint64(const decimal##size rhs)                 \
  {                                                                      \
    decNumber rhsdn;                                                     \
    uLong result;                                                        \
    decContext * dnctx = &(Tls_Load()->dn##size##Context);               \
    Set_decNumber_Context(dnctx)                                         \
    decimal##size##ToNumber(&rhs, &rhsdn);                               \
    result=dfpalUnsignedInt64FromNumber(&rhsdn,dnctx);                   \
    Update_FPSCR_Status(dnctx)                                           \
    return(result);                                                      \
  }


#define Decimal64_To_Aint32(fnrtn, fnname, ppcfn, asgtype,               \
  dnfnrtn, dnfn, min, max)                                               \
  fnrtn fnname(const decimal64 rhs)                                      \
  {                                                                      \
    if(globalContext.dfpRealMode==PPCHW) {                               \
      dfp_integer ires;                                                  \
      ppc_fpscr beforeFPSCR;                                             \
                                                                         \
      Read_FPSCR_Set_RM(beforeFPSCR, DFP_ROUND_TOWARD_ZERO)              \
      ires.d=ppcfn(*((dfp_double *)&rhs));                               \
      Restore_FPSCR_XX_RM(beforeFPSCR)                                   \
                                                                         \
      if (ires.asgtype > (max)) {                                        \
        ires.asgtype=(max);                                              \
        Change_FPSCR_Status(vxcvi, ve, 1)                                \
      }                                                                  \
      else if (ires.asgtype < (min)) {                                   \
        ires.asgtype=(min);                                              \
        Change_FPSCR_Status(vxcvi, ve, 1)                                \
      }                                                                  \
      return((fnrtn)ires.asgtype);                                       \
    }                                                                    \
    else {                                                               \
      decNumber rhsdn;                                                   \
      dnfnrtn result;                                                    \
      decContext * dnctx = &(Tls_Load()->dn64Context);                   \
      decimal64ToNumber(&rhs, &rhsdn);                                   \
      result=dnfn(&rhsdn, dnctx);                                        \
      if (result > (max)) {                                              \
        result=(max);                                                    \
        Set_decNumber_Status(dnctx, DEC_Invalid_operation)               \
      }                                                                  \
      else if (result < (min)) {                                         \
        result=(min);                                                    \
        Set_decNumber_Status(dnctx, DEC_Invalid_operation)               \
      }                                                                  \
      return ((fnrtn)result);                                            \
    }                                                                    \
  }


#define Decimal128_To_Aint32(fnrtn, fnname, ppcfn, asgtype,              \
  dnfnrtn, dnfn, min, max)                                               \
  fnrtn fnname(const decimal128 rhs)                                     \
  {                                                                      \
    if(globalContext.dfpRealMode==PPCHW) {                               \
      dfp_integer ires;                                                  \
      dfp_pad qwpad=0.0;                                                 \
      ppc_fpscr beforeFPSCR;                                             \
                                                                         \
      Read_FPSCR_Set_RM(beforeFPSCR, DFP_ROUND_TOWARD_ZERO)              \
      ires.d=ppcfn(qwpad, *((dfp_quad *)&rhs));                          \
      Restore_FPSCR_XX_RM(beforeFPSCR)                                   \
                                                                         \
      if (ires.asgtype > (max)) {                                        \
        ires.asgtype=(max);                                              \
        Change_FPSCR_Status(vxcvi, ve, 1)                                \
      }                                                                  \
      else if (ires.asgtype < (min)) {                                   \
        ires.asgtype=(min);                                              \
        Change_FPSCR_Status(vxcvi, ve, 1)                                \
      }                                                                  \
      return((fnrtn)ires.asgtype);                                       \
    }                                                                    \
    else {                                                               \
      decNumber rhsdn;                                                   \
      dnfnrtn result;                                                    \
      decContext * dnctx = &(Tls_Load()->dn128Context);                  \
      decimal128ToNumber(&rhs, &rhsdn);                                  \
      result=dnfn(&rhsdn, dnctx);                                        \
      if (result > (max)) {                                              \
        result=(max);                                                    \
        Set_decNumber_Status(dnctx, DEC_Invalid_operation)               \
      }                                                                  \
      else if (result < (min)) {                                         \
        result=(min);                                                    \
        Set_decNumber_Status(dnctx, DEC_Invalid_operation)               \
      }                                                                  \
      return ((fnrtn)result);                                            \
    }                                                                    \
  }


#define Decimal128_To_Aint64(fnrtn, fnname, ppcfn, asgtype, dnfn)        \
  fnrtn fnname(const decimal128 rhs)                                     \
  {                                                                      \
    if(globalContext.dfpRealMode==PPCHW) {                               \
      dfp_integer ires;                                                  \
      dfp_pad qwpad=0.0;                                                 \
      ppc_fpscr beforeFPSCR;                                             \
                                                                         \
      Read_FPSCR_Set_RM(beforeFPSCR, DFP_ROUND_TOWARD_ZERO)              \
      ires.d=ppcfn(qwpad, *((dfp_quad *)&rhs));                          \
      Restore_FPSCR_XX_RM(beforeFPSCR)                                   \
      return((fnrtn)ires.asgtype);                                       \
    }                                                                    \
    else {                                                               \
      decNumber rhsdn;                                                   \
      decContext * dnctx = &(Tls_Load()->dn128Context);                  \
      decimal128ToNumber(&rhs, &rhsdn);                                  \
      return(dnfn(&rhsdn,dnctx));                                        \
    }                                                                    \
  }                                                                      
 

#define Decimal_From_Aint32_64(size, fnname, fninput, ppcfn, asgtype,    \
  dnfn)                                                                  \
  decimal##size fnname(const fninput rhs)                                \
  {                                                                      \
    if(globalContext.dfpRealMode==PPCHW) {                               \
      dec##size##DataXchg xres;                                          \
      dfp_integer irhs;                                                  \
      irhs.asgtype=rhs;                                                  \
      xres.dhw=ppcfn(irhs.d);                                            \
      return(xres.dsw);                                                  \
    }                                                                    \
    else {                                                               \
      decNumber rhsdn;                                                   \
      decimal##size result##size;                                        \
      decContext * dnctx = &(Tls_Load()->dn##size##Context);             \
      dnfn(&rhsdn, rhs, dnctx);                                          \
      decimal##size##FromNumber(&result##size, &rhsdn, dnctx);           \
      return (result##size);                                             \
    }                                                                    \
  }   


#define Decimal_From_Uint64(size)                                        \
  decimal##size decimal##size##FromUint64(const uLong rhs)               \
  {                                                                      \
    decNumber rhsdn;                                                     \
    decimal##size result##size;                                          \
    decContext * dnctx = &(Tls_Load()->dn##size##Context);               \
    Set_decNumber_Context(dnctx)                                         \
    dfpalUnsignedInt64ToNumber(&rhsdn, rhs, dnctx);                      \
    decimal##size##FromNumber(&result##size, &rhsdn, dnctx);             \
    Update_FPSCR_Status(dnctx)                                           \
    return (result##size);                                               \
  }                                                                      \


/* --------------------------------------------------------------- */
/* --- interge setting overflow/underflow (non-IEEE compliant) --- */
/* --------------------------------------------------------------- */
#if defined(DFPAL_INTEGER_XCPOFUF) /* { */
#define Decimal64_To_Aint32_OU(fnrtn, fnname, ppcfn, asgtype,            \
  dnfnrtn, dnfn, min, max, hwminx, hwmint, hwmaxx, hwmaxt,               \
  dnminx, dnmaxx)                                                        \
  fnrtn fnname(const decimal64 rhs)                                      \
  {                                                                      \
    if(globalContext.dfpRealMode==PPCHW) {                               \
      dec64DataXchg xrhs;                                                \
      dfp_integer ires;                                                  \
      xrhs.dsw=rhs;                                                      \
      ires.d=ppcfn(xrhs.dhw);                                            \
      if (ires.asgtype > max) {                                          \
        ires.asgtype=max;                                                \
        Change_FPSCR_Status(hwmaxx, hwmaxt, 1)                           \
      }                                                                  \
      else if (ires.asgtype < min) {                                     \
        ires.asgtype=min;                                                \
        Change_FPSCR_Status(hwminx, hwmint, 1)                           \
      }                                                                  \
      return((fnrtn)ires.sll);                                           \
    }                                                                    \
    else {                                                               \
      decNumber rhsdn;                                                   \
      dnfnrtn result;                                                    \
      decContext * dnctx = &(Tls_Load()->dn64Context);                   \
      decimal64ToNumber(&rhs, &rhsdn);                                   \
      result=dnfn(&rhsdn, dnctx);                                        \
      if (result > max) {                                                \
        result=max;                                                      \
        Set_decNumber_Status(dnctx, dnmaxx)                              \
      }                                                                  \
      else if (result < min) {                                           \
        result=min;                                                      \
        Set_decNumber_Status(dnctx, dnminx)                              \
      }                                                                  \
      return ((fnrtn)result);                                            \
    }                                                                    \
  }

#define Decimal128_To_Aint32_OU(fnrtn, fnname, ppcfn, asgtype,           \
  dnfnrtn, dnfn, min, max, hwminx, hwmint, hwmaxx, hwmaxt,               \
  dnminx, dnmaxx)                                                        \
  fnrtn fnname(const decimal128 rhs)                                     \
  {                                                                      \
    if(globalContext.dfpRealMode==PPCHW) {                               \
      dfp_integer ires;                                                  \
      dfp_pad qwpad=0.0;                                                 \
      xrhs.dsw=rhs;                                                      \
      ires.d=ppcfn(qwpad, *((dfp_quad *)&rhs);                           \
      if (ires.asgtype > max) {                                          \
        ires.asgtype=max;                                                \
        Change_FPSCR_Status(hwmaxx, hwmaxt, 1)                           \
      }                                                                  \
      else if (ires.asgtype < min) {                                     \
        ires.asgtype=min;                                                \
        Change_FPSCR_Status(hwminx, hwmint, 1)                           \
      }                                                                  \
      return((fnrtn)ires.sll);                                           \
    }                                                                    \
    else {                                                               \
      decNumber rhsdn;                                                   \
      dnfnrtn result;                                                    \
      decContext * dnctx = &(Tls_Load()->dn128Context);                  \
      decimal128ToNumber(&rhs, &rhsdn);                                  \
      result=dnfn(&rhsdn, dnctx);                                        \
      if (result > max) {                                                \
        result=max;                                                      \
        Set_decNumber_Status(dnctx, dnmaxx)                              \
      }                                                                  \
      else if (result < min) {                                           \
        result=min;                                                      \
        Set_decNumber_Status(dnctx, dnminx)                              \
      }                                                                  \
      return ((fnrtn)result);                                            \
    }                                                                    \
  }
#endif /* } #if defined(DFPAL_INTEGER_XCPOFUF) */

/* ----------------------------------------------------------------------- */
/* Special value fast-test                                                 */
/* ----------------------------------------------------------------------- */
/* to be used only under 'if(globalContext.dfpRealMode==PPCHW)' */
#define dfpalIsNaN(rhs)  (((rhs).bytes[0] & 0x7C)==DECIMAL_NaN)
#define dfpalIsSNaN(rhs) (((rhs).bytes[0] & 0x7E)==DECIMAL_sNaN)
#define dfpalIsQNaN(rhs) (((rhs).bytes[0] & 0x7E)==DECIMAL_NaN)


/* -------------------------------------------------------------------- */
/* Exception, status and traps                                          */
/* -------------------------------------------------------------------- */

/* -------------------------- Status ---------------------------------- */
void dfpalClearAllStatusFlag()
{
  if(globalContext.dfpRealMode==PPCHW) {
    ppc_fpscr curFPSCR;
    /* read current FPSCR */
    curFPSCR.all.d=ppc_mffs();
    curFPSCR.all.fpscr.lower32 &= (DFPAL_FP_PPC_CLEAR_ALL);
    /* now, set new FPSCR */
    ppc_mtfs(curFPSCR.all.d);
  }
  else {
    dfpalThreadContext *thc=Tls_Load();
    (thc->dn64Context).status=(uInt)0;
    (thc->dn128Context).status=(uInt)0;
  }
}


void dfpalClearStatusFlag(dfpalflag_t mask)
{
  if(globalContext.dfpRealMode==PPCHW) {
    ppc_fpscr curFPSCR;
    dfpalflag_t clearWord=(mask & DFPAL_FP_ALL);
    /* read current FPSCR */
    curFPSCR.all.d=ppc_mffs();
    if (mask & DFPAL_FP_INVALID) {
      clearWord |= DFPAL_FP_INVALID_RC;
    }
    curFPSCR.all.fpscr.lower32&=(uInt)(~clearWord);
    /* now, set new FPSCR */
    ppc_mtfs(curFPSCR.all.d);
  }
  else {
    dfpalThreadContext *thc=Tls_Load();
    (thc->dn64Context).status&=(uInt)(~mask);
    (thc->dn128Context).status&=(uInt)(~mask);
  }
}


void dfpalSetStatusFlag(dfpalflag_t mask)
{
  if(globalContext.dfpRealMode==PPCHW) {
    ppc_fpscr curFPSCR;
    dfpalflag_t setWord=(mask & DFPAL_FP_ALL);
    /* read current FPSCR */
    curFPSCR.all.d=ppc_mffs();
    if (mask & DFPAL_FP_INVALID) {
      setWord |= DFPAL_FP_INVALID_VXSOFT;
    }
    curFPSCR.all.fpscr.lower32|=((uInt)setWord);
    /* now, set new FPSCR */
    ppc_mtfs(curFPSCR.all.d);
  }
  else {
    dfpalThreadContext *thc=Tls_Load();
    (thc->dn64Context).status|=((uInt)mask);
    (thc->dn128Context).status|=((uInt)mask);
  }
}


dfpalflag_t dfpalReadStatusFlag(void)
{
  if(globalContext.dfpRealMode==PPCHW) {
    ppc_fpscr curFPSCR;
    /* read current FPSCR */
    curFPSCR.all.d=ppc_mffs();
    return((dfpalflag_t)curFPSCR.all.fpscr.lower32);
  }
  else {
    dfpalThreadContext *thc=Tls_Load();
    /* return aggregate of both status flags */
    return((dfpalflag_t)((thc->dn64Context).status|(thc->dn128Context).status));
  }
}


#define Read_Clear_All_Status_Flag()                             \
{                                                                \
  ppc_fpscr curFPSCR;                                            \
  dfpalflag_t savedStatus;                                       \
                                                                 \
  curFPSCR.all.d=ppc_mffs();                                     \
  savedStatus=(dfpalflag_t)curFPSCR.all.fpscr.lower32;           \
  curFPSCR.all.fpscr.lower32=                                    \
    (curFPSCR.all.fpscr.lower32 & DFPAL_FP_PPC_CLEAR_ALL);       \
                                                                 \
  ppc_mtfs(curFPSCR.all.d);                                      \
  return(savedStatus);                                           \
}


dfpalflag_t dfpalReadClearAllStatusFlag()
{
  if (globalContext.dfpRealMode==PPCHW) {
    Read_Clear_All_Status_Flag()
  }
  else {
    dfpalflag_t sr;
    dfpalThreadContext *thc=Tls_Load();
    sr=(dfpalflag_t)((thc->dn64Context).status|(thc->dn128Context).status);
    (thc->dn64Context).status=(uInt)0;
    (thc->dn128Context).status=(uInt)0;
    return(sr);
  }
}


dfpalflag_t dfpalSwapStatusFlag(dfpalflag_t mask)
{
  if(globalContext.dfpRealMode==PPCHW) {
    ppc_fpscr curFPSCR;
    dfpalflag_t savedStatus;
    dfpalflag_t setWord=(mask & DFPAL_FP_ALL);

    if (mask == (dfpalflag_t)0x0) {
      Read_Clear_All_Status_Flag()
    }

    /* read current FPSCR */
    curFPSCR.all.d=ppc_mffs();
    savedStatus=(dfpalflag_t)curFPSCR.all.fpscr.lower32;

    if (mask & DFPAL_FP_INVALID) {  /* setting invalid */
      setWord |= DFPAL_FP_INVALID_VXSOFT;
    }

    curFPSCR.all.fpscr.lower32=
      (curFPSCR.all.fpscr.lower32 & (~(DFPAL_FP_ALL|DFPAL_FP_INVALID_RC))) 
      | setWord;
    /* now, set new FPSCR */
    ppc_mtfs(curFPSCR.all.d);
    return(savedStatus);
  }
  else {
    dfpalflag_t sr;
    dfpalThreadContext *thc=Tls_Load();
    sr=(dfpalflag_t)((thc->dn64Context).status|(thc->dn128Context).status);
    (thc->dn64Context).status=(uInt)mask;
    (thc->dn128Context).status=(uInt)mask;
    return(sr);
  }
}


/* -------------------------- Traps ----------------------------------- */
void dfpalEnableTrap(dfpaltrap_t mask)
{
  if(globalContext.dfpRealMode==PPCHW) {
    ppc_fpscr curFPSCR;
    /* read current FPSCR */
    curFPSCR.all.d=ppc_mffs();
    curFPSCR.all.fpscr.lower32|=(uInt)mask;
    /* now, set new FPSCR */
    ppc_mtfs(curFPSCR.all.d);
  }
  else {
    dfpalThreadContext *thc=Tls_Load();
    (thc->dn64Context).traps|=(uInt)mask;
    (thc->dn128Context).traps|=(uInt)mask;

    /* raise trap immediately if the exception is present, like hardware */
    if ( ((thc->dn64Context).status | (thc->dn128Context).status) 
      & (thc->dn64Context).traps) {
      raise(SIGFPE);
    }
  }
}


void dfpalEnableAllTrap(void)
{
  if(globalContext.dfpRealMode==PPCHW) {
    ppc_fpscr curFPSCR;
    /* read current FPSCR */
    curFPSCR.all.d=ppc_mffs();
    curFPSCR.all.fpscr.lower32|=(uInt)DFPAL_TRP_ALL;
    /* now, set new FPSCR */
    ppc_mtfs(curFPSCR.all.d);
  }
  else {
    dfpalThreadContext *thc=Tls_Load();
    (thc->dn64Context).traps|=(uInt)DFPAL_TRP_ALL;
    (thc->dn128Context).traps|=(uInt)DFPAL_TRP_ALL;

    /* raise trap immediately if the exception is present, like hardware */
    if ( ((thc->dn64Context).status | (thc->dn128Context).status)
      & (thc->dn64Context).traps) {
      raise(SIGFPE);
    }
  }
}


void dfpalDisableTrap(dfpaltrap_t mask)
{
  if(globalContext.dfpRealMode==PPCHW) {
    ppc_fpscr curFPSCR;
    /* read current FPSCR */
    curFPSCR.all.d=ppc_mffs();
    curFPSCR.all.fpscr.lower32&=(uInt)(~mask);
    /* now, set new FPSCR */
    ppc_mtfs(curFPSCR.all.d);
  }
  else {
    dfpalThreadContext *thc=Tls_Load();
    (thc->dn64Context).traps&=(uInt)(~mask);
    (thc->dn128Context).traps&=(uInt)(~mask);
  }
}

void dfpalDisableAllTrap(void)
{
  if(globalContext.dfpRealMode==PPCHW) {
    ppc_fpscr curFPSCR;
    /* read current FPSCR */
    curFPSCR.all.d=ppc_mffs();
    curFPSCR.all.fpscr.lower32&=(uInt)(~(DFPAL_TRP_ALL));
    /* now, set new FPSCR */
    ppc_mtfs(curFPSCR.all.d);
  }
  else {
    dfpalThreadContext *thc=Tls_Load();
    (thc->dn64Context).traps&=(uInt)(~(DFPAL_TRP_ALL));
    (thc->dn128Context).traps&=(uInt)(~(DFPAL_TRP_ALL));
  }
}


Flag dfpalAnyTrapEnabled(void)
{
  if(globalContext.dfpRealMode==PPCHW) {
    ppc_fpscr curFPSCR;
    /* read current FPSCR */
    curFPSCR.all.d=ppc_mffs();
    return((Flag)(curFPSCR.all.fpscr.lower32&((uInt)(DFPAL_TRP_ALL))));
  }
  else {
    dfpalThreadContext *thc=Tls_Load();
    return((Flag)((thc->dn64Context).traps&((uInt)DFPAL_TRP_ALL)));
  }
}


Flag dfpalTrapEnabled(dfpaltrap_t mask)
{
  if(globalContext.dfpRealMode==PPCHW) {
    ppc_fpscr curFPSCR;
    /* read current FPSCR */
    curFPSCR.all.d=ppc_mffs();
    return((Flag)(curFPSCR.all.fpscr.lower32&((uInt)mask)));
  }
  else {
    dfpalThreadContext *thc=Tls_Load();
    return((thc->dn64Context).traps&((uInt)mask));
  }
}

/* ----------------------- Rounding mode ------------------------------ */
dfpalrnd_t dfpalReadRoundingMode()
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_integer fpscr;
    fpscr.d=ppc_mffs();
    return (fpscr.fpscr.drm);
  }
  else {
    dfpalThreadContext *thc=Tls_Load();
    return((dfpalrnd_t)((thc->dn64Context).round));
  }
}


void dfpalSetRoundingMode(dfpalrnd_t r)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_integer fpscr;
    fpscr.fpscr.drm=r;
    ppc_mtfsf_drm(fpscr.d);
  }
  else {
    dfpalThreadContext *thc=Tls_Load();
    (thc->dn64Context).round=(enum rounding)r;
    (thc->dn128Context).round=(enum rounding)r;
  }
}

dfpalrnd_t dfpalSwapRoundingMode(dfpalrnd_t r)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfpalrnd_t crm;
    dfp_integer fpscr;
    fpscr.d=ppc_mffs();
    crm=(dfpalrnd_t)fpscr.fpscr.drm;

    fpscr.fpscr.drm=r;
    ppc_mtfsf_drm(fpscr.d);
    return(crm);
  }
  else {
    dfpalrnd_t crm;
    dfpalThreadContext *thc=Tls_Load();
    crm=(dfpalrnd_t)((thc->dn64Context).round);
    (thc->dn64Context).round=(enum rounding)r;
    (thc->dn128Context).round=(enum rounding)r;
    return(crm);
  }
}


uByte dfpalSetExponentClamp(uByte cl)
{
  if(globalContext.dfpRealMode==PPCHW) {
    /* no-op */
    return(cl);
  }
  else {
    uByte ccl;
    dfpalThreadContext *thc=Tls_Load();
    ccl=(dfpalrnd_t)((thc->dn64Context).clamp);
    (thc->dn64Context).clamp=cl;
    (thc->dn128Context).clamp=cl;
    return(ccl);
  }
}



/* -------------------------------------------------------------------- */
/* Double word arithmetic                                               */
/* -------------------------------------------------------------------- */

/* -------------------- utility functions ----------------------------- */
uInt dec64Sign(const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    return((uInt)(rhs.bytes[0]>>7));
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;
    decimal64ToNumber(&rhs, &dn);
    return (dn.bits & DECNEG );
  #else
    return((uInt)decDoubleIsSigned((decDouble *)&rhs));
  #endif

  }
}


uInt dec64Comb(const decimal64 rhs)
{
  if (DFPAL_LITTLE_ENDIAN) 
    return((uInt)((rhs.bytes[7] & 0x7c)>>2));
  else
    return((uInt)((rhs.bytes[0] & 0x7c)>>2));
}


uInt dec64ExpCon(const decimal64 rhs)
{
  if (DFPAL_LITTLE_ENDIAN) 
    return((uInt)(((rhs.bytes[7] & 0x03)<<6) | ((unsigned)rhs.bytes[6]>>2)));
  else
    return((uInt)(((rhs.bytes[0] & 0x03)<<6) | ((unsigned)rhs.bytes[1]>>2)));
}


Flag decimal64IsInfinite(const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    /* don't care for the sign, comparing with +ve is enough */
    return((Flag)((rhs.bytes[0] & 0x7C)==DECIMAL_Inf)); 
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;
    decimal64ToNumber(&rhs, &dn);
    return (dn.bits & DECINF);
  #else
    return((Flag)(decDoubleIsInfinite((decDouble *)&rhs)?1:0));
  #endif
  }
}

/* any NaN, qNaN or sNaN */
Flag decimal64IsNaN (const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    /* don't care for the sign, comparing with +ve is enough */
    return((Flag)((rhs.bytes[0] & DECIMAL_NaN)==DECIMAL_NaN)); 
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;
    decimal64ToNumber(&rhs, &dn);
    return (dn.bits & (DECNAN|DECSNAN));
  #else
    return((Flag)(decDoubleIsNaN((decDouble *)&rhs)?1:0));
  #endif
  }
}

Flag decimal64IsQNaN (const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    /* don't care for the sign, comparing with +ve is enough */
    return((Flag)((rhs.bytes[0] & 0x7E)==DECIMAL_NaN)); 
  }
  else {
    decNumber dn;
    decimal64ToNumber(&rhs, &dn);
    return (dn.bits & DECNAN);
  }
}

Flag decimal64IsSNaN (const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    /* don't care for the sign, comparing with +ve is enough */
    return((Flag)((rhs.bytes[0] & 0x7E)==DECIMAL_sNaN)); 
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;
    decimal64ToNumber(&rhs, &dn);
    return (dn.bits & DECSNAN);
  #else
    return((Flag)(decDoubleIsSignaling((decDouble *)&rhs)?1:0));
  #endif
  }
}


Flag decimal64IsNegative (const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    return((Flag)(rhs.bytes[0]>>7));
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;
    decimal64ToNumber(&rhs, &dn);
    return (dn.bits & DECNEG );
  #else
    return((Flag)(decDoubleIsSigned((decDouble *)&rhs)?1:0));
  #endif
  }
}

Flag decimal64IsZero (const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    if (dfpalIsSNaN(rhs)) return((Flag) 0); /* to avoid INVALID by dcmpu */
    return((Flag)(ppc_dcmpu(*((dfp_double *)&rhs), dfp_double_zero) & 
      DFPAL_COMP_EQ));
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;
    decimal64ToNumber(&rhs, &dn);
    return((Flag)decNumberIsZero(&dn));
  #else
    return((Flag)(decDoubleIsZero((decDouble *)&rhs)?1:0));
  #endif
  }
}

decimal64 decimal64Trim(const decimal64 rhs)
{
  /* no error possible, no need to call decNumber_For_PPC_XXX macro */
  decNumber rhsdn;
  decimal64 result64;
  decContext * dnctx = &(Tls_Load()->dn64Context);
  decimal64ToNumber(&rhs, &rhsdn);
  decNumberTrim(&rhsdn);
  decimal64FromNumber(&result64, &rhsdn, dnctx);
  return(result64);
}

decimal64 decimal64Zero()
{
  if(globalContext.dfpRealMode==PPCHW) {
    return(*((decimal64 *)&dfp_double_zero));
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber rhsdn;
    decimal64 result64;
    decContext * dnctx = &(Tls_Load()->dn64Context);
    decNumberZero(&rhsdn);
    decimal64FromNumber(&result64, &rhsdn, dnctx);
    return(result64);
  #else
    decimal64 zero64;
    decDoubleZero((decDouble *)&zero64);
    return(zero64);
  #endif
  }
}


/* ------------------------- field access ----------------------------- */
Int decimal64GetDigits(const decimal64 rhs)
{
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;
    decimal64ToNumber(&rhs, &dn);
    return ((Int) dn.digits);
  #else
    return((Int) decDoubleDigits((decDouble *)&rhs));
  #endif
}

Int decimal64GetExponent(const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dec64DataXchg xrhs;
    dfp_integer exp;
    xrhs.dsw=rhs;
    exp.d=ppc_dxex(xrhs.dhw);
    return((Int)(exp.sll-DFPAL_DECIMAL64_Bias));
  }
  else {
    decNumber dn;
    decimal64ToNumber(&rhs, &dn);
    
    if ((dn.bits & (DECNAN|DECSNAN|DECINF))!=0) {
      if ((dn.bits & DECINF)!=0)  dn.exponent=-(DFPAL_DECIMAL64_Bias+1);
      if ((dn.bits & DECNAN)!=0)  dn.exponent=-(DFPAL_DECIMAL64_Bias+2);
      if ((dn.bits & DECSNAN)!=0) dn.exponent=-(DFPAL_DECIMAL64_Bias+3);
    }
    return ((Int)dn.exponent);
  }
}

/* ------------------------- conversion ------------------------------- */
char *dfpal_decimal64ToString(const decimal64 rhs, char* result)
{
  #if defined(DFPAL_USE_DECFLOAT)
    return(decDoubleToString((decDouble *)&rhs, result));
  #else
    return(decimal64ToString(&rhs, result));
  #endif
}


decimal64 dfpal_decimal64FromString(const char *string)
{
  decimal64 result64;
  decContext workSet; /* temp context, interested in exceptions */
                      /* raised by decimal64FromString only     */
  decContext* dnctx= &(Tls_Load()->dn64Context);
  Set_decNumber_Context(dnctx);
  workSet = *dnctx;
  workSet.status=0;
  #if defined(DFPAL_USE_DECFLOAT)
    decDoubleFromString((decDouble *)&result64, string, &workSet);
  #else
    decimal64FromString(&result64, string, &workSet);
  #endif
  if ((workSet.status & (DEC_Overflow|DEC_Underflow)) != 0 ) {
    errno=ERANGE; /* to match with strtod() routine behavior */
  }
  dnctx->status |= workSet.status; /* copy status so that FPSCR can */
                                   /* be updated                    */
  Update_FPSCR_Status(dnctx);
  return(result64);
}

/* Note: For decimal32 <-> decimal64 conversion, POWER6 does not signal */
/* sNaN. sNaNs are copied quitely just like decNumber. On other hand    */
/* for decimal64 <-> deciml128 conversion, POWER6 signals sNaN. DFPAL   */
/* implements POWER6 behavior.                                          */
decimal64 decimal64FromDecimal32(const decimal32 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_single_parameter xin;
    dfp_double xres;

    xin.f.f=*((dfp_single *)&rhs);
    xres=ppc_dctdp(xin.d);
    return(*((decimal64 *)&xres));
  }
  else {
    decNumber rhsdn;
    decimal64 result64;
    decContext *dnctx = &(Tls_Load()->dn64Context);

    decimal32ToNumber(&rhs, &rhsdn);
    decimal64FromNumber(&result64, &rhsdn, dnctx);
    return(result64);
  }
}

decimal32 decimal64ToDecimal32(const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_single xres;
    dfp_single_parameter xout;

    xout.d=ppc_drsp(*((dfp_double *)&rhs));
    xres=xout.f.f;
    return(*((decimal32 *)&xres));
  }
  else {
    decNumber rhsdn;
    decimal32 result32;
    decContext set32, *dnctx;

    dnctx = &(Tls_Load()->dn64Context);

    decContextDefault(&set32, DEC_INIT_DECIMAL32);

    /* copy current rounding mode and traps */
    set32.round=dnctx->round;
    set32.traps=dnctx->traps;
    decimal64ToNumber(&rhs, &rhsdn);
    decimal32FromNumber(&result32, &rhsdn, &set32);

    /* copy status */
    dnctx->status |= set32.status;
    return(result32);
  }
}


decimal64 decimal64FromDecimal128(const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_double x64res;
    dfp_pad qwpad=0.0;
    x64res=ppc_drdpq(qwpad, *((dfp_quad *)&rhs));
    return(*((decimal64 *)&x64res));
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber rhsdn;
    decimal64 result64;
    decContext* dnctx= &(Tls_Load()->dn64Context);

    decimal128ToNumber(&rhs, &rhsdn);
    if (rhsdn.bits & DECSNAN) {
      rhsdn.bits ^= (DECSNAN|DECNAN);
      Set_decNumber_Status(dnctx, DEC_Invalid_operation)
    }
    decimal64FromNumber(&result64, &rhsdn, dnctx);
    return(result64);
  #else
    decimal64 result64;
    decContext* dnctx= &(Tls_Load()->dn64Context);
    decDoubleFromWider((decDouble *)&result64, (decQuad *)&rhs, dnctx);
    if (decQuadIsSignaling((decQuad *)&rhs) != 0) {
      Set_decNumber_Status(dnctx, DEC_Invalid_operation)
      decDoubleCopySign((decDouble *)&result64, &decDouble_quiet_NaN, 
        (decDouble *)&result64);
    }
    return(result64);
  #endif
  }
}


#if defined(DFPAL_INTEGER_XCPOFUF) /* { */

  Decimal64_To_Aint32_OU(Int, decimal64ToInt32, ppc_dctfix, sll,
    Long, dfpalSignedInt64FromNumber, DFPAL_INT32_MIN, DFPAL_INT32_MAX,
    ox, oe, ox, oe, DEC_Overflow, DEC_Overflow)
 
  Decimal64_To_Aint32_OU(uInt, decimal64ToUint32, ppc_dctfix, ull,
    uLong, dfpalUnsignedInt64FromNumber, DFPAL_UINT32_MIN, DFPAL_UINT32_MAX,
    ox, oe, ox, oe, DEC_Overflow, DEC_Overflow)

  Decimal_From_Aint32_64(64, decimal64FromInt32, Int, ppc_dcffix,
    sll, dfpalSignedInt64ToNumber)

  Decimal_From_Aint32_64(64, decimal64FromUint32, uInt, ppc_dcffix,
    ull, dfpalUnsignedInt64ToNumber)

#else /* } #if defined(DFPAL_INTEGER_XCPOFUF) { */
  Decimal64_To_Aint64(Long, decimal64ToInt64, ppc_dctfix, 
    sll, dfpalSignedInt64FromNumber)

  Decimal_To_Uint64(64)

  Decimal64_To_Aint32(Int, decimal64ToInt32, ppc_dctfix, sll,
    Long, dfpalSignedInt64FromNumber, DFPAL_INT32_MIN, DFPAL_INT32_MAX)
  
  Decimal64_To_Aint32(uInt, decimal64ToUint32, ppc_dctfix, sll, 
    uLong, dfpalUnsignedInt64FromNumber, DFPAL_UINT32_MIN, DFPAL_UINT32_MAX)

  /* p=16 for decimal64, signed long needs maximum of 19 digits, this */
  /* routine will round (rounding may set inexact) and set exponent   */
  /* if input is > 16 digits                                          */
  Decimal_From_Aint32_64(64, decimal64FromInt64, Long, ppc_dcffix_via_dcffixq,
    sll, dfpalSignedInt64ToNumber)

  Decimal_From_Uint64(64)

  Decimal_From_Aint32_64(64, decimal64FromInt32, Int, ppc_dcffix_via_dcffixq,
    sll, dfpalSignedInt64ToNumber)

  Decimal_From_Aint32_64(64, decimal64FromUint32, uInt, ppc_dcffix_via_dcffixq,
    ull, dfpalUnsignedInt64ToNumber)
#endif /* } #if defined(DFPAL_INTEGER_XCPOFUF) */

/* 754-1985 @6.2 Operations with NaNs: Every operation involving a   */
/* signaling NaN or invalid operation (7.1) shall, if no trap occurs */
/* and if a floating-point result is to be delivered, deliver a      */
/* quiet NaN as its result.                                          */

double decimal64ToDouble(const decimal64 rhs)
{
  double dblOut=0.0;
  decNumber dnTemp;
  char str64[DFPAL_DECIMAL64_STRLEN];
  ppc_fpscr savedFPSCR;

  decimal64ToNumber(&rhs, &dnTemp);

  if (globalContext.dfpRealMode==PPCHW) {
    Read_FPSCR(savedFPSCR);
    if (decNumberIsSNaN(&dnTemp))
    {
      dnTemp.bits ^= (DECNAN|DECSNAN);     /* change sNaN to NaN          */
                                           /* sNaN double is not portable */
      savedFPSCR.fpscr_attrib.vxsoft=1;
    }
    decNumberToString(&dnTemp, str64);
    errno=0;
    dblOut=strtod(str64, NULL);
    if (((HUGE_VAL==dblOut) || ((-HUGE_VAL)==dblOut)) && (ERANGE==errno)) {
      savedFPSCR.fpscr_attrib.ox=1;
    }
    else if ((0.0==dblOut) && (ERANGE==errno)) {
      savedFPSCR.fpscr_attrib.ux=1;
    }

    Restore_FPSCR(savedFPSCR);   /* this will trigger traps, if enabled */

    return(dblOut);
  }
  else {
    decContext *dnctx = &(Tls_Load()->dn64Context);
    if (decNumberIsSNaN(&dnTemp))
    {
      dnTemp.bits ^= (DECNAN|DECSNAN);       /* change sNaN to NaN          */
                                             /* sNaN double is not portable */
      Set_decNumber_Status(dnctx, DEC_Invalid_operation)
    }
    decNumberToString(&dnTemp, str64);
    errno=0;
    dblOut=strtod(str64, NULL);
    if (((HUGE_VAL==dblOut) || ((-HUGE_VAL)==dblOut)) && (ERANGE==errno)) {
      Set_decNumber_Status(dnctx, DEC_Overflow)
    }
    else if ((0.0==dblOut) && (ERANGE==errno)) {
      Set_decNumber_Status(dnctx, DEC_Underflow)
    }

    return(dblOut);
  }
}


decimal64 decimal64FromDouble(const double rhs)
{
  char dblStr[DFPAL_DOUBLE_STRLEN];
  decimal64 result64;
  decNumber dnTemp;
  decContext * dnctx = &(Tls_Load()->dn64Context);
  Set_decNumber_Context(dnctx);

  DFPAL_SNPRINTF(dblStr,DFPAL_DOUBLE_STRLEN,"%.*G",DFPAL_MAX_DOUBLE_PREC,rhs);
  decNumberFromString(&dnTemp, dblStr, dnctx);
  decimal64FromNumber(&result64, &dnTemp, dnctx);
  Update_FPSCR_Status(dnctx);
  return (result64);
}

uByte * decimal64ToPackedBCD(
  decimal64 rhs,     /* number to convert to        */
  uByte *bcdOut,     /* output: BCD array, must have storage allocated */
  Int length,        /* length of input (bcd) array */
  Int *scale)        /* output: scale of a number */
{
  if ( (length < ((DFPAL_DECIMAL64_Pmax/2)+1)) ||
    decimal64IsNaN(rhs) || decimal64IsInfinite(rhs) )
    return((uByte *) NULL);

  if(globalContext.dfpRealMode==PPCHW) {
    dec64DataXchg xrhs, xres, xshiftr;
    dfp_integer exp;
    xrhs.dsw=rhs;

    /* extract exponent and set scale */
    exp.d=ppc_dxex(xrhs.dhw);
    *scale=-((Int)(exp.sll-DFPAL_DECIMAL64_Bias));
    *((dfp_double *)(bcdOut+(length-8)))=ppc_ddedpd_c(xrhs.dhw);
                                                   /* conv 16 LSD nibbles */
    xshiftr.dhw=ppc_dscri_15(xrhs.dhw);            /* 15 >> rhs */
    xres.dhw=ppc_ddedpd_u(xshiftr.dhw);            /* unsigned conv. */
    bcdOut[length-9]=xres.bcd[7];                  /* last(MSD) nibble */

    return(bcdOut);
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;

    decimal64ToNumber(&rhs, &dn);
    return (decPackedFromNumber(bcdOut, length, scale, &dn));
  #else
    decDoubleToPacked((decDouble *)&rhs, scale, bcdOut);
    *scale=-*scale;
    return(bcdOut);
  #endif
  }
}


/* Notes:                                                                  */
/*   (1) Input array must be at least of DFPAL_DECIMAL64_BCDLEN bytes.     */
/*   (2) In case of input array larger than DFPAL_DECIMAL64_BCDLEN bytes   */
/*   digit nibbles must be right aligned, e.g. 12 bytes array with 4       */
/*   digits should look like 0x00 ... (7 times) ... 0x00 0x01 0x23 0x4c    */
/*   (3) In case of array larger than DFPAL_DECIMAL64_BCDLEN bytes         */
/*   there should not be more than DFPAL_DECIMAL64_Pmax digits present     */
/*   because of the effect of rounding is not same with hardware and       */
/*   software.                                                             */
decimal64 decimal64FromPackedBCD (
  uByte *bcdin,      /* input: BCD array to convert. Input must be */
                     /* at least DFPAL_DECIMAL64_BCDLEN long and   */
                     /* right aligned (in case of input arrary     */
                     /* larger than DFPAL_DECIMAL64_BCDLEN)        */
  Int length,        /* length of input (bcd) array                */
  Int scale)         /* scale: exponent will be derived from this  */
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_double lsd, res;
    dfp_integer newexp;
    dfp_double tmplsd, tmpmsd;
    uByte msd[8]={0};
    uByte *bcdPtr=bcdin;

    if (length < DFPAL_DECIMAL64_BCDLEN) {
      return(dec64_quiet_NaN);
    }

    #if defined(DFPAL_OS_LOP)
      /* Align input variable for possible use of prctl(PR_SET_ALIGN...) */
      dfp_integer_in_double locdid;
      memcpy ((char*)&locdid, (char *)(bcdPtr+(length-8)),sizeof(locdid));
      tmplsd=ppc_dendpd_s( locdid );
    #else
      tmplsd=ppc_dendpd_s( *((dfp_integer_in_double *)(bcdPtr+(length-8))) );
    #endif

    newexp.sll=DFPAL_DECIMAL64_Bias+(-scale);  
    lsd=ppc_diex(newexp.d, tmplsd);
    if (bcdPtr[length-DFPAL_DECIMAL64_BCDLEN]=='\0') 
      return (*((decimal64 *)&lsd)); /* fast-path */

    msd[7]=(bcdPtr[length-DFPAL_DECIMAL64_BCDLEN]<<4)|(bcdPtr[length-1]&0x0F);
                                                            /* insert sign */
    tmpmsd=ppc_dendpd_s(*((dfp_integer_in_double *)msd));
    newexp.sll+=15;     /* append 15 0's */
    tmpmsd=ppc_diex(newexp.d, tmpmsd);
    res=ppc_dadd(tmpmsd, lsd);
    return(*((decimal64 *)&res));
  }
  else {
    decNumber   dn;
    decNumber *dnErr;
    decimal64 result64;
    decContext * dnctx = &(Tls_Load()->dn64Context);

    if (length < DFPAL_DECIMAL64_BCDLEN) {
      dn.bits=DECNAN;
      dn.digits=0;
      dn.exponent=0;
      decimal64FromNumber(&result64, &dn, dnctx);
      return (result64);
    }

  #if !defined(DFPAL_USE_DECFLOAT)
    result64=decimal64Zero();
    dnErr=decPackedToNumber(bcdin, length, &scale, &dn);
    if(dnErr==NULL) {
      return(result64);
    }
    decimal64FromNumber(&result64, &dn, dnctx);
    return (result64);
  #else
    d64Err=(decimal64 *)
      decDoubleFromPackedChecked((decDouble *)&result64, -scale, bcdin);
    if(d64Err==NULL) {
      return(*((decimal64 *)&decDouble_zero));
    }
    return (result64);
  #endif
  }
}


/* ------------------------- arithmetic ------------------------------- */
decimal64 decimal64Abs(const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    decimal64 result64=rhs;
    result64.bytes[0] &= ((uByte)0x7F);
    return(result64);
  }
  else {
    /* do not use decNumberAbs(), it generates INVALID for sNaN */
    decimal64 result64;
    decNumber rhsdn;
    decimal64ToNumber(&rhs, &rhsdn);
    rhsdn.bits &= ~DECNEG;
    decimal64FromNumber(&result64, &rhsdn, &(Tls_Load()->dn64Context));
    return(result64);
  }
}


Double_Word_Function2(Add,lhs,rhs,ppc_dadd)

decimal64 decimal64Compare(const decimal64 lhs, const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    compare_result cmp;
    dfp_double xres;                                  
  
    cmp=ppc_dcmpu(*((dfp_double *)&lhs),*((dfp_double *)&rhs));
    if(cmp&DFPAL_COMP_EQ)      xres=dfp_double_zero;
    else if(cmp&DFPAL_COMP_LT) xres=dfp_double_negative_one;
    else if(cmp&DFPAL_COMP_GT) xres=dfp_double_positive_one;
    else                       xres=dfp_double_quiet_NaN;
  
    return(*((decimal64 *)&xres));
  }
  else {
    dec_Arithmetic2(64,Double,Compare,lhs,rhs);
  }
}

#define PPC_Compare_Total64(lhssw, rhssw, cmpOut)                            \
  {                                                                          \
    dec64DataXchg xlhs, xrhs;                                                \
    uByte cmpwLHS=0, cmpwRHS=0;                                              \
    ppc_fpscr savedFPSCR;                                                    \
                                                                             \
    xlhs.dsw=(lhssw);                                                        \
    xrhs.dsw=(rhssw);                                                        \
    savedFPSCR.all.d=ppc_mffs();                                             \
    (cmpOut)=ppc_dcmpu((xlhs.dhw), (xrhs.dhw));                              \
    if ((cmpOut) & (DFPAL_COMP_UO|DFPAL_COMP_EQ)) {                          \
      cmpwLHS=(lhssw).bytes[0] & 0x80;                                       \
      cmpwRHS=(rhssw).bytes[0] & 0x80;                                       \
      if (cmpwLHS < cmpwRHS) { (cmpOut)=DFPAL_COMP_GT; }                     \
      else if (cmpwLHS > cmpwRHS) { (cmpOut)=DFPAL_COMP_LT; }                \
      else { /* same sign; need to compare exponent for finites */           \
        if ((cmpOut) == DFPAL_COMP_EQ) { /* finite numbers (incl Inf) */     \
          (cmpOut)=ppc_dtstex((xlhs.dhw), (xrhs.dhw));                       \
        }                                                                    \
        else { /* must be DFPAL_COMP_UO, i.e. at least one {s|q}NaN     */   \
          /* sign are same, just like decNumber treat as +ve and negate */   \
          /* +0 < +finites < +Infinity < +sNaN < +NaN                   */   \
          (cmpOut)=DFPAL_COMP_EQ;                                            \
          if (! dfpalIsNaN((lhssw))) { (cmpOut)=DFPAL_COMP_LT; }             \
          else if (! dfpalIsNaN((rhssw))) { (cmpOut)=DFPAL_COMP_GT; }        \
          /* both are NaNs */                                                \
          else if (dfpalIsSNaN((lhssw)) && dfpalIsQNaN((rhssw)))             \
            { (cmpOut)=DFPAL_COMP_LT; }                                      \
          else if (dfpalIsQNaN((lhssw)) && dfpalIsSNaN((rhssw)))             \
            { (cmpOut)=DFPAL_COMP_GT; }                                      \
        }                                                                    \
        if (cmpwLHS) {                                                       \
          if ((cmpOut)==DFPAL_COMP_LT) { (cmpOut)=DFPAL_COMP_GT; }           \
          else if ((cmpOut)==DFPAL_COMP_GT) { (cmpOut)=DFPAL_COMP_LT; }      \
        }                                                                    \
      }                                                                      \
    }                                                                        \
    /* restore FPSCR, sNaN may have caused INVALID */                        \
    ppc_mtfs(savedFPSCR.all.d);                                              \
  }

compare_result decimal64Cmpop(const decimal64 lhs, 
                              const decimal64 rhs, const Flag op)
{
  if(globalContext.dfpRealMode==PPCHW) {
    compare_result cmpres;

    if(op==DFPAL_COMP_ORDERED) {
      PPC_Compare_Total64(lhs,rhs, cmpres)
      return(cmpres);
    }
    else if(op==DFPAL_COMP_UNORDERED) {
      return(ppc_dcmpu(*((dfp_double *)&lhs),*((dfp_double *)&rhs)));
    }
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber lhsdn, rhsdn, resultdn;                             
    decContext * dnctx = &(Tls_Load()->dn64Context);             
    decimal64ToNumber(&(lhs),&lhsdn);                  
    decimal64ToNumber(&(rhs),&rhsdn);                  
    if(op==DFPAL_COMP_ORDERED) {
      decNumberCompareTotal(&resultdn, &lhsdn, &rhsdn, dnctx);           
    }
    else if(op==DFPAL_COMP_UNORDERED) {
      decNumberCompare(&resultdn, &lhsdn, &rhsdn, dnctx);           
    }

    if(decNumberIsNaN(&resultdn)) return(DFPAL_COMP_UO);
    if(decNumberIsNegative(&resultdn)) return(DFPAL_COMP_LT);
    if(decNumberIsZero(&resultdn)) return(DFPAL_COMP_EQ);

    /* must be +ve number (+1) */
    return(DFPAL_COMP_GT);
  #else
    decDouble result64;
    decContext * dnctx = &(Tls_Load()->dn64Context);
    if(op==DFPAL_COMP_ORDERED) {
      decDoubleCompareTotal(&result64, (decDouble *)&lhs, 
        (decDouble *)&rhs);
    }
    else if(op==DFPAL_COMP_UNORDERED) {
      decDoubleCompare(&result64, (decDouble *)&lhs, 
        (decDouble *)&rhs, dnctx);
    }

    if(decDoubleIsNaN(&result64)) return(DFPAL_COMP_UO);
    if(decDoubleIsSigned(&result64)) return(DFPAL_COMP_LT);
    if(decDoubleIsZero(&result64)) return(DFPAL_COMP_EQ);

    /* must be +ve number (+1) */
    return(DFPAL_COMP_GT);
  #endif
  }
  
  /* not reached, some compilers issue warning without this */
  return(DFPAL_COMP_GT);
}


#define decDoubleCompareTotal(rr,x,y,set) decDoubleCompareTotal(rr,x,y)
decimal64 decimal64CompareTotal(const decimal64 lhs, const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    compare_result cmpres;
    dec64DataXchg xres;

    PPC_Compare_Total64(lhs, rhs, cmpres);
    if (cmpres==DFPAL_COMP_GT)      xres.dhw=dfp_double_positive_one;
    else if (cmpres==DFPAL_COMP_LT) xres.dhw=dfp_double_negative_one;
    else                            xres.dhw=dfp_double_zero;

    return(xres.dsw);
  }
  else {
    dec_Arithmetic2(64,Double,CompareTotal,lhs,rhs)
  }
}
#undef decDoubleCompareTotal

Double_Word_Function2(Divide,lhs,rhs,ppc_ddiv)

#define dfpalIsInfinite(a) (((a).bytes[0] & 0x7C)==DECIMAL_Inf)
#define dfpalIsSpecial(a)  (((a).bytes[0] & 0x78)==0x78)
decimal64 decimal64DivideInteger(const decimal64 lhs, const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dec64DataXchg xlhs, xrhs, xres, xdiv;
    ppc_fpscr savedFPSCR, afterDivFPSCR;
    xlhs.dsw=lhs;
    xrhs.dsw=rhs;

    Read_Clear_FPSCR_Set_RM(savedFPSCR, DFP_ROUND_TOWARD_ZERO)
    xdiv.dhw=ppc_ddiv(xlhs.dhw, xrhs.dhw);
    Read_FPSCR(afterDivFPSCR);
    savedFPSCR.fpscr_attrib.vxsoft=afterDivFPSCR.fpscr_attrib.vx;
                                     /* only INVALID exception from divide */
    if ((!afterDivFPSCR.fpscr_attrib.ox) &&
      dfpalIsInfinite(xdiv.dsw)) {   /* abort if result is +/-Inf w/o OF   */
      savedFPSCR.fpscr_attrib.zx=afterDivFPSCR.fpscr_attrib.zx;
      Restore_FPSCR(savedFPSCR);
      return(xdiv.dsw);
    }
    Restore_FPSCR(savedFPSCR);

    xres.dhw=ppc_dquai_rtz_0(xdiv.dhw);

    if (! savedFPSCR.fpscr_attrib.xx) {
      Read_FPSCR(savedFPSCR);
      savedFPSCR.fpscr_attrib.xx=0;
      Restore_FPSCR(savedFPSCR);
    }

    return(xres.dsw); 
  }
  else
  {
    dec_Arithmetic2(64,Double,DivideInteger,lhs,rhs)
  }
}


decimal64 decimal64Exp(const decimal64 rhs) 
{
  decNumber_For_PPC_Arithmetic1(64,Exp,rhs)
}

decimal64 decimal64Ln(const decimal64 rhs)
{
  decNumber_For_PPC_Arithmetic1(64,Ln,rhs)
}

decimal64 decimal64Log10(const decimal64 rhs)
{
  decNumber_For_PPC_Arithmetic1(64,Log10,rhs)
}


#define Max_Min_NaN(lhs, rhs, rtnNaN)                               \
{                                                                   \
  if (dfpalIsSNaN(lhs) || dfpalIsSNaN(rhs)) return(rtnNaN);         \
  if (!dfpalIsNaN(lhs)) return(lhs);                                \
  if (!dfpalIsNaN(rhs)) return(rhs);                                \
  return(rtnNaN);                                                   \
}
   
decimal64 decimal64Max(const decimal64 lhs, const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dec64DataXchg xlhs, xrhs;
    compare_result compr;
    xlhs.dsw=lhs;
    xrhs.dsw=rhs;
    compr=ppc_dcmpu(xlhs.dhw,xrhs.dhw);

    if(compr==DFPAL_COMP_LT) return(rhs);
    if(compr==DFPAL_COMP_UO) Max_Min_NaN(lhs, rhs, dec64_quiet_NaN);
    
    return(lhs);
  }
  else {
    dec_Arithmetic2(64,Double,Max,lhs,rhs)
  }
}


decimal64 decimal64Min(const decimal64 lhs, const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dec64DataXchg xlhs, xrhs;
    compare_result compr;
    xlhs.dsw=lhs;
    xrhs.dsw=rhs;
    compr=ppc_dcmpu(xlhs.dhw,xrhs.dhw);

    if(compr==DFPAL_COMP_GT) return(rhs);
    if(compr==DFPAL_COMP_UO) Max_Min_NaN(lhs, rhs, dec64_quiet_NaN);

    return(lhs);
  }
  else {
    dec_Arithmetic2(64,Double,Min,lhs,rhs)
  }
}


decimal64 decimal64Minus(const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    decimal64 res=rhs;
    res.bytes[0] ^= (uByte)0x80;
    return(res);
  }
  else {
    /*decNumber_Arithmetic1(64,Minus,rhs)*/
    /* do not use decNumberMinus(), it sets INVALID for sNaN */
    decimal64 result64;
    decNumber rhsdn;
    decimal64ToNumber(&rhs, &rhsdn);
    rhsdn.bits ^= DECNEG;
    decimal64FromNumber(&result64, &rhsdn, &(Tls_Load()->dn64Context));
    return(result64);
  }
}


Double_Word_Function2(Multiply,lhs,rhs,ppc_dmul)

decimal64 decimal64Normalize(const decimal64 rhs)
{
  decNumber_For_PPC_Arithmetic1(64,Normalize,rhs)
}


decimal64 decimal64Plus(const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    /* essentially, no-op */
    decimal64 res=rhs;
    res.bytes[0] |= (uByte)0x00;
    return(res);
  }
  else {
    /*decNumber_Arithmetic1(64,Plus,rhs);*/
    /* do not use decNumberMinus(), it sets INVALID for sNaN */
    decimal64 result64;
    decNumber rhsdn;
    decimal64ToNumber(&rhs, &rhsdn);
    rhsdn.bits |= (uByte)0x00;
    decimal64FromNumber(&result64, &rhsdn, &(Tls_Load()->dn64Context));
    return(result64);
  }
}

decimal64 decimal64Power(const decimal64 lhs, const decimal64 rhs)
{
  decNumber_For_PPC_Arithmetic2(64,Power,lhs,rhs)
}


decimal64 decimal64PowerInt(const decimal64 lhs, const Int rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_integer irhs;
    dec64DataXchg xres;

    irhs.sll=rhs;
    xres.dhw=ppc_dcffix_via_dcffixq(irhs.d);
    return(decimal64Power(lhs, xres.dsw));
  }
  else {
    decNumber rhsdn, lhsdn, resdn;
    decimal64 result64;
    decContext * dnctx = &(Tls_Load()->dn64Context);
    
    decimal64ToNumber(&lhs,&lhsdn);
    dfpalSignedInt64ToNumber(&rhsdn, (Long)rhs, dnctx);
    decNumberPower(&resdn, &lhsdn, &rhsdn, dnctx);  
    decimal64FromNumber(&result64, &resdn, dnctx);
    return (result64);
  }
}

decimal64 decimal64Quantize(const decimal64 lhs, const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_double xres;
    xres=ppc_dqua_rfpscr(*((dfp_double *)&rhs), *((dfp_double *)&lhs));  
                                                   /* note transposed */
                                                   /* argument order  */
    return(*((decimal64 *)&xres));
  }
  else {
    dec_Arithmetic2(64,Double,Quantize,lhs,rhs)
  }
}

decimal64 decimal64Remainder(const decimal64 lhs, const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_double xlhs, xrhs, xres;
    decimal64 res;
    dfp_double rdiv;
    dfp_pad qwpad=0.0;
    ppc_fpscr savedFPSCR, afterDivFPSCR;

    if ( (!dfpalIsSpecial(lhs)) && dfpalIsInfinite(rhs) ) {
      return(lhs);
    }

    xlhs=*((dfp_double *)&lhs);
    xrhs=*((dfp_double *)&rhs);

    /* set rounding mode to zero */
    Read_FPSCR_Set_RM(savedFPSCR, DFP_ROUND_TOWARD_ZERO)
    rdiv=ppc_ddiv(xlhs, xrhs);

    xres=ppc_drdpq(qwpad, ppc_dsubq(qwpad, ppc_dctqpq(xlhs), 
           ppc_dmulq(qwpad, ppc_dctqpq(xrhs), 
           ppc_dctqpq(ppc_dquai_rtz_0(rdiv)))));

    Read_FPSCR(afterDivFPSCR);
    savedFPSCR.fpscr_attrib.vxsoft=afterDivFPSCR.fpscr_attrib.vx;
    Restore_FPSCR(savedFPSCR);  /* Nothing but INVLID exception. Restore RM */

    res=*((decimal64 *)&xres);
    if (ppc_dcmpu(xres, dfp_double_zero) & DFPAL_COMP_EQ) {
      res.bytes[0] |= (lhs.bytes[0] & (uByte)0x80);
    }
    return(res);
  }
  else {
    dec_Arithmetic2(64,Double,Remainder,lhs,rhs)
/*
    decimal64 result64;
    decNumber lhsdn, rhsdn, resultdn, cmpresdn, zerodn;
    decContext * dnctx = &(Tls_Load()->dn64Context);
    decimal64ToNumber(&lhs,&lhsdn);
    decimal64ToNumber(&rhs,&rhsdn);
    decNumberRemainder(&resultdn, &lhsdn, &rhsdn, dnctx);
    decNumberZero(&zerodn);
    decNumberCompare(&cmpresdn, &resultdn, &zerodn, dnctx);
    if (!decNumberIsNaN(&cmpresdn) && decNumberIsZero(&cmpresdn)) {
      resultdn.bits |= (lhsdn.bits & DECNEG);
    }
    decimal64FromNumber(&result64, &resultdn, dnctx);
    return(result64);
*/
  }
}

decimal64 decimal64RemainderNear(const decimal64 lhs, const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_double xlhs, xrhs, xres, rdiv;
    decimal64 res;
    dfp_pad qwpad=0.0;
    ppc_fpscr savedFPSCR, afterDivFPSCR;

    if ( (!dfpalIsSpecial(lhs)) && dfpalIsInfinite(rhs) ) {
      return(lhs);
    }

    xlhs=*((dfp_double *)&lhs);
    xrhs=*((dfp_double *)&rhs);

    /* set rounding mode to nearest with tie to even  */
    Read_FPSCR_Set_RM(savedFPSCR, DFP_ROUND_TO_NEAREST_WITH_TIES_TO_EVEN)
    rdiv=ppc_ddiv(xlhs, xrhs);

    xres=ppc_drdpq(qwpad, ppc_dsubq(qwpad, ppc_dctqpq(xlhs),
           ppc_dmulq(qwpad, ppc_dctqpq(xrhs),
           ppc_dctqpq(ppc_dquai_rne_0(rdiv)))));

    Read_FPSCR(afterDivFPSCR);
    savedFPSCR.fpscr_attrib.vxsoft=afterDivFPSCR.fpscr_attrib.vx;
    Restore_FPSCR(savedFPSCR);  /* Nothing but INVLID exception. Restore RM */

    res=*((decimal64 *)&xres);
    if (ppc_dcmpu(xres, dfp_double_zero) & DFPAL_COMP_EQ) {
      res.bytes[0] |= (lhs.bytes[0] & (uByte)0x80);
    }
    return(res);
  }
  else {
    dec_Arithmetic2(64,Double,RemainderNear,lhs,rhs)
/*
    decimal64 result64;
    decNumber lhsdn, rhsdn, resultdn, cmpresdn, zerodn;
    decContext * dnctx = &(Tls_Load()->dn64Context);
    decimal64ToNumber(&lhs,&lhsdn);
    decimal64ToNumber(&rhs,&rhsdn);
    decNumberRemainderNear(&resultdn, &lhsdn, &rhsdn, dnctx);
    decNumberZero(&zerodn);
    decNumberCompare(&cmpresdn, &resultdn, &zerodn, dnctx);
    if (!decNumberIsNaN(&cmpresdn) && decNumberIsZero(&cmpresdn)) {
      resultdn.bits |= (lhsdn.bits & DECNEG);
    }
    decimal64FromNumber(&result64, &resultdn, dnctx);
    return(result64);
*/
  }
}


decimal64 decimal64Rescale(const decimal64 lhs, const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    ppc_fpscr beforeFPSCR, afterFPSCR, clearINEXACT;
    dec64DataXchg xlhs, xrhs, xres;
    dfp_double tmp64;
    dfp_integer ires;

    xlhs.dsw=lhs;
    xrhs.dsw=rhs;

    if (dfpalIsSpecial(lhs) || dfpalIsSpecial(rhs)) {
      xres.dhw=ppc_dqua_rfpscr(xrhs.dhw, xlhs.dhw);
      return(xres.dsw);
    }

    /* read current FPSCR and clear INEXACT if it is set */
    beforeFPSCR.all.d=ppc_mffs();
    if (beforeFPSCR.fpscr_attrib.xx) {
      clearINEXACT=beforeFPSCR;
      clearINEXACT.fpscr_attrib.xx=0;
      ppc_mtfs(clearINEXACT.all.d);
    }

    /* convert decimal64 -> singed long long */
    ires.d=ppc_dctfix(xrhs.dhw);

    afterFPSCR.all.d=ppc_mffs();
    if (afterFPSCR.fpscr_attrib.xx ||
      (ires.sll > DFPAL_DECIMAL64_Emax) || 
      (ires.sll < (DFPAL_DECIMAL64_Emin-DFPAL_DECIMAL64_Pmax+1))) {
      /* input scale was non-integer or out of bound */

      /* set invalid into pre-FPSCR */
      beforeFPSCR.fpscr_attrib.vxsoft=1;

      /* restore FPSCR */
      ppc_mtfs(beforeFPSCR.all.d);

      return(dec64_quiet_NaN);
    }

    /* restore FPSCR, we need to care about status bit set by */
    /* following operations                                   */
    ppc_mtfs(beforeFPSCR.all.d);

    /* Insert required exponent into '1' */
    if (ires.sll <= (DFPAL_DECIMAL64_Emax-DFPAL_DECIMAL64_Pmax+1)) {
      ires.sll+=DFPAL_DECIMAL64_Bias;
      tmp64=ppc_diex(ires.d, dfp_double_positive_one);
    }
    else {
      tmp64=ppc_dscli_15(dfp_double_positive_one);
      ires.sll+=DFPAL_DECIMAL64_Bias;
      ires.sll-=(DFPAL_DECIMAL64_Pmax-1);
      tmp64=ppc_diex(ires.d, tmp64);
    }

    /* quantize with new exponent */
    xres.dhw=ppc_dqua_rfpscr(tmp64, xlhs.dhw);

    return(xres.dsw);
  }
  else {
    decNumber_Arithmetic2(64,Rescale,lhs,rhs)
  }
}

decimal64 decimal64SameQuantum(const decimal64 lhs, const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_double xres;

    if(ppc_dtstex(*((dfp_double *)&lhs), *((dfp_double *)&rhs))&DFPAL_COMP_EQ)
      xres=dfp_double_positive_one;
    else
      xres=dfp_double_zero;

    return(*((decimal64 *)&xres));
  }
  else {
    decimal64 result64;
    decNumber lhsdn, rhsdn, resultdn;
    decContext * dnctx = &(Tls_Load()->dn64Context);
    decimal64ToNumber(&lhs,&lhsdn);
    decimal64ToNumber(&rhs,&rhsdn);
    decNumberSameQuantum(&resultdn, &lhsdn, &rhsdn);
    decimal64FromNumber(&result64, &resultdn, dnctx);
    return(result64);
  }
}

decimal64 decimal64SquareRoot(const decimal64 rhs)
{
  decNumber_For_PPC_Arithmetic1(64,SquareRoot,rhs)
}

Double_Word_Function2(Subtract,lhs,rhs,ppc_dsub)

decimal64 decimal64ToIntegralValue(const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_double xres;
    xres=ppc_drintn_rfpscr(*((dfp_double *)&rhs));
    return(*((decimal64 *)&xres));
  }
  else {
    #if !defined(DFPAL_USE_DECFLOAT)
      decNumber_Arithmetic1(64,ToIntegralValue,rhs)
    #else
      decDouble resultDouble;
      decContext * dnctx = &(Tls_Load()->dn64Context);
      decDoubleToIntegralValue(&resultDouble, (decDouble *)&rhs, dnctx,
        dnctx->round);
      return(*((decimal64 *)&resultDouble));
    #endif
  }
}

decimal64 decimal64Ceil(const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dec64DataXchg xrhs, xres;
    xrhs.dsw=rhs;
    xres.dhw=ppc_drintn_rtpi(xrhs.dhw);
    return(xres.dsw);
  }
  else {
    dec_To_Integer_RM(64, Double, DFPAL_ROUND_TOWARD_POSITIVE_INFINITY, rhs);
  }
}

decimal64 decimal64Floor(const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dec64DataXchg xrhs, xres;
    xrhs.dsw=rhs;
    xres.dhw=ppc_drintn_rtmi(xrhs.dhw);
    return(xres.dsw);
  }
  else {
    dec_To_Integer_RM(64, Double, DFPAL_ROUND_TOWARD_NEGATIVE_INFINITY, rhs);
  }
}

/* -------------------------------------------------------------------- */
/* Quad word arithmetic                                                 */
/* -------------------------------------------------------------------- */

/* -------------------- utility functions ----------------------------- */
uInt dec128Sign(const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    return((uInt)rhs.bytes[0]>>7);
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;
    decimal128ToNumber(&rhs, &dn);
    return (dn.bits & DECNEG );
  #else
    return((uInt)decQuadIsSigned((decQuad *)&rhs));
  #endif
  }
}


uInt dec128Comb(const decimal128 rhs)
{
  if (DFPAL_LITTLE_ENDIAN)
    return((uInt)((rhs.bytes[15] & 0x7c)>>2));
  else
    return((uInt)((rhs.bytes[0] & 0x7c)>>2));
}


uInt dec128ExpCon(const decimal128 rhs)
{
  if (DFPAL_LITTLE_ENDIAN) {
    return((uInt)(((rhs.bytes[15] & 0x03)<<10) | 
      ((unsigned)rhs.bytes[14]<<2) | 
      ((unsigned)rhs.bytes[13]>>6)));
  }
  else {
    return((uInt)(((rhs.bytes[0] & 0x03)<<10) |
      ((unsigned)rhs.bytes[1]<<2) |
      ((unsigned)rhs.bytes[2]>>6)));
  }
}


Flag decimal128IsInfinite(const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    /* don't care for the sign, comparing with +ve is enough */
    return((Flag)((rhs.bytes[0] & 0x7C)==DECIMAL_Inf));
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;
    decimal128ToNumber(&rhs, &dn);
    return (dn.bits & DECINF);
  #else
    return((Flag)(decQuadIsInfinite((decQuad *)&rhs)?1:0));
  #endif
  }
}


Flag decimal128IsNaN (const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    /* don't care for the sign, comparing with +ve is enough */
    return((Flag)((rhs.bytes[0] & DECIMAL_NaN)==DECIMAL_NaN));
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;
    decimal128ToNumber(&rhs, &dn);
    return (dn.bits & (DECNAN|DECSNAN));
  #else
    return((Flag)(decQuadIsNaN((decQuad *)&rhs)?1:0));
  #endif

  }
}


Flag decimal128IsQNaN (const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    /* don't care for the sign, comparing with +ve is enough */
    return((Flag)((rhs.bytes[0] & 0x7E)==DECIMAL_NaN));
  }
  else {
    decNumber dn;
    decimal128ToNumber(&rhs, &dn);
    return (dn.bits & DECNAN);
  }
}

Flag decimal128IsSNaN (const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    /* don't care for the sign, comparing with +ve is enough */
    return((Flag)((rhs.bytes[0] & 0x7E)==DECIMAL_sNaN));
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;
    decimal128ToNumber(&rhs, &dn);
    return (dn.bits & DECSNAN);
  #else
    return((Flag)(decQuadIsSignaling((decQuad *)&rhs)?1:0));
  #endif
  }
}


Flag decimal128IsNegative (const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    return((Flag)(rhs.bytes[0]>>7));
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;
    decimal128ToNumber(&rhs, &dn);
    return (dn.bits & DECNEG);
  #else
    return((Flag)(decQuadIsSigned((decQuad *)&rhs)?1:0));
  #endif
  }
}

Flag decimal128IsZero (const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_pad qwpad=0.0;
    if (dfpalIsSNaN(rhs)) return((Flag) 0); /* to avoid INVALID by dcmpu */
    return((Flag)(ppc_dcmpuq(qwpad,*((dfp_quad *)&rhs), dfp_quad_zero) & 
      DFPAL_COMP_EQ));
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;
    decimal128ToNumber(&rhs, &dn);
    return((Flag)decNumberIsZero(&dn));
  #else
    return((Flag)(decQuadIsZero((decQuad *)&rhs)?1:0));
  #endif
  }
}

decimal128 decimal128Trim(const decimal128 rhs)
{
  /* no error possible, no need to call decNumber_For_PPC_XXX macro */
  decimal128 result128;
  decNumber rhsdn;
  decContext * dnctx = &(Tls_Load()->dn128Context);

  decimal128ToNumber(&rhs, &rhsdn);
  decNumberTrim(&rhsdn);
  decimal128FromNumber(&result128, &rhsdn, dnctx);
  return(result128);
}

decimal128 decimal128Zero()
{
  if(globalContext.dfpRealMode==PPCHW) {
    return(*((decimal128 *)&dfp_quad_zero));
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber rhsdn;
    decimal128 result128;
    decContext * dnctx = &(Tls_Load()->dn128Context);
    decNumberZero(&rhsdn);
    decimal128FromNumber(&result128, &rhsdn, dnctx);
    return(result128);
  #else
    decimal128 zero128;
    decQuadZero((decQuad *)&zero128);
    return(zero128);
  #endif
  }
}


/* ------------------------- field access ----------------------------- */
Int decimal128GetDigits(const decimal128 rhs)
{
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;
    decimal128ToNumber(&rhs, &dn);
    return((Int)dn.digits);
  #else
    return((Int)decQuadDigits((decQuad *)&rhs));
  #endif
}

Int decimal128GetExponent(const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dec128DataXchg xrhs; 
    dfp_integer exp;
    dfp_pad qwpad=0.0;

    xrhs.dsw=rhs;
    exp.d=ppc_dxexq(qwpad, xrhs.dhw);
    return((Int)(exp.sll-DFPAL_DECIMAL128_Bias));
  }
  else {
    decNumber dn;
    decimal128ToNumber(&rhs, &dn);

    if ((dn.bits & (DECNAN|DECSNAN|DECINF))!=0) {
      if ((dn.bits & DECINF)!=0)  dn.exponent=-(DFPAL_DECIMAL128_Bias+1);
      if ((dn.bits & DECNAN)!=0)  dn.exponent=-(DFPAL_DECIMAL128_Bias+2);
      if ((dn.bits & DECSNAN)!=0) dn.exponent=-(DFPAL_DECIMAL128_Bias+3);
    }

    return ((Int) dn.exponent);
  }
}


/* ------------------------- conversion ------------------------------- */
char *dfpal_decimal128ToString(const decimal128 rhs, char* result)
{
  #if defined(DFPAL_USE_DECFLOAT)
    return(decQuadToString((decQuad *)&rhs, result));
  #else
    return(decimal128ToString(&rhs, result));
  #endif
}


decimal128 dfpal_decimal128FromString(const char *string)
{
  decimal128 result128;
  decContext workSet; /* temp context, interested in exceptions */
                      /* raised by decimal128FromString only    */
  decContext* dnctx=&(Tls_Load()->dn128Context);
  Set_decNumber_Context(dnctx);
  workSet = *dnctx;
  workSet.status=0;
  #if defined(DFPAL_USE_DECFLOAT)
    decQuadFromString((decQuad *)&result128, string, &workSet);
  #else
    decimal128FromString(&result128, string, &workSet);
  #endif
  if ((workSet.status & (DEC_Overflow|DEC_Underflow)) != 0) {
    errno=ERANGE; /* to match with strtod() routine behavior */
  }
  dnctx->status |= workSet.status; /* copy status so that FPSCR can */
                                   /* be updated                    */
  Update_FPSCR_Status(dnctx);
  return(result128);
}


decimal128 decimal128FromDecimal64(const decimal64 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_quad x128res;
    x128res=ppc_dctqpq(*((dfp_double *)&rhs));
    return(*((decimal128 *)&x128res));
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber rhsdn;
    decimal128 result128;
    decContext* dnctx=&(Tls_Load()->dn128Context);

    decimal64ToNumber(&rhs, &rhsdn);
    if (rhsdn.bits & DECSNAN) {
      rhsdn.bits ^= (DECSNAN|DECNAN);
      Set_decNumber_Status(dnctx, DEC_Invalid_operation)
    }
    decimal128FromNumber(&result128, &rhsdn, dnctx);
    return(result128);
  #else
    decimal128 result128;
    decContext* dnctx=&(Tls_Load()->dn128Context);
    decDoubleToWider((decDouble *)&rhs, (decQuad *)&result128);
    if (decDoubleIsSignaling((decDouble *)&rhs) != 0) {
      Set_decNumber_Status(dnctx, DEC_Invalid_operation)
      decQuadCopySign((decQuad *)&result128, &decQuad_quiet_NaN,
        (decQuad *)&result128);
    }
    return(result128);
  #endif
  }
}


#if defined(DFPAL_INTEGER_XCPOFUF) /* { */

  Decimal128_To_Aint32_OU(Int, decimal128ToInt32, ppc_dctfixq, sll,
    Long, dfpalSignedInt64FromNumber, DFPAL_INT32_MIN, DFPAL_INT32_MAX,
    ox, oe, ox, oe, DEC_Overflow, DEC_Overflow)

  Decimal128_To_Aint32_OU(uInt, decimal128ToUint32, ppc_dctfixq, ull,
    uLong, dfpalUnsignedInt64FromNumber, DFPAL_UINT32_MIN, DFPAL_UINT32_MAX,
    ox, oe, ox, oe, DEC_Overflow, DEC_Overflow)

  Decimal_From_Aint32_64(128, decimal128FromInt32, Int, ppc_dcffixq,
    sll, dfpalSignedInt64ToNumber)

  Decimal_From_Aint32_64(128, decimal128FromUint32, uInt, ppc_dcffixq,
    ull, dfpalUnsignedInt64ToNumber)
#else /* } #if defined(DFPAL_INTEGER_XCPOFUF) { */
  Decimal128_To_Aint64(Long, decimal128ToInt64, ppc_dctfixq,
    sll, dfpalSignedInt64FromNumber)

  Decimal_To_Uint64(128)

  Decimal128_To_Aint32(Int, decimal128ToInt32, ppc_dctfixq, sll,
    Long, dfpalSignedInt64FromNumber, DFPAL_INT32_MIN, DFPAL_INT32_MAX)

  Decimal128_To_Aint32(uInt, decimal128ToUint32, ppc_dctfixq, sll,
    uLong, dfpalUnsignedInt64FromNumber, DFPAL_UINT32_MIN, DFPAL_UINT32_MAX)

  Decimal_From_Aint32_64(128, decimal128FromInt64, Long, ppc_dcffixq,
    sll, dfpalSignedInt64ToNumber)

  Decimal_From_Uint64(128)

  Decimal_From_Aint32_64(128, decimal128FromInt32, Int, ppc_dcffixq,
    sll, dfpalSignedInt64ToNumber)

  Decimal_From_Aint32_64(128, decimal128FromUint32, uInt, ppc_dcffixq,
    ull, dfpalUnsignedInt64ToNumber)
#endif /* } #if defined(DFPAL_INTEGER_XCPOFUF) */


double decimal128ToDouble(const decimal128 rhs)
{
  double dblOut=0.0;
  decNumber dnTemp;
  char str128[DFPAL_DECIMAL128_STRLEN];
  ppc_fpscr savedFPSCR;

  decimal128ToNumber(&rhs, &dnTemp);

  if (globalContext.dfpRealMode==PPCHW) {
    Read_FPSCR(savedFPSCR);
    if (decNumberIsSNaN(&dnTemp))
    {
      dnTemp.bits ^= (DECNAN|DECSNAN);     /* change sNaN to NaN          */
                                           /* sNaN double is not portable */
      savedFPSCR.fpscr_attrib.vxsoft=1;
    }
    decNumberToString(&dnTemp, str128);
    errno=0;
    dblOut=strtod(str128, NULL);
    if (((HUGE_VAL==dblOut) || ((-HUGE_VAL)==dblOut)) && (ERANGE==errno)) {
      savedFPSCR.fpscr_attrib.ox=1;
    }
    else if ((0.0==dblOut) && (ERANGE==errno)) {
      savedFPSCR.fpscr_attrib.ux=1;
    }

    Restore_FPSCR(savedFPSCR);   /* this will trigger traps, if enabled */

    return(dblOut);
  }
  else {
    decContext *dnctx = &(Tls_Load()->dn128Context);
    if (decNumberIsSNaN(&dnTemp))
    {
      dnTemp.bits ^= (DECNAN|DECSNAN);       /* change sNaN to NaN          */
                                             /* sNaN double is not portable */
      Set_decNumber_Status(dnctx, DEC_Invalid_operation)
    }
    decNumberToString(&dnTemp, str128);
    errno=0;
    dblOut=strtod(str128, NULL);
    if (((HUGE_VAL==dblOut) || ((-HUGE_VAL)==dblOut)) && (ERANGE==errno)) {
      Set_decNumber_Status(dnctx, DEC_Overflow)
    }
    else if ((0.0==dblOut) && (ERANGE==errno)) {
      Set_decNumber_Status(dnctx, DEC_Underflow)
    }

    return(dblOut);
  }
}


decimal128 decimal128FromDouble(const double rhs)
{
  char dblStr[DFPAL_DOUBLE_STRLEN];
  decimal128 result128;
  decNumber dnTemp;
  decContext * dnctx = &(Tls_Load()->dn128Context);
  Set_decNumber_Context(dnctx);

  DFPAL_SNPRINTF(dblStr,DFPAL_DOUBLE_STRLEN,"%.*G",DFPAL_MAX_DOUBLE_PREC,rhs);
  decNumberFromString(&dnTemp, dblStr, dnctx);
  decimal128FromNumber(&result128, &dnTemp, dnctx);
  Update_FPSCR_Status(dnctx);
  return (result128);
}

uByte * decimal128ToPackedBCD(
  decimal128 rhs,    /* number to convert to        */
  uByte *bcdOut,     /* output: BCD array, must have storage allocated */
  Int length,        /* length of input (bcd) array */
  Int *scale)        /* output: scale of a number */
{
  if ( (length < ((DFPAL_DECIMAL128_Pmax/2)+1)) ||
    decimal128IsNaN(rhs) || decimal128IsInfinite(rhs) )
    return((uByte *) NULL);

  if(globalContext.dfpRealMode==PPCHW) {
    dec128DataXchg xrhs, xres = {0}, xshiftr;
    dfp_integer exp;
    dfp_pad qwpad=0.0;

    xrhs.dsw=rhs;

    /* extract exponent and set scale */
    exp.d=ppc_dxexq(qwpad, xrhs.dhw);
    *scale=-((Int)(exp.sll-DFPAL_DECIMAL128_Bias));
    *((dfp_quad *)(bcdOut+(length-16)))=ppc_ddedpdq_c(qwpad, xrhs.dhw);
                                                   /* conv 32 LSD nibbles */
    xshiftr.dhw=ppc_dscriq_31(qwpad, xrhs.dhw);    /* 31 >> rhs */
    xres.dhw=ppc_ddedpdq_u(qwpad, xshiftr.dhw);    /* unsigned conv. */
    bcdOut[length-17]=xres.bcd[15];                /* last 3(MSD) nibbles */
    bcdOut[length-18]=xres.bcd[14];                /*  ...                */

    return(bcdOut);
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber dn;

    decimal128ToNumber(&rhs, &dn);
    return (decPackedFromNumber(bcdOut, length, scale, &dn));
  #else
    decQuadToPacked((decQuad *)&rhs, scale, bcdOut);
    *scale=-*scale;
    return(bcdOut);
  #endif
  }
}


/* Notes:                                                                  */
/*   (1) Input array must be at least of DFPAL_DECIMAL128_BCDLEN bytes.    */
/*   (2) In case of input array larger than DFPAL_DECIMAL128_BCDLEN bytes  */
/*   digit nibbles must be right aligned, e.g. 22 bytes array with 4       */
/*   digits should look like 0x00 ... (17 times) ... 0x00 0x01 0x23 0x4c   */
/*   (3) In case of array larger than DFPAL_DECIMAL128_BCDLEN bytes,       */
/*   there should not be more than DFPAL_DECIMAL128_Pmax digits present    */
/*   because of the effect of rounding is not same with hardware and       */
/*   software.                                                             */
decimal128 decimal128FromPackedBCD (
  uByte *bcdin,      /* input: BCD array to convert                */
                     /* at least DFPAL_DECIMAL128_BCDLEN long and  */
                     /* right aligned (in case of input arrary     */
                     /* larger than DFPAL_DECIMAL128_BCDLEN)       */
  Int length,        /* length of input (bcd) array                */
  Int scale)         /* scale: exponent will be derived from this  */
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_quad lsd, res;
    dfp_pad qwpad=0.0;
    dfp_integer newexp;
    dfp_quad tmplsd, tmpmsd;
    uByte msd[16]={0};
    uByte *bcdPtr=bcdin;

    if (length < DFPAL_DECIMAL128_BCDLEN) {
      return(dec128_quiet_NaN);
    }

    #if defined(DFPAL_OS_LOP)
      /* Align input variable for possible use of prctl(PR_SET_ALIGN...) */
      dfp_integer_pair_in_quad locdid;
      memcpy ((char*)&locdid, (char *)(bcdPtr+(length-16)),sizeof(locdid));
      tmplsd=ppc_dendpdq_s(qwpad, locdid );
    #else
      tmplsd=ppc_dendpdq_s(qwpad, 
        *((dfp_integer_pair_in_quad *)(bcdPtr+(length-16))));
    #endif

    newexp.sll=DFPAL_DECIMAL128_Bias+(-scale);
    lsd=ppc_diexq(newexp.d, tmplsd);
    if (bcdPtr[length-17]=='\0' && bcdPtr[length-18]=='\0')
      return (*((decimal128 *)&lsd)); /* fast-path */

    msd[15]=(bcdPtr[length-17]<<4)|(bcdPtr[length-1]&0x0F);  /* insert sign */
    msd[14]=(bcdPtr[length-17]>>4)|(bcdPtr[length-18]<<4); /* combine 33&34 */
    tmpmsd=ppc_dendpdq_s(qwpad, *((dfp_integer_pair_in_quad *)msd));
    newexp.sll+=31;     /* append 31 0's */
    tmpmsd=ppc_diexq(newexp.d, tmpmsd);
    res=ppc_daddq(qwpad, tmpmsd, lsd);
    return(*((decimal128 *)&res));
  }
  else {
    decNumber   dn;
    decNumber *dnErr;
    decimal128 result128;
    decContext * dnctx = &(Tls_Load()->dn128Context);

    if (length < DFPAL_DECIMAL128_BCDLEN) {
      dn.bits=DECNAN;
      dn.digits=0;
      dn.exponent=0;
      decimal128FromNumber(&result128, &dn, dnctx);
      return (result128);
    }

  #if !defined(DFPAL_USE_DECFLOAT)
    result128=decimal128Zero();
    dnErr=decPackedToNumber(bcdin, length, &scale, &dn);
    if(dnErr==NULL) {
      return(result128);
    }
    decimal128FromNumber(&result128, &dn, dnctx);
    return (result128);
  #else
    decimal128 *d128Err;
    d128Err=(decimal128 *)
      decQuadFromPackedChecked((decQuad *)&result128, -scale, bcdin);
    if(d128Err==NULL) {
      return(*((decimal128 *)&decQuad_zero));
    }
    return (result128);
  #endif
  }
}


/* ------------------------- arithmetic ------------------------------- */
decimal128 decimal128Abs(const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    decimal128 result128=rhs;
    result128.bytes[0] &= ((uByte)0x7F);
    return(result128);
  }
  else {
    /* do not use decNumberAbs(), it sets INVALID for sNaN */
    decimal128 result128;
    decNumber rhsdn;
    decimal128ToNumber(&rhs, &rhsdn);
    rhsdn.bits &= ~DECNEG;
    decimal128FromNumber(&result128, &rhsdn, &(Tls_Load()->dn128Context));
    return(result128);
  }
}


Quad_Word_Function2(Add,lhs,rhs,ppc_daddq)

decimal128 decimal128Compare(const decimal128 lhs, const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_pad qwpad=0.0;
    compare_result cmp;
    dfp_quad xres;                                  
  
    cmp=ppc_dcmpuq(qwpad,*((dfp_quad *)&lhs),*((dfp_quad *)&rhs));
    if(cmp&DFPAL_COMP_EQ)      xres=dfp_quad_zero;
    else if(cmp&DFPAL_COMP_LT) xres=dfp_quad_negative_one;
    else if(cmp&DFPAL_COMP_GT) xres=dfp_quad_positive_one;
    else                       xres=dfp_quad_quiet_NaN;
  
    return(*((decimal128 *)&xres));
  }
  else {
    dec_Arithmetic2(128,Quad,Compare,lhs,rhs);
  }
}


#define PPC_Compare_Total128(lhssw, rhssw, cmpOut)                           \
  {                                                                          \
    dec128DataXchg xlhs, xrhs;                                               \
    uByte cmpwLHS=0, cmpwRHS=0;                                              \
    dfp_pad mqwpad=0.0;                                                      \
    ppc_fpscr savedFPSCR;                                                    \
                                                                             \
    xlhs.dsw=(lhssw);                                                        \
    xrhs.dsw=(rhssw);                                                        \
    savedFPSCR.all.d=ppc_mffs();                                             \
    (cmpOut)=ppc_dcmpuq(mqwpad, (xlhs.dhw), (xrhs.dhw));                     \
    if ((cmpOut) & (DFPAL_COMP_UO|DFPAL_COMP_EQ)) {                          \
      cmpwLHS=(lhssw).bytes[0] & 0x80;                                       \
      cmpwRHS=(rhssw).bytes[0] & 0x80;                                       \
      if (cmpwLHS < cmpwRHS) { (cmpOut)=DFPAL_COMP_GT; }                     \
      else if (cmpwLHS > cmpwRHS) { (cmpOut)=DFPAL_COMP_LT; }                \
      else { /* same sign; need to compare exponent for finites */           \
        if ((cmpOut) == DFPAL_COMP_EQ) { /* finite numbers (incl Inf) */     \
          (cmpOut)=ppc_dtstexq(mqwpad, (xlhs.dhw), (xrhs.dhw));              \
        }                                                                    \
        else { /* must be DFPAL_COMP_UO, i.e. at least one {s|q}NaN     */   \
          /* sign are same, just like decNumber treat as +ve and negate */   \
          /* +0 < +finites < +Infinity < +sNaN < +NaN                   */   \
          (cmpOut)=DFPAL_COMP_EQ;                                            \
          if (! dfpalIsNaN((lhssw))) { (cmpOut)=DFPAL_COMP_LT; }             \
          else if (! dfpalIsNaN((rhssw))) { (cmpOut)=DFPAL_COMP_GT; }        \
          /* both are NaNs */                                                \
          else if (dfpalIsSNaN((lhssw)) && dfpalIsQNaN((rhssw)))             \
            { (cmpOut)=DFPAL_COMP_LT; }                                      \
          else if (dfpalIsQNaN((lhssw)) && dfpalIsSNaN((rhssw)))             \
            { (cmpOut)=DFPAL_COMP_GT; }                                      \
        }                                                                    \
        if (cmpwLHS) {                                                       \
          if ((cmpOut)==DFPAL_COMP_LT) { (cmpOut)=DFPAL_COMP_GT; }           \
          else if ((cmpOut)==DFPAL_COMP_GT) { (cmpOut)=DFPAL_COMP_LT; }      \
        }                                                                    \
      }                                                                      \
    }                                                                        \
    /* restore FPSCR, sNaN may have caused INVALID */                        \
    ppc_mtfs(savedFPSCR.all.d);                                              \
  }


compare_result decimal128Cmpop(const decimal128 lhs, 
                              const decimal128 rhs, const Flag op)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_pad qwpad=0.0;
    compare_result cmpres;

    if(op==DFPAL_COMP_ORDERED) {
      PPC_Compare_Total128(lhs, rhs, cmpres);
      return(cmpres);
    }
    else if(op==DFPAL_COMP_UNORDERED) {
      return(ppc_dcmpuq(qwpad,*((dfp_quad *)&lhs),*((dfp_quad *)&rhs)));
    }
  }
  else {
  #if !defined(DFPAL_USE_DECFLOAT)
    decNumber lhsdn, rhsdn, resultdn;                             
    decContext * dnctx = &(Tls_Load()->dn128Context);            
    decimal128ToNumber(&(lhs),&lhsdn);                  
    decimal128ToNumber(&(rhs),&rhsdn);                  
    if(op==DFPAL_COMP_ORDERED) {
      decNumberCompareTotal(&resultdn, &lhsdn, &rhsdn, dnctx);           
    }
    else if(op==DFPAL_COMP_UNORDERED) {
      decNumberCompare(&resultdn, &lhsdn, &rhsdn, dnctx);           
    }

    if(decNumberIsNaN(&resultdn)) return(DFPAL_COMP_UO);
    if(decNumberIsNegative(&resultdn)) return(DFPAL_COMP_LT);
    if(decNumberIsZero(&resultdn)) return(DFPAL_COMP_EQ);

    /* must be +ve number (+1) */
    return(DFPAL_COMP_GT);
  #else
    decimal128 result128;                                   
    decQuad result128;
    decContext * dnctx = &(Tls_Load()->dn128Context);
    if(op==DFPAL_COMP_ORDERED) {
      decQuadCompareTotal(&result128, (decQuad *)&lhs, (decQuad *)&rhs);
    }
    else if(op==DFPAL_COMP_UNORDERED) {
      decQuadCompare(&result128, (decQuad *)&lhs, (decQuad *)&rhs, dnctx);
    }

    if(decQuadIsNaN(&result128)) return(DFPAL_COMP_UO);
    if(decQuadIsSigned(&result128)) return(DFPAL_COMP_LT);
    if(decQuadIsZero(&result128)) return(DFPAL_COMP_EQ);

    /* must be +ve number (+1) */
    return(DFPAL_COMP_GT);
  #endif
  }

  /* not reached, some compilers issue warning without this */
  return(DFPAL_COMP_GT);
}


#define decQuadCompareTotal(rr,x,y,set) decQuadCompareTotal(rr,x,y)
decimal128 decimal128CompareTotal(const decimal128 lhs, const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    compare_result cmpres;
    dec128DataXchg xres;

    PPC_Compare_Total128(lhs, rhs, cmpres);
    if (cmpres==DFPAL_COMP_GT)      xres.dhw=dfp_quad_positive_one;
    else if (cmpres==DFPAL_COMP_LT) xres.dhw=dfp_quad_negative_one;
    else                            xres.dhw=dfp_quad_zero;

    return(xres.dsw);
  }
  else {
    dec_Arithmetic2(128,Quad,CompareTotal,lhs,rhs)
  }
}
#undef decQuadCompareTotal

Quad_Word_Function2(Divide,lhs,rhs,ppc_ddivq)

decimal128 decimal128DivideInteger(const decimal128 lhs, const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dec128DataXchg xlhs, xrhs, xres, xdiv;
    ppc_fpscr savedFPSCR, afterDivFPSCR;
    dfp_pad qwpad=0.0;

    xlhs.dsw=lhs;
    xrhs.dsw=rhs;

    Read_Clear_FPSCR_Set_RM(savedFPSCR, DFP_ROUND_TOWARD_ZERO)
    xdiv.dhw=ppc_ddivq(qwpad, xlhs.dhw, xrhs.dhw);
    Read_FPSCR(afterDivFPSCR);
    savedFPSCR.fpscr_attrib.vxsoft=afterDivFPSCR.fpscr_attrib.vx;
                                     /* only INVALID exception from divide */
    if ((!afterDivFPSCR.fpscr_attrib.ox) &&
      dfpalIsInfinite(xdiv.dsw)) {   /* abort if result is +/-Inf w/o OF   */
      savedFPSCR.fpscr_attrib.zx=afterDivFPSCR.fpscr_attrib.zx;
      Restore_FPSCR(savedFPSCR);
      return(xdiv.dsw);
    }
    Restore_FPSCR(savedFPSCR);

    xres.dhw=ppc_dquaiq_rtz_0(qwpad, xdiv.dhw);

    if (! savedFPSCR.fpscr_attrib.xx) {
      Read_FPSCR(savedFPSCR);
      savedFPSCR.fpscr_attrib.xx=0;
      Restore_FPSCR(savedFPSCR);
    }

    return(xres.dsw); 
  }
  else
  {
    dec_Arithmetic2(128,Quad,DivideInteger,lhs,rhs)
  }
}


decimal128 decimal128Exp(const decimal128 rhs) 
{
  decNumber_For_PPC_Arithmetic1(128,Exp,rhs)
}

decimal128 decimal128Ln(const decimal128 rhs)
{
  decNumber_For_PPC_Arithmetic1(128,Ln,rhs)
}

decimal128 decimal128Log10(const decimal128 rhs)
{
  decNumber_For_PPC_Arithmetic1(128,Log10,rhs)
}


decimal128 decimal128Max(const decimal128 lhs, const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_pad qwpad=0.0;
    dec128DataXchg xlhs, xrhs;
    compare_result compr;
    xlhs.dsw=lhs;
    xrhs.dsw=rhs;
    compr=ppc_dcmpuq(qwpad,xlhs.dhw,xrhs.dhw);

    if(compr==DFPAL_COMP_LT) return(rhs);
    if(compr==DFPAL_COMP_UO) Max_Min_NaN(lhs, rhs, dec128_quiet_NaN);
    
    return(lhs);
  }
  else {
    dec_Arithmetic2(128,Quad,Max,lhs,rhs)
  }
}


decimal128 decimal128Min(const decimal128 lhs, const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_pad qwpad=0.0;
    dec128DataXchg xlhs, xrhs;
    compare_result compr;
    xlhs.dsw=lhs;
    xrhs.dsw=rhs;
    compr=ppc_dcmpuq(qwpad,xlhs.dhw,xrhs.dhw);

    if(compr==DFPAL_COMP_GT) return(rhs);
    if(compr==DFPAL_COMP_UO) Max_Min_NaN(lhs, rhs, dec128_quiet_NaN);
    
    return(lhs);
  }
  else {
    dec_Arithmetic2(128,Quad,Min,lhs,rhs)
  }
}


decimal128 decimal128Minus(const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    decimal128 res=rhs;
    res.bytes[0] ^= (uByte)0x80;
    return(res);
  }
  else {
    /*decNumber_Arithmetic1(128,Minus,rhs)*/
    /* do not use decNumberMinus(), it sets INVALID for sNaN */
    decimal128 result128;
    decNumber rhsdn;
    decimal128ToNumber(&rhs, &rhsdn);
    rhsdn.bits ^= DECNEG;
    decimal128FromNumber(&result128, &rhsdn, &(Tls_Load()->dn128Context));
    return(result128);
  }
}


Quad_Word_Function2(Multiply,lhs,rhs,ppc_dmulq)

decimal128 decimal128Normalize(const decimal128 rhs)
{
  decNumber_For_PPC_Arithmetic1(128,Normalize,rhs)
}


decimal128 decimal128Plus(const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    /* essentially, no-op */
    decimal128 res=rhs;
    res.bytes[0] |= (uByte)0x00;
    return(res);
  }
  else {
    /*decNumber_Arithmetic1(128,Plus,rhs);*/
    /* do not use decNumberPlus(), it sets INVALID for sNaN */
    decimal128 result128;
    decNumber rhsdn;
    decimal128ToNumber(&rhs, &rhsdn);
    rhsdn.bits |= 0x00;
    decimal128FromNumber(&result128, &rhsdn, &(Tls_Load()->dn128Context));
    return(result128);
  }
}

decimal128 decimal128Power(const decimal128 lhs, const decimal128 rhs)
{
  decNumber_For_PPC_Arithmetic2(128,Power,lhs,rhs)
}


decimal128 decimal128PowerInt(const decimal128 lhs, const Int rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_integer irhs;
    dec128DataXchg xres;

    irhs.sll=rhs;
    xres.dhw=ppc_dcffixq(irhs.d);
    return(decimal128Power(lhs, xres.dsw));
  }
  else {
    decNumber rhsdn;
    decNumber lhsdn;
    decNumber resdn;
    decimal128 result128;
    decContext * dnctx = &(Tls_Load()->dn128Context);
    
    decimal128ToNumber(&lhs,&lhsdn);
    dfpalSignedInt64ToNumber(&rhsdn, (Long)rhs, dnctx);
    decNumberPower(&resdn, &lhsdn, &rhsdn, dnctx);  
    decimal128FromNumber(&result128, &resdn, dnctx);
    return (result128);
  }
}

decimal128 decimal128Quantize(const decimal128 lhs, const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_quad xres;
    dfp_pad qwpad=0.0;
    xres=ppc_dquaq_rfpscr(qwpad, *((dfp_quad *)&rhs), *((dfp_quad *)&lhs)); 
                                                 /* note transposed */
                                                 /* argument order */
    return(*((decimal128 *)&xres));
  }
  else {
    dec_Arithmetic2(128,Quad,Quantize,lhs,rhs)
  }

}


decimal128 decimal128Remainder(const decimal128 lhs, const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dec128DataXchg xlhs, xrhs, xres, xintq;
    dfp_quad rdiv, splitQ, intqH, intqL, rhsH, rhsL, rhse0, rhs1e;
    dfp_integer expIntq, expRhs, iexp;
    ppc_fpscr savedFPSCR, afterDivFPSCR;
    dfp_significance numDigits;
    dfp_pad qwpad=0.0;

    if ( (!dfpalIsSpecial(lhs)) && dfpalIsInfinite(rhs) ) {
      return(lhs);
    }

    xlhs.dsw=lhs;
    xrhs.dsw=rhs;

    /* set rounding mode to zero */
    Read_FPSCR_Set_RM(savedFPSCR, DFP_ROUND_TOWARD_ZERO)
    rdiv=ppc_ddivq(qwpad, xlhs.dhw, xrhs.dhw);

  #if defined(DFPAL_FAST_REM128)
    xres.dhw=ppc_dsubq(qwpad, xlhs.dhw,
               ppc_dmulq(qwpad, xrhs.dhw,
               ppc_dquaiq_rtz_0(qwpad, rdiv)));
  #else
    /* takes care of rounding in intermediate multiply operation      */
    /* As a last resort, split quotient and rhs each into two numbers */

    do {
      xintq.dhw=ppc_dquaiq_rtz_0(qwpad, rdiv);
      if (dfpalIsQNaN(xintq.dsw)) {
        xres.dhw=xintq.dhw;
        break;
      }

      numDigits.ull=(uLong)(DFPAL_DECIMAL128_Pmax/2);

      if ((ppc_dtstsfq(numDigits.d, xrhs.dhw) & 
          ppc_dtstsfq(numDigits.d, xintq.dhw) & 
          (DFPAL_COMP_GT|DFPAL_COMP_EQ)) ||
          (ppc_dcmpuq(qwpad, xintq.dhw, dfp_quad_zero) & DFPAL_COMP_EQ)) {
          xres.dhw=ppc_dsubq(qwpad, xlhs.dhw,
                     ppc_dmulq(qwpad, xrhs.dhw, xintq.dhw));
        break;
      }
  
      /* split Quotient */
      expIntq.sll = DFPAL_DECIMAL128_Bias + (DFPAL_DECIMAL128_Pmax/2); 
      splitQ=ppc_diexq(expIntq.d, dfp_quad_positive_one);
      intqH=ppc_dquaq_rtz(qwpad, splitQ, xintq.dhw);
      intqL=ppc_dsubq(qwpad, xintq.dhw, intqH);
  
      /* split RHS after setting exp=0. Otherwise NaN will result, if e>6111 */
      expRhs.d=ppc_dxexq(qwpad, xrhs.dhw); /* save exponent before setting 0 */
      iexp.sll=DFPAL_DECIMAL128_Bias;
      rhse0=ppc_diexq(iexp.d, xrhs.dhw);
      /* split rhse0, i.e. rhs with exp=0 */
      iexp.sll= DFPAL_DECIMAL128_Bias + DFPAL_DECIMAL128_Pmax/2;
      splitQ=ppc_diexq(iexp.d, dfp_quad_positive_one);
      rhsH=ppc_dquaq_rtz(qwpad, splitQ, rhse0);
      rhsL=ppc_dsubq(qwpad, rhse0, rhsH);

      rhs1e=ppc_diexq(expRhs.d, dfp_quad_positive_one);

      /* R = N - Q * D                                             */
      /*   = ( ( N - Qh * Dh ) - (Ql * Dh + Qh * Dl) ) - (Ql * Dl) */
      xres.dhw=ppc_dsubq(qwpad, ppc_dsubq(qwpad,
        ppc_dsubq(qwpad, xlhs.dhw, ppc_dmulq(qwpad, rhs1e, 
        ppc_dmulq(qwpad, intqH, rhsH))),
        ppc_daddq(qwpad, ppc_dmulq(qwpad, rhs1e, 
        ppc_dmulq(qwpad, intqL, rhsH)), ppc_dmulq(qwpad, rhs1e, 
        ppc_dmulq(qwpad, intqH, rhsL)))),
        ppc_dmulq(qwpad, rhs1e, ppc_dmulq(qwpad, intqL, rhsL)));
      } while(0);
    #endif
    Read_FPSCR(afterDivFPSCR);
    savedFPSCR.fpscr_attrib.vxsoft=afterDivFPSCR.fpscr_attrib.vx;
    Restore_FPSCR(savedFPSCR);  /* Nothing but INVLID exception. Restore RM */

    if (ppc_dcmpuq(qwpad, xres.dhw, dfp_quad_zero) & DFPAL_COMP_EQ) {
      xres.dsw.bytes[0] |= (lhs.bytes[0] & (uByte)0x80);
    }

    return(xres.dsw); 
  }
  else {
    dec_Arithmetic2(128,Quad,Remainder,lhs,rhs)
/*
    decimal128 result128;
    decNumber lhsdn, rhsdn, resultdn, cmpresdn, zerodn;
    decContext * dnctx = &(Tls_Load()->dn128Context);
    decimal128ToNumber(&lhs,&lhsdn);
    decimal128ToNumber(&rhs,&rhsdn);
    decNumberRemainder(&resultdn, &lhsdn, &rhsdn, dnctx);
    decNumberZero(&zerodn);
    decNumberCompare(&cmpresdn, &resultdn, &zerodn, dnctx);
    if (!decNumberIsNaN(&cmpresdn) && decNumberIsZero(&cmpresdn)) {
      resultdn.bits |= (lhsdn.bits & DECNEG);
    }
    decimal128FromNumber(&result128, &resultdn, dnctx);
    return(result128);
*/
  }
}

decimal128 decimal128RemainderNear(const decimal128 lhs, const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dec128DataXchg xlhs, xrhs, xres, xintq;
    dfp_quad rdiv, splitQ, intqH, intqL, rhsH, rhsL, rhse0, rhs1e;
    dfp_integer expIntq, expRhs, iexp, rndmode;
    ppc_fpscr savedFPSCR, afterDivFPSCR;
    dfp_significance numDigits;
    dfp_pad qwpad=0.0;

    if ( (!dfpalIsSpecial(lhs)) && dfpalIsInfinite(rhs) ) {
      return(lhs);
    }

    xlhs.dsw=lhs;
    xrhs.dsw=rhs;

    /* set rounding mode to nearest with tie to even  */
    Read_FPSCR_Set_RM(savedFPSCR, DFP_ROUND_TO_NEAREST_WITH_TIES_TO_EVEN)
    rdiv=ppc_ddivq(qwpad, xlhs.dhw, xrhs.dhw);

  #if defined(DFPAL_FAST_REMNEAR128)
    xres.dhw=ppc_dsubq(qwpad, xlhs.dhw,
               ppc_dmulq(qwpad, xrhs.dhw,
               ppc_dquaiq_rne_0(qwpad, rdiv)));
  #else
    /* takes care of rounding in intermediate multiply operation      */
    /* As a last resort, split quotient and rhs each into two numbers */

    do {
      xintq.dhw=ppc_dquaiq_rne_0(qwpad, rdiv);
      if (dfpalIsQNaN(xintq.dsw)) {
        xres.dhw=xintq.dhw;
        break;
      }

      numDigits.ull=(uLong)(DFPAL_DECIMAL128_Pmax/2);

      if ((ppc_dtstsfq(numDigits.d, xrhs.dhw) & 
        ppc_dtstsfq(numDigits.d, xintq.dhw) & 
        (DFPAL_COMP_GT|DFPAL_COMP_EQ)) ||
        (ppc_dcmpuq(qwpad, xintq.dhw, dfp_quad_zero) & DFPAL_COMP_EQ)) {
        xres.dhw=ppc_dsubq(qwpad, xlhs.dhw,
                   ppc_dmulq(qwpad, xrhs.dhw, xintq.dhw));
        break;
      }
  
      /* split Quotient and RHS, but before set rounding mode to truncate */
      rndmode.fpscr.drm=DFP_ROUND_TOWARD_ZERO;
      ppc_mtfsf_drm(rndmode.d);
     
      /* split Quotient */
      expIntq.sll = DFPAL_DECIMAL128_Bias + (DFPAL_DECIMAL128_Pmax/2); 
      splitQ=ppc_diexq(expIntq.d, dfp_quad_positive_one);
      intqH=ppc_dquaq_rtz(qwpad, splitQ, xintq.dhw);
      intqL=ppc_dsubq(qwpad, xintq.dhw, intqH);
  
      /* split RHS after setting exp=0. Otherwise NaN will result, if e>6111 */
      expRhs.d=ppc_dxexq(qwpad, xrhs.dhw); /* save exponent before setting 0 */
      iexp.sll=DFPAL_DECIMAL128_Bias;
      rhse0=ppc_diexq(iexp.d, xrhs.dhw);
      /* split rhse0, i.e. rhs with exp=0 */
      iexp.sll= DFPAL_DECIMAL128_Bias + DFPAL_DECIMAL128_Pmax/2;
      splitQ=ppc_diexq(iexp.d, dfp_quad_positive_one);
      rhsH=ppc_dquaq_rtz(qwpad, splitQ, rhse0);
      rhsL=ppc_dsubq(qwpad, rhse0, rhsH);

      rhs1e=ppc_diexq(expRhs.d, dfp_quad_positive_one);

      /* restore rounding mode to nearest */
      rndmode.fpscr.drm=DFP_ROUND_TO_NEAREST_WITH_TIES_TO_EVEN;
      ppc_mtfsf_drm(rndmode.d);

      /* R = N - Q * D                                             */
      /*   = ( ( N - Qh * Dh ) - (Ql * Dh + Qh * Dl) ) - (Ql * Dl) */
      xres.dhw=ppc_dsubq(qwpad, ppc_dsubq(qwpad,
        ppc_dsubq(qwpad, xlhs.dhw, ppc_dmulq(qwpad, rhs1e, 
        ppc_dmulq(qwpad, intqH, rhsH))),
        ppc_daddq(qwpad, ppc_dmulq(qwpad, rhs1e, 
        ppc_dmulq(qwpad, intqL, rhsH)), ppc_dmulq(qwpad, rhs1e, 
        ppc_dmulq(qwpad, intqH, rhsL)))),
        ppc_dmulq(qwpad, rhs1e, ppc_dmulq(qwpad, intqL, rhsL)));
      } while(0);
  #endif
    Read_FPSCR(afterDivFPSCR);
    savedFPSCR.fpscr_attrib.vxsoft=afterDivFPSCR.fpscr_attrib.vx;
    Restore_FPSCR(savedFPSCR); /* Nothing but INVLID exception. Restore RM */

    if (ppc_dcmpuq(qwpad, xres.dhw, dfp_quad_zero) & DFPAL_COMP_EQ) {
      xres.dsw.bytes[0] |= (lhs.bytes[0] & (uByte)0x80);
    }

    return(xres.dsw);
  }
  else {
    dec_Arithmetic2(128,Quad,RemainderNear,lhs,rhs)
/*
    decimal128 result128;
    decNumber lhsdn, rhsdn, resultdn, cmpresdn, zerodn;
    decContext * dnctx = &(Tls_Load()->dn128Context);
    decimal128ToNumber(&lhs,&lhsdn);
    decimal128ToNumber(&rhs,&rhsdn);
    decNumberRemainderNear(&resultdn, &lhsdn, &rhsdn, dnctx);
    decNumberZero(&zerodn);
    decNumberCompare(&cmpresdn, &resultdn, &zerodn, dnctx);
    if (!decNumberIsNaN(&cmpresdn) && decNumberIsZero(&cmpresdn)) {
      resultdn.bits |= (lhsdn.bits & DECNEG);
    }
    decimal128FromNumber(&result128, &resultdn, dnctx);
    return(result128);
*/
  }
}

decimal128 decimal128Rescale(const decimal128 lhs, const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    ppc_fpscr beforeFPSCR, afterFPSCR, clearINEXACT;
    dec128DataXchg xlhs, xrhs, xres;
    dfp_pad qwpad=0.0;
    dfp_quad tmp128;
    dfp_integer ires;

    xlhs.dsw=lhs;
    xrhs.dsw=rhs;

    if (dfpalIsSpecial(lhs) || dfpalIsSpecial(rhs)) {
      xres.dhw=ppc_dquaq_rfpscr(qwpad, xrhs.dhw, xlhs.dhw);
      return(xres.dsw);
    }

    /* read current FPSCR and clear INEXACT if it is set */
    beforeFPSCR.all.d=ppc_mffs();
    if (beforeFPSCR.fpscr_attrib.xx) {
      clearINEXACT=beforeFPSCR;
      clearINEXACT.fpscr_attrib.xx=0;
      ppc_mtfs(clearINEXACT.all.d);
    }

    /* convert decimal128 -> singed long long */
    ires.d=ppc_dctfixq(qwpad, xrhs.dhw);

    afterFPSCR.all.d=ppc_mffs();
    if (afterFPSCR.fpscr_attrib.xx ||
      (ires.sll > DFPAL_DECIMAL128_Emax) ||
      (ires.sll < (DFPAL_DECIMAL128_Emin-DFPAL_DECIMAL128_Pmax+1))) {
      /* input scale was non-integer */

      /* set invalid into pre-FPSCR */
      beforeFPSCR.fpscr_attrib.vxsoft=1;

      /* restore FPSCR */
      ppc_mtfs(beforeFPSCR.all.d);

      return(dec128_quiet_NaN);
    }

    /* restore FPSCR, we do need to care about status bit set by */
    /* following operations                                      */
    ppc_mtfs(beforeFPSCR.all.d);

    /* Insert required exponent into '1' */
    if (ires.sll <= (DFPAL_DECIMAL128_Emax-DFPAL_DECIMAL128_Pmax+1)) {
      ires.sll+=DFPAL_DECIMAL128_Bias;
      tmp128=ppc_diexq(ires.d, dfp_quad_positive_one);
    }
    else {
      tmp128=ppc_dscriq_33(qwpad, dfp_double_positive_one);
      ires.sll+=DFPAL_DECIMAL128_Bias;
      ires.sll-=(DFPAL_DECIMAL128_Pmax-1);
      tmp128=ppc_diexq(ires.d, tmp128);
    }

    /* quantize with new exponent */
    xres.dhw=ppc_dquaq_rfpscr(qwpad, tmp128, xlhs.dhw);

    return(xres.dsw);
  }
  else {
    decNumber_Arithmetic2(128,Rescale,lhs,rhs)
  }
}

decimal128 decimal128SameQuantum(const decimal128 lhs, const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_quad xres;
    dfp_pad qwpad=0.0;

    if (ppc_dtstexq(qwpad, *((dfp_quad *)&lhs), *((dfp_quad *)&rhs)) & 
      DFPAL_COMP_EQ)
      xres=dfp_quad_positive_one;
    else
      xres=dfp_quad_zero;

    return(*((decimal128 *)&xres));
  }
  else {
    decimal128 result128;
    decNumber lhsdn, rhsdn, resultdn;
    decContext * dnctx = &(Tls_Load()->dn128Context);
    decimal128ToNumber(&lhs,&lhsdn);
    decimal128ToNumber(&rhs,&rhsdn);
    decNumberSameQuantum(&resultdn, &lhsdn, &rhsdn);
    decimal128FromNumber(&result128, &resultdn, dnctx);
    return(result128);
  }
}

decimal128 decimal128SquareRoot(const decimal128 rhs)
{
  decNumber_For_PPC_Arithmetic1(128,SquareRoot,rhs)
}

Quad_Word_Function2(Subtract,lhs,rhs,ppc_dsubq)


decimal128 decimal128ToIntegralValue(const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dfp_quad xres;
    dfp_pad qwpad=0.0;
    xres=ppc_drintnq_rfpscr(qwpad, *((dfp_quad *)&rhs));
    return(*((decimal128 *)&xres));
  }
  else {
    #if !defined(DFPAL_USE_DECFLOAT)
      decNumber_Arithmetic1(128,ToIntegralValue,rhs)
    #else
      decQuad resultQuad;
      decContext * dnctx = &(Tls_Load()->dn128Context);
      decQuadToIntegralValue(&resultQuad, (decQuad *)&rhs, dnctx,
        dnctx->round);
      return(*((decimal128 *)&resultQuad));
    #endif
  }
}

decimal128 decimal128Ceil(const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dec128DataXchg xrhs, xres;
    dfp_pad qwpad=0.0;
    xrhs.dsw=rhs;
    xres.dhw=ppc_drintnq_rtpi(qwpad, xrhs.dhw);
    return(xres.dsw);
  }
  else {
    dec_To_Integer_RM(128, Quad, DFPAL_ROUND_TOWARD_POSITIVE_INFINITY, rhs);
  }
}

decimal128 decimal128Floor(const decimal128 rhs)
{
  if(globalContext.dfpRealMode==PPCHW) {
    dec128DataXchg xrhs, xres;
    dfp_pad qwpad=0.0;
    xrhs.dsw=rhs;
    xres.dhw=ppc_drintnq_rtmi(qwpad, xrhs.dhw);
    return(xres.dsw);
  }
  else {
    dec_To_Integer_RM(128, Quad, DFPAL_ROUND_TOWARD_NEGATIVE_INFINITY, rhs);
  }
}
