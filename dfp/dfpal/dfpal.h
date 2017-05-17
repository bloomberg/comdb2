/* ------------------------------------------------------------------ */
/* Decimal Floating Point Abstraction Layer (DFPAL)                   */
/* dfpal.h                                                            */
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
#if !defined(__DFPAL_H__)
  #define __DFPAL_H__

  /* system includes */
  #include <stdio.h>
  #if defined(DFPAL_OS_AIX5L) || defined(DFPAL_OS_I5OS)
    #include <fpxcp.h>
    #include <fptrap.h>
  #elif defined(DFPAL_OS_LOP) || defined(DFPAL_OS_LOP_XLC)
    #define __USE_GNU
    #include <fenv.h>
    #include <fpu_control.h>
    #define FP_INVALID FE_INVALID
    #define FP_OVERFLOW FE_OVERFLOW
    #define FP_UNDERFLOW FE_UNDERFLOW
    #define FP_DIV_BY_ZERO FE_DIVBYZERO
    #define FP_INEXACT FE_INEXACT
    #define FP_INV_SNAN FE_INVALID_SNAN
    #define FP_INV_ISI FE_INVALID_ISI
    #define FP_INV_IDI FE_INVALID_IDI
    #define FP_INV_ZDZ FE_INVALID_ZDZ
    #define FP_INV_IMZ FE_INVALID_IMZ
    #define FP_INV_CMP FE_INVALID_COMPARE
    #define FP_INV_SQRT FE_INVALID_SQRT
    #define FP_INV_CVI FE_INVALID_INTEGER_CONVERSION
    #define FP_INV_VXSOFT FE_INVALID_SOFTWARE
    #define TRP_INVALID _FPU_MASK_IM
    #define TRP_OVERFLOW _FPU_MASK_OM
    #define TRP_UNDERFLOW _FPU_MASK_UM
    #define TRP_DIV_BY_ZERO _FPU_MASK_ZM
    #define TRP_INEXACT _FPU_MASK_XM
  #endif

  #include "dfpaltypes.h"
  #include "dfpalerr.h"

  typedef enum compare_result_e {
    DFPAL_COMP_LT = 8, /* 1000 */
    DFPAL_COMP_GT = 4, /* 0100 */
    DFPAL_COMP_EQ = 2, /* 0010 */
    DFPAL_COMP_UO = 1  /* unordered, 0001 */
  } compare_result;

  enum {
    XCP_INVALID=0,
    XCP_OVERFLOW,
    XCP_UNDERFLOW,
    XCP_DIV_BY_ZERO,
    XCP_INEXACT,
    XCP_NXCP          /* number of exceptions */
    };

  enum {
    RND_NT2E,         /* DEC_ROUND_HALF_EVEN: Round toward */
                      /* nearest, with ties to even (default) */
    RND_TZ,           /* DEC_ROUND_DOWN: round towards zero */
    RND_TPINF,        /* DEC_ROUND_CEILING: Round toward plus infinity */
    RND_TMINF,        /* DEC_ROUND_FLOOR: Round toward minus infinity */
    RND_NTAZ,         /* DEC_ROUND_HALF_UP: 0.5 rounds up */
    RND_NT2Z,         /* DEC_ROUND_HALF_DOWN: 0.5 rounds down */
    RND_AZ,           /* DEC_ROUND_UP: round away from 0 */
    RND_NRND          /* number of rounding modes */
  };

  /* DFPAL control and status flag */
  typedef struct {
    dfpalflag_t dfpalXcpStatus[XCP_NXCP];    /* floating point status */
    dfpaltrap_t dfpalTrap[XCP_NXCP];         /* traps */
    dfpalrnd_t dfpalRoundingMode[RND_NRND];  /* rounding modes */
  } dfpalCSFlag;


  #if !defined(__DFPAL_C__)
    #if defined (DFPAL_OS_WINDOWS)
      __declspec(dllimport) extern dfpalCSFlag controlFlags;
    #else
      extern dfpalCSFlag controlFlags;
    #endif
  #endif


  /* -------------------------------------------------------------------- */
  /* limits for the 64 bit and 128 bit numbers                            */
  /* -------------------------------------------------------------------- */
  #define DFPAL_DECIMAL64_Bias   398        /* bias for the exponent */
  #define DFPAL_DECIMAL64_Pmax   16         /* maximum precision (digits) */
  #define DFPAL_DECIMAL64_Emax   384        /* maximum unbiased exponent */
  #define DFPAL_DECIMAL64_Emin  -383        /* minimum unbiased exponent */

  #define DFPAL_DECIMAL128_Bias   6176
  #define DFPAL_DECIMAL128_Pmax   34
  #define DFPAL_DECIMAL128_Emax   6144    
  #define DFPAL_DECIMAL128_Emin  -6143

  /* -------------------------------------------------------------------- */
  /* DFP abstraction layer management routines                            */
  /* -------------------------------------------------------------------- */

  /* ---------------- Initialization and error handling ----------------- */
  uint32_t dfpalMemSize(void);
  int32_t dfpalInit(void *);
  dfpalCSFlag *dfpalGetFlagHandle(void);
  void dfpalEnd(void (*)(void *));
  enum dfpalExeMode dfpalGetExeMode(void);
  void dfpalResetContext(void);
  int32_t dfpalGetError(int32_t *, int32_t *, char **);
  void dfpalClearError(void);
  int32_t dfpalErrorFlag(void);
  const char * dfpalVersion(void);

  /* NOTE: Unix platforms shall not use dfpalInitProcessContext(). */
  /* It is automatically called by dfpalInit() on Unix platforms.  */
  void dfpalInitProcessContext(void);

  /* ------------------ Exception status and traps ---------------------- */
  /* Exception status */
  #define DFPAL_FP_INVALID       controlFlags.dfpalXcpStatus[XCP_INVALID]
  #define DFPAL_FP_OVERFLOW      controlFlags.dfpalXcpStatus[XCP_OVERFLOW]
  #define DFPAL_FP_UNDERFLOW     controlFlags.dfpalXcpStatus[XCP_UNDERFLOW]
  #define DFPAL_FP_DIV_BY_ZERO   controlFlags.dfpalXcpStatus[XCP_DIV_BY_ZERO]
  #define DFPAL_FP_INEXACT       controlFlags.dfpalXcpStatus[XCP_INEXACT]
  #define DFPAL_FP_ALL           (DFPAL_FP_INVALID|DFPAL_FP_OVERFLOW|\
                                 DFPAL_FP_UNDERFLOW|DFPAL_FP_DIV_BY_ZERO|\
                                 DFPAL_FP_INEXACT)


  /* defines for run-time linking */
  #define S_DFPAL_FP_INVALID(p)       ((p)->dfpalXcpStatus[XCP_INVALID])
  #define S_DFPAL_FP_OVERFLOW(p)      ((p)->dfpalXcpStatus[XCP_OVERFLOW])
  #define S_DFPAL_FP_UNDERFLOW(p)     ((p)->dfpalXcpStatus[XCP_UNDERFLOW])
  #define S_DFPAL_FP_DIV_BY_ZERO(p)   ((p)->dfpalXcpStatus[XCP_DIV_BY_ZERO])
  #define S_DFPAL_FP_INEXACT(p)       ((p)->dfpalXcpStatus[XCP_INEXACT])
  #define S_DFPAL_FP_ALL(p)           (S_DFPAL_FP_INVALID(p)|     \
                                      S_DFPAL_FP_OVERFLOW(p)|     \
                                      S_DFPAL_FP_UNDERFLOW(p)|    \
                                      S_DFPAL_FP_DIV_BY_ZERO(p)|  \
                                      S_DFPAL_FP_INEXACT(p))


  void dfpalClearStatusFlag(dfpalflag_t);
  void dfpalClearAllStatusFlag(void);
  void dfpalSetStatusFlag(dfpalflag_t);
  dfpalflag_t dfpalReadStatusFlag(void);
  dfpalflag_t dfpalReadClearAllStatusFlag(void);
  dfpalflag_t dfpalSwapStatusFlag(dfpalflag_t);

  /*                           ---------                               */

  /* Trap control */
  #define DFPAL_TRP_INVALID       controlFlags.dfpalTrap[XCP_INVALID]
  #define DFPAL_TRP_OVERFLOW      controlFlags.dfpalTrap[XCP_OVERFLOW]
  #define DFPAL_TRP_UNDERFLOW     controlFlags.dfpalTrap[XCP_UNDERFLOW]
  #define DFPAL_TRP_DIV_BY_ZERO   controlFlags.dfpalTrap[XCP_DIV_BY_ZERO]
  #define DFPAL_TRP_INEXACT       controlFlags.dfpalTrap[XCP_INEXACT]
  #define DFPAL_TRP_ALL           (DFPAL_TRP_INVALID|DFPAL_TRP_OVERFLOW|     \
                                  DFPAL_TRP_UNDERFLOW|DFPAL_TRP_DIV_BY_ZERO| \
                                  DFPAL_TRP_INEXACT)

  /* defines for run-time linking */
  #define S_DFPAL_TRP_INVALID(p)       ((p)->dfpalTrap[XCP_INVALID])
  #define S_DFPAL_TRP_OVERFLOW(p)      ((p)->dfpalTrap[XCP_OVERFLOW])
  #define S_DFPAL_TRP_UNDERFLOW(p)     ((p)->dfpalTrap[XCP_UNDERFLOW])
  #define S_DFPAL_TRP_DIV_BY_ZERO(p)   ((p)->dfpalTrap[XCP_DIV_BY_ZERO])
  #define S_DFPAL_TRP_INEXACT(p)       ((p)->dfpalTrap[XCP_INEXACT])
  #define S_DFPAL_TRP_ALL(p)           (S_DFPAL_TRP_INVALID(p)|      \
                                       S_DFPAL_TRP_OVERFLOW(p)|      \
                                       S_DFPAL_TRP_UNDERFLOW(p)|     \
                                       S_DFPAL_TRP_DIV_BY_ZERO(p)|   \
                                       S_DFPAL_TRP_INEXACT(p))

  void dfpalEnableTrap(dfpaltrap_t);
  void dfpalEnableAllTrap(void);
  void dfpalDisableTrap(dfpaltrap_t);
  void dfpalDisableAllTrap(void);
  uint8_t dfpalAnyTrapEnabled(void);
  uint8_t dfpalTrapEnabled(dfpaltrap_t);

  /* ----------------------- Rounding mode ------------------------------ */
  dfpalrnd_t dfpalReadRoundingMode(void);
  dfpalrnd_t dfpalSwapRoundingMode(dfpalrnd_t);
  void dfpalSetRoundingMode(dfpalrnd_t);
  uint8_t dfpalSetExponentClamp(uint8_t);

  #define DFPAL_ROUND_TO_NEAREST_WITH_TIES_TO_EVEN         \
    controlFlags.dfpalRoundingMode[RND_NT2E]
  #define DFPAL_ROUND_TOWARD_ZERO                          \
    controlFlags.dfpalRoundingMode[RND_TZ]
  #define DFPAL_ROUND_TOWARD_POSITIVE_INFINITY             \
    controlFlags.dfpalRoundingMode[RND_TPINF]
  #define DFPAL_ROUND_TOWARD_NEGATIVE_INFINITY             \
    controlFlags.dfpalRoundingMode[RND_TMINF]
  #define DFPAL_ROUND_TO_NEAREST_WITH_TIES_AWAY_FROM_ZERO  \
    controlFlags.dfpalRoundingMode[RND_NTAZ]
  #define DFPAL_ROUND_TO_NEAREST_WITH_TIES_TOWARD_ZERO     \
    controlFlags.dfpalRoundingMode[RND_NT2Z]
  #define DFPAL_ROUND_AWAY_FROM_ZERO                       \
    controlFlags.dfpalRoundingMode[RND_AZ]


  /* defines for run-time linking */
  #define S_DFPAL_ROUND_TO_NEAREST_WITH_TIES_TO_EVEN(p)         \
    ((p)->dfpalRoundingMode[RND_NT2E]) 
  #define S_DFPAL_ROUND_TOWARD_ZERO(p)                          \
    ((p)->dfpalRoundingMode[RND_TZ]) 
  #define S_DFPAL_ROUND_TOWARD_POSITIVE_INFINITY(p)             \
    ((p)->dfpalRoundingMode[RND_TPINF]) 
  #define S_DFPAL_ROUND_TOWARD_NEGATIVE_INFINITY(p)             \
    ((p)->dfpalRoundingMode[RND_TMINF]) 
  #define S_DFPAL_ROUND_TO_NEAREST_WITH_TIES_AWAY_FROM_ZERO(p)  \
    ((p)->dfpalRoundingMode[RND_NTAZ]) 
  #define S_DFPAL_ROUND_TO_NEAREST_WITH_TIES_TOWARD_ZERO(p)     \
    ((p)->dfpalRoundingMode[RND_NT2Z]) 
  #define S_DFPAL_ROUND_AWAY_FROM_ZERO(p)                       \
    ((p)->dfpalRoundingMode[RND_AZ]) 

  /* -------------------------------------------------------------------- */
  /* Arithmetic operations                                                */
  /* -------------------------------------------------------------------- */
  #define DFPAL_COMP_UNORDERED  0x01
  #define DFPAL_COMP_ORDERED    0x02


  /* ----------------- Double word operations --------------------------- */
  /* conversions */
  char * dfpal_decimal64ToString(const decimal64, char *);
  decimal64 dfpal_decimal64FromString(const char *);
  decimal64 decimal64FromDecimal128(const decimal128);
  double decimal64ToDouble(const decimal64);
  decimal64 decimal64FromDouble(const double);
  uint8_t * decimal64ToPackedBCD(decimal64, uint8_t *, int32_t, int32_t *);
  decimal64 decimal64FromPackedBCD(uint8_t *, int32_t, int32_t);

  decimal32 decimal64ToDecimal32(const decimal64);
  decimal64 decimal64FromDecimal32(const decimal32);

  int64_t decimal64ToInt64(const decimal64);
  uint64_t decimal64ToUint64(const decimal64);
  int32_t decimal64ToInt32(const decimal64);
  uint32_t decimal64ToUint32(const decimal64);
  decimal64 decimal64FromInt64(const int64_t);
  decimal64 decimal64FromUint64(const uint64_t);
  decimal64 decimal64FromInt32(const int32_t);
  decimal64 decimal64FromUint32(const uint32_t);

  /* Utilities */
  uint32_t dec64Sign(const decimal64);
  uint32_t dec64Comb(const decimal64);
  uint32_t dec64ExpCon(const decimal64);
  uint8_t decimal64IsInfinite(const decimal64);
  uint8_t decimal64IsNaN (const decimal64);
  uint8_t decimal64IsQNaN (const decimal64);
  uint8_t decimal64IsSNaN (const decimal64);
  uint8_t decimal64IsNegative (const decimal64);
  uint8_t decimal64IsZero (const decimal64);
  decimal64 decimal64Trim(const decimal64);
  decimal64 decimal64Zero(void);

  /* field access */
  int32_t decimal64GetDigits(const decimal64);
  int32_t decimal64GetExponent(const decimal64);

  /* Operations; wrppers around hardware instructions or decNumber */
  decimal64 decimal64Abs(const decimal64);
  decimal64 decimal64Add(const decimal64, const decimal64);
  decimal64 decimal64Compare(const decimal64, const decimal64);
  compare_result decimal64Cmpop(const decimal64, const decimal64, const uint8_t);
  #define decimal64CompareLT(l,r) \
    (decimal64Cmpop((l),(r),DFPAL_COMP_UNORDERED)&DFPAL_COMP_LT)
  #define decimal64CompareLE(l,r) \
    (decimal64Cmpop((l),(r),DFPAL_COMP_UNORDERED)&(DFPAL_COMP_LT|DFPAL_COMP_EQ))
  #define decimal64CompareEQ(l,r) \
    (decimal64Cmpop((l),(r),DFPAL_COMP_UNORDERED)&DFPAL_COMP_EQ)
  #define decimal64CompareNE(l,r) \
    (decimal64Cmpop((l),(r),DFPAL_COMP_UNORDERED)&(DFPAL_COMP_LT|DFPAL_COMP_GT))
  #define decimal64CompareGT(l,r) \
    (decimal64Cmpop((l),(r),DFPAL_COMP_UNORDERED)&DFPAL_COMP_GT)
  #define decimal64CompareGE(l,r) \
    (decimal64Cmpop((l),(r),DFPAL_COMP_UNORDERED)&(DFPAL_COMP_GT|DFPAL_COMP_EQ))
  decimal64 decimal64CompareTotal(const decimal64, const decimal64);
  #define decimal64CompareTotalLT(l,r) \
    (decimal64Cmpop((l),(r),DFPAL_COMP_ORDERED)&DFPAL_COMP_LT)
  #define decimal64CompareTotalLE(l,r) \
    (decimal64Cmpop((l),(r),DFPAL_COMP_ORDERED)&(DFPAL_COMP_LT|DFPAL_COMP_EQ))
  #define decimal64CompareTotalEQ(l,r) \
    (decimal64Cmpop((l),(r),DFPAL_COMP_ORDERED)&DFPAL_COMP_EQ)
  #define decimal64CompareTotalNE(l,r) \
    (decimal64Cmpop((l),(r),DFPAL_COMP_ORDERED)&(DFPAL_COMP_LT|DFPAL_COMP_GT))
  #define decimal64CompareTotalGT(l,r) \
    (decimal64Cmpop((l),(r),DFPAL_COMP_ORDERED)&DFPAL_COMP_GT)
  #define decimal64CompareTotalGE(l,r) \
    (decimal64Cmpop((l),(r),DFPAL_COMP_ORDERED)&(DFPAL_COMP_GT|DFPAL_COMP_EQ))
  decimal64 decimal64Divide(const decimal64, const decimal64);
  decimal64 decimal64DivideInteger(const decimal64, const decimal64);
  decimal64 decimal64Exp(const decimal64);
  decimal64 decimal64Ln(const decimal64);
  decimal64 decimal64Log10(const decimal64);
  decimal64 decimal64Max(const decimal64, const decimal64);
  decimal64 decimal64Min(const decimal64, const decimal64);
  decimal64 decimal64Minus(const decimal64);
  decimal64 decimal64Multiply(const decimal64, const decimal64);
  decimal64 decimal64Normalize(const decimal64);
  decimal64 decimal64Plus(const decimal64);
  decimal64 decimal64Power(const decimal64, const decimal64);
  decimal64 decimal64PowerInt(const decimal64, const int32_t);
  decimal64 decimal64Quantize(const decimal64, const decimal64);
  decimal64 decimal64Remainder(const decimal64, const decimal64);
  decimal64 decimal64RemainderNear(const decimal64, const decimal64);
  decimal64 decimal64Rescale(const decimal64, const decimal64);
  decimal64 decimal64SameQuantum(const decimal64, const decimal64);
  decimal64 decimal64SquareRoot(const decimal64);
  decimal64 decimal64Subtract(const decimal64, const decimal64);
  decimal64 decimal64ToIntegralValue(const decimal64);
  decimal64 decimal64Ceil(const decimal64);
  decimal64 decimal64Floor(const decimal64);
  /* trigonometry */
  decimal64 decimal64Acos(const decimal64);
  decimal64 decimal64Asin(const decimal64);
  decimal64 decimal64Atan(const decimal64);
  decimal64 decimal64Cos(const decimal64);
  decimal64 decimal64Sin(const decimal64);
  decimal64 decimal64Tan(const decimal64);
  decimal64 decimal64Cosh(const decimal64);
  decimal64 decimal64Sinh(const decimal64);
  decimal64 decimal64Tanh(const decimal64);

  /* -------------- Quad word operations -------------------------------- */
  /* conversions */
  char * dfpal_decimal128ToString( const decimal128, char *);
  decimal128 dfpal_decimal128FromString( const char *);
  decimal128 decimal128FromDecimal64(const decimal64);
  double decimal128ToDouble(const decimal128);
  decimal128 decimal128FromDouble(const double);
  uint8_t * decimal128ToPackedBCD(decimal128, uint8_t *, int32_t, int32_t *);
  decimal128 decimal128FromPackedBCD(uint8_t *, int32_t, int32_t);

  int64_t decimal128ToInt64(const decimal128);
  uint64_t decimal128ToUint64(const decimal128);
  int32_t decimal128ToInt32(const decimal128);
  uint32_t decimal128ToUint32(const decimal128);
  decimal128 decimal128FromInt64(const int64_t);
  decimal128 decimal128FromUint64(const uint64_t);
  decimal128 decimal128FromInt32(const int32_t);
  decimal128 decimal128FromUint32(const uint32_t);

  /* Utilities */
  uint32_t dec128Sign(const decimal128);
  uint32_t dec128Comb(const decimal128);
  uint32_t dec128ExpCon(const decimal128);
  uint8_t decimal128IsInfinite(const decimal128);
  uint8_t decimal128IsNaN (const decimal128);
  uint8_t decimal128IsQNaN (const decimal128);
  uint8_t decimal128IsSNaN (const decimal128);
  uint8_t decimal128IsNegative (const decimal128);
  uint8_t decimal128IsZero (const decimal128);
  decimal128 decimal128Trim(const decimal128);
  decimal128 decimal128Zero(void);

  /* field access */
  int32_t decimal128GetDigits(const decimal128);
  int32_t decimal128GetExponent(const decimal128);

  /* Operations; wrppers around hardware instructions or decNumber */
  decimal128 decimal128Abs(const decimal128);
  decimal128 decimal128Add(const decimal128, const decimal128);
  decimal128 decimal128Compare(const decimal128, const decimal128);
  compare_result decimal128Cmpop(const decimal128, const decimal128, const uint8_t);
  #define decimal128CompareLT(l,r) \
    (decimal128Cmpop((l),(r),DFPAL_COMP_UNORDERED)&DFPAL_COMP_LT)
  #define decimal128CompareLE(l,r) \
    (decimal128Cmpop((l),(r),DFPAL_COMP_UNORDERED)&(DFPAL_COMP_LT|DFPAL_COMP_EQ))
  #define decimal128CompareEQ(l,r) \
    (decimal128Cmpop((l),(r),DFPAL_COMP_UNORDERED)&DFPAL_COMP_EQ)
  #define decimal128CompareNE(l,r) \
    (decimal128Cmpop((l),(r),DFPAL_COMP_UNORDERED)&(DFPAL_COMP_LT|DFPAL_COMP_GT))
  #define decimal128CompareGT(l,r) \
    (decimal128Cmpop((l),(r),DFPAL_COMP_UNORDERED)&DFPAL_COMP_GT)
  #define decimal128CompareGE(l,r) \
    (decimal128Cmpop((l),(r),DFPAL_COMP_UNORDERED)&(DFPAL_COMP_GT|DFPAL_COMP_EQ))
  decimal128 decimal128CompareTotal(const decimal128, const decimal128);
  #define decimal128CompareTotalLT(l,r) \
    (decimal128Cmpop((l),(r),DFPAL_COMP_ORDERED)&DFPAL_COMP_LT)
  #define decimal128CompareTotalLE(l,r) \
    (decimal128Cmpop((l),(r),DFPAL_COMP_ORDERED)&(DFPAL_COMP_LT|DFPAL_COMP_EQ))
  #define decimal128CompareTotalEQ(l,r) \
    (decimal128Cmpop((l),(r),DFPAL_COMP_ORDERED)&DFPAL_COMP_EQ)
  #define decimal128CompareTotalNE(l,r) \
    (decimal128Cmpop((l),(r),DFPAL_COMP_ORDERED)&(DFPAL_COMP_LT|DFPAL_COMP_GT))
  #define decimal128CompareTotalGT(l,r) \
    (decimal128Cmpop((l),(r),DFPAL_COMP_ORDERED)&DFPAL_COMP_GT)
  #define decimal128CompareTotalGE(l,r) \
    (decimal128Cmpop((l),(r),DFPAL_COMP_ORDERED)&(DFPAL_COMP_GT|DFPAL_COMP_EQ))
  decimal128 decimal128Divide(const decimal128, const decimal128);
  decimal128 decimal128DivideInteger(const decimal128, const decimal128);
  decimal128 decimal128Exp(const decimal128);
  decimal128 decimal128Ln(const decimal128);
  decimal128 decimal128Log10(const decimal128);
  decimal128 decimal128Max(const decimal128, const decimal128);
  decimal128 decimal128Min(const decimal128, const decimal128);
  decimal128 decimal128Minus(const decimal128);
  decimal128 decimal128Multiply(const decimal128, const decimal128);
  decimal128 decimal128Normalize(const decimal128);
  decimal128 decimal128Plus(const decimal128);
  decimal128 decimal128Power(const decimal128, const decimal128);
  decimal128 decimal128PowerInt(const decimal128, const int32_t);
  decimal128 decimal128Quantize(const decimal128, const decimal128);
  decimal128 decimal128Remainder(const decimal128, const decimal128);
  decimal128 decimal128RemainderNear(const decimal128, const decimal128);
  decimal128 decimal128Rescale(const decimal128, const decimal128);
  decimal128 decimal128SameQuantum(const decimal128, const decimal128);
  decimal128 decimal128SquareRoot(const decimal128);
  decimal128 decimal128Subtract(const decimal128, const decimal128);
  decimal128 decimal128ToIntegralValue(const decimal128);
  decimal128 decimal128Ceil(const decimal128);
  decimal128 decimal128Floor(const decimal128);
  /* trigonometry */
  decimal128 decimal128Acos(const decimal128);
  decimal128 decimal128Asin(const decimal128);
  decimal128 decimal128Atan(const decimal128);
  decimal128 decimal128Cos(const decimal128);
  decimal128 decimal128Sin(const decimal128);
  decimal128 decimal128Tan(const decimal128);
  decimal128 decimal128Cosh(const decimal128);
  decimal128 decimal128Sinh(const decimal128);
  decimal128 decimal128Tanh(const decimal128);

  /* -------------------------------------------------------------------- */
  /* Macros for native DFP use. Application should code to decSSOOO()     */
  /* macros, rather than directly using decimalSSOOO() routines.          */
  /* -------------------------------------------------------------------- */

  #if !defined(DFPAL_USE_COMPILER_DFP)
    /* ---------------- double word operations ------------------------- */
    #define dec64ToString(a,b) dfpal_decimal64ToString((a),(b))
    #define dec64FromString(a) dfpal_decimal64FromString((a))
    #define dec64FromDecimal128(a) decimal64FromDecimal128((a))
    #define dec64ToDouble(a) decimal64ToDouble((a))
    #define dec64FromDouble(a) decimal64FromDouble((a))
    #define dec64ToPackedBCD(a,b,c,d) decimal64ToPackedBCD((a),(b),(c),(d))
    #define dec64FromPackedBCD(a,b,c) decimal64FromPackedBCD((a),(b),(c))

    #define dec64ToInt64(a) decimal64ToInt64((a))
    #define dec64ToUint64(a) decimal64ToUint64((a))
    #define dec64ToInt32(a) decimal64ToInt32((a))
    #define dec64ToUint32(a) decimal64ToUint32((a))
    #define dec64FromInt64(a) decimal64FromInt64((a))
    #define dec64FromUint64(a) decimal64FromUint64((a))
    #define dec64FromInt32(a) decimal64FromInt32((a))
    #define dec64FromUint32(a) decimal64FromUint32((a))
    #define dec64FromDecimal32(a) decimal64FromDecimal32((a))
    #define dec64ToDecimal32(a) decimal64ToDecimal32((a))

    #define dec64IsInfinite(a) decimal64IsInfinite((a))
    #define dec64IsNaN(a) decimal64IsNaN((a))
    #define dec64IsQNaN(a) decimal64IsQNaN((a))
    #define dec64IsSNaN(a) decimal64IsSNaN((a))
    #define dec64IsNegative(a) decimal64IsNegative((a))
    #define dec64IsZero(a) decimal64IsZero ((a))
    #define dec64Trim(a) decimal64Trim((a))
    #define dec64Zero() decimal64Zero()
    #define dec64GetDigits(a) decimal64GetDigits((a))
    #define dec64GetExponent(a) decimal64GetExponent((a))

    #define dec64Abs(a) decimal64Abs((a))
    #define dec64Add(a,b) decimal64Add((a), (b))
    #define dec64Compare(a,b) decimal64Compare((a),(b))
    #define dec64CompareLT(a,b) decimal64CompareLT((a),(b))
    #define dec64CompareLE(a,b) decimal64CompareLE((a),(b))
    #define dec64CompareEQ(a,b) decimal64CompareEQ((a),(b))
    #define dec64CompareNE(a,b) decimal64CompareNE((a),(b))
    #define dec64CompareGT(a,b) decimal64CompareGT((a),(b))
    #define dec64CompareGE(a,b) decimal64CompareGE((a),(b))
    #define dec64CompareTotal(a,b) decimal64CompareTotal((a),(b))
    #define dec64Divide(a,b) decimal64Divide((a),(b))
    #define dec64DivideInteger(a,b) decimal64DivideInteger((a),(b))
    #define dec64Exp(a) decimal64Exp((a));
    #define dec64Ln(a) decimal64Ln((a));
    #define dec64Log10(a) decimal64Log10((a));
    #define dec64Max(a,b) decimal64Max((a),(b))
    #define dec64Min(a,b) decimal64Min((a),(b))
    #define dec64Minus(a) decimal64Minus((a))
    #define dec64Multiply(a,b) decimal64Multiply((a),(b))
    #define dec64Normalize(a) decimal64Normalize((a))
    #define dec64Plus(a) decimal64Plus((a))
    #define dec64Power(a,b) decimal64Power((a),(b))
    #define dec64PowerInt(a,b) decimal64Power((a),(b))
    #define dec64Quantize(a,b) decimal64Quantize((a),(b))
    #define dec64Remainder(a,b) decimal64Remainder((a),(b))
    #define dec64RemainderNear(a,b) decimal64RemainderNear((a),(b))
    #define dec64Rescale(a,b) decimal64Rescale((a),(b))
    #define dec64SameQuantum(a,b) decimal64SameQuantum((a),(b))
    #define dec64SquareRoot(a) decimal64SquareRoot((a))
    #define dec64Subtract(a,b) decimal64Subtract((a),(b))
    #define dec64ToIntegralValue(a) decimal64ToIntegralValue((a))
    #define dec64Ceil(a) decimal64Ceil((a))
    #define dec64Floor(a) decimal64Floor((a))
    #define dec64Acos(a) decimal64Acos((a))
    #define dec64Acos(a) decimal64Acos((a))
    #define dec64Asin(a) decimal64Asin((a))
    #define dec64Atan(a) decimal64Atan((a))
    #define dec64Cos(a) decimal64Cos((a))
    #define dec64Sin(a) decimal64Sin((a))
    #define dec64Tan(a) decimal64Tan((a))
    #define dec64Cosh(a) decimal64Cosh((a))
    #define dec64Sinh(a) decimal64Sinh((a))
    #define dec64Tanh(a) decimal64Tanh((a))

    /* -------------- quad word operations --------------------------- */
    #define dec128ToString(a,b) dfpal_decimal128ToString((a),(b))
    #define dec128FromString(a) dfpal_decimal128FromString((a))
    #define dec128FromDecimal64(a) decimal128FromDecimal64((a))
    #define dec128ToDouble(a) decimal128ToDouble((a))
    #define dec128FromDouble(a) decimal128FromDouble((a))
    #define dec128ToPackedBCD(a,b,c,d) decimal128ToPackedBCD((a),(b),(c),(d))
    #define dec128FromPackedBCD(a,b,c) decimal128FromPackedBCD((a),(b),(c))

    #define dec128ToInt64(a) decimal128ToInt64((a))
    #define dec128ToUint64(a) decimal128ToUint64((a))
    #define dec128ToInt32(a) decimal128ToInt32((a))
    #define dec128ToUint32(a) decimal128ToUint32((a))
    #define dec128FromInt64(a) decimal128FromInt64((a))
    #define dec128FromUint64(a) decimal128FromUint64((a))
    #define dec128FromInt32(a) decimal128FromInt32((a))
    #define dec128FromUint32(a) decimal128FromUint32((a))

    #define dec128IsInfinite(a) decimal128IsInfinite((a))
    #define dec128IsNaN(a) decimal128IsNaN((a))
    #define dec128IsQNaN(a) decimal128IsQNaN((a))
    #define dec128IsSNaN(a) decimal128IsSNaN((a))
    #define dec128IsNegative(a) decimal128IsNegative((a))
    #define dec128IsZero(a) decimal128IsZero ((a))
    #define dec128Trim(a) decimal128Trim((a))
    #define dec128Zero() decimal128Zero()
    #define dec128GetDigits(a) decimal128GetDigits((a))
    #define dec128GetExponent(a) dec128GetExponent((a))

    #define dec128Abs(a) decimal128Abs((a))
    #define dec128Add(a,b) decimal128Add((a), (b))
    #define dec128Compare(a,b) decimal128Compare((a),(b))
    #define dec128CompareLT(a,b) decimal128CompareLT((a),(b))
    #define dec128CompareLE(a,b) decimal128CompareLE((a),(b))
    #define dec128CompareEQ(a,b) decimal128CompareEQ((a),(b))
    #define dec128CompareNE(a,b) decimal128CompareNE((a),(b))
    #define dec128CompareGT(a,b) decimal128CompareGT((a),(b))
    #define dec128CompareGE(a,b) decimal128CompareGE((a),(b))
    #define dec128CompareTotal(a,b) decimal128CompareTotal((a),(b))
    #define dec128Divide(a,b) decimal128Divide((a),(b))
    #define dec128DivideInteger(a,b) decimal128DivideInteger((a),(b))
    #define dec128Exp(a) decimal128Exp((a));
    #define dec128Ln(a) decimal128Ln((a));
    #define dec128Log10(a) decimal128Log10((a));
    #define dec128Max(a,b) decimal128Max((a),(b))
    #define dec128Min(a,b) decimal128Min((a),(b))
    #define dec128Minus(a) decimal128Minus((a))
    #define dec128Multiply(a,b) decimal128Multiply((a),(b))
    #define dec128Normalize(a) decimal128Normalize((a))
    #define dec128Plus(a) decimal128Plus((a))
    #define dec128Power(a,b) decimal128Power((a),(b))
    #define dec128PowerInt(a,b) decimal128Power((a),(b))
    #define dec128Quantize(a,b) decimal128Quantize((a),(b))
    #define dec128Remainder(a,b) decimal128Remainder((a),(b))
    #define dec128RemainderNear(a,b) decimal128RemainderNear((a),(b))
    #define dec128Rescale(a,b) decimal128Rescale((a),(b))
    #define dec128SameQuantum(a,b) decimal128SameQuantum((a),(b))
    #define dec128SquareRoot(a) decimal128SquareRoot((a))
    #define dec128Subtract(a,b) decimal128Subtract((a),(b))
    #define dec128ToIntegralValue(a) decimal128ToIntegralValue((a))
    #define dec128Ceil(a) decimal128Ceil((a))
    #define dec128Floor(a) decimal128Floor((a))
    #define dec128Acos(a) decimal128Acos((a))
    #define dec128Acos(a) decimal128Acos((a))
    #define dec128Asin(a) decimal128Asin((a))
    #define dec128Atan(a) decimal128Atan((a))
    #define dec128Cos(a) decimal128Cos((a))
    #define dec128Sin(a) decimal128Sin((a))
    #define dec128Tan(a) decimal128Tan((a))
    #define dec128Cosh(a) decimal128Cosh((a))
    #define dec128Sinh(a) decimal128Sinh((a))

  #else /* #if !defined(DFPAL_USE_COMPILER_DFP) */
    /* ---------------- double word operations -------------------------- */
    #define dec64ToString(a,b) dfpal_decimal64ToString((a),(b))
    #define dec64FromString(a) dfpal_decimal64FromString((a))
    #define dec64FromDecimal128(a) ((decimal64)(a))
    #define dec64ToDouble(a) ((double)(a))
    #define dec64FromDouble(a) ((decimal64)(a))
    #define dec64ToPackedBCD(a,b,c,d) decimal64ToPackedBCD((a),(b),(c),(d))
    #define dec64FromPackedBCD(a,b,c) decimal64FromPackedBCD((a),(b),(c))

    #define dec64ToInt64(a) ((int64_t)(a))
    #define dec64ToUint64(a) ((uint64_t)(a))
    #define dec64ToInt32(a) ((int32_t)(a))
    #define dec64ToUint32(a) ((uint32_t)(a))
    #define dec64FromInt64(a) ((decimal64)(a))
    #define dec64FromUint64(a) ((decimal64)(a))
    #define dec64FromInt32(a) ((decimal64)(a))
    #define dec64FromUint32(a) ((decimal64)(a))
    #define dec64FromDecimal32(a) ((decimal64)(a))
    #define dec64ToDecimal32(a) ((decimal32)(a))

    #define dec64IsInfinite(a) decimal64IsInfinite((a))
    #define dec64IsNaN(a) decimal64IsNaN((a))
    #define dec64IsQNaN(a) decimal64IsQNaN((a))
    #define dec64IsSNaN(a) decimal64IsSNaN((a))
    #define dec64IsNegative(a) decimal64IsNegative((a))
    #define dec64IsZero(a) decimal64IsZero ((a))
    #define dec64Trim(a) decimal64Trim((a))
    #define dec64Zero() (0)
    #define dec64GetDigits(a) decimal64GetDigits((a))
    #define dec64GetExponent(a) decimal64GetExponent((a))

    #define dec64Abs(a) decimal64Abs((a))
    #define dec64Add(a,b) ((a)+(b))
    #define dec64Compare(a,b) decimal64Compare((a),(b))
    #define dec64CompareLT(a,b) ((a)<(b))
    #define dec64CompareLE(a,b) ((a)<=(b))
    #define dec64CompareEQ(a,b) ((a)==(b))
    #define dec64CompareNE(a,b) ((a)!=(b))
    #define dec64CompareGT(a,b) ((a)>(b))
    #define dec64CompareGE(a,b) ((a)>=(b))
    #define dec64CompareTotal(a,b) decimal64CompareTotal((a),(b))
    #define dec64Divide(a,b) ((a)/(b))
    #define dec64DivideInteger(a,b) decimal64DivideInteger((a),(b))
    #define dec64Exp(a) decimal64Exp((a));
    #define dec64Ln(a) decimal64Ln((a));
    #define dec64Log10(a) decimal64Log10((a));
    #define dec64Max(a,b) decimal64Max((a),(b))
    #define dec64Min(a,b) decimal64Min((a),(b))
    #define dec64Minus(a) (-(a))
    #define dec64Multiply(a,b) ((a)*(b))
    #define dec64Normalize(a) decimal64Normalize((a))
    #define dec64Plus(a) (+(a))
    #define dec64Power(a,b) decimal64Power((a),(b))
    #define dec64PowerInt(a,b) decimal64Power((a),(b))
    #define dec64Quantize(a,b) decimal64Quantize((a),(b))
    #define dec64Remainder(a,b) ((a)%(b))
    #define dec64RemainderNear(a,b) decimal64RemainderNear((a),(b))
    #define dec64Rescale(a,b) decimal64Rescale((a),(b))
    #define dec64SameQuantum(a,b) decimal64SameQuantum((a),(b))
    #define dec64SquareRoot(a) decimal64SquareRoot((a))
    #define dec64Subtract(a,b) ((a)-(b))
    #define dec64ToIntegralValue(a) decimal64ToIntegralValue((a))
    #define dec64Ceil(a) decimal64Ceil((a))
    #define dec64Floor(a) decimal64Floor((a))
    #define dec64Acos(a) decimal64Acos((a))
    #define dec64Acos(a) decimal64Acos((a))
    #define dec64Asin(a) decimal64Asin((a))
    #define dec64Atan(a) decimal64Atan((a))
    #define dec64Cos(a) decimal64Cos((a))
    #define dec64Sin(a) decimal64Sin((a))
    #define dec64Tan(a) decimal64Tan((a))
    #define dec64Cosh(a) decimal64Cosh((a))
    #define dec64Sinh(a) decimal64Sinh((a))

    /* -------------- quad word operations --------------------------- */
    #define dec128ToString(a,b) dfpal_decimal128ToString((a),(b))
    #define dec128FromString(a) dfpal_decimal128FromString((a))
    #define dec128FromDecimal64(a) ((decimal128)(a))
    #define dec128ToDouble(a) ((double)(a))
    #define dec128FromDouble(a) ((decimal128)(a))
    #define dec128ToPackedBCD(a,b,c,d) decimal128ToPackedBCD((a),(b),(c),(d))
    #define dec128FromPackedBCD(a,b,c) decimal128FromPackedBCD((a),(b),(c))

    #define dec128ToInt64(a) ((int64_t)(a))
    #define dec128ToUint64(a) ((uint64_t)(a))
    #define dec128ToInt32(a) ((int32_t)(a))
    #define dec128ToUint32(a) ((uint32_t)(a))
    #define dec128FromInt64(a) ((decimal128)(a))
    #define dec128FromUint64(a) ((decimal128)(a))
    #define dec128FromInt32(a) ((decimal128)(a))
    #define dec128FromUint32(a) ((decimal128)(a))

    #define dec128IsInfinite(a) decimal128IsInfinite((a))
    #define dec128IsNaN(a) decimal128IsNaN((a))
    #define dec128IsQNaN(a) decimal128IsQNaN((a))
    #define dec128IsSNaN(a) decimal128IsSNaN((a))
    #define dec128IsNegative(a) decimal128IsNegative((a))
    #define dec128IsZero(a) decimal128IsZero ((a))
    #define dec128Trim(a) decimal128Trim((a))
    #define dec128Zero() (0)
    #define dec128GetDigits(a) decimal128GetDigits((a))
    #define dec128GetExponent(a) dec128GetExponent((a))

    #define dec128Abs(a) decimal128Abs((a))
    #define dec128Add(a,b) ((a)+(b))
    #define dec128Compare(a,b) decimal128Compare((a),(b))
    #define dec128CompareLT(a,b) ((a)<(b))
    #define dec128CompareLE(a,b) ((a)<=(b))
    #define dec128CompareEQ(a,b) ((a)==(b))
    #define dec128CompareNE(a,b) ((a)!=(b))
    #define dec128CompareGT(a,b) ((a)>(b))
    #define dec128CompareGE(a,b) ((a)>=(b))
    #define dec128CompareTotal(a,b) decimal128CompareTotal((a),(b))
    #define dec128Divide(a,b) ((a)/(b))
    #define dec128DivideInteger(a,b) decimal128DivideInteger((a),(b))
    #define dec128Exp(a) decimal128Exp((a));
    #define dec128Ln(a) decimal128Ln((a));
    #define dec128Log10(a) decimal128Log10((a));
    #define dec128Max(a,b) decimal128Max((a),(b))
    #define dec128Min(a,b) decimal128Min((a),(b))
    #define dec128Minus(a) (-(a))
    #define dec128Multiply(a,b) ((a)*(b))
    #define dec128Normalize(a) decimal128Normalize((a))
    #define dec128Plus(a) (+(a))
    #define dec128Power(a,b) decimal128Power((a),(b))
    #define dec128PowerInt(a,b) decimal128Power((a),(b))
    #define dec128Quantize(a,b) decimal128Quantize((a),(b))
    #define dec128Remainder(a,b) ((a)%(b))
    #define dec128RemainderNear(a,b) decimal128RemainderNear((a),(b))
    #define dec128Rescale(a,b) decimal128Rescale((a),(b))
    #define dec128SameQuantum(a,b) decimal128SameQuantum((a),(b))
    #define dec128SquareRoot(a) decimal128SquareRoot((a))
    #define dec128Subtract(a,b) ((a)-(b))
    #define dec128ToIntegralValue(a) decimal128ToIntegralValue((a))
    #define dec128Ceil(a) decimal128Ceil((a))
    #define dec128Floor(a) decimal128Floor((a))
    #define dec128Acos(a) decimal128Acos((a))
    #define dec128Acos(a) decimal128Acos((a))
    #define dec128Asin(a) decimal128Asin((a))
    #define dec128Atan(a) decimal128Atan((a))
    #define dec128Cos(a) decimal128Cos((a))
    #define dec128Sin(a) decimal128Sin((a))
    #define dec128Tan(a) decimal128Tan((a))
    #define dec128Cosh(a) decimal128Cosh((a))
    #define dec128Sinh(a) decimal128Sinh((a))
  #endif /* #if !defined(DFPAL_USE_COMPILER_DFP) */

#endif /* #if !defined(__DFPAL_H__) */
