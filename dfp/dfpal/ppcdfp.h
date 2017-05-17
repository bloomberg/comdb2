/* ------------------------------------------------------------------ */
/* Decimal Floating Point Abstraction Layer (DFPAL)                   */
/* ppcdfp.h                                                           */
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

/* ------------------------------------------------------------------ */
/* This MUST be compiled with 128-bit long doubles for dfp_quad       */
/* to work!                                                           */
/* ------------------------------------------------------------------ */

/* ------------------------------------------------------------------ */
/* These functions work on decimal floating point values, disguised   */
/* as binary floating point types.  Typedefs dfp_double and dfp_quad  */
/* are used to slightly reduce the confusion.                         */
/* These values should NEVER be used as binary floating point, because*/
/* they are not.  Treating them as binary will give incorrect results.*/
/*                                                                    */
/* An understanding of the hardware is useful in using these functions*/
/* See ibmdfp.h for correct parameter and result usage.               */
/* ------------------------------------------------------------------ */


#if !defined(__PPCDFP_H__)
#define __PPCDFP_H__

#include "dfpaltypes.h"

/***************************************/
/* decimal floating point pseudo types */
/***************************************/

typedef float       dfp_single;
typedef double      dfp_double;
/*
 * dfp_quad defintion is in dfpaltypes.h for portability reasons, to 
 * support platforms/compilers with no 'long double' datatype
 */
#if defined (STAND_ALONE_PPCDFP_H)
  typedef long double dfp_quad;
#endif

typedef union dfp_single_parameter_u {
  struct dfp_single_parameter_s {
    int        pad;  /* padding need not be initialized          */
    dfp_single f;    /* set this to the desired dfp_single value */
  } f;
  dfp_double d;      /* then pass this as the parameter          */
} dfp_single_parameter;

/* padding to align quad precision parameters */

typedef dfp_double dfp_pad;  /* to align to even/odd register pair */

/* integer in floating point register */

typedef dfp_double dfp_integer_in_double;

typedef union dfp_integer_u {
  signed long long      sll;      /* set this                        */
  unsigned long long    ull;      /* or this to the desired value    */
  char                  bytes[8]; /* or these                        */
  dfp_integer_in_double d;        /* then pass this as the parameter */
  struct fpscr_u {
    unsigned upper28 : 28;
    unsigned drm     : 4;         /* or use this to access drm       */
    unsigned lower32 : 32;
  } fpscr;
} dfp_integer;

/* integer pair in floating point register pair */

typedef struct sllpair_s {
    signed long long upper;
    signed long long lower;
} signed_long_long_pair;

typedef struct ullpair_s {
    unsigned long long upper;
    unsigned long long lower;
} unsigned_long_long_pair;

typedef dfp_quad dfp_integer_pair_in_quad;

typedef union dfp_integer_pair_u {
  signed_long_long_pair       sll_pair;  /* extract this                 */
  unsigned_long_long_pair     ull_pair;  /* or this                      */
  char                        bytes[16]; /* or these                     */
  dfp_integer_pair_in_quad    q;         /* after setting this to result */
} dfp_integer_pair;

/* biased exponent */

typedef enum dfp_biased_exponent_e {
    biased_infinity         = -1,
    biased_quiet_NaN        = -2,
    biased_signaling_NaN    = -3,
    biased_minimum_exponent = 0
} dfp_biased_exponent;

typedef union dfp_biased_exponent_in_double_u {
  long long             e;  /* set this to the desired 64 bit value */
  dfp_integer_in_double d;  /* then pass this as the parameter      */
} dfp_biased_exponent_in_double;

typedef dfp_double dfp_significance_double;
typedef union dfp_significance_u {
  unsigned long long ull;     /* set this to the desired 6 bit integer value */
  dfp_significance_double d;  /* then pass this as the parameter             */
} dfp_significance;

/****************************************************/
/* FPSCR Floating Point Status and Control Register */
/****************************************************/

typedef union {
  dfp_integer all;
  struct ppc_fpscr_s {
    /* upper half */
    unsigned reserved_0_27 : 28; /*  0:27 - reserved */
    unsigned drn           :  4; /* 28:31 - Decimal Rounding Control */
    /* lower half */
    unsigned fx            :  1; /* 0 - FP Exception Summary                                     */
    unsigned fex           :  1; /* 1 - FP Enabled Exception Summary                             */
    unsigned vx            :  1; /* 2 - FP Invalid Operation Exception Summary                   */
    unsigned ox            :  1; /* 3 - FP Overflow Exception                                    */
    unsigned ux            :  1; /* 4 - FP Underflow Exception                                   */
    unsigned zx            :  1; /* 5 - FP Zero Divide Exception                                 */
    unsigned xx            :  1; /* 6 - FP Inexact Exception                                     */
    unsigned vxsnan        :  1; /* 7 - FP Invalid Operation Exception (SNaN)                    */
    unsigned vxisi         :  1; /* 8 - FP Invalid Operation Exception (Inf - Inf)               */
    unsigned vxidi         :  1; /* 9 - FP Invalid Operation Exception (Inf + Inf)               */
    unsigned vxzdz         :  1; /* 10 - FP Invalid Operation Exception (0 / 0)                   */
    unsigned vximz         :  1; /* 11 - FP Invalid Operation Exception (Inf x 0)                 */
    unsigned vxvc          :  1; /* 12 - FP Invalid Operation Exception (Invalid Compare)         */
    unsigned fr            :  1; /* 13 - FP Fraction Rounded                                      */
    unsigned fi            :  1; /* 14 - FP Fraction Inexact                                      */
  /*unsigned fprf          :  5;    15:19 - FP Result Flags                                       */
    unsigned c             :  1; /* 15    - FP Class Descriptor                                   */
  /*unsigned fpcc          :  4;    16:19 - FP Condition Code                                     */
    unsigned fl            :  1; /* 16    - FP Less Than or Negative                              */
    unsigned fg            :  1; /* 17    - FP Greater Than or Positive                           */
    unsigned fe            :  1; /* 18    - FP Equal or Zero                                      */
    unsigned fu            :  1; /* 19    - FP Unordered or NaN                                   */
    unsigned reserved_20   :  1; /* 20 - reserved                                                 */
    unsigned vxsoft        :  1; /* 21 - FP Invalid Operation Exception (Software Request)        */
    unsigned vxsqrt        :  1; /* 22 - FP Invalid Operation Exception (Invalid Square Root)     */
    unsigned vxcvi         :  1; /* 23 - FP Invalid Operation Exception (Invalid Integer Convert) */
    unsigned ve            :  1; /* 24 - FP Invalid Operation Exception Enable                    */
    unsigned oe            :  1; /* 25 - FP Overflow Exception Enable                             */
    unsigned ue            :  1; /* 26 - FP Underflow Exception Enable                            */
    unsigned ze            :  1; /* 27 - FP Zero Divide Exception Enable                          */
    unsigned xe            :  1; /* 28 - FP Inexact Exception Enable                              */
    unsigned ni            :  1; /* 29 - FP Non-IEEE Mode                                         */
    unsigned rn            :  2; /* 30:31 - (Binary) Rounding Control                             */
  } fpscr_attrib;
} ppc_fpscr;


#if defined (STAND_ALONE_PPCDFP_H)
typedef enum compare_result_e {
  DFPAL_COMP_LT = 8, /* 1000 */
  DFPAL_COMP_GT = 4, /* 0100 */
  DFPAL_COMP_EQ = 2, /* 0010 */
  DFPAL_COMP_UO = 1  /* 0001 = unordered */
} compare_result;
#endif

#if !defined(DFPAL_NO_HW_DFP) /* { */
#if defined(DFPAL_OS_AIX5L) || defined(DFPAL_OS_I5OS) \
  || defined(DFPAL_OS_LOP_XLC) /* { */

/************************/
/* Support Instructions */
/************************/

/* Floating Point Move Register */
/* fmr: opcode=63 frt frb subopcode=72=0x48 rc=0 */
/* fmr fr1=fr2: opcode=63=111111 frt=1=00001 fra=00000 frb=2=00010 subopcode=72=0x048=00_0100_1000 rc=0 = FC201090 */
/* fmr fr2=fr3: opcode=63=111111 frt=2=00010 fra=00000 frb=3=00011 subopcode=72=0x048=00_0100_1000 rc=0 = FC401890 */
#define PPC_FMR_1_2 "FC201090"
#define PPC_FMR_2_3 "FC401890"

/* Move Condition Register to General Purpose Register */
/* mfcr gr3: opcode=31=011111 rt=3=00011 00000 00000 subopcode=19=0x013=00_0001_0011 rc=0 = 7C600026 */
#define PPC_MFCR_3 "7C600026"

/* Rotate Left Word Immediate then AND with Mask */
/* rlwinm gr3=gr3,4,28,31: opcode=21=010101 rt=3=00011 ra=3=00011 sh=4=00100 mb=28=11100 me=31=11111 rc=0 = 5463273E */
#define PPC_RLWINM_3_3_4_28_31 "5463273E"

/***********************************/
/* Decimal Rounding Mode and FPSCR */
/***********************************/

/* Get FPSCR */

/* mffs: opcode=63=111111 frt=1=00001 00000 00000 subopcode=583=0x247=10_0100_0111 rc=0 = FC20048E */
/* The Decimal Rounding Mode is the lower 4 bits of the upper half of the FPSCR. */
/* NOTE this returns an integer in an FPR that needs to be stored into a union to extract an int. */
dfp_integer_in_double ppc_mffs (void);
#pragma mc_func ppc_mffs { "FC20048E" }
#pragma reg_killed_by ppc_mffs fp1 /* actually none */

/* Set FPSCR */

/* mtfs(f): opcode=63=111111 l=1 flm=00000000 w=0 frb=1=00001 subopcode=711=0x2C7=10_1100_0111 rc=0 = FE000D8E */
/* NOTE the parameter is bits in an FPR that needs to be stored into a union to extract a dfp_double. */
void ppc_mtfs (dfp_integer_in_double fpscr);
#pragma mc_func ppc_mtfs { "FE000D8E" }
#pragma reg_killed_by ppc_mtfs fp1 /* actually none */


/* Set FPSCR Decimal Rounding Mode */

/* mtfsf: opcode=63=111111 l=0 flm=00000001 w=1 frb=1=00001 subopcode=711=0x2C7=10_1100_0111 rc=0 = FC030D8E */
/* The Decimal Rounding Mode is the lower 4 bits of the upper half of the FPSCR. */
/* The parameter must have the drm in that position. Other bits will be ignored. */
/* NOTE the parameter is bits in an FPR that needs to be stored into a union to extract a dfp_double. */
void ppc_mtfsf_drm (dfp_integer_in_double fpscr_drm);
#pragma mc_func ppc_mtfsf_drm { "FC030D8E" }
#pragma reg_killed_by ppc_mtfsf_drm fp1 /* actually none */

/********************************************************************************/
/* FPSCR Decimal Rounding Modes                                                 */
/* The following decimal rounding modes are declared in ibmdfp.h:               */
/*   0 = rm_to_nearest_with_ties_to_even                                        */
/*   1 = rm_toward_zero                                                         */
/*   2 = rm_toward_positive_infinity                                            */
/*   3 = rm_toward_negative_infinity                                            */
/*   4 = rm_to_nearest_with_ties_away_from_zero                                 */
/*   5 = rm_to_nearest_with_ties_toward_zero                                    */
/*   6 = rm_away_from_zero                                                      */
/*   7 = rm_round_to_prepare_for_shorter_precision                              */
/********************************************************************************/

/********************************************************************************/
/* Rounding Mode Control                                                        */
/*                                                                              */
/* Some instructions always use the rounding mode in the FPSCR.                 */
/* Others allow specifying a primary rounding mode control in the instruction.  */
/* A few allow specifying a secondary rounding mode control in the instruction, */
/* using the r field to select primary or secondary RMC.                        */
/* The functions for these instructions have RMC suffixes.                      */
/* These rounding mode controls do NOT have the same values as the FPSCR field. */
/* Primary Rounding Mode Control:                                               */
/*   (r=0) rmc=00 = _rne    = round to nearest, ties to even                    */
/*   (r=0) rmc=01 = _rtz    = round towards zero                                */
/*   (r=0) rmc=10 = _rnaz   = round to nearest, ties away from zero             */
/*   (r=0) rmc=11 = _rfpscr = round using FPSCR mode                            */
/* Secondary Rounding Mode Control:                                             */
/*    r=1  rmc=00 = _rtpi   = round towards ppositive infinity                  */ 
/*    r=1  rmc=01 = _rtmi   = round towards negative infinity                   */
/*    r=1  rmc=10 = _rafz   = round away from zero                              */
/*    r=1  rmc=11 = _rntz   = round to nearest, ties towards zero               */
/********************************************************************************/

/**************/
/* Arithmetic */
/**************/

/* Add */

/* dadd: opcode=59=111011 frt=1=00001 fra=1=00001 frb=2=00010 subopcode=2=0x002=00_0000_0010 rc=0 = EC211004 */
dfp_double ppc_dadd (dfp_double x, dfp_double y);
#pragma mc_func ppc_dadd { "EC211004" }
#pragma reg_killed_by ppc_dadd fsr

/* daddq: opcode=63=111111 frt=2and3=00010 fra=2and3=00010 frb=4and5=00100 subopcode=2=0x002=00_0000_0010 rc=0 = FC422004 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
dfp_quad ppc_daddq (dfp_pad pad, dfp_quad x, dfp_quad y);
#pragma mc_func ppc_daddq { "FC422004FC201090FC401890" }
#pragma reg_killed_by ppc_daddq fp3,fsr

/* Subtract */

/* dsub: opcode=59=111011 frt=1=00001 fra=1=00001 frb=2=00010 subopcode=514=0x202=10_0000_0010 rc=0 = EC211404 */
dfp_double ppc_dsub (dfp_double x, dfp_double y);
#pragma mc_func ppc_dsub { "EC211404" }
#pragma reg_killed_by ppc_dsub fsr

/* dsubq: opcode=63=111111 frt=2and3=00010 fra=2and3=00010 frb=4and5=00100 subopcode=514=0x202=10_0000_0010 rc=0 = FC422404 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
dfp_quad ppc_dsubq (dfp_pad pad, dfp_quad x, dfp_quad y);
#pragma mc_func ppc_dsubq { "FC422404FC201090FC401890" }
#pragma reg_killed_by ppc_dsubq fp3,fsr

/* Multiply */

/* dmul: opcode=59=111011 frt=1=00001 fra=1=00001 frb=2=00010 subopcode=34=0x022=00_0010_0010 rc=0 = EC211044 */
dfp_double ppc_dmul (dfp_double x, dfp_double y);
#pragma mc_func ppc_dmul { "EC211044" }
#pragma reg_killed_by ppc_dmul fsr

/* dmulq: opcode=63=111111 frt=2and3=00010 fra=2and3=00010 frb=4and5=00100 subopcode=34=0x022=00_0010_0010 rc=0 = FC422044 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
dfp_quad ppc_dmulq (dfp_pad pad, dfp_quad x, dfp_quad y);
#pragma mc_func ppc_dmulq { "FC422044FC201090FC401890" }
#pragma reg_killed_by ppc_dmulq fp3,fsr

/* Divide */

/* ddiv: opcode=59=111011 frt=1=00001 fra=1=00001 frb=2=00010 subopcode=546=0x222=10_0010_0010 rc=0 = EC211444 */
dfp_double ppc_ddiv (dfp_double x, dfp_double y);
#pragma mc_func ppc_ddiv { "EC211444" }
#pragma reg_killed_by ppc_ddiv fsr

/* ddivq: opcode=63=111111 frt=2and3=00010 fra=2and3=00010 frb=4and5=00100 subopcode=546=0x222=10_0010_0010 rc=0 = FC422444 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
dfp_quad ppc_ddivq (dfp_pad pad, dfp_quad x, dfp_quad y);
#pragma mc_func ppc_ddivq { "FC422444FC201090FC401890" }
#pragma reg_killed_by ppc_ddivq fp3,fsr

/* Negate */

/* fneg: opcode=63=111111 frt=1=00001 x=xxxxx frb=1=00001 subopcode=40=0x028=00_0010_1000 rc=0 = FC200850 */ 
dfp_double ppc_fneg (dfp_double x);
#pragma mc_func ppc_fneg { "FC200850" }

/* fnegq: opcode=63=111111 frt=1=00001 x=xxxxx frb=1=00001 subopcode=40=0x028=00_0010_1000 rc=0 = FC200850 */ 
dfp_quad ppc_fnegq (dfp_quad x);  /* no pad needed */
#pragma mc_func ppc_fnegq { "FC200850" }

/* Absolute Value */

/* fabs: opcode=63=111111 frt=1=00001 x=xxxxx frb=1=00001 subopcode=264=x108=01_0000_1000 rc=0 = FC200A10 */ 
dfp_double ppc_fabs (dfp_double x);
#pragma mc_func ppc_fabs { "FC200A10" }

/* fabsq: opcode=63=111111 frt=1=00001 x=xxxxx frb=1=00001 subopcode=264=x108=01_0000_1000 rc=0 = FC200A10 */ 
dfp_quad ppc_fabsq (dfp_quad x);
#pragma mc_func ppc_fabsq { "FC200A10" }  /* no pad needed */

/************/
/* Quantize */
/************/

/* Quantize */

/* dqua: opcode=59=111011 frt=1=00001 fra=1=00001 frb=2=00010 rmc=xx subopcode=3=0x03=0000_0011 rc=0 = EC211X(0xx0)06 */
/* NOTE: The primary rounding mode control must be specified in the hex parameter. */
/*       To allow a 2 bit rmc the subopcode is 8 not 10 bits. */
dfp_double ppc_dqua_rne    (dfp_double x, dfp_double y);
dfp_double ppc_dqua_rtz    (dfp_double x, dfp_double y);
dfp_double ppc_dqua_rnaz   (dfp_double x, dfp_double y);
dfp_double ppc_dqua_rfpscr (dfp_double x, dfp_double y);
#pragma mc_func ppc_dqua_rne    { "EC211006" }
#pragma mc_func ppc_dqua_rtz    { "EC211206" }
#pragma mc_func ppc_dqua_rnaz   { "EC211406" }
#pragma mc_func ppc_dqua_rfpscr { "EC211606" }
#pragma reg_killed_by ppc_dqua_rne    fsr
#pragma reg_killed_by ppc_dqua_rtz    fsr
#pragma reg_killed_by ppc_dqua_rnaz   fsr
#pragma reg_killed_by ppc_dqua_rfpscr fsr

/* dquaq: opcode=63=111111 frt=2and3=00010 fra=2and3=00010 frb=4and5=00100 rmc=xx subopcode=3=0x03=0000_0011 rc=0 = FC422X(0xx0)06 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
/* NOTE: The primary rounding mode control must be specified in the hex parameter. */
/*       To allow a 2 bit rmc the subopcode is 8 not 10 bits. */
dfp_quad ppc_dquaq_rne    (dfp_pad pad, dfp_quad x, dfp_quad y);
dfp_quad ppc_dquaq_rtz    (dfp_pad pad, dfp_quad x, dfp_quad y);
dfp_quad ppc_dquaq_rnaz   (dfp_pad pad, dfp_quad x, dfp_quad y);
dfp_quad ppc_dquaq_rfpscr (dfp_pad pad, dfp_quad x, dfp_quad y);
#pragma mc_func ppc_dquaq_rne    { "FC422006FC201090FC401890" }
#pragma mc_func ppc_dquaq_rtz    { "FC422206FC201090FC401890" }
#pragma mc_func ppc_dquaq_rnaz   { "FC422406FC201090FC401890" }
#pragma mc_func ppc_dquaq_rfpscr { "FC422606FC201090FC401890" }
#pragma reg_killed_by ppc_dquaq_rne    fp3,fsr
#pragma reg_killed_by ppc_dquaq_rtz    fp3,fsr
#pragma reg_killed_by ppc_dquaq_rnaz   fp3,fsr
#pragma reg_killed_by ppc_dquaq_rfpscr fp3,fsr

/* Quantize Immediate */

/* dquai: opcode=59=111011 frt=1=00001 te=xxxxx frb=00001 rmc=xx subopcode=67=0x43=0100_0011 rc=0 = EC2X0X(1xx0)86 */
/* NOTE: The immediate value (te) must be specified in the hex parameter. */
/*       A _0 suffix means it is 0. */
/*       Others could be added. */
/* NOTE: The primary rounding mode control must be specified in the hex parameter. */
/*       To allow a 2 bit rmc the subopcode is 8 not 10 bits. */
dfp_double ppc_dquai_rne_0    (dfp_double x);
dfp_double ppc_dquai_rtz_0    (dfp_double x);
dfp_double ppc_dquai_rnaz_0   (dfp_double x);
dfp_double ppc_dquai_rfpscr_0 (dfp_double x);
#pragma mc_func ppc_dquai_rne_0    { "EC200886" }
#pragma mc_func ppc_dquai_rtz_0    { "EC200A86" }
#pragma mc_func ppc_dquai_rnaz_0   { "EC200C86" }
#pragma mc_func ppc_dquai_rfpscr_0 { "EC200E86" }
#pragma reg_killed_by ppc_dquai_rne_0    fsr
#pragma reg_killed_by ppc_dquai_rtz_0    fsr
#pragma reg_killed_by ppc_dquai_rnaz_0   fsr
#pragma reg_killed_by ppc_dquai_rfpscr_0 fsr

/* dquaiq: opcode=63=111111 frt=2=00010 te=xxxxx frb=2and3=00010 rmc=xx subopcode=67=0x43=0100_0011 rc=0 = FC4X1X(0xx0)86 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
/* NOTE: The immediate value (te) must be specified in the hex parameter. */
/*       A _0 suffix means it is 0. */
/*       Others could be added. */
/* NOTE: The rounding mode control must be specified in the hex parameter. */
/*       To allow a 2 bit rmc the subopcode is 8 not 10 bits. */
dfp_quad ppc_dquaiq_rne_0    (dfp_pad pad, dfp_quad x);
dfp_quad ppc_dquaiq_rtz_0    (dfp_pad pad, dfp_quad x);
dfp_quad ppc_dquaiq_rnaz_0   (dfp_pad pad, dfp_quad x);
dfp_quad ppc_dquaiq_rfpscr_0 (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_dquaiq_rne_0    { "FC401086FC201090FC401890" }
#pragma mc_func ppc_dquaiq_rtz_0    { "FC401286FC201090FC401890" }
#pragma mc_func ppc_dquaiq_rnaz_0   { "FC401486FC201090FC401890" }
#pragma mc_func ppc_dquaiq_rfpscr_0 { "FC401686FC201090FC401890" }
#pragma reg_killed_by ppc_dquaiq_rne_0    fp3,fsr
#pragma reg_killed_by ppc_dquaiq_rtz_0    fp3,fsr
#pragma reg_killed_by ppc_dquaiq_rnaz_0   fp3,fsr
#pragma reg_killed_by ppc_dquaiq_rfpscr_0 fp3,fsr

/***********/
/* Reround */
/***********/

/* drrnd: opcode=59=111011 frt=1=00001 x=xxxxx frb=2=00010 rmc=xx subopcode=35=0x23=0010_0011 rc=0 = EC2X1X(0xx0)46 */
/* NOTE: The significance must be specified in the "hex parameter. */
/*       A _7 suffix means it is 7. */
/*       Others could be added. */
/* NOTE: The primary rounding mode control must be specified in the hex parameter. */
/*       To allow a 2 bit rmc the subopcode is 8 not 10 bits. */
dfp_double ppc_drrnd_rne_7    (dfp_double x);
dfp_double ppc_drrnd_rtz_7    (dfp_double x);
dfp_double ppc_drrnd_rnaz_7   (dfp_double x);
dfp_double ppc_drrnd_rfpscr_7 (dfp_double x);
#pragma mc_func ppc_drrnd_rne_7    { "EC271046" }
#pragma mc_func ppc_drrnd_rtz_7    { "EC271246" }
#pragma mc_func ppc_drrnd_rnaz_7   { "EC271446" }
#pragma mc_func ppc_drrnd_rfpscr_7 { "EC271646" }
#pragma reg_killed_by ppc_drrnd_rne_7    fsr
#pragma reg_killed_by ppc_drrnd_rtz_7    fsr
#pragma reg_killed_by ppc_drrnd_rnaz_7   fsr
#pragma reg_killed_by ppc_drrnd_rfpscr_7 fsr

/* drrndq: opcode=63=111111 frt=2and3=00010 x=xxxxx frb=2and3=00010 rmc=xx subopcode=35=0x23=0010_0011 rc=0 = FC4X1X(0xx0)46 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
/* NOTE: The significance must be specified in the hex parameter. */
/*       A _7 suffix means it is 7. */
/*       A _16 suffix means it is 16. */
/*       Others could be added. */
/* NOTE: The rounding mode control must be specified in the hex parameter. */
/*       To allow a 2 bit rmc the subopcode is 8 not 10 bits. */
dfp_quad ppc_drrndq_rne_7    (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drrndq_rtz_7    (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drrndq_rnaz_7   (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drrndq_rfpscr_7 (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_drrndq_rne_7    { "FC471046FC201090FC401890" }
#pragma mc_func ppc_drrndq_rtz_7    { "FC471246FC201090FC401890" }
#pragma mc_func ppc_drrndq_rnaz_7   { "FC471446FC201090FC401890" }
#pragma mc_func ppc_drrndq_rfpscr_7 { "FC471646FC201090FC401890" }
#pragma reg_killed_by ppc_drrndq_rne_7    fp3,fsr
#pragma reg_killed_by ppc_drrndq_rtz_7    fp3,fsr
#pragma reg_killed_by ppc_drrndq_rnaz_7   fp3,fsr
#pragma reg_killed_by ppc_drrndq_rfpscr_7 fp3,fsr
dfp_quad ppc_drrndq_rne_16    (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drrndq_rtz_16    (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drrndq_rnaz_16   (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drrndq_rfpscr_16 (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_drrndq_rne_16    { "FC501046FC201090FC401890" }
#pragma mc_func ppc_drrndq_rtz_16    { "FC501246FC201090FC401890" }
#pragma mc_func ppc_drrndq_rnaz_16   { "FC501446FC201090FC401890" }
#pragma mc_func ppc_drrndq_rfpscr_16 { "FC501646FC201090FC401890" }
#pragma reg_killed_by ppc_drrndq_rne_16    fp3,fsr
#pragma reg_killed_by ppc_drrndq_rtz_16    fp3,fsr
#pragma reg_killed_by ppc_drrndq_rnaz_16   fp3,fsr
#pragma reg_killed_by ppc_drrndq_rfpscr_16 fp3,fsr

/***********/
/* Compare */
/***********/


/* Compare Ordered */

/* dcmpo: opcode=59=111011 bf=0=000 00 fra=1=00001 frb=2=00010 subopcode=130=0x082=00_1000_0010 rc=0 = EC011104 */
compare_result ppc_dcmpo (dfp_double x, dfp_double y);
#pragma mc_func ppc_dcmpo { "EC0111047C6000265463273E" }
#pragma reg_killed_by ppc_dcmpo cr1,fsr

/* dcmpoq: opcode=63=111111 bf=0=000 00 fra=2and3=00010 frb=4and5=00100 subopcode=130=0x082=00_1000_0010 rc=0 = FC022104 */
compare_result ppc_dcmpoq (dfp_pad pad, dfp_quad x, dfp_quad y);
#pragma mc_func ppc_dcmpoq { "FC0221047C6000265463273E" }
#pragma reg_killed_by ppc_dcmpoq cr1,fsr

/* Compare Unordered */

/* dcmpu: opcode=59=111011 bf=0=000 00 fra=1=00001 frb=2=00010 subopcode=642=0x282=10_1000_0010 rc=0 = EC011504 */
compare_result ppc_dcmpu (dfp_double x, dfp_double y);
#pragma mc_func ppc_dcmpu { "EC0115047C6000265463273E" }
#pragma reg_killed_by ppc_dcmpu cr1,fsr

/* dcmpuq: opcode=63=111111 bf=0=000 00 fra=2and3=00010 frb=4and5=00100 subopcode=642=0x282=10_1000_0010 rc=0 = FC022504 */
compare_result ppc_dcmpuq (dfp_pad pad, dfp_quad x, dfp_quad y);
#pragma mc_func ppc_dcmpuq { "FC0225047C6000265463273E" }
#pragma reg_killed_by ppc_dcmpuq cr1,fsr

/* Test Exponent */

/* dtstex: opcode=59=111011 bf=0=000 00 fra=1=00001 frb=2=00010 subopcode=162=0x0A2=00_1010_0010 rc=0 = EC011144 */
compare_result ppc_dtstex (dfp_double x, dfp_double y);
#pragma mc_func ppc_dtstex { "EC0111447C6000265463273E" }
#pragma reg_killed_by ppc_dtstex cr1,fsr

/* dtstexq: opcode=63=111111 bf=0=000 00 fra=2and3=00010 frb=4and5=00100 subopcode=162=0x0A2=00_1010_0010 rc=0 = FC022144 */
compare_result ppc_dtstexq (dfp_pad pad, dfp_quad x, dfp_quad y);
#pragma mc_func ppc_dtstexq { "FC0221447C6000265463273E" }
#pragma reg_killed_by ppc_dtstexq cr1,fsr

/* Test Significance */

/* dtstsf: opcode=59=111011 bf=0=000 00 fra=1=00001 frb=2=00010 subopcode=674=0x2A2=10_1010_0010 rc=0 = EC011544 */
compare_result ppc_dtstsf (dfp_significance_double x, dfp_double y);
#pragma mc_func ppc_dtstsf { "EC0115447C6000265463273E" }
#pragma reg_killed_by ppc_dtstsf cr1,fsr

/* dtstsfq: opcode=63=111111 bf=0=000 00 fra=1=00001 frb=2and3=00010 subopcode=674=0x2A2=10_1010_0010 rc=0 = FC011544 */
compare_result ppc_dtstsfq (dfp_significance_double x, dfp_quad y);
#pragma mc_func ppc_dtstsfq { "FC0115447C6000265463273E" }
#pragma reg_killed_by ppc_dtstsfq cr1,fsr

/*************/
/* Test Data */
/*************/

typedef enum test_data_result_e {
  positive_no_match = 0, /* 0000 */
  positive_match    = 2, /* 0010 */
  negative_no_match = 8, /* 1000 */
  negative_match    = 10 /* 1010 */
} test_data_result;

typedef enum test_data_result_mask_e {
  negative_mask = 8, /* 1xxx */
  negative      = 8, /* 1000 */
  positive      = 0, /* 0000 */
  match_mask    = 2, /* xx1x */
  match         = 2, /* 0010 */
  no_match      = 0  /* 0000 */
} test_data_result_mask;

/* Mask for Test Data Class:
   bit 0 = 100000 = zero
   bit 1 = 010000 = subnormal
   bit 2 = 001000 = normal
   bit 3 = 000100 = infinity
   bit 4 = 000010 = quiet NaN
   bit 5 = 000001 = signalling NaN
*/

/* Mask for Test Data Group:
   bit 0 = 100000 = zero with non-extreme exponent
   bit 1 = 010000 = zero with extreme exponent
   bit 2 = 001000 = subnormal or (normal with extreme exponent)
   bit 3 = 000100 = normal with non-extreme exponent and leftmost zero digit
   bit 4 = 000010 = normal with non-extreme exponent and leftmost nonzero digit
   bit 5 = 000001 = special symbol (infinity, quiet NaN, or signalling NaN)
*/

/* Test Data Class */

/* dtstdc: opcode=59=111011 bf=0=000 00 fra=1=00001 xxxxxx subopcode=194=0C2=0_1100_0010 rc=0 = EC01x184 */
/* NOTE: The mask must be specified in the hex parameter. */
/*       To allow a 6 bit mask the subopcode is 9 not 10 bits */
/* unimplemented:
test_data_result ppc_dtstdc (dfp_double x);
#pragma mc_func ppc_dtstdc { "EC0101847C6000265463273E" }
#pragma reg_killed_by ppc_dtstdc cr1,fsr
*/

/* dtstdcq: opcode=63=111111 bf=0=000 00 fra=2and3=00010 xxxxxx subopcode=194=0C2=0_1100_0010 rc=0 = FC02x184 */
/* NOTE: The mask must be specified in the hex parameter. */
/*       To allow a 6 bit mask the subopcode is 9 not 10 bits */
/* unimplemented:
test_data_result ppc_dtstdcq (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_dtstdcq { "FC0201847C6000265463273E" }
#pragma reg_killed_by ppc_dtstdcq cr1,fsr
*/

/* Test Data Group */

/* dtstdg: opcode=59=111011 bf=0=000 00 fra=1=00001 xxxxxx subopcode=226=0E2=0_1110_0010 rc=0 = EC01x1C4 */
/* NOTE: The mask must be specified in the "hex parameter. */
/*       To allow a 6 bit mask the subopcode is 9 not 10 bits */
/* unimplemented:
test_data_result ppc_dtstdg (dfp_double x);
#pragma mc_func ppc_dtstdg { "EC0101C47C6000265463273E" }
#pragma reg_killed_by ppc_dtstdg cr1,fsr
*/

/* dtstdgq: opcode=63=111111 bf=0=000 00 fra=2and3=00010 xxxxxx subopcode=226=0E2=0_1110_0010 rc=0 = FC02x1C4 */
/* NOTE: The mask must be specified in the "hex parameter. */
/*       To allow a 6 bit mask the subopcode is 9 not 10 bits */
/* unimplemented:
test_data_result ppc_dtstdcq (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_dtstdcq { "FC0201C47C6000265463273E" }
#pragma reg_killed_by ppc_dtstdcq cr1,fsr
*/

/***********************/
/* Round to FP Integer */
/***********************/

/* Round to FP Integer With Inexact */

/* drintx: opcode=59=111011 frt=1=00001 0000 r=0or1 frb=1=00001 rmc=xx subopcode=99=0x63=0110_0011 rc=0 = EC2x0X(1xx0)C6 */
/* NOTE: The rounding mode control must be specified in the hex parameter. */
/*       To allow a 2 bit primary or secondary (depending on r) rmc the subopcode is 8 not 10 bits. */
dfp_double ppc_drintx_rne  (dfp_double x);
dfp_double ppc_drintx_rtz  (dfp_double x);
dfp_double ppc_drintx_rnaz (dfp_double x);
dfp_double ppc_drintx_rfpscr (dfp_double x);
dfp_double ppc_drintx_rtpi (dfp_double x);
dfp_double ppc_drintx_rtmi (dfp_double x);
dfp_double ppc_drintx_rafz (dfp_double x);
dfp_double ppc_drintx_rntz (dfp_double x);
#pragma mc_func ppc_drintx_rne  { "EC2008C6" }
#pragma mc_func ppc_drintx_rtz  { "EC200AC6" }
#pragma mc_func ppc_drintx_rnaz { "EC200CC6" }
#pragma mc_func ppc_drintx_rfpscr { "EC200EC6" }
#pragma mc_func ppc_drintx_rtpi { "EC2108C6" }
#pragma mc_func ppc_drintx_rtmi { "EC210AC6" }
#pragma mc_func ppc_drintx_rafz { "EC210CC6" }
#pragma mc_func ppc_drintx_rntz { "EC210EC6" }
#pragma reg_killed_by ppc_drintx_rne  fsr
#pragma reg_killed_by ppc_drintx_rtz  fsr
#pragma reg_killed_by ppc_drintx_rnaz fsr
#pragma reg_killed_by ppc_drintx_rfpscr fsr
#pragma reg_killed_by ppc_drintx_rtpi fsr
#pragma reg_killed_by ppc_drintx_rtmi fsr
#pragma reg_killed_by ppc_drintx_rafz fsr
#pragma reg_killed_by ppc_drintx_rntz fsr

/* drintxq: opcode=63=111111 frt=2=00010 0000 r=0or1 frb=2and3=00010 rmc=xx subopcode=99=0x63=0110_0011 rc=0 = FC4x1X(0xx0)C6 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
/* NOTE: The rounding mode control must be specified in the hex parameter. */
/*       To allow a 2 bit primary or secondary (depending on r) rmc the subopcode is 8 not 10 bits. */
dfp_quad ppc_drintxq_rne  (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drintxq_rtz  (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drintxq_rnaz (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drintxq_rfpscr (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drintxq_rtpi (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drintxq_rtmi (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drintxq_rafz (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drintxq_rntz (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_drintxq_rne  { "FC4010C6FC201090FC401890" }
#pragma mc_func ppc_drintxq_rtz  { "FC4012C6FC201090FC401890" }
#pragma mc_func ppc_drintxq_rnaz { "FC4014C6FC201090FC401890" }
#pragma mc_func ppc_drintxq_rfpscr { "FC4016C6FC201090FC401890" }
#pragma mc_func ppc_drintxq_rtpi { "FC4110C6FC201090FC401890" }
#pragma mc_func ppc_drintxq_rtmi { "FC4112C6FC201090FC401890" }
#pragma mc_func ppc_drintxq_rafz { "FC4114C6FC201090FC401890" }
#pragma mc_func ppc_drintxq_rntz { "FC4116C6FC201090FC401890" }
#pragma reg_killed_by ppc_drintxq_rne  fp3,fsr
#pragma reg_killed_by ppc_drintxq_rtz  fp3,fsr
#pragma reg_killed_by ppc_drintxq_rnaz fp3,fsr
#pragma reg_killed_by ppc_drintxq_rfpscr fp3,fsr
#pragma reg_killed_by ppc_drintxq_rtpi fp3,fsr
#pragma reg_killed_by ppc_drintxq_rtmi fp3,fsr
#pragma reg_killed_by ppc_drintxq_rafz fp3,fsr
#pragma reg_killed_by ppc_drintxq_rntz fp3,fsr

/* Round to FP Integer Without Inexact */

/* drintn: opcode=59=111011 frt=1=00001 0000 r=0or1 frb=1=00001 rmc=xx subopcode=227=0xE3=1110_0011 rc=0 = EC2x0X(1xx1)C6 */
/* NOTE: The rounding mode control must be specified in the hex parameter. */
/*       To allow a 2 bit primary or secondary (depending on r) rmc the subopcode is 8 not 10 bits. */
dfp_double ppc_drintn_rne  (dfp_double x);
dfp_double ppc_drintn_rtz  (dfp_double x);
dfp_double ppc_drintn_rnaz (dfp_double x);
dfp_double ppc_drintn_rfpscr (dfp_double x);
dfp_double ppc_drintn_rtpi (dfp_double x);
dfp_double ppc_drintn_rtmi (dfp_double x);
dfp_double ppc_drintn_rafz (dfp_double x);
dfp_double ppc_drintn_rntz (dfp_double x);
#pragma mc_func ppc_drintn_rne  { "EC2009C6" }
#pragma mc_func ppc_drintn_rtz  { "EC200BC6" }
#pragma mc_func ppc_drintn_rnaz { "EC200DC6" }
#pragma mc_func ppc_drintn_rfpscr { "EC200FC6" }
#pragma mc_func ppc_drintn_rtpi { "EC2109C6" }
#pragma mc_func ppc_drintn_rtmi { "EC210BC6" }
#pragma mc_func ppc_drintn_rafz { "EC210DC6" }
#pragma mc_func ppc_drintn_rntz { "EC210FC6" }
#pragma reg_killed_by ppc_drintn_rne  fsr
#pragma reg_killed_by ppc_drintn_rtz  fsr
#pragma reg_killed_by ppc_drintn_rnaz fsr
#pragma reg_killed_by ppc_drintn_rfpscr fsr
#pragma reg_killed_by ppc_drintn_rtpi fsr
#pragma reg_killed_by ppc_drintn_rtmi fsr
#pragma reg_killed_by ppc_drintn_rafz fsr
#pragma reg_killed_by ppc_drintn_rntz fsr

/* drintnq: opcode=63=111111 frt=2=00010 0000 r=0or1 frb=2and3=00010 rmc=xx subopcode=227=0xE3=1110_0011 rc=0 = FC4x1X(0xx1)C6 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
/* NOTE: The rounding mode control must be specified in the hex parameter. */
/*       To allow a 2 bit primary or secondary (depending on r) rmc the subopcode is 8 not 10 bits. */
dfp_quad ppc_drintnq_rne  (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drintnq_rtz  (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drintnq_rnaz (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drintnq_rfpscr (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drintnq_rtpi (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drintnq_rtmi (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drintnq_rafz (dfp_pad pad, dfp_quad x);
dfp_quad ppc_drintnq_rntz (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_drintnq_rne  { "FC4011C6FC201090FC401890" }
#pragma mc_func ppc_drintnq_rtz  { "FC4013C6FC201090FC401890" }
#pragma mc_func ppc_drintnq_rnaz { "FC4015C6FC201090FC401890" }
#pragma mc_func ppc_drintnq_rfpscr { "FC4017C6FC201090FC401890" }
#pragma mc_func ppc_drintnq_rtpi { "FC4111C6FC201090FC401890" }
#pragma mc_func ppc_drintnq_rtmi { "FC4113C6FC201090FC401890" }
#pragma mc_func ppc_drintnq_rafz { "FC4115C6FC201090FC401890" }
#pragma mc_func ppc_drintnq_rntz { "FC4117C6FC201090FC401890" }
#pragma reg_killed_by ppc_drintnq_rne  fp3,fsr
#pragma reg_killed_by ppc_drintnq_rtz  fp3,fsr
#pragma reg_killed_by ppc_drintnq_rnaz fp3,fsr
#pragma reg_killed_by ppc_drintnq_rfpscr fp3,fsr
#pragma reg_killed_by ppc_drintnq_rtpi fp3,fsr
#pragma reg_killed_by ppc_drintnq_rtmi fp3,fsr
#pragma reg_killed_by ppc_drintnq_rafz fp3,fsr
#pragma reg_killed_by ppc_drintnq_rntz fp3,fsr

/************************/
/* Precision Conversion */
/************************/

/* Convert DFP32 to DFP64 */

/* dctdp: opcode=59=111011 frt=1=00001 00000 frb=1=00001 subopcode=258=0x102=01_0000_0010 rc=0 = EC200A04 */
dfp_double ppc_dctdp (dfp_double x);
#pragma mc_func ppc_dctdp { "EC200A04" }
#pragma reg_killed_by ppc_dctdp fsr

/* Convert DFP64 to DFP128 */

/* dctqpq: opcode=63=111111 frt=2and3=00002 00000 frb=1=00001 subopcode=258=0x102=01_0000_0010 rc=0 = FC400A04 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
dfp_quad ppc_dctqpq (dfp_double x);
#pragma mc_func ppc_dctqpq { "FC400A04FC201090FC401890" }
#pragma reg_killed_by ppc_dctqpq fp3,fsr

/* Round DFP64 to DFP32 */

/* drsp: opcode=59=111011 frt=1=00001 00000 frb=1=00001 subopcode=770=0x302=11_0000_0010 rc=0 = EC200E04 */
dfp_double ppc_drsp (dfp_double x);
#pragma mc_func ppc_drsp { "EC200E04" }
#pragma reg_killed_by ppc_drsp fsr

/* Round DFP128 to DFP64 */

/* drdpq: opcode=63=111111 frt=2and3=00010 00000 frb=2and3=00010 subopcode=770=0x302=11_0000_0010 rc=0 = FC401604 */
dfp_double ppc_drdpq (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_drdpq { "FC401604FC201090" }
#pragma reg_killed_by ppc_drdpq fp2,fp3,fsr

/**********************/
/* Integer Conversion */
/**********************/

/* Convert From Fixed */

/* dcffix: opcode=59=111011 frt=1=00001 00000 frb=1=00001 subopcode=802=0x322=11_0010_0010 rc=0 = EC200E44 */
/* NOTE the parameter is an integer in an FPR that needs to be stored into a union to extract a dfp_double. */
/*
dfp_double ppc_dcffix (dfp_integer_in_double x);
#pragma mc_func ppc_dcffix { "EC200E44" }
#pragma reg_killed_by ppc_dcffix fsr
*/

dfp_double ppc_dcffix_via_dcffixq (dfp_integer_in_double x);
#pragma mc_func ppc_dcffix_via_dcffixq { "FC400E44FC401604FC201090" }
#pragma reg_killed_by ppc_dcffix_via_dcffixq fp2,fp3,fsr


/* Convert From Fixed Quad */

/* dcffixq: opcode=63=111111 frt=2and3=00010 00000 frb=1=00001 subopcode=802=0x322=11_0010_0010 rc=0 = FC400E44 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
/* NOTE the parameter is an integer in an FPR (not a pair) that needs to be stored into a union to extract a dfp_double. */
dfp_quad ppc_dcffixq (dfp_integer_in_double x);
#pragma mc_func ppc_dcffixq { "FC400E44FC201090FC401890" }
#pragma reg_killed_by ppc_dcffixq fp3,fsr

/* Convert To Fixed */

/* dctfix: opcode=59=111011 frt=1=00001 00000 frb=1=00001 subopcode=290=0x122=01_0010_0010 rc=0 = EC200A44 */
/* NOTE this returns an integer in an FPR that needs to be stored into a union to extract an int. */
dfp_integer_in_double ppc_dctfix (dfp_double x);
#pragma mc_func ppc_dctfix { "EC200A44" }
#pragma reg_killed_by ppc_dctfix fsr

/* Convert To Fixed Quad */

/* dctfixq: opcode=63=111111 frt=1=00001 00000 frb=2and3=00010 subopcode=290=0x122=01_0010_0010 rc=0 = FC201244 */
/* NOTE this returns an integer in an FPR (not a pair) that needs to be stored into a union to extract a long long. */
dfp_integer_in_double ppc_dctfixq (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_dctfixq { "FC201244" }
#pragma reg_killed_by ppc_dctfixq fsr

/******************/
/* BCD Conversion */
/******************/

/* Decode DPD To BCD - Unsigned */
/* Decode DPD To BCD - Signed +ve C */
/* Decode DPD To BCD - Signed +ve F */

/* ddedpd_u: opcode=59=111011 frt=1=00001 sp=00 000 frb=1=00001 subopcode=322=0x142=01_0100_0010 rc=0 = EC200A84 */
/* ddedpd_c: opcode=59=111011 frt=1=00001 sp=10 000 frb=1=00001 subopcode=322=0x142=01_0100_0010 rc=0 = EC300A84 */
/* ddedpd_f: opcode=59=111011 frt=1=00001 sp=11 000 frb=1=00001 subopcode=322=0x142=01_0100_0010 rc=0 = EC380A84 */
/* NOTE these return bits in an FPR that needs to be stored into a union to extract an unsigned. */
dfp_integer_in_double ppc_ddedpd_u (dfp_double x);
dfp_integer_in_double ppc_ddedpd_c (dfp_double x);
dfp_integer_in_double ppc_ddedpd_f (dfp_double x);
#pragma mc_func ppc_ddedpd_u { "EC200A84" }
#pragma mc_func ppc_ddedpd_c { "EC300A84" }
#pragma mc_func ppc_ddedpd_f { "EC380A84" }
#pragma reg_killed_by ppc_ddedpd_u fsr
#pragma reg_killed_by ppc_ddedpd_c fsr
#pragma reg_killed_by ppc_ddedpd_f fsr

/* Decode DPD To BCD Quad - Unsigned */
/* Decode DPD To BCD Quad - Signed +ve C */
/* Decode DPD To BCD Quad - Signed +ve F */

/* ddedpdq_u: opcode=63=111111 frt=2and3=00010 sp=00 000 frb=2and3=00010 subopcode=322=0x142=01_0100_0010 rc=0 = FC401284 */
/* ddedpdq_c: opcode=63=111111 frt=2and3=00010 sp=10 000 frb=2and3=00010 subopcode=322=0x142=01_0100_0010 rc=0 = FC501284 */
/* ddedpdq_f: opcode=63=111111 frt=2and3=00010 sp=11 000 frb=2and3=00010 subopcode=322=0x142=01_0100_0010 rc=0 = FC581284 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
/* NOTE these return bits in an FPR pair that needs to be stored into a union to extract a pair of unsigned long longs. */
dfp_integer_pair_in_quad ppc_ddedpdq_u (dfp_pad pad, dfp_quad x);
dfp_integer_pair_in_quad ppc_ddedpdq_c (dfp_pad pad, dfp_quad x);
dfp_integer_pair_in_quad ppc_ddedpdq_f (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_ddedpdq_u { "FC401284FC201090FC401890" }
#pragma mc_func ppc_ddedpdq_c { "FC501284FC201090FC401890" }
#pragma mc_func ppc_ddedpdq_f { "FC581284FC201090FC401890" }
#pragma reg_killed_by ppc_ddedpdq_u fp3,fsr
#pragma reg_killed_by ppc_ddedpdq_c fp3,fsr
#pragma reg_killed_by ppc_ddedpdq_f fp3,fsr

/* Encode BCD To DPD - Unsigned */
/* Encode BCD To DPD - Signed */

/* dendpd_u: opcode=59=111011 frt=1=00001 S=0 0000 frb=1=00001 subopcode=834=0x342=11_0100_0010 rc=0 = EC200E84 */
/* dendpd_s: opcode=59=111011 frt=1=00001 S=1 0000 frb=1=00001 subopcode=834=0x342=11_0100_0010 rc=0 = EC300E84 */
/* NOTE the parameter is bits in an FPR that needs to be stored into a union to extract a dfp_double. */
dfp_double ppc_dendpd_u (dfp_integer_in_double x);
dfp_double ppc_dendpd_s (dfp_integer_in_double x);
#pragma mc_func ppc_dendpd_u { "EC200E84" }
#pragma mc_func ppc_dendpd_s { "EC300E84" }
#pragma reg_killed_by ppc_dendpd_u fsr
#pragma reg_killed_by ppc_dendpd_s fsr

/* Encode BCD To DPD Quad - Unsigned */
/* Encode BCD To DPD Quad - Signed */

/* dendpdq_u: opcode=63=111111 frt=2and3=00010 S=0 0000 frb=2and3=00010 subopcode=834=0x342=11_0100_0010 rc=0 = FC401684 */
/* dendpdq_s: opcode=63=111111 frt=2and3=00010 S=1 0000 frb=2and3=00010 subopcode=834=0x342=11_0100_0010 rc=0 = FC501684 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
/* NOTE the parameter is bits in an FPR pair that needs to be stored into a union to extract a dfp_quad. */
dfp_quad ppc_dendpdq_u (dfp_pad pad, dfp_integer_pair_in_quad x);
dfp_quad ppc_dendpdq_s (dfp_pad pad, dfp_integer_pair_in_quad x);
#pragma mc_func ppc_dendpdq_u { "FC401684FC201090FC401890" }
#pragma mc_func ppc_dendpdq_s { "FC501684FC201090FC401890" }
#pragma reg_killed_by ppc_dendpdq_u fp3,fsr
#pragma reg_killed_by ppc_dendpdq_s fp3,fsr

/************/
/* Exponent */
/************/

/* Insert Biased Exponent */

/* diex: opcode=59=111011 frt=1=00001 fra=1=00001 frb=2=00010 subopcode=866=0x362=11_0110_0010 rc=0 = EC2116C4 */
/* NOTE the first parameter is an integer in an FPR that needs to be stored into a union to extract a dfp_double. */
dfp_double ppc_diex (dfp_integer_in_double exp, dfp_double x);
#pragma mc_func ppc_diex { "EC2116C4" }
#pragma reg_killed_by ppc_diex fsr

/* diexq: opcode=63=111111 frt=2and3=00010 fra=1=00001 frb=2and3=00010 subopcode=866=0x362=11_0110_0010 rc=0 = FC4116C4 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
/* NOTE the first parameter is an integer in an FPR that needs to be stored into a union to extract a dfp_double. */
dfp_quad ppc_diexq (dfp_integer_in_double exp, dfp_quad x);
#pragma mc_func ppc_diexq { "FC4116C4FC201090FC401890" }
#pragma reg_killed_by ppc_diexq fp3,fsr

/* Extract Biased Exponent */

/* dxex: opcode=59=111011 frt=1=00001 00000 frb=1=00001 subopcode=354=0x162=01_0110_0010 rc=0 = EC200AC4 */
/* NOTE this returns an integer in an FPR that needs to be stored into a union to extract an int. */
dfp_integer_in_double ppc_dxex (dfp_double x);
#pragma mc_func ppc_dxex { "EC200AC4" }
#pragma reg_killed_by ppc_dxex fsr

/* dxexq: opcode=63=111111 frt=1=00001 00000 frb=2and3=00010 subopcode=354=0x162=01_0110_0010 rc=0 = FC2012C4 */
/* NOTE this returns an integer in an FPR that needs to be stored into a union to extract an int. */
dfp_integer_in_double ppc_dxexq (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_dxexq { "FC2012C4" }
#pragma reg_killed_by ppc_dxexq fsr

/*********/
/* Shift */
/*********/

/* Shift Coefficient Left Immediate */

/* dscli: opcode=59=111011 frt=1=00001 fra=1=00001 sh=xxxxxx subopcode=66=0x042=0_0100_0010 rc=0 = EC21XX84 */
/* NOTE: The immediate value must be specified in the hex parameter. */
/*       A _1 suffix means it is 1. */
/*       A _15 suffix means it is 15, useful for conversion from BCD. */
/*       Others could be added. */
/* To allow a 6 bit shift count the subopcode is 9 not 10 bits */
dfp_double ppc_dscli_1 (dfp_double x);
#pragma mc_func ppc_dscli_1 { "EC210484" }
#pragma reg_killed_by ppc_dscli_1 fsr

dfp_double ppc_dscli_15 (dfp_double x);
#pragma mc_func ppc_dscli_15 { "EC213C84" }
#pragma reg_killed_by ppc_dscli_15 fsr

/* dscliq: opcode=63=111111 frt=2and3=00010 fra=2and3=00010 sh=xxxxxx subopcode=66=0x042=0_0100_0010 rc=0 = FC42XX84 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
/* NOTE: The immediate value must be specified in the hex parameter. */
/*       A _1 suffix means it is 1. */
/*       A _31 suffix means it is 31, useful for conversion from BCD. */
/*       Others could be added. */
/* To allow a 6 bit shift count the subopcode is 9 not 10 bits */
dfp_quad ppc_dscliq_1 (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_dscliq_1 { "FC420484FC201090FC401890" }
#pragma reg_killed_by ppc_dscliq_1 fp3,fsr

dfp_quad ppc_dscliq_31 (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_dscliq_31 { "FC427C84FC201090FC401890" }
#pragma reg_killed_by ppc_dscliq_31 fp3,fsr

/* Shift Coefficient Right Immediate */

/* dscri: opcode=59=111011 frt=1=00001 fra=1=00001 sh=xxxxxx subopcode=98=0x062=0_0110_0010 rc=0 = EC21XXC4 */
/* NOTE: The immediate value must be specified in the hex parameter. */
/*       A _1 suffix means it is 1. */
/*       A _15 suffix means it is 15, useful for conversion to BCD. */
/*       Others could be added. */
/* To allow a 6 bit shift count the subopcode is 9 not 10 bits */
dfp_double ppc_dscri_1 (dfp_double x);
#pragma mc_func ppc_dscri_1 { "EC2104C4" }
#pragma reg_killed_by ppc_dscri_1 fsr

dfp_double ppc_dscri_15 (dfp_double x);
#pragma mc_func ppc_dscri_15 { "EC213CC4" }
#pragma reg_killed_by ppc_dscri_15 fsr

/* dscriq: opcode=63=111111 frt=2and3=00010 fra=2and3=00010 sh=xxxxxx subopcode=98=0x062=0_0110_0010 rc=0 = FC42XXC4 */
/* then: fmr fr1=fr2; fmr fr2=fr3 */
/* NOTE: The immediate value must be specified in the hex parameter. */
/*       A _1 suffix means it is 1. */
/*       A _31 suffix means it is 31, useful for conversion to BCD. */
/*       Others could be added. */
/* To allow a 6 bit shift count the subopcode is 9 not 10 bits */
dfp_quad ppc_dscriq_1 (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_dscriq_1 { "FC4204C4FC201090FC401890" }
#pragma reg_killed_by ppc_dscriq_1 fp3,fsr

dfp_quad ppc_dscriq_31 (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_dscriq_31 { "FC427CC4FC201090FC401890" }
#pragma reg_killed_by ppc_dscriq_31 fp3,fsr

dfp_quad ppc_dscriq_33 (dfp_pad pad, dfp_quad x);
#pragma mc_func ppc_dscriq_33 { "FC4284C4FC201090FC401890" }
#pragma reg_killed_by ppc_dscriq_33 fp3,fsr


#elif defined(DFPAL_OS_LOP_GCC) 
  /* } { #if defined(DFPAL_OS_AIX5L)||DFPAL_OS_I5OS||DFPAL_OS_LOP_XLC */
  #include "asmdfp.h"
#else /* } { #if defined(DFPAL_OS_AIX5L)||DFPAL_OS_I5OS||DFPAL_OS_LOP_XLC */
  #if defined(__DFPAL_C__)
    #error Specify platform or build with -DDFPAL_NO_HW_DFP flag.
  #endif
#endif /* } #if defined(DFPAL_OS_AIX5L)||DFPAL_OS_I5OS||DFPAL_OS_LOP_XLC */

#else /* } { #if !defined(DFPAL_NO_HW_DFP) */
  #include "dfpstub.h"
#endif /* } #if !defined(DFPAL_NO_HW_DFP) */

#endif /* #if !defined(__PPCDFP_H__) */
