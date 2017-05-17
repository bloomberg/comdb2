/* ------------------------------------------------------------------ */
/* Decimal Floating Point Abstraction Layer (DFPAL)                   */
/* dfpaltypes.h                                                       */
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
#if !defined (__DFPALTYPES_H__)
	#define __DFPALTYPES_H__

  #if defined(DFPAL_USE_INTTYPES_H)
    #include <inttypes.h>
  #elif defined(DFPAL_LOCAL_INTTYPES)
    /* If your platform does not support inttypes.h or stdint.h,  */
    /* update following datatype definitions. Following datatypes */
    /* are correctly defined for the Windows platform.            */
    typedef unsigned char     uint8_t;
    typedef signed short      int16_t;
    typedef unsigned short    uint16_t;
    typedef signed int        int32_t;
    typedef unsigned int      uint32_t;
    typedef signed __int64    int64_t;
    typedef unsigned __int64  uint64_t;
  #else  /* default, stdint.h is the most common case */
    #include <stdint.h>
  #endif

  #if !defined(FALSE)
    #define TRUE  1
    #define FALSE 0
  #endif

  #if !defined(DECIMAL32)
    typedef struct{ uint8_t b[4]; } decimal32;
  #endif

  #if !defined(DECIMAL64)
    typedef struct{ uint8_t b[8]; } decimal64;
  #endif

  #if !defined(DECIMAL128)
    typedef struct{ uint8_t b[16]; } decimal128;
  #endif

  enum dfpalExeMode {
    AUTO,                   /* detect automatically */
    PPCHW,                  /* POWER hardware DFP */
    DNSW                    /* decNumber DFP */
    };

  typedef uint32_t dfpaltrap_t;
  typedef uint32_t dfpalflag_t;
  typedef uint16_t dfpalrnd_t;


  #if !defined(DFPAL_NO_HW_DFP)
    typedef long double dfp_quad;
  #else
    typedef int64_t dfp_quad;
  #endif

#endif /* #if !defined (__DFPALTYPES_H__) */
