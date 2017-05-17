/* ------------------------------------------------------------------ */
/* Decimal Floating Point Abstraction Layer (DFPAL)                   */
/* dfpalerr.h                                                         */
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
#if !defined(__DFPALERR_H__)
  #define __DFPALERR_H__

  enum {
    DFPAL_ERR_NO_ERROR,
    DFPAL_ERR_MUTEX_TYPE,
    DFPAL_ERR_MUTEX_ATTR_INIT,
    DFPAL_ERR_MUTEX_INIT,
    DFPAL_ERR_NO_HW_SUPPORT,
    DFPAL_ERR_DEBUG_FNAME_LONG,
    DFPAL_ERR_DEBUG_FOPEN,
    DFPAL_ERR_CREATE_TLS_KEY,
    DFPAL_ERR_TLS_STORE,
    DFPAL_ERR_NO_INIT,
    DFPAL_ERR_NO_MEM,
    DFPAL_ERR_WRONG_ENDIAN,
    DFPAL_ERR_UNKNOWN                /* must be last error number */
  };


  #if defined(__DFPAL_C__)
  static const char * const dfpalErrMessageCtlog[] = {
    "no error",
    "can not set specified mutex type",
    "can not initialize mutex attribute",
    "can not initialize mutex attribute",
    "no hardware DFP support available",
    "DFP abstraction layer debug file name too long",
    "can not open specified DFP abstraction layer debug file",
    "can not create thread local storage key",
    "can not access thread local storage",
    "DFP abstraction layer is not initialized or corrupted",
    "no memory, NULL pointer input",
    "incompatible endian, check value of DECLITEND parameter",
    "unknown error"
  };
  #endif
#endif /* #if !defined(__DFPALERR_H__) */
