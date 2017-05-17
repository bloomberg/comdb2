/* ------------------------------------------------------------------ */
/* Decimal Floating Point Abstraction Layer (DFPAL)                   */
/* asmdfp.h                                                           */
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
/*                                                                    */
/* Acknowledgement:                                                   */
/*   Steve Munroe, Janis Johnson, Caecilie Hampel, Patrick Hartman    */
/* ------------------------------------------------------------------ */
#if !defined(__ASMDFP_H__)
  #define __ASMDFP_H__
  extern dfp_integer_in_double ppc_mffs (void);
  extern void ppc_mtfs (const dfp_integer_in_double fpscr);
  extern void ppc_mtfsf_drm (const dfp_integer_in_double fpscr_drm);
  extern dfp_double ppc_dadd (const dfp_double x, const  dfp_double y);
  extern dfp_quad ppc_daddq (const dfp_pad pad, const  dfp_quad x, dfp_quad y);
  extern dfp_double ppc_dsub (const dfp_double x, const  dfp_double y);
  extern dfp_quad ppc_dsubq (const dfp_pad pad, const  dfp_quad x, dfp_quad y);
  extern dfp_double ppc_dmul (const dfp_double x, const  dfp_double y);
  extern dfp_quad ppc_dmulq (const dfp_pad pad, const  dfp_quad x, dfp_quad y);
  extern dfp_double ppc_ddiv (const dfp_double x, const  dfp_double y);
  extern dfp_quad ppc_ddivq (const dfp_pad pad, const  dfp_quad x, dfp_quad y);
  extern dfp_double ppc_fneg (const dfp_double x);
  extern dfp_quad ppc_fnegq (const dfp_quad x);
  extern dfp_double ppc_fabs (const dfp_double x);
  extern dfp_quad ppc_fabsq (const dfp_quad x);
  extern dfp_double ppc_dqua_rne    (const dfp_double x, const  dfp_double y);
  extern dfp_double ppc_dqua_rtz    (const dfp_double x, const  dfp_double y);
  extern dfp_double ppc_dqua_rnaz   (const dfp_double x, const  dfp_double y);
  extern dfp_double ppc_dqua_rfpscr (const dfp_double x, const  dfp_double y);
  extern dfp_quad ppc_dquaq_rne    (const dfp_pad pad, const  dfp_quad x, dfp_quad y);
  extern dfp_quad ppc_dquaq_rtz    (const dfp_pad pad, const  dfp_quad x, dfp_quad y);
  extern dfp_quad ppc_dquaq_rnaz   (const dfp_pad pad, const  dfp_quad x, dfp_quad y);
  extern dfp_quad ppc_dquaq_rfpscr (const dfp_pad pad, const  dfp_quad x, dfp_quad y);
  extern dfp_double ppc_dquai_rne_0    (const dfp_double x);
  extern dfp_double ppc_dquai_rtz_0    (const dfp_double x);
  extern dfp_double ppc_dquai_rnaz_0   (const dfp_double x);
  extern dfp_double ppc_dquai_rfpscr_0 (const dfp_double x);
  extern dfp_quad ppc_dquaiq_rne_0    (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_dquaiq_rtz_0    (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_dquaiq_rnaz_0   (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_dquaiq_rfpscr_0 (const dfp_pad pad, const  dfp_quad x);
  extern dfp_double ppc_drrnd_rne_7    (const dfp_double x);
  extern dfp_double ppc_drrnd_rtz_7    (const dfp_double x);
  extern dfp_double ppc_drrnd_rnaz_7   (const dfp_double x);
  extern dfp_double ppc_drrnd_rfpscr_7 (const dfp_double x);
  extern dfp_quad ppc_drrndq_rne_7    (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drrndq_rtz_7    (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drrndq_rnaz_7   (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drrndq_rfpscr_7 (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drrndq_rne_16    (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drrndq_rtz_16    (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drrndq_rnaz_16   (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drrndq_rfpscr_16 (const dfp_pad pad, const  dfp_quad x);
  extern compare_result ppc_dcmpo (const dfp_double x, const  dfp_double y);
  extern compare_result ppc_dcmpoq (const dfp_pad pad, const  dfp_quad x, dfp_quad y);
  extern compare_result ppc_dcmpu (const dfp_double x, const  dfp_double y);
  extern compare_result ppc_dcmpuq (const dfp_pad pad, const  dfp_quad x, dfp_quad y);
  extern compare_result ppc_dtstex (const dfp_double x, const  dfp_double y);
  extern compare_result ppc_dtstexq (const dfp_pad pad, const  dfp_quad x, dfp_quad y);
  extern compare_result ppc_dtstsf (const dfp_significance_double x, const  dfp_double y);
  extern compare_result ppc_dtstsfq (const dfp_significance_double x, const  dfp_quad y);
  extern dfp_double ppc_drintx_rne  (const dfp_double x);
  extern dfp_double ppc_drintx_rtz  (const dfp_double x);
  extern dfp_double ppc_drintx_rnaz (const dfp_double x);
  extern dfp_double ppc_drintx_rfpscr (const dfp_double x);
  extern dfp_double ppc_drintx_rtpi (const dfp_double x);
  extern dfp_double ppc_drintx_rtmi (const dfp_double x);
  extern dfp_double ppc_drintx_rafz (const dfp_double x);
  extern dfp_double ppc_drintx_rntz (const dfp_double x);
  extern dfp_quad ppc_drintxq_rne  (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drintxq_rtz  (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drintxq_rnaz (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drintxq_rfpscr (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drintxq_rtpi (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drintxq_rtmi (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drintxq_rafz (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drintxq_rntz (const dfp_pad pad, const  dfp_quad x);
  extern dfp_double ppc_drintn_rne  (const dfp_double x);
  extern dfp_double ppc_drintn_rtz  (const dfp_double x);
  extern dfp_double ppc_drintn_rnaz (const dfp_double x);
  extern dfp_double ppc_drintn_rfpscr (const dfp_double x);
  extern dfp_double ppc_drintn_rtpi (const dfp_double x);
  extern dfp_double ppc_drintn_rtmi (const dfp_double x);
  extern dfp_double ppc_drintn_rafz (const dfp_double x);
  extern dfp_double ppc_drintn_rntz (const dfp_double x);
  extern dfp_quad ppc_drintnq_rne  (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drintnq_rtz  (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drintnq_rnaz (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drintnq_rfpscr (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drintnq_rtpi (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drintnq_rtmi (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drintnq_rafz (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_drintnq_rntz (const dfp_pad pad, const  dfp_quad x);
  extern dfp_double ppc_dctdp (const dfp_double x);
  extern dfp_quad ppc_dctqpq (const dfp_double x);
  extern dfp_double ppc_drsp (const dfp_double x);
  extern dfp_double ppc_drdpq (const dfp_pad pad, const  dfp_quad x);
  extern dfp_double ppc_dcffix_via_dcffixq (const dfp_integer_in_double x);
  extern dfp_quad ppc_dcffixq (const dfp_integer_in_double x);
  extern dfp_integer_in_double ppc_dctfix (const dfp_double x);
  extern dfp_integer_in_double ppc_dctfixq (const dfp_pad pad, const  dfp_quad x);
  extern dfp_integer_in_double ppc_ddedpd_u (const dfp_double x);
  extern dfp_integer_in_double ppc_ddedpd_c (const dfp_double x);
  extern dfp_integer_in_double ppc_ddedpd_f (const dfp_double x);
  extern dfp_integer_pair_in_quad ppc_ddedpdq_u (const dfp_pad pad, const  dfp_quad x);
  extern dfp_integer_pair_in_quad ppc_ddedpdq_c (const dfp_pad pad, const  dfp_quad x);
  extern dfp_integer_pair_in_quad ppc_ddedpdq_f (const dfp_pad pad, const  dfp_quad x);
  extern dfp_double ppc_dendpd_u (const dfp_integer_in_double x);
  extern dfp_double ppc_dendpd_s (const dfp_integer_in_double x);
  extern dfp_quad ppc_dendpdq_u (const dfp_pad pad, const  dfp_integer_pair_in_quad x);
  extern dfp_quad ppc_dendpdq_s (const dfp_pad pad, const  dfp_integer_pair_in_quad x);
  extern dfp_double ppc_diex (const dfp_integer_in_double exp, const  dfp_double x);
  extern dfp_quad ppc_diexq (const dfp_integer_in_double exp, const  dfp_quad x);
  extern dfp_integer_in_double ppc_dxex (const dfp_double x);
  extern dfp_integer_in_double ppc_dxexq (const dfp_pad pad, const  dfp_quad x);
  extern dfp_double ppc_dscli_1 (const dfp_double x);
  extern dfp_double ppc_dscli_15 (const dfp_double x);
  extern dfp_quad ppc_dscliq_1 (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_dscliq_31 (const dfp_pad pad, const  dfp_quad x);
  extern dfp_double ppc_dscri_1 (const dfp_double x);
  extern dfp_double ppc_dscri_15 (const dfp_double x);
  extern dfp_quad ppc_dscriq_1 (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_dscriq_31 (const dfp_pad pad, const  dfp_quad x);
  extern dfp_quad ppc_dscriq_33 (const dfp_pad pad, const  dfp_quad x);
#endif /* #if !defined(__ASMDFP_H__) */
