/* ------------------------------------------------------------------ */
/* Decimal Floating Point Abstraction Layer (DFPAL)                   */
/* asmdfp.S                                                           */
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
.file	"asmdfp.S"

#if defined(__powerpc64__)
  .section ".toc","aw"
  .section ".text"
  #define ENTRY(NAME)                   \
    .align 2;                           \
    .p2align 4,,15;                     \
    .globl NAME;                        \
    .section ".opd","aw";               \
    .align 3;                           \
  NAME:;                                \
    .quad .L.NAME,.TOC.@tocbase,0;      \
    .previous;                          \
    .type  NAME, @function;             \
  .L.NAME:
  
  #define END(NAME)                     \
    .long 0;                            \
    .byte 0,0,0,1,128,0,0,0;            \
    .size NAME,.-.L.NAME
#else
  #define ENTRY(NAME)                   \
    .section ".text";                   \
    .align 2;                           \
    .p2align 4,,15;                     \
    .globl NAME;                        \
    .type NAME, @function;              \
  NAME:

  #define END(NAME)                     \
    .size   NAME,.-NAME
#endif


/* dfp_integer_in_double ppc_mffs (void) */
ENTRY(ppc_mffs)
  .long 0xFC20048E
  blr
  END(ppc_mffs)



/* void ppc_mtfs (dfp_integer_in_double fpscr) */
ENTRY(ppc_mtfs)
  .long 0xFE000D8E
  blr
  END(ppc_mtfs)



/* void ppc_mtfsf_drm (dfp_integer_in_double fpscr_drm) */
ENTRY(ppc_mtfsf_drm)
  .long 0xFC030D8E
  blr
  END(ppc_mtfsf_drm)



/* dfp_double ppc_dadd (dfp_double x, dfp_double y) */
ENTRY(ppc_dadd)
  .long 0xEC211004
  blr
  END(ppc_dadd)



/* dfp_quad ppc_daddq (dfp_pad pad, dfp_quad x, dfp_quad y) */
ENTRY(ppc_daddq)
  .long 0xFC422004
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_daddq)



/* dfp_double ppc_dsub (dfp_double x, dfp_double y) */
ENTRY(ppc_dsub)
  .long 0xEC211404
  blr
  END(ppc_dsub)



/* dfp_quad ppc_dsubq (dfp_pad pad, dfp_quad x, dfp_quad y) */
ENTRY(ppc_dsubq)
  .long 0xFC422404
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dsubq)



/* dfp_double ppc_dmul (dfp_double x, dfp_double y) */
ENTRY(ppc_dmul)
  .long 0xEC211044
  blr
  END(ppc_dmul)



/* dfp_quad ppc_dmulq (dfp_pad pad, dfp_quad x, dfp_quad y) */
ENTRY(ppc_dmulq)
  .long 0xFC422044
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dmulq)



/* dfp_double ppc_ddiv (dfp_double x, dfp_double y) */
ENTRY(ppc_ddiv)
  .long 0xEC211444
  blr
  END(ppc_ddiv)



/* dfp_quad ppc_ddivq (dfp_pad pad, dfp_quad x, dfp_quad y) */
ENTRY(ppc_ddivq)
  .long 0xFC422444
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_ddivq)



/* dfp_double ppc_fneg (dfp_double x) */
ENTRY(ppc_fneg)
  .long 0xFC200850
  blr
  END(ppc_fneg)



/* dfp_quad ppc_fnegq (dfp_quad x) */
ENTRY(ppc_fnegq)
  .long 0xFC200850
  blr
  END(ppc_fnegq)



/* dfp_double ppc_fabs (dfp_double x) */
ENTRY(ppc_fabs)
  .long 0xFC200A10
  blr
  END(ppc_fabs)



/* dfp_quad ppc_fabsq (dfp_quad x) */
ENTRY(ppc_fabsq)
  .long 0xFC200A10
  blr
  END(ppc_fabsq)



/* dfp_double ppc_dqua_rne    (dfp_double x, dfp_double y) */
ENTRY(ppc_dqua_rne)
  .long 0xEC211006
  blr
  END(ppc_dqua_rne)



/* dfp_double ppc_dqua_rtz    (dfp_double x, dfp_double y) */
ENTRY(ppc_dqua_rtz)
  .long 0xEC211206
  blr
  END(ppc_dqua_rtz)



/* dfp_double ppc_dqua_rnaz   (dfp_double x, dfp_double y) */
ENTRY(ppc_dqua_rnaz)
  .long 0xEC211406
  blr
  END(ppc_dqua_rnaz)



/* dfp_double ppc_dqua_rfpscr (dfp_double x, dfp_double y) */
ENTRY(ppc_dqua_rfpscr)
  .long 0xEC211606
  blr
  END(ppc_dqua_rfpscr)



/* dfp_quad ppc_dquaq_rne    (dfp_pad pad, dfp_quad x, dfp_quad y) */
ENTRY(ppc_dquaq_rne)
  .long 0xFC422006
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dquaq_rne)



/* dfp_quad ppc_dquaq_rtz    (dfp_pad pad, dfp_quad x, dfp_quad y) */
ENTRY(ppc_dquaq_rtz)
  .long 0xFC422206
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dquaq_rtz)



/* dfp_quad ppc_dquaq_rnaz   (dfp_pad pad, dfp_quad x, dfp_quad y) */
ENTRY(ppc_dquaq_rnaz)
  .long 0xFC422406
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dquaq_rnaz)



/* dfp_quad ppc_dquaq_rfpscr (dfp_pad pad, dfp_quad x, dfp_quad y) */
ENTRY(ppc_dquaq_rfpscr)
  .long 0xFC422606
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dquaq_rfpscr)



/* dfp_double ppc_dquai_rne_0    (dfp_double x) */
ENTRY(ppc_dquai_rne_0)
  .long 0xEC200886
  blr
  END(ppc_dquai_rne_0)



/* dfp_double ppc_dquai_rtz_0    (dfp_double x) */
ENTRY(ppc_dquai_rtz_0)
  .long 0xEC200A86
  blr
  END(ppc_dquai_rtz_0)



/* dfp_double ppc_dquai_rnaz_0   (dfp_double x) */
ENTRY(ppc_dquai_rnaz_0)
  .long 0xEC200C86
  blr
  END(ppc_dquai_rnaz_0)



/* dfp_double ppc_dquai_rfpscr_0 (dfp_double x) */
ENTRY(ppc_dquai_rfpscr_0)
  .long 0xEC200E86
  blr
  END(ppc_dquai_rfpscr_0)



/* dfp_quad ppc_dquaiq_rne_0    (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_dquaiq_rne_0)
  .long 0xFC401086
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dquaiq_rne_0)



/* dfp_quad ppc_dquaiq_rtz_0    (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_dquaiq_rtz_0)
  .long 0xFC401286
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dquaiq_rtz_0)



/* dfp_quad ppc_dquaiq_rnaz_0   (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_dquaiq_rnaz_0)
  .long 0xFC401486
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dquaiq_rnaz_0)



/* dfp_quad ppc_dquaiq_rfpscr_0 (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_dquaiq_rfpscr_0)
  .long 0xFC401686
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dquaiq_rfpscr_0)



/* dfp_double ppc_drrnd_rne_7    (dfp_double x) */
ENTRY(ppc_drrnd_rne_7)
  .long 0xEC271046
  blr
  END(ppc_drrnd_rne_7)



/* dfp_double ppc_drrnd_rtz_7    (dfp_double x) */
ENTRY(ppc_drrnd_rtz_7)
  .long 0xEC271246
  blr
  END(ppc_drrnd_rtz_7)



/* dfp_double ppc_drrnd_rnaz_7   (dfp_double x) */
ENTRY(ppc_drrnd_rnaz_7)
  .long 0xEC271446
  blr
  END(ppc_drrnd_rnaz_7)



/* dfp_double ppc_drrnd_rfpscr_7 (dfp_double x) */
ENTRY(ppc_drrnd_rfpscr_7)
  .long 0xEC271646
  blr
  END(ppc_drrnd_rfpscr_7)



/* dfp_quad ppc_drrndq_rne_7    (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drrndq_rne_7)
  .long 0xFC471046
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drrndq_rne_7)



/* dfp_quad ppc_drrndq_rtz_7    (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drrndq_rtz_7)
  .long 0xFC471246
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drrndq_rtz_7)



/* dfp_quad ppc_drrndq_rnaz_7   (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drrndq_rnaz_7)
  .long 0xFC471446
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drrndq_rnaz_7)



/* dfp_quad ppc_drrndq_rfpscr_7 (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drrndq_rfpscr_7)
  .long 0xFC471646
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drrndq_rfpscr_7)



/* dfp_quad ppc_drrndq_rne_16    (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drrndq_rne_16)
  .long 0xFC501046
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drrndq_rne_16)



/* dfp_quad ppc_drrndq_rtz_16    (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drrndq_rtz_16)
  .long 0xFC501246
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drrndq_rtz_16)



/* dfp_quad ppc_drrndq_rnaz_16   (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drrndq_rnaz_16)
  .long 0xFC501446
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drrndq_rnaz_16)



/* dfp_quad ppc_drrndq_rfpscr_16 (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drrndq_rfpscr_16)
  .long 0xFC501646
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drrndq_rfpscr_16)



/* compare_result ppc_dcmpo (dfp_double x, dfp_double y) */
ENTRY(ppc_dcmpo)
  .long 0xEC011104
  .long 0x7C600026
  .long 0x5463273E
  blr
  END(ppc_dcmpo)



/* compare_result ppc_dcmpoq (dfp_pad pad, dfp_quad x, dfp_quad y) */
ENTRY(ppc_dcmpoq)
  .long 0xFC022104
  .long 0x7C600026
  .long 0x5463273E
  blr
  END(ppc_dcmpoq)



/* compare_result ppc_dcmpu (dfp_double x, dfp_double y) */
ENTRY(ppc_dcmpu)
  .long 0xEC011504
  .long 0x7C600026
  .long 0x5463273E
  blr
  END(ppc_dcmpu)



/* compare_result ppc_dcmpuq (dfp_pad pad, dfp_quad x, dfp_quad y) */
ENTRY(ppc_dcmpuq)
  .long 0xFC022504
  .long 0x7C600026
  .long 0x5463273E
  blr
  END(ppc_dcmpuq)



/* compare_result ppc_dtstex (dfp_double x, dfp_double y) */
ENTRY(ppc_dtstex)
  .long 0xEC011144
  .long 0x7C600026
  .long 0x5463273E
  blr
  END(ppc_dtstex)



/* compare_result ppc_dtstexq (dfp_pad pad, dfp_quad x, dfp_quad y) */
ENTRY(ppc_dtstexq)
  .long 0xFC022144
  .long 0x7C600026
  .long 0x5463273E
  blr
  END(ppc_dtstexq)



/* compare_result ppc_dtstsf (dfp_significance_double x, dfp_double y) */
ENTRY(ppc_dtstsf)
  .long 0xEC011544
  .long 0x7C600026
  .long 0x5463273E
  blr
  END(ppc_dtstsf)



/* compare_result ppc_dtstsfq (dfp_significance_double x, dfp_quad y) */
ENTRY(ppc_dtstsfq)
  .long 0xFC011544
  .long 0x7C600026
  .long 0x5463273E
  blr
  END(ppc_dtstsfq)



/* dfp_double ppc_drintx_rne  (dfp_double x) */
ENTRY(ppc_drintx_rne)
  .long 0xEC2008C6
  blr
  END(ppc_drintx_rne)



/* dfp_double ppc_drintx_rtz  (dfp_double x) */
ENTRY(ppc_drintx_rtz)
  .long 0xEC200AC6
  blr
  END(ppc_drintx_rtz)



/* dfp_double ppc_drintx_rnaz (dfp_double x) */
ENTRY(ppc_drintx_rnaz)
  .long 0xEC200CC6
  blr
  END(ppc_drintx_rnaz)



/* dfp_double ppc_drintx_rfpscr (dfp_double x) */
ENTRY(ppc_drintx_rfpscr)
  .long 0xEC200EC6
  blr
  END(ppc_drintx_rfpscr)



/* dfp_double ppc_drintx_rtpi (dfp_double x) */
ENTRY(ppc_drintx_rtpi)
  .long 0xEC2108C6
  blr
  END(ppc_drintx_rtpi)



/* dfp_double ppc_drintx_rtmi (dfp_double x) */
ENTRY(ppc_drintx_rtmi)
  .long 0xEC210AC6
  blr
  END(ppc_drintx_rtmi)



/* dfp_double ppc_drintx_rafz (dfp_double x) */
ENTRY(ppc_drintx_rafz)
  .long 0xEC210CC6
  blr
  END(ppc_drintx_rafz)



/* dfp_double ppc_drintx_rntz (dfp_double x) */
ENTRY(ppc_drintx_rntz)
  .long 0xEC210EC6
  blr
  END(ppc_drintx_rntz)



/* dfp_quad ppc_drintxq_rne  (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintxq_rne)
  .long 0xFC4010C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintxq_rne)



/* dfp_quad ppc_drintxq_rtz  (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintxq_rtz)
  .long 0xFC4012C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintxq_rtz)



/* dfp_quad ppc_drintxq_rnaz (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintxq_rnaz)
  .long 0xFC4014C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintxq_rnaz)



/* dfp_quad ppc_drintxq_rfpscr (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintxq_rfpscr)
  .long 0xFC4016C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintxq_rfpscr)



/* dfp_quad ppc_drintxq_rtpi (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintxq_rtpi)
  .long 0xFC4110C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintxq_rtpi)



/* dfp_quad ppc_drintxq_rtmi (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintxq_rtmi)
  .long 0xFC4112C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintxq_rtmi)



/* dfp_quad ppc_drintxq_rafz (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintxq_rafz)
  .long 0xFC4114C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintxq_rafz)



/* dfp_quad ppc_drintxq_rntz (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintxq_rntz)
  .long 0xFC4116C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintxq_rntz)



/* dfp_double ppc_drintn_rne  (dfp_double x) */
ENTRY(ppc_drintn_rne)
  .long 0xEC2009C6
  blr
  END(ppc_drintn_rne)



/* dfp_double ppc_drintn_rtz  (dfp_double x) */
ENTRY(ppc_drintn_rtz)
  .long 0xEC200BC6
  blr
  END(ppc_drintn_rtz)



/* dfp_double ppc_drintn_rnaz (dfp_double x) */
ENTRY(ppc_drintn_rnaz)
  .long 0xEC200DC6
  blr
  END(ppc_drintn_rnaz)



/* dfp_double ppc_drintn_rfpscr (dfp_double x) */
ENTRY(ppc_drintn_rfpscr)
  .long 0xEC200FC6
  blr
  END(ppc_drintn_rfpscr)



/* dfp_double ppc_drintn_rtpi (dfp_double x) */
ENTRY(ppc_drintn_rtpi)
  .long 0xEC2109C6
  blr
  END(ppc_drintn_rtpi)



/* dfp_double ppc_drintn_rtmi (dfp_double x) */
ENTRY(ppc_drintn_rtmi)
  .long 0xEC210BC6
  blr
  END(ppc_drintn_rtmi)



/* dfp_double ppc_drintn_rafz (dfp_double x) */
ENTRY(ppc_drintn_rafz)
  .long 0xEC210DC6
  blr
  END(ppc_drintn_rafz)



/* dfp_double ppc_drintn_rntz (dfp_double x) */
ENTRY(ppc_drintn_rntz)
  .long 0xEC210FC6
  blr
  END(ppc_drintn_rntz)



/* dfp_quad ppc_drintnq_rne  (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintnq_rne)
  .long 0xFC4011C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintnq_rne)



/* dfp_quad ppc_drintnq_rtz  (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintnq_rtz)
  .long 0xFC4013C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintnq_rtz)



/* dfp_quad ppc_drintnq_rnaz (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintnq_rnaz)
  .long 0xFC4015C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintnq_rnaz)



/* dfp_quad ppc_drintnq_rfpscr (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintnq_rfpscr)
  .long 0xFC4017C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintnq_rfpscr)



/* dfp_quad ppc_drintnq_rtpi (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintnq_rtpi)
  .long 0xFC4111C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintnq_rtpi)



/* dfp_quad ppc_drintnq_rtmi (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintnq_rtmi)
  .long 0xFC4113C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintnq_rtmi)



/* dfp_quad ppc_drintnq_rafz (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintnq_rafz)
  .long 0xFC4115C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintnq_rafz)



/* dfp_quad ppc_drintnq_rntz (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drintnq_rntz)
  .long 0xFC4117C6
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_drintnq_rntz)



/* dfp_double ppc_dctdp (dfp_double x) */
ENTRY(ppc_dctdp)
  .long 0xEC200A04
  blr
  END(ppc_dctdp)



/* dfp_quad ppc_dctqpq (dfp_double x) */
ENTRY(ppc_dctqpq)
  .long 0xFC400A04
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dctqpq)



/* dfp_double ppc_drsp (dfp_double x) */
ENTRY(ppc_drsp)
  .long 0xEC200E04
  blr
  END(ppc_drsp)



/* dfp_double ppc_drdpq (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_drdpq)
  .long 0xFC401604
  fmr 1,2
  blr
  END(ppc_drdpq)



/* dfp_double ppc_dcffix_via_dcffixq (dfp_integer_in_double x) */
ENTRY(ppc_dcffix_via_dcffixq)
  .long 0xFC400E44
  .long 0xFC401604
  fmr 1,2
  blr
  END(ppc_dcffix_via_dcffixq)



/* dfp_quad ppc_dcffixq (dfp_integer_in_double x) */
ENTRY(ppc_dcffixq)
  .long 0xFC400E44
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dcffixq)



/* dfp_integer_in_double ppc_dctfix (dfp_double x) */
ENTRY(ppc_dctfix)
  .long 0xEC200A44
  blr
  END(ppc_dctfix)



/* dfp_integer_in_double ppc_dctfixq (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_dctfixq)
  .long 0xFC201244
  blr
  END(ppc_dctfixq)



/* dfp_integer_in_double ppc_ddedpd_u (dfp_double x) */
ENTRY(ppc_ddedpd_u)
  .long 0xEC200A84
  blr
  END(ppc_ddedpd_u)



/* dfp_integer_in_double ppc_ddedpd_c (dfp_double x) */
ENTRY(ppc_ddedpd_c)
  .long 0xEC300A84
  blr
  END(ppc_ddedpd_c)



/* dfp_integer_in_double ppc_ddedpd_f (dfp_double x) */
ENTRY(ppc_ddedpd_f)
  .long 0xEC380A84
  blr
  END(ppc_ddedpd_f)



/* dfp_integer_pair_in_quad ppc_ddedpdq_u (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_ddedpdq_u)
  .long 0xFC401284
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_ddedpdq_u)



/* dfp_integer_pair_in_quad ppc_ddedpdq_c (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_ddedpdq_c)
  .long 0xFC501284
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_ddedpdq_c)



/* dfp_integer_pair_in_quad ppc_ddedpdq_f (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_ddedpdq_f)
  .long 0xFC581284
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_ddedpdq_f)



/* dfp_double ppc_dendpd_u (dfp_integer_in_double x) */
ENTRY(ppc_dendpd_u)
  .long 0xEC200E84
  blr
  END(ppc_dendpd_u)



/* dfp_double ppc_dendpd_s (dfp_integer_in_double x) */
ENTRY(ppc_dendpd_s)
  .long 0xEC300E84
  blr
  END(ppc_dendpd_s)



/* dfp_quad ppc_dendpdq_u (dfp_pad pad, dfp_integer_pair_in_quad x) */
ENTRY(ppc_dendpdq_u)
  .long 0xFC401684
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dendpdq_u)



/* dfp_quad ppc_dendpdq_s (dfp_pad pad, dfp_integer_pair_in_quad x) */
ENTRY(ppc_dendpdq_s)
  .long 0xFC501684
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dendpdq_s)



/* dfp_double ppc_diex (dfp_integer_in_double exp, dfp_double x) */
ENTRY(ppc_diex)
  .long 0xEC2116C4
  blr
  END(ppc_diex)



/* dfp_quad ppc_diexq (dfp_integer_in_double exp, dfp_quad x) */
ENTRY(ppc_diexq)
  .long 0xFC4116C4
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_diexq)



/* dfp_integer_in_double ppc_dxex (dfp_double x) */
ENTRY(ppc_dxex)
  .long 0xEC200AC4
  blr
  END(ppc_dxex)



/* dfp_integer_in_double ppc_dxexq (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_dxexq)
  .long 0xFC2012C4
  blr
  END(ppc_dxexq)



/* dfp_double ppc_dscli_1 (dfp_double x) */
ENTRY(ppc_dscli_1)
  .long 0xEC210484
  blr
  END(ppc_dscli_1)



/* dfp_double ppc_dscli_15 (dfp_double x) */
ENTRY(ppc_dscli_15)
  .long 0xEC213C84
  blr
  END(ppc_dscli_15)



/* dfp_quad ppc_dscliq_1 (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_dscliq_1)
  .long 0xFC420484
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dscliq_1)



/* dfp_quad ppc_dscliq_31 (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_dscliq_31)
  .long 0xFC427C84
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dscliq_31)



/* dfp_double ppc_dscri_1 (dfp_double x) */
ENTRY(ppc_dscri_1)
  .long 0xEC2104C4
  blr
  END(ppc_dscri_1)



/* dfp_double ppc_dscri_15 (dfp_double x) */
ENTRY(ppc_dscri_15)
  .long 0xEC213CC4
  blr
  END(ppc_dscri_15)



/* dfp_quad ppc_dscriq_1 (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_dscriq_1)
  .long 0xFC4204C4
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dscriq_1)



/* dfp_quad ppc_dscriq_31 (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_dscriq_31)
  .long 0xFC427CC4
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dscriq_31)



/* dfp_quad ppc_dscriq_33 (dfp_pad pad, dfp_quad x) */
ENTRY(ppc_dscriq_33)
  .long 0xFC4284C4
  fmr 1,2
  fmr 2,3
  blr
  END(ppc_dscriq_33)
