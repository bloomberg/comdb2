/* ------------------------------------------------------------------ */
/* Decimal Floating Point Abstraction Layer (DFPAL)                   */
/* dfpal-sample.c                                                     */
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
#include <stdio.h>
#include <stdlib.h>
#include "dfpal.h"

int main(int32_t argc, char **argv)
{
decimal64 n1, n2, n3, ddz, n4;
decimal128 N1, N2,N3;
char res[100];
int32_t init_err, init_os_err;
char *err_str=NULL;
dfpalCSFlag *flhandle;
dfpalflag_t st;
int factN, i;


  if (argc < 3) {
    printf("Please supply two numbers\n");
    return(1);
  }

  if (argc == 4)
     factN = atoi(argv[3]);
  else
     factN = 10;

  /* must call dfpalInit() before performing   any operation on     */
  /* DFPAL. For multi-threaded program, EACH thread must initialize */
  /* using dfpalInit(). In case of multi-threaded process,          */
  /* application is responsible for ensuring that memory passed to  */
  /* dfpalInit() is thread safe memory.                             */
  if (dfpalInit((void *)malloc(dfpalMemSize())) != DFPAL_ERR_NO_ERROR) {
    dfpalGetError(&init_err, &init_os_err, &err_str);
    fprintf(stderr, "DFPAL Init error number:%d, error: %s\n", 
      init_err, err_str);
    dfpalEnd(free);
    return (1);
  }

  printf("Version of DFPAL: %s\n", dfpalVersion());

  /* Is it running in softwar or hardware? */
  if (dfpalGetExeMode()!=PPCHW) 
    printf("DFPAL is operating in software\n");
  else
    printf("DFPAL is operating in hardware\n");

  n1=dec64FromString(argv[1]);
  n2=dec64FromString(argv[2]);

  N1=dec128FromString(argv[1]);
  N2=dec128FromString(argv[2]);

  /* let's try for the overflow. For 64-bit, p=16 and Emax=384 */
  n4=dec64FromString("9E+384");
  printf("value of n4 = %s\n", dec64ToString(n4, res));

  n3=dec64Multiply(n4,n4);
  printf("9E+384 * 9E+384 = %s\n", dec64ToString(n3, res));
  if (dfpalReadStatusFlag() & DFPAL_FP_OVERFLOW)
    printf("9E+384 * 9E+384 => Overflow\n");

  /* set trap and check its status */
  dfpalEnableTrap(DFPAL_TRP_INVALID);
  if (dfpalTrapEnabled(DFPAL_TRP_INVALID)) 
    printf ("DFPAL_TRP_INVALID is enabled!\n");

  /* if you're doing run-time linking, run-time loaders do not      */
  /* resolve extern variables on dlopen(). In that case you need to */
  /* use dfpalGetFlagHandle() get pointer to defined flgs and use   */
  /* status, traps, rounding mode as following, i.e using           */
  /* S_DFPAL_XXXXX(p) macros. You can mix two types of macros       */
  /* (i.e. DFPAL_XXXX and S_DFPAL_XXXX(s) without any side effect.  */
  flhandle=dfpalGetFlagHandle();
  if (!dfpalTrapEnabled(S_DFPAL_TRP_OVERFLOW(flhandle))) 
    printf ("DFPAL_TRP_OVERFLOW is not enabled!\n");

  dfpalEnableTrap(S_DFPAL_TRP_INVALID(flhandle));
  if (dfpalTrapEnabled(S_DFPAL_TRP_INVALID(flhandle))) 
    printf ("DFPAL_TRP_INVALID is enabled!\n");

  printf("sign of n1 %d\n", dec64Sign(n1));

  ddz=dec64Zero();
  printf("Zero value = %s\n", dec64ToString(ddz, res));

  /* compare two numbers */
  if (dec64CompareLT(n1,n2))
    printf("n1 is less than n2\n");
  else if (dec64CompareGT(n1,n2))
    printf("n1 is greater than n2\n");
  else if (dec64CompareEQ(n1,n2))
    printf("n1 is equal to n2\n");

  n3 = dec64Subtract(n1,n2);
  printf("n1 - n2 = %s\n", dec64ToString(n3, res));

  N3 = dec128Subtract(N1,N2);
  printf("N1 - N2 = %s\n", dec128ToString(N3, res));

  printf("original decimal64 n1=%s\n", dec64ToString(n1, res));
  n3=dec64Minus(n1);
  printf("minus n1 = %s\n", dec64ToString(n3, res));

  /* make sure status is retained until cleared */
  st=dfpalReadStatusFlag();
  if (st & DFPAL_FP_OVERFLOW )
    printf("It is still Overflow\n");

  dfpalClearStatusFlag(DFPAL_FP_OVERFLOW);
  st=dfpalReadStatusFlag();
  if (! (st & DFPAL_FP_OVERFLOW) )
    printf("Now, it is not Overflow\n");

#if 0
  {
     char ap[1024];

     while (fgets(ap, sizeof(ap), stdin))
     {
        printf("Got %s\n", ap);
        decimal64 nn = dec64FromInt32(atoi(ap));

        printf( "%s -> ", dec64ToString(nn, res));
        int i=0;
        char * pnn = (char*)&pnn;
        for(;i<8;i++)
           printf("%x", pnn[i]);
        printf("\n");
     }
  }
#endif
  { 
     int a=0;
     struct timeval tms,tme;
     gettimeofday(&tms, NULL);
     for (;a<1000; a++)
  {
     decimal128 fact;
     fact=dec128FromInt32(1);

     i = 1;
     while (i<=factN)
     {
        fact = dec128Multiply(fact, dec128FromInt32(i));
        i++;
     }
     /*printf("Fact(%d) = %s\n", factN, dec128ToString(fact, res));*/

     i = 0;
     while(dec128CompareGT(fact, dec128FromInt32(1)))
     {
        fact = dec128Divide(fact, dec128FromInt32(3));
        /*printf("(%d) = %s\n", i, dec128ToString(fact, res));*/
        i++;
     }
     /*printf("Ran %d iterations\n", i);*/

  }
     gettimeofday(&tme, NULL);

     printf("Time %d\n", (tme.tv_sec-tms.tv_sec)*1000+tme.tv_usec-tms.tv_usec);
  }

  /* Call this to free memory used by DFPAL. Pass your memory free */
  /* routine, corresponding to memory allocation routine used with */
  /* dfpalInit(...)                                                */
  dfpalEnd(free);

  return(0);
}
