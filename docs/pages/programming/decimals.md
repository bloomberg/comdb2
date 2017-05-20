---
title: Decimal types
keywords: code
sidebar: mydoc_sidebar
permalink: decimals.html
---

## Decimal 32/64/128 datatype

Comdb2 implements the decimal format for real numbers, as described in [IEEE 754-2008 standard](https://standards.ieee.org/findstds/standard/754-2008.html), published 
in August 2008.  The decimal format avoids the pitfall of floating point representation to provide error-less 
arithmetic using real numbers.

Comdb2 provides three datatypes, ```decimal32```, ```decimal64```, and ```decimal128```, providing single, 
double and quad precision, respectively.  Each number is represented as a triplet (sign, significand, exponent), 
and the number value is ((-1)^sign)*significand*(10^exponent).

These are the limitations of each datatype:

* ```decimal32``` supports exponents between -95 and +96; significand has 7 digits (i.e. 0.000000-9.999999).  
  The range of numbers representable by this format is +-0.000000x10&#8722;95 to +-9.999999x10+96
* ```decimal64``` supports exponents between -383 and +384; significand has 16 
  digits (i.e. 0.000000000000000-9.999999999999999).  The range of numbers 
  is +-0.000000000000000x10&#8722;383 to +-9.999999999999999x10+384
* ```decimal128``` supports exponents between -6143 and +6144; significand has 34 
  digits (i.e. 0.000000000000000000000000000000000-9.999999999999999999999999999999999).  
  The range of numbers is +-0.000000000000000000000000000000000x10&#8722;6143 
  to +-9.999999999999999999999999999999999x10+6144

Note: the decimal representation allows for un-normalized significand values.  This makes decimal manipulation a slower operation compared to binarty floating point.  Since comdb2 allows decimal types to be used as part of the key, it was paramount that a simple binary comparison is enough to determine the ordering of two decimal values.  Comdb2 chose to normalize the significand and reduce the range of the numbers by the size of the significand.
