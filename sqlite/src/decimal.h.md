# decimal.h

## Summary

Header file defining the **SQL decimal type** for high-precision arithmetic.

## Contents

```c
#ifndef _DECIMAL_H_
#define _DECIMAL_H_

typedef decQuad   sql_decimal_t;
int sqlite3DecimalToString(sql_decimal_t * dec, char *str, int len);

#endif
```

## Definitions

### sql_decimal_t
A typedef for `decQuad`, which is IBM's 128-bit decimal floating-point type from the decNumber library. This provides:
- 34 decimal digits of precision
- Range of approximately 10^-6143 to 10^6144
- IEEE 754-2008 compliant decimal arithmetic

### sqlite3DecimalToString()
```c
int sqlite3DecimalToString(sql_decimal_t *dec, char *str, int len);
```
Converts a decimal value to its string representation.

## Purpose

Comdb2 adds a DECIMAL data type to SQLite for:
1. **Financial applications** - Where floating-point rounding errors are unacceptable
2. **Precise calculations** - Scientific or engineering computations requiring exact decimal representation
3. **SQL standard compliance** - DECIMAL is a standard SQL type

## Why This File Exists

SQLite natively only supports REAL (double-precision floating-point). The decimal type provides:
- Exact representation of decimal fractions (0.1 is exact, not 0.10000000000000001)
- Consistent rounding behavior
- Better match for business/financial data types
