/*
   Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "tcl.h"
#include "tclcdb2.h"
#include "cdb2api.h"

/*
 * NOTE: Flag values for the <pkg>_Unload callback function (Tcl 8.5+).
 */

#if !defined(TCL_UNLOAD_DETACH_FROM_INTERPRETER)
  #define TCL_UNLOAD_DETACH_FROM_INTERPRETER	(1<<0)
#endif /* TCL_UNLOAD_DETACH_FROM_INTERPRETER */

#if !defined(TCL_UNLOAD_DETACH_FROM_PROCESS)
  #define TCL_UNLOAD_DETACH_FROM_PROCESS	(1<<1)
#endif /* TCL_UNLOAD_DETACH_FROM_PROCESS */

#if !defined(TCL_UNLOAD_FROM_INIT)
  #define TCL_UNLOAD_FROM_INIT			(1<<2)
#endif /* TCL_UNLOAD_FROM_INIT */

#if !defined(TCL_UNLOAD_FROM_CMD_DELETE)
  #define TCL_UNLOAD_FROM_CMD_DELETE		(1<<3)
#endif /* TCL_UNLOAD_FROM_CMD_DELETE */

#if !defined(FIXED_BUFFER_SIZE)
  #define FIXED_BUFFER_SIZE			(CDB2_MAX_TZNAME) + \
						(TCL_RESULT_SIZE) + 1
#endif /* FIXED_BUFFER_SIZE */

#if !defined(COUNT_OF)
  #define COUNT_OF(array) 	(sizeof((array)) / sizeof((array)[0]))
#endif /* COUNT_OF */

#if !defined(CDB2_NULL)
  #define CDB2_NULL				(0)
#endif /* CDB2_NULL */

#if !defined(BYTE_SWAP_WIDE_INT)
#define BYTE_SWAP_WIDE_INT(a)                                   \
    do {                                                        \
	unsigned char *bytePtr = (unsigned char *)&(a);         \
	size_t wideSize = sizeof(Tcl_WideInt);                  \
	int index1, index2;                                     \
	for (index1 = 0; index1 < wideSize / 2; index1++) {     \
	    unsigned char byteValue;                            \
	    index2 = (wideSize - 1) - index1;                   \
	    byteValue = bytePtr[index1];                        \
	    bytePtr[index1] = bytePtr[index2];                  \
	    bytePtr[index2] = byteValue;                        \
	}                                                       \
    } while (0);
#endif /* BYTE_SWAP_WIDE_INT */

#if !defined(MAYBE_OUT_OF_MEMORY)
#define MAYBE_OUT_OF_MEMORY(a)                                  \
    do {                                                        \
	if ((a) == NULL) {                                      \
	    Tcl_AppendResult(interp, "out of memory: ", #a,     \
		"\n", NULL);                                    \
	    code = TCL_ERROR;                                   \
	    goto done;                                          \
	}                                                       \
    } while (0);
#endif /* MAYBE_OUT_OF_MEMORY */

#if !defined(GET_CDB2_HANDLE_BY_NAME_OR_FAIL)
#define GET_CDB2_HANDLE_BY_NAME_OR_FAIL(a)                      \
    do {                                                        \
	const char *handleName = Tcl_GetString((a));            \
	pCdb2 = GetCdb2HandleByName(interp, handleName);        \
	if (pCdb2 == NULL) {                                    \
	    Tcl_AppendResult(interp,                            \
		"connection \"", handleName, "\" not found\n",  \
		NULL);                                          \
	    code = TCL_ERROR;                                   \
	    goto done;                                          \
	}                                                       \
    } while (0);
#endif /* GET_CDB2_HANDLE_BY_NAME_OR_FAIL */

#if !defined(GET_CDB2_COLUMN_INDEX_OR_FAIL)
#define GET_CDB2_COLUMN_INDEX_OR_FAIL(a)                        \
    do {                                                        \
	int colCount;                                           \
	code = Tcl_GetIntFromObj(interp, (a), &colIndex);       \
	if (code != TCL_OK) goto done;                          \
	colCount = cdb2_numcolumns(pCdb2);                      \
	if (colCount == 0) {                                    \
	    Tcl_AppendResult(interp,                            \
		"there are no columns available\n", NULL);      \
	    code = TCL_ERROR;                                   \
	    goto done;                                          \
	}                                                       \
	if (colIndex < 0) {                                     \
	    Tcl_AppendResult(interp,                            \
		"column index cannot be negative\n", NULL);     \
	    code = TCL_ERROR;                                   \
	    goto done;                                          \
	}                                                       \
	if (colIndex >= colCount) {                             \
	    char intBuf[TCL_INTEGER_SPACE + 1];                 \
	    memset(intBuf, 0, sizeof(intBuf));                  \
	    snprintf(intBuf, sizeof(intBuf), "%d", colCount);   \
	    Tcl_AppendResult(interp,                            \
		"column index must be less than ", intBuf,      \
		"\n", NULL);                                    \
	    code = TCL_ERROR;                                   \
	    goto done;                                          \
	}                                                       \
    } while (0);
#endif /* GET_CDB2_COLUMN_INDEX_OR_FAIL */

#if !defined(GET_AUXILIARY_DATA)
#define GET_AUXILIARY_DATA(a) Tcl_GetAssocData(interp, (a), NULL)
#endif /* GET_AUXILIARY_DATA */

#if !defined(SET_AUXILIARY_DATA)
#define SET_AUXILIARY_DATA(a,b) Tcl_SetAssocData(interp, (a), NULL, (b));
#endif /* SET_AUXILIARY_DATA */

#if !defined(DELETE_AUXILIARY_DATA)
#define DELETE_AUXILIARY_DATA(a) Tcl_DeleteAssocData(interp, (a));
#endif /* DELETE_AUXILIARY_DATA */

#if !defined(GET_STR_OR_NULL)
#define GET_STR_OR_NULL(a) ((a) != NULL ? (a) : "(null)")
#endif /* GET_STR_OR_NULL */

#if !defined(IS_LITTLE_ENDIAN)
#define IS_LITTLE_ENDIAN (*(char *)(&iTheValueOfOne) == 1)
#endif /* IS_LITTLE_ENDIAN */

typedef struct NameAndValue {
    char *name;
    int value;
} NameAndValue;

typedef struct BoundValue {
    int type;
    int length;
    int index;
    char *name;
    union {
	Tcl_WideInt wideValue;
	double doubleValue;
	char *cstringValue;
	unsigned char *blobValue;
	cdb2_client_datetime_t dateTimeValue;
	cdb2_client_datetimeus_t dateTimeUsValue;
	cdb2_client_intv_ym_t intervalYmValue;
	cdb2_client_intv_ds_t intervalDsValue;
	cdb2_client_intv_dsus_t intervalDsUsValue;
    } value;
} BoundValue;

static int		IsValidInterp(Tcl_Interp *interp);
static int		GetPairFromName(Tcl_Interp *interp,
			    const char *name, const NameAndValue pairs[],
			    const NameAndValue **pairPtr);
static int		GetPairFromValue(Tcl_Interp *interp,
			    int value, const NameAndValue pairs[],
			    const NameAndValue **pairPtr);
static int		GetValueFromName(Tcl_Interp *interp,
			    const char *name, const NameAndValue pairs[],
			    int *valuePtr);
static int		GetNameFromValue(Tcl_Interp *interp,
			    int value, const NameAndValue pairs[],
			    const char **namePtr);
static int		GetFlagsFromList(Tcl_Interp *interp, Tcl_Obj *listPtr,
			    const NameAndValue pairs[], int *flagsPtr);
static int		ProcessStructFieldsFromElements(Tcl_Interp *interp,
			    Tcl_Obj **elemPtrs, int elemCount,
			    const NameAndValue fields[], void *valuePtr,
			    size_t valueLength);
static int		GetListFromValueStruct(Tcl_Interp *interp, int type,
			    size_t valueLength, const void *valuePtr,
			    size_t stringLength, char *stringPtr);
static int		GetValueStructFromObj(Tcl_Interp *interp, int type,
			    Tcl_Obj *listObj, size_t valueLength,
			    void *valuePtr);
static cdb2_hndl_tp *	GetCdb2HandleByName(Tcl_Interp *interp,
			    const char *name);
static int		AddCdb2HandleByName(Tcl_Interp *interp,
			    cdb2_hndl_tp *pCdb2, char *name);
static int		RemoveCdb2HandleByName(Tcl_Interp *interp,
			    const char *name);
static void		AppendCdb2ErrorMessage(Tcl_Interp *interp, int rc,
			    cdb2_hndl_tp *pCdb2);
static void		FreeBoundValue(BoundValue *pBoundValue);
static void		FreeParameterValues(Tcl_Interp *interp);
static void		tclcdb2ExitProc(ClientData clientData);
static int		tclcdb2ObjCmd(ClientData clientData, Tcl_Interp *interp,
			    int objc, Tcl_Obj *CONST objv[]);
static void		tclcdb2ObjCmdDeleteProc(ClientData clientData);

static NameAndValue aOpenFlags[] = {
    { "direct_cpu",           CDB2_DIRECT_CPU           },
    { "random",               CDB2_RANDOM               },
    { "randomroom",           CDB2_RANDOMROOM           },
    { "read_intrans_results", CDB2_READ_INTRANS_RESULTS },
    { "room",                 CDB2_ROOM                 },
    { NULL,                   0                         }
};

static NameAndValue aColumnTypes[] = {
    { "null",                 CDB2_NULL                 },
    { "blob",                 CDB2_BLOB                 },
    { "cstring",              CDB2_CSTRING              },
    { "datetime",             CDB2_DATETIME             },
    { "datetimeus",           CDB2_DATETIMEUS           },
    { "integer",              CDB2_INTEGER              },
    { "intervalds",           CDB2_INTERVALDS           },
    { "intervaldsus",         CDB2_INTERVALDSUS         },
    { "intervalym",           CDB2_INTERVALYM           },
    { "real",                 CDB2_REAL                 },
    { NULL,                   0                         }
};

enum tcl_cdb2_element_counts {
    CDB2_DATETIME_MIN_ELEMENTS   =  6,
    CDB2_DATETIME_MAX_ELEMENTS   = 22,
    CDB2_INTERVALYM_ELEMENTS     =  6,
    CDB2_INTERVALDS_ELEMENTS     = 12,
    CDB2_DATETIMEUS_MIN_ELEMENTS =  6,
    CDB2_DATETIMEUS_MAX_ELEMENTS = 22,
    CDB2_DATETIMEUS_ELEMENTS     = 22,
    CDB2_INTERVALDSUS_ELEMENTS   = 12
};

TCL_DECLARE_MUTEX(packageMutex);

const int iTheValueOfOne = 1;
static int bHaveTclStubs = 0;

/*
 *----------------------------------------------------------------------
 *
 * IsValidInterp --
 *
 *	This function attempts to determine if the specified Tcl
 *	interpreter is usable.  In general, this function should
 *	only raise an exception if the underlying memory for the
 *	specified Tcl interpreter has been freed.  In that case,
 *	calling this function may result in undefined behavior.
 *
 * Results:
 *	Non-zero if the specified Tcl interpreter is valid and
 *	usable (e.g. not deleted, etc).
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static int IsValidInterp(
    Tcl_Interp *interp)			/* Current Tcl interpreter. */
{
    return (interp != NULL) && !Tcl_InterpDeleted(interp);
}

/*
 *----------------------------------------------------------------------
 *
 * GetPairFromName --
 *
 *	This function attempts to find the name/value pair based on its
 *	name.  Upon success, the name/value pair pointer pointed to by
 *	pairPtr is set to the appropriate value.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static int GetPairFromName(
    Tcl_Interp *interp,			/* Current Tcl interpreter. */
    const char *name,			/* Name for value to be found. */
    const NameAndValue pairs[],		/* Possible names and values. */
    const NameAndValue **pairPtr)	/* OUT: Name/value pointer. */
{
    if (pairs != NULL) {
	size_t count = strlen(name);
	int index = 0;

	for (;; index++) {
	    if (pairs[index].name == NULL)
		break;

	    if (strncmp(pairs[index].name, name, count) == 0) {
		if (pairPtr != NULL)
		    *pairPtr = &pairs[index];

		return TCL_OK;
	    }
	}
    }

    if (interp != NULL) {
	Tcl_AppendResult(interp,
	    "name \"", GET_STR_OR_NULL(name), "\" not found\n", NULL);
    }

    return TCL_ERROR;
}

/*
 *----------------------------------------------------------------------
 *
 * GetPairFromValue --
 *
 *	This function attempts to find the name/value pair based on its
 *	name.  Upon success, the name/value pair pointer pointed to by
 *	pairPtr is set to the appropriate value.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static int GetPairFromValue(
    Tcl_Interp *interp,			/* Current Tcl interpreter. */
    int value,				/* Value for name to be found. */
    const NameAndValue pairs[],		/* Possible names and values. */
    const NameAndValue **pairPtr)	/* OUT: Name/value pointer. */
{
    if (pairs != NULL) {
	int index = 0;

	for (;; index++) {
	    if (pairs[index].name == NULL)
		break;

	    if (pairs[index].value == value) {
		if (pairPtr != NULL)
		    *pairPtr = &pairs[index];

		return TCL_OK;
	    }
	}
    }

    if (interp != NULL) {
	char intBuf[TCL_INTEGER_SPACE + 1];

	memset(intBuf, 0, sizeof(intBuf));
	snprintf(intBuf, sizeof(intBuf), "%d", value);

	Tcl_AppendResult(interp,
	    "value \"", intBuf, "\" not found\n", NULL);
    }

    return TCL_ERROR;
}

/*
 *----------------------------------------------------------------------
 *
 * GetValueFromName --
 *
 *	This function attempts to convert a string into an integer
 *	value based on an array of "well-known" named values.  Upon
 *	success, the integer value pointed to by valuePtr is set to
 *	the value associated with the name specified.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static int GetValueFromName(
    Tcl_Interp *interp,			/* Current Tcl interpreter. */
    const char *name,			/* Name for value to be found. */
    const NameAndValue pairs[],		/* Possible names and values. */
    int *valuePtr)			/* OUT: Integer value. */
{
    int value;
    const NameAndValue *pair = NULL;

    if (name == NULL) {
	if (interp != NULL) {
	    Tcl_AppendResult(interp, "invalid name\n", NULL);
	}

	return TCL_ERROR;
    }

    if (Tcl_GetInt(NULL, name, &value) == TCL_OK) {
	if (valuePtr != NULL)
	    *valuePtr = value;

	return TCL_OK;
    }

    if (GetPairFromName(interp, name, pairs, &pair) == TCL_OK) {
	if ((valuePtr != NULL) && (pair != NULL))
	    *valuePtr = pair->value;

	return TCL_OK;
    }

    if (interp != NULL) {
	Tcl_AppendResult(interp,
	    "value for name \"", GET_STR_OR_NULL(name), "\" not found\n",
	    NULL);
    }

    return TCL_ERROR;
}

/*
 *----------------------------------------------------------------------
 *
 * GetNameFromValue --
 *
 *	This function attempts to convert an integer value to a string
 *	based on an array of "well-known" named values.  Upon success,
 *	the string value pointed to by namePtr is set to the name
 *	associated with the value specified.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static int GetNameFromValue(
    Tcl_Interp *interp,			/* Current Tcl interpreter. */
    int value,				/* Value for name to be found. */
    const NameAndValue pairs[],		/* Possible names and values. */
    const char **namePtr)		/* OUT: String name. */
{
    const NameAndValue *pair = NULL;

    if (GetPairFromValue(interp, value, pairs, &pair) == TCL_OK) {
	if ((namePtr != NULL) && (pair != NULL))
	    *namePtr = pair->name;

	return TCL_OK;
    }

    if (interp != NULL) {
	char intBuf[TCL_INTEGER_SPACE + 1];

	memset(intBuf, 0, sizeof(intBuf));
	snprintf(intBuf, sizeof(intBuf), "%d", value);

	Tcl_AppendResult(interp,
	    "name for value \"", intBuf, "\" not found\n", NULL);
    }

    return TCL_ERROR;
}

/*
 *----------------------------------------------------------------------
 *
 * GetFlagsFromList --
 *
 *	This function converts a list of strings into an integer flags
 *	value based on an array of "well-known" named and values.  Upon
 *	success, the integer flags value pointed to by flagsPtr is set
 *	to the OR'd combination of all flag values present in the list
 *	of strings.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static int GetFlagsFromList(
    Tcl_Interp *interp,			/* Current Tcl interpreter. */
    Tcl_Obj *listPtr,			/* List of flag strings. */
    const NameAndValue pairs[],		/* Possible flag names and values. */
    int *flagsPtr)			/* OUT: OR'd flags value. */
{
    int index, objc;
    Tcl_Obj **objv;
    int flags = 0;

    if (Tcl_ListObjGetElements(interp, listPtr, &objc, &objv) != TCL_OK) {
	return TCL_ERROR;
    }

    for (index = 0; index < objc; index++) {
	const char *name = Tcl_GetString(objv[index]);
	int value;

	if (GetValueFromName(interp, name, pairs, &value) != TCL_OK) {
	    return TCL_ERROR;
	}

	flags |= value;
    }

    if (flagsPtr != NULL)
	*flagsPtr = flags;

    return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * ProcessStructFieldsFromElements --
 *
 *	This function attempts to process a list of field specifiers,
 *	which may be a constant string value or one of "%s", "%d", or
 *	"%u".  The results will be placed into the memory pointed to
 *	by valuePtr.  If a field specifier is a constant string value,
 *	it will be matched exactly and a mismatch will cause an error
 *	to be returned.  If a field specifier is "%s", any string will
 *	be considered valid.  Otherwise, the value will be checked
 *	against the numeric field specifier, to check its conformance,
 *	and an error will be returned upon failure.  Any error will
 *	halt further processing.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static int ProcessStructFieldsFromElements(
    Tcl_Interp *interp,
    Tcl_Obj **elemPtrs,
    int elemCount,
    const NameAndValue fields[],
    void *valuePtr,
    size_t valueLength)
{
    int index;

    assert(interp != NULL);

    if (elemPtrs == NULL) {
	Tcl_AppendResult(interp, "invalid value list\n", NULL);
	return TCL_ERROR;
    }

    assert(elemCount > 0);

    if (elemCount & 1) {
	Tcl_AppendResult(interp, "malformed value dictionary\n", NULL);
	return TCL_ERROR;
    }

    if (fields == NULL) {
	Tcl_AppendResult(interp, "invalid field list\n", NULL);
	return TCL_ERROR;
    }

    if (valuePtr == NULL) {
	Tcl_AppendResult(interp, "invalid value structure\n", NULL);
	return TCL_ERROR;
    }

    assert(valueLength > 0);

    for (index = 0; index < elemCount; index += 2) {
	Tcl_Obj *elemObj;
	const NameAndValue *pair;
	const char *format;
	size_t offset;

	/*
	 * NOTE: Grab the name part of this name/value pair from the
	 *       Tcl dictionary.  It must be found in the field list
	 *       provided by the caller.
	 */

	elemObj = elemPtrs[index];
	assert(elemObj != NULL);

	if (GetPairFromName(interp, Tcl_GetString(elemObj), fields,
		&pair) != TCL_OK) {
	    return TCL_ERROR;
	}

	if (pair == NULL)
	    break;

	pair++; /* NOTE: Advance to the value descriptor. */
	format = pair->name;

	if (format == NULL)
	    break;

	if (*format == 0)
	    continue;

	offset = (size_t)pair->value;
	assert(offset <= sizeof(cdb2_client_datetime_t)); /* SANITY */

	/*
	 * NOTE: Grab the value part of this name/value pair from the
	 *       Tcl dictionary.  Accessing this array element is safe
	 *       based on the invariant for the containing loop -AND-
	 *       the fact this array has an even number of elements.
	 */

	elemObj = elemPtrs[index + 1];
	assert(elemObj != NULL);

	if ((strcmp(format, "%d") == 0) || (strcmp(format, "%u") == 0)) {
	    int intValue;

	    if (Tcl_GetIntFromObj(interp, elemObj, &intValue) != TCL_OK) {
		return TCL_ERROR;
	    }

	    assert(sizeof(int) == sizeof(unsigned int));
	    assert(offset + sizeof(int) <= valueLength);

	    memcpy(valuePtr + offset, &intValue, sizeof(int));
	} else {
	    int length;
	    const char *stringValue;

	    stringValue = Tcl_GetStringFromObj(elemObj, &length);

	    assert(stringValue != NULL && length >= 0);

	    if (strcmp(format, "%s") == 0) { /* SAFE: Literally "%s". */
		assert(offset + length <= valueLength);
		memcpy(valuePtr + offset, stringValue, (size_t)length);
	    } else {
		stringValue = Tcl_GetString(elemObj);
		assert(offset == 0);

		if (strncmp(format, stringValue, length) != 0) {
		    return TCL_ERROR;
		}
	    }
	}
    }

    return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * GetListFromValueStruct --
 *
 *	This function attempts to convert the specified structure to
 *	its canonical list representation based on the given type.  No
 *	memory will be allocated and all conversions are performed in
 *	place.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static int GetListFromValueStruct(
    Tcl_Interp *interp,		/* Current Tcl interpreter. */
    int type,			/* Data type, e.g. CDB2_DATETIME, et al. */
    size_t valueLength,		/* Size of structure buffer in bytes. */
    const void *valuePtr,	/* Pointer to structure of given type. */
    size_t stringLength,	/* Size of string buffer in bytes. */
    char *stringPtr)		/* OUT: Pointer to string buffer. */
{
    cdb2_client_datetime_t *pDateTimeValue;
    cdb2_client_datetimeus_t *pDateTimeUsValue;
    cdb2_client_intv_ym_t *pIntervalYmValue;
    cdb2_client_intv_ds_t *pIntervalDsValue;
    cdb2_client_intv_dsus_t *pIntervalDsUsValue;

    assert(interp != NULL);
    memset(stringPtr, 0, stringLength);

    switch (type) {
	case CDB2_DATETIME: {
	    assert(valueLength >= sizeof(cdb2_client_datetime_t));
	    pDateTimeValue = (cdb2_client_datetime_t *)valuePtr;

	    snprintf(stringPtr, stringLength,
		"sec %d min %d hour %d mday %d mon %d year %d "
		"wday %d yday %d isdst %d msec %u tzname {%s}",
		pDateTimeValue->tm.tm_sec, pDateTimeValue->tm.tm_min,
		pDateTimeValue->tm.tm_hour, pDateTimeValue->tm.tm_mday,
		pDateTimeValue->tm.tm_mon, pDateTimeValue->tm.tm_year,
		pDateTimeValue->tm.tm_wday, pDateTimeValue->tm.tm_yday,
		pDateTimeValue->tm.tm_isdst, pDateTimeValue->msec,
		pDateTimeValue->tzname);

	    return TCL_OK;
	}
	case CDB2_INTERVALYM: {
	    assert(valueLength >= sizeof(cdb2_client_intv_ym_t));
	    pIntervalYmValue = (cdb2_client_intv_ym_t *)valuePtr;

	    snprintf(stringPtr, stringLength,
		"sign %d years %u months %u", pIntervalYmValue->sign,
		pIntervalYmValue->years, pIntervalYmValue->months);

	    return TCL_OK;
	}
	case CDB2_INTERVALDS: {
	    assert(valueLength >= sizeof(cdb2_client_intv_ds_t));
	    pIntervalDsValue = (cdb2_client_intv_ds_t *)valuePtr;

	    snprintf(stringPtr, stringLength,
		"sign %d days %u hours %u mins %u secs %u msecs %u",
		pIntervalDsValue->sign, pIntervalDsValue->days,
		pIntervalDsValue->hours, pIntervalDsValue->mins,
		pIntervalDsValue->sec, pIntervalDsValue->msec);

	    return TCL_OK;
	}
	case CDB2_DATETIMEUS: {
	    assert(valueLength >= sizeof(cdb2_client_datetimeus_t));
	    pDateTimeUsValue = (cdb2_client_datetimeus_t *)valuePtr;

	    snprintf(stringPtr, stringLength,
		"sec %d min %d hour %d mday %d mon %d year %d "
		"wday %d yday %d isdst %d usec %u tzname {%s}",
		pDateTimeUsValue->tm.tm_sec, pDateTimeUsValue->tm.tm_min,
		pDateTimeUsValue->tm.tm_hour, pDateTimeUsValue->tm.tm_mday,
		pDateTimeUsValue->tm.tm_mon, pDateTimeUsValue->tm.tm_year,
		pDateTimeUsValue->tm.tm_wday, pDateTimeUsValue->tm.tm_yday,
		pDateTimeUsValue->tm.tm_isdst, pDateTimeUsValue->usec,
		pDateTimeUsValue->tzname);

	    return TCL_OK;
	}
	case CDB2_INTERVALDSUS: {
	    assert(valueLength >= sizeof(cdb2_client_intv_dsus_t));
	    pIntervalDsUsValue = (cdb2_client_intv_dsus_t *)valuePtr;

	    snprintf(stringPtr, stringLength,
		"sign %d days %u hours %u mins %u secs %u usecs %u",
		pIntervalDsUsValue->sign, pIntervalDsUsValue->days,
		pIntervalDsUsValue->hours, pIntervalDsUsValue->mins,
		pIntervalDsUsValue->sec, pIntervalDsUsValue->usec);

	    return TCL_OK;
	}
	default: {
	    snprintf(stringPtr, stringLength,
		"unsupported value type %d\n", type);

	    return TCL_ERROR;
	}
    }
}

/*
 *----------------------------------------------------------------------
 *
 * GetValueStructFromObj --
 *
 *	This function attempts to convert the specified (canonical)
 *	list representation of a structure to the structure itself,
 *	based on the given type.  No memory will be allocated and all
 *	conversions are performed in place.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static int GetValueStructFromObj(
    Tcl_Interp *interp,		/* Current Tcl interpreter. */
    int type,			/* Data type, e.g. CDB2_DATETIME, et al. */
    Tcl_Obj *listObj,		/* List of name/value pairs for given type. */
    size_t valueLength,		/* Size of structure buffer in bytes. */
    void *valuePtr)		/* OUT: Pointer to structure of given type. */
{
    int code;
    int elemCount = 0;
    Tcl_Obj **elemPtrs = NULL;

    assert(interp != NULL);
    memset(valuePtr, 0, valueLength);

    code = Tcl_ListObjGetElements(interp, listObj, &elemCount, &elemPtrs);

    if (code != TCL_OK)
	goto done;

    if (elemCount == 0) /* NOTE: Empty list, do nothing. */
	goto done;

    switch (type) {
	case CDB2_DATETIME: {
	    const NameAndValue fields[] = {
		{   "sec", 0},
		{    "%d", offsetof(cdb2_client_datetime_t, tm) +
			   offsetof(cdb2_tm_t, tm_sec)},
		{   "min", 0},
		{    "%d", offsetof(cdb2_client_datetime_t, tm) +
			   offsetof(cdb2_tm_t, tm_min)},
		{  "hour", 0},
		{    "%d", offsetof(cdb2_client_datetime_t, tm) +
			   offsetof(cdb2_tm_t, tm_hour)},
		{  "mday", 0},
		{    "%d", offsetof(cdb2_client_datetime_t, tm) +
			   offsetof(cdb2_tm_t, tm_mday)},
		{   "mon", 0},
		{    "%d", offsetof(cdb2_client_datetime_t, tm) +
			   offsetof(cdb2_tm_t, tm_mon)},
		{  "year", 0},
		{    "%d", offsetof(cdb2_client_datetime_t, tm) +
			   offsetof(cdb2_tm_t, tm_year)},
		{  "wday", 0},
		{    "%d", offsetof(cdb2_client_datetime_t, tm) +
			   offsetof(cdb2_tm_t, tm_wday)},
		{  "yday", 0},
		{    "%d", offsetof(cdb2_client_datetime_t, tm) +
			   offsetof(cdb2_tm_t, tm_yday)},
		{ "isdst", 0},
		{    "%d", offsetof(cdb2_client_datetime_t, tm) +
			   offsetof(cdb2_tm_t, tm_isdst)},
		{  "msec", 0},
		{    "%u", offsetof(cdb2_client_datetime_t, msec)},
		{"tzname", 0},
		{    "%s", offsetof(cdb2_client_datetime_t, tzname)},
		{    NULL, 0}
	    };

	    cdb2_client_datetime_t *pDateTimeValue =
		(cdb2_client_datetime_t *)valuePtr;

	    assert(valueLength >= sizeof(cdb2_client_datetime_t));
	    assert(COUNT_OF(fields) == CDB2_DATETIME_MAX_ELEMENTS);

	    code = ProcessStructFieldsFromElements(interp, elemPtrs,
		elemCount, fields, valuePtr, valueLength);

	    if (code != TCL_OK)
		goto done;

	    break;
	}
	case CDB2_INTERVALYM: {
	    const NameAndValue fields[] = {
		{  "sign", 0},
		{    "%d", offsetof(cdb2_client_intv_ym_t, sign)},
		{ "years", 0},
		{    "%u", offsetof(cdb2_client_intv_ym_t, years)},
		{"months", 0},
		{    "%u", offsetof(cdb2_client_intv_ym_t, months)},
		{    NULL, 0}
	    };

	    assert(valueLength >= sizeof(cdb2_client_intv_ym_t));
	    assert(COUNT_OF(fields) == CDB2_INTERVALYM_ELEMENTS);

	    code = ProcessStructFieldsFromElements(interp, elemPtrs,
		elemCount, fields, valuePtr, valueLength);

	    break;
	}
	case CDB2_INTERVALDS: {
	    const NameAndValue fields[] = {
		{  "sign", 0},
		{    "%d", offsetof(cdb2_client_intv_ds_t, sign)},
		{  "days", 0},
		{    "%u", offsetof(cdb2_client_intv_ds_t, days)},
		{ "hours", 0},
		{    "%u", offsetof(cdb2_client_intv_ds_t, hours)},
		{  "mins", 0},
		{    "%u", offsetof(cdb2_client_intv_ds_t, mins)},
		{  "secs", 0},
		{    "%u", offsetof(cdb2_client_intv_ds_t, sec)},
		{ "msecs", 0},
		{    "%u", offsetof(cdb2_client_intv_ds_t, msec)},
		{    NULL, 0}
	    };

	    assert(valueLength >= sizeof(cdb2_client_intv_ds_t));
	    assert(COUNT_OF(fields) == CDB2_INTERVALDS_ELEMENTS);

	    code = ProcessStructFieldsFromElements(interp, elemPtrs,
		elemCount, fields, valuePtr, valueLength);

	    break;
	}
	case CDB2_DATETIMEUS: {
	    const NameAndValue fields[] = {
		{   "sec", 0},
		{    "%d", offsetof(cdb2_client_datetimeus_t, tm) +
			   offsetof(cdb2_tm_t, tm_sec)},
		{   "min", 0},
		{    "%d", offsetof(cdb2_client_datetimeus_t, tm) +
			   offsetof(cdb2_tm_t, tm_min)},
		{  "hour", 0},
		{    "%d", offsetof(cdb2_client_datetimeus_t, tm) +
			   offsetof(cdb2_tm_t, tm_hour)},
		{  "mday", 0},
		{    "%d", offsetof(cdb2_client_datetimeus_t, tm) +
			   offsetof(cdb2_tm_t, tm_mday)},
		{   "mon", 0},
		{    "%d", offsetof(cdb2_client_datetimeus_t, tm) +
			   offsetof(cdb2_tm_t, tm_mon)},
		{  "year", 0},
		{    "%d", offsetof(cdb2_client_datetimeus_t, tm) +
			   offsetof(cdb2_tm_t, tm_year)},
		{  "wday", 0},
		{    "%d", offsetof(cdb2_client_datetimeus_t, tm) +
			   offsetof(cdb2_tm_t, tm_wday)},
		{  "yday", 0},
		{    "%d", offsetof(cdb2_client_datetimeus_t, tm) +
			   offsetof(cdb2_tm_t, tm_yday)},
		{ "isdst", 0},
		{    "%d", offsetof(cdb2_client_datetimeus_t, tm) +
			   offsetof(cdb2_tm_t, tm_isdst)},
		{  "usec", 0},
		{    "%u", offsetof(cdb2_client_datetimeus_t, usec)},
		{"tzname", 0},
		{    "%s", offsetof(cdb2_client_datetimeus_t, tzname)},
		{    NULL, 0}
	    };

	    cdb2_client_datetimeus_t *pDateTimeUsValue =
		(cdb2_client_datetimeus_t *)valuePtr;

	    assert(valueLength >= sizeof(cdb2_client_datetimeus_t));
	    assert(COUNT_OF(fields) == CDB2_DATETIMEUS_MAX_ELEMENTS);

	    code = ProcessStructFieldsFromElements(interp, elemPtrs,
		elemCount, fields, valuePtr, valueLength);

	    if (code != TCL_OK)
		goto done;

	    break;
	}
	case CDB2_INTERVALDSUS: {
	    const NameAndValue fields[] = {
		{ "sign", 0},
		{   "%d", offsetof(cdb2_client_intv_dsus_t, sign)},
		{ "days", 0},
		{   "%u", offsetof(cdb2_client_intv_dsus_t, days)},
		{"hours", 0},
		{   "%u", offsetof(cdb2_client_intv_dsus_t, hours)},
		{ "mins", 0},
		{   "%u", offsetof(cdb2_client_intv_dsus_t, mins)},
		{ "secs", 0},
		{   "%u", offsetof(cdb2_client_intv_dsus_t, sec)},
		{"usecs", 0},
		{   "%u", offsetof(cdb2_client_intv_dsus_t, usec)},
		{   NULL, 0}
	    };

	    assert(valueLength >= sizeof(cdb2_client_intv_dsus_t));
	    assert(COUNT_OF(fields) == CDB2_INTERVALDSUS_ELEMENTS);

	    code = ProcessStructFieldsFromElements(interp, elemPtrs,
		elemCount, fields, valuePtr, valueLength);

	    break;
	}
	default: {
	    char buffer[FIXED_BUFFER_SIZE + 1];

	    memset(buffer, 0, sizeof(buffer));
	    snprintf(buffer, sizeof(buffer), "%d", type);

	    Tcl_AppendResult(interp, "unsupported value type ", buffer,
		"\n", NULL);

	    code = TCL_ERROR;
	    goto done;
	}
    }

done:
    return code;
}

/*
 *----------------------------------------------------------------------
 *
 * GetCdb2HandleByName --
 *
 *	This function locates a CDB2 connection handle and returns it,
 *	based on the Tcl interpreter and name specified by the caller.
 *
 * Results:
 *	A pointer to the CDB2 connection handle -OR- NULL if it cannot
 *	be found for any reason.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static cdb2_hndl_tp *GetCdb2HandleByName(
    Tcl_Interp *interp,		/* Current Tcl interpreter. */
    const char *name)		/* Name of the connection handle. */
{
    Tcl_HashTable *hTablePtr;
    Tcl_HashEntry *hPtr;

    if (!IsValidInterp(interp) || (name == NULL))
	return NULL;

    hTablePtr = GET_AUXILIARY_DATA("tclcdb2_handles");

    if (hTablePtr == NULL)
	return NULL;

    hPtr = Tcl_FindHashEntry(hTablePtr, name);

    if (hPtr == NULL)
	return NULL;

    return (cdb2_hndl_tp *)Tcl_GetHashValue(hPtr);
}

/*
 *----------------------------------------------------------------------
 *
 * AddCdb2HandleByName --
 *
 *	This function creates an assocation between a CDB2 connection
 *	handle and the specified Tcl interpreter.  The output buffer,
 *	when specified, must be at least (FIXED_BUFFER_SIZE + 1) bytes
 *	in size.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	The name used to identify the CDB2 connection handle within
 *	the Tcl interpreter is written into the buffer specified by
 *	the associated parameter, if applicable.
 *
 *----------------------------------------------------------------------
 */

static int AddCdb2HandleByName(
    Tcl_Interp *interp,
    cdb2_hndl_tp *pCdb2,
    char *name)
{
    Tcl_HashTable *hTablePtr;
    Tcl_HashEntry *hPtr;
    int wasNew;
    char buffer[FIXED_BUFFER_SIZE + 1];

    if (!IsValidInterp(interp))
	return TCL_ERROR;

    if (pCdb2 == NULL) {
	Tcl_AppendResult(interp, "can't add: invalid connection\n", NULL);
	return TCL_ERROR;
    }

    hTablePtr = GET_AUXILIARY_DATA("tclcdb2_handles");

    if (hTablePtr == NULL) {
	Tcl_AppendResult(interp, "can't add: missing table\n", NULL);
	return TCL_ERROR;
    }

    memset(buffer, 0, sizeof(buffer));
    snprintf(buffer, sizeof(buffer), "cdb2_%p", pCdb2);

    hPtr = Tcl_CreateHashEntry(hTablePtr, buffer, &wasNew);

    if (hPtr == NULL) {
	Tcl_AppendResult(interp, "can't add: entry not created\n", NULL);
	return TCL_ERROR;
    }

    if (!wasNew) {
	Tcl_AppendResult(interp, "can't add: expected new entry\n", NULL);
	return TCL_ERROR;
    }

    Tcl_SetHashValue(hPtr, pCdb2);

    if (name != NULL)
	memcpy(name, buffer, sizeof(buffer));

    return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * RemoveCdb2HandleByName --
 *
 *	This function deletes the assocation between a CDB2 connection
 *	handle and the specified Tcl interpreter.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	Upon failure, the Tcl inerpreter result may be modified to add
 *	an error message.
 *
 *----------------------------------------------------------------------
 */

static int RemoveCdb2HandleByName(
    Tcl_Interp *interp,
    const char *name)
{
    Tcl_HashTable *hTablePtr;
    Tcl_HashEntry *hPtr;

    if (!IsValidInterp(interp))
	return TCL_ERROR;

    if (name == NULL) {
	Tcl_AppendResult(interp, "can't remove: invalid name\n", NULL);
	return TCL_ERROR;
    }

    hTablePtr = GET_AUXILIARY_DATA("tclcdb2_handles");

    if (hTablePtr == NULL) {
	Tcl_AppendResult(interp, "can't remove: missing table\n", NULL);
	return TCL_ERROR;
    }

    hPtr = Tcl_FindHashEntry(hTablePtr, name);

    if (hPtr == NULL) {
	Tcl_AppendResult(interp, "can't remove: entry not found\n", NULL);
	return TCL_ERROR;
    }

    Tcl_DeleteHashEntry(hPtr);
    return TCL_OK;
}

/*
 *----------------------------------------------------------------------
 *
 * AppendCdb2ErrorMessage --
 *
 *	This function accepts a CDB2 return code (e.g. CDB2_OK) and
 *	optionally an associated connection handle and then creates
 *	an appropriate error message as a string.
 *
 * Results:
 *	None.
 *
 * Side effects:
 *	An error message string based on the specified CDB2 return
 *	code and/or connection handle may be appended to the Tcl
 *	interpreter result.
 *
 *----------------------------------------------------------------------
 */

static void AppendCdb2ErrorMessage(
    Tcl_Interp *interp,		/* Current Tcl interpreter. */
    int rc,			/* The CDB2 return code. */
    cdb2_hndl_tp *pCdb2)	/* Connection handle, may be NULL. */
{
    int fullErrors = 0;

    if (!IsValidInterp(interp))
	return;

    if (Tcl_GetVar2(interp, "cdb2", "fullErrors", TCL_GLOBAL_ONLY) != NULL)
	fullErrors = 1;

    if (fullErrors) {
	char intBuf[TCL_INTEGER_SPACE + 1];

	memset(intBuf, 0, sizeof(intBuf));
	snprintf(intBuf, sizeof(intBuf), "%d", rc);

	Tcl_AppendResult(interp, " cdb2 ",
	    (rc == CDB2_OK) ? "success" : "failure", " (rc=", intBuf, ")",
	    NULL);
    }

    if (pCdb2 != NULL)
	Tcl_AppendResult(interp, cdb2_errstr(pCdb2), NULL);

    if (fullErrors)
        Tcl_AppendResult(interp, "\n", NULL);
}

/*
 *----------------------------------------------------------------------
 *
 * FreeBoundValue --
 *
 *	This function frees any memory that was allocated for use by
 *	the specified bound value.
 *
 * Results:
 *	None.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static void FreeBoundValue(
    BoundValue *pBoundValue)		/* Bound value to free. */
{
    if (pBoundValue == NULL)
	return;

    switch (pBoundValue->type) {
	case CDB2_BLOB: {
	    if (pBoundValue->value.blobValue != NULL) {
		free(pBoundValue->value.blobValue);
		pBoundValue->value.blobValue = NULL;
	    }
	}
	case CDB2_CSTRING: {
	    if (pBoundValue->value.cstringValue != NULL) {
		free(pBoundValue->value.cstringValue);
		pBoundValue->value.cstringValue = NULL;
	    }
	}
    }

    if (pBoundValue->name != NULL) {
	free(pBoundValue->name);
	pBoundValue->name = NULL;
    }

    free(pBoundValue);
}

/*
 *----------------------------------------------------------------------
 *
 * FreeParameterValues --
 *
 *	This function frees any memory values that were allocated for
 *	use with bound parameters.  It should only be called if all
 *	bindings have been cleared -OR- it will be impossible to use
 *	them again (i.e. the associated database connection is being
 *	closed).
 *
 * Results:
 *	None.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static void FreeParameterValues(
    Tcl_Interp *interp)		/* Current Tcl interpreter. */
{
    Tcl_HashTable *hTablePtr = GET_AUXILIARY_DATA("tclcdb2_params");

    if (hTablePtr != NULL) {
	Tcl_HashSearch hSearch;
	Tcl_HashEntry *hPtr;

	for (hPtr = Tcl_FirstHashEntry(hTablePtr, &hSearch);
		hPtr != NULL; hPtr = Tcl_NextHashEntry(&hSearch)) {
	    BoundValue *pBoundValue = Tcl_GetHashValue(hPtr);

	    FreeBoundValue(pBoundValue);
	    Tcl_SetHashValue(hPtr, NULL);
	}

	Tcl_DeleteHashTable(hTablePtr);
	ckfree((char *) hTablePtr);
	hTablePtr = NULL;

	DELETE_AUXILIARY_DATA("tclcdb2_params");
    }
}

/*
 *----------------------------------------------------------------------
 *
 * Tclcdb2_Init --
 *
 *	This function initializes the package for the specified Tcl
 *	interpreter.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

int Tclcdb2_Init(
    Tcl_Interp *interp)			/* Current Tcl interpreter. */
{
    int code = TCL_OK;
    Tcl_Command command;
    Tcl_HashTable *hTablePtr;

    /*
     * NOTE: Make sure the Tcl interpreter is valid and then try to initialize
     *       the Tcl stubs table.  We cannot call any Tcl API unless this call
     *       succeeds.
     *
     * WARNING: The Tcl interpreter is only checked against NULL here, because
     *          any other checks would likely require using Tcl APIs that may
     *          not be available yet.
     *
     */

    if ((interp == NULL) || !Tcl_InitStubs(interp, "8.4", 0)) {
	return TCL_ERROR;
    }

    /*
     * NOTE: Mark the Tcl stubs mechanism as being fully initialized now and
     *       then grab the package lock for the entire time we are loading and
     *       setting up the package.
     */

    bHaveTclStubs = 1;
    Tcl_MutexLock(&packageMutex);

    /*
     * NOTE: Add our exit handler prior to performing any actions that need to
     *       be undone by it.  However, first delete it in case it has already
     *       been added.  If it has never been added, trying to delete it will
     *       be a harmless no-op.  This appears to be necessary to ensure that
     *       our exit handler has been added exactly once after this point.
     */

    Tcl_DeleteExitHandler(tclcdb2ExitProc, NULL);
    Tcl_CreateExitHandler(tclcdb2ExitProc, NULL);

    command = Tcl_CreateObjCommand(interp, "cdb2", tclcdb2ObjCmd, interp,
	tclcdb2ObjCmdDeleteProc);

    if (command == NULL) {
	Tcl_AppendResult(interp, "command creation failed\n", NULL);
	code = TCL_ERROR;
	goto done;
    }

    SET_AUXILIARY_DATA("tclcdb2cmd", command);

    hTablePtr = (Tcl_HashTable *)attemptckalloc(sizeof(Tcl_HashTable));
    MAYBE_OUT_OF_MEMORY(hTablePtr);

    memset(hTablePtr, 0, sizeof(Tcl_HashTable));
    Tcl_InitHashTable(hTablePtr, TCL_STRING_KEYS);

    SET_AUXILIARY_DATA("tclcdb2_handles", hTablePtr);

    code = Tcl_PkgProvide(interp, "tclcdb2", "1.0");

done:
    Tcl_MutexUnlock(&packageMutex);

    if (code != TCL_OK) {
	if (Tclcdb2_Unload(interp, TCL_UNLOAD_FROM_INIT |
		TCL_UNLOAD_DETACH_FROM_INTERPRETER) != TCL_OK) {
	    fprintf(stderr, "Tclcdb2_Unload: failed via Tclcdb2_Init\n");
	}
    }

    return code;
}

/*
 *----------------------------------------------------------------------
 *
 * Tclcdb2_Unload --
 *
 *	This function unloads the package from the specified Tcl
 *	interpreter -OR- from the entire process.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

int Tclcdb2_Unload(
    Tcl_Interp *interp,			/* Current Tcl interpreter. */
    int flags)				/* Unload behavior flags. */
{
    int code = TCL_OK;
    int bShutdown = (flags & TCL_UNLOAD_DETACH_FROM_PROCESS);
    int bFromCmdDelete = (flags & TCL_UNLOAD_FROM_CMD_DELETE);

    if (bHaveTclStubs == 0) {
	fprintf(stderr, "Tclcdb2_Unload: Tcl stubs are not initialized\n");
	return TCL_ERROR;
    }

    Tcl_MutexLock(&packageMutex);

    if (interp != NULL) {
	Tcl_HashTable *hTablePtr;

	if (!bFromCmdDelete) {
	    Tcl_Command command = GET_AUXILIARY_DATA("tclcdb2cmd");

	    if (command != NULL) {
		if (Tcl_DeleteCommandFromToken(interp, command) != 0) {
		    Tcl_AppendResult(interp, "command deletion failed\n", NULL);
		    code = TCL_ERROR;
		    goto done;
		}
	    }
	}

	DELETE_AUXILIARY_DATA("tclcdb2cmd");
	hTablePtr = GET_AUXILIARY_DATA("tclcdb2_handles");

	if (hTablePtr != NULL) {
	    Tcl_HashSearch hSearch;
	    Tcl_HashEntry *hPtr;

	    for (hPtr = Tcl_FirstHashEntry(hTablePtr, &hSearch);
		    hPtr != NULL; hPtr = Tcl_NextHashEntry(&hSearch)) {
		char *name = Tcl_GetHashKey(hTablePtr, hPtr);
		cdb2_hndl_tp *pCdb2 = Tcl_GetHashValue(hPtr);

		if (pCdb2 != NULL) {
		    int rc = cdb2_close(pCdb2);

		    if (rc != CDB2_OK) {
			AppendCdb2ErrorMessage(interp, rc, pCdb2);
			code = TCL_ERROR;
			goto done;
		    }

		    pCdb2 = NULL;
		    Tcl_SetHashValue(hPtr, pCdb2);
		}
	    }

	    Tcl_DeleteHashTable(hTablePtr);
	    ckfree((char *) hTablePtr);
	    hTablePtr = NULL;
	}

	DELETE_AUXILIARY_DATA("tclcdb2_handles");
	FreeParameterValues(interp);
    }

    if (bShutdown)
	Tcl_DeleteExitHandler(tclcdb2ExitProc, NULL);

done:
    Tcl_MutexUnlock(&packageMutex);

    if ((code == TCL_OK) && bShutdown)
	Tcl_MutexFinalize(&packageMutex);

    return code;
}

/*
 *----------------------------------------------------------------------
 *
 * tclcdb2ExitProc --
 *
 *	Cleanup all the resources allocated by this package.
 *
 * Results:
 *	None.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static void tclcdb2ExitProc(
    ClientData clientData)		/* Not used. */
{
    if (Tclcdb2_Unload(NULL,
	    TCL_UNLOAD_DETACH_FROM_PROCESS) != TCL_OK) {
	fprintf(stderr, "Tclcdb2_Unload: failed via tclcdb2ExitProc\n");
    }
}

/*
 *----------------------------------------------------------------------
 *
 * tclcdb2ObjCmd --
 *
 *	Handles the command added by this package.
 *
 * Results:
 *	A standard Tcl result.
 *
 * Side effects:
 *	The side effects of this command vary based on the sub-command
 *	used and may be arbitrary.
 *
 *----------------------------------------------------------------------
 */

static int tclcdb2ObjCmd(
    ClientData clientData,	/* Not used. */
    Tcl_Interp *interp,		/* Current Tcl interpreter. */
    int objc,			/* Number of arguments. */
    Tcl_Obj *CONST objv[])	/* The arguments. */
{
    char buffer[FIXED_BUFFER_SIZE + 1];
    int code = TCL_OK;
    int option;
    cdb2_hndl_tp *pCdb2;
    int rc;
    Tcl_HashTable *hTablePtr;
    Tcl_HashEntry *hPtr = NULL;
    Tcl_Obj *listPtr = NULL;
    Tcl_Obj *valuePtr = NULL;
    BoundValue *pBoundValue = NULL;
    int *types = NULL;

    static const char *cmdOptions[] = {
	"bind", "close", "cnonce", "colcount", "colname", "colsize",
	"coltype", "colvalue", "configure", "debug", "effects",
	"encrypted", "error", "hint", "next", "open", "run",
	"sockpool", "ssl", "unbind", (char *) NULL
    };

    enum options {
	OPT_BIND, OPT_CLOSE, OPT_CNONCE, OPT_COLCOUNT, OPT_COLNAME,
	OPT_COLSIZE, OPT_COLTYPE, OPT_COLVALUE, OPT_CONFIGURE,
	OPT_DEBUG, OPT_EFFECTS, OPT_ENCRYPTED, OPT_ERROR, OPT_HINT,
	OPT_NEXT, OPT_OPEN, OPT_RUN, OPT_SOCKPOOL, OPT_SSL,
	OPT_UNBIND
    };

    if (!IsValidInterp(interp)) {
	return TCL_ERROR;
    }

    if (objc < 2) {
	Tcl_WrongNumArgs(interp, 1, objv, "option ?arg ...?");
	return TCL_ERROR;
    }

    if (Tcl_GetIndexFromObj(interp, objv[1], cmdOptions, "option", 0,
	    &option) != TCL_OK) {
	return TCL_ERROR;
    }

    Tcl_MutexLock(&packageMutex);

    switch ((enum options)option) {
	case OPT_BIND: {
	    const char *typeName;
	    const char *bindName = NULL;
	    void *valuePtr = NULL;
	    int wasNew = 0, index = 0;

	    if ((objc != 5) && (objc != 6)) {
		Tcl_WrongNumArgs(interp, 2, objv,
		    "connection nameOrIndex type ?value?");

		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    hTablePtr = GET_AUXILIARY_DATA("tclcdb2_params");

	    if (hTablePtr == NULL) {
		hTablePtr = (Tcl_HashTable *)attemptckalloc(
		    sizeof(Tcl_HashTable));

		MAYBE_OUT_OF_MEMORY(hTablePtr);

		memset(hTablePtr, 0, sizeof(Tcl_HashTable));
		Tcl_InitHashTable(hTablePtr, TCL_STRING_KEYS);

		SET_AUXILIARY_DATA("tclcdb2_params", hTablePtr);
	    }

	    pBoundValue = malloc(sizeof(BoundValue));
	    MAYBE_OUT_OF_MEMORY(pBoundValue);
	    memset(pBoundValue, 0, sizeof(BoundValue));

	    code = Tcl_GetIntFromObj(interp, objv[3], &index);
	    memset(buffer, 0, sizeof(buffer));

	    if (code == TCL_OK) {
		bindName = NULL;
		pBoundValue->index = index;

		snprintf(buffer, sizeof(buffer), "index_%d", index);
	    } else {
		Tcl_ResetResult(interp); /* INTL: Expected error. */

		bindName = Tcl_GetString(objv[3]);
		pBoundValue->index = -1;

		snprintf(buffer, sizeof(buffer), "name_%s", bindName);
	    }

	    hPtr = Tcl_CreateHashEntry(hTablePtr, buffer, &wasNew);

	    if (!wasNew) {
		if (bindName != NULL) {
		    memset(buffer, 0, sizeof(buffer));
		    snprintf(buffer, sizeof(buffer), "%s", bindName);
		} else if (index != 0) {
		    memset(buffer, 0, sizeof(buffer));
		    snprintf(buffer, sizeof(buffer), "%d", index);
		}

		Tcl_AppendResult(interp, "parameter \"", buffer,
		    "\" was already bound\n", NULL);

		code = TCL_ERROR;
		goto done;
	    }

	    if (bindName != NULL) {
		pBoundValue->name = strdup(bindName);
		MAYBE_OUT_OF_MEMORY(pBoundValue->name);
	    }

	    typeName = Tcl_GetString(objv[4]);

	    code = GetValueFromName(interp, typeName, aColumnTypes,
		&pBoundValue->type);

	    if (code != TCL_OK)
		goto done;

	    switch (pBoundValue->type) {
		case CDB2_NULL: {
		    if (objc != 5) {
			Tcl_AppendResult(interp,
			    "cannot specify a value for type \"",
			    typeName, "\"\n", NULL);

			code = TCL_ERROR;
			goto done;
		    }

		    valuePtr = NULL;
		    pBoundValue->length = 0;
		    break;
		}
		case CDB2_INTEGER: {
		    if (objc != 6) {
			Tcl_AppendResult(interp,
			    "must specify a value for type \"",
			    typeName, "\"\n", NULL);

			code = TCL_ERROR;
			goto done;
		    }

		    code = Tcl_GetWideIntFromObj(interp, objv[5],
			&pBoundValue->value.wideValue);

		    if (code != TCL_OK)
			goto done;

		    valuePtr = &pBoundValue->value.wideValue;
		    pBoundValue->length = sizeof(Tcl_WideInt);
		    break;
		}
		case CDB2_REAL: {
		    int length = 0;
		    const char *doubleString;

		    if (objc != 6) {
			Tcl_AppendResult(interp,
			    "must specify a value for type \"",
			    typeName, "\"\n", NULL);

			code = TCL_ERROR;
			goto done;
		    }

		    doubleString = Tcl_GetStringFromObj(objv[5], &length);

		    if ((doubleString != NULL) && (length > 2) &&
			    (strncmp(doubleString, "0x", 2) == 0)) {
			Tcl_WideInt wideValue = 0;

			code = Tcl_GetWideIntFromObj(interp, objv[5],
			    &wideValue);

			if (code != TCL_OK)
			    goto done;

			if (!IS_LITTLE_ENDIAN) {
			    BYTE_SWAP_WIDE_INT(wideValue);
			}

			assert(sizeof(Tcl_WideInt) == sizeof(double));

			memcpy(&pBoundValue->value.doubleValue,
			    &wideValue, sizeof(double));

			goto gotDouble;
		    }

		    code = Tcl_GetDoubleFromObj(interp, objv[5],
			&pBoundValue->value.doubleValue);

		    if (code != TCL_OK)
			goto done;

		gotDouble:

		    valuePtr = &pBoundValue->value.doubleValue;
		    pBoundValue->length = sizeof(double);
		    break;
		}
		case CDB2_CSTRING: {
		    const char *cstringValue;
		    int length;

		    if (objc != 6) {
			Tcl_AppendResult(interp,
			    "must specify a value for type \"",
			    typeName, "\"\n", NULL);

			code = TCL_ERROR;
			goto done;
		    }

		    cstringValue = Tcl_GetStringFromObj(objv[5], &length);

		    if (cstringValue != NULL) {
			char *newCstringValue = malloc(
			    (length + 1) * sizeof(char));

			MAYBE_OUT_OF_MEMORY(newCstringValue);
			memset(newCstringValue, 0, (length + 1) * sizeof(char));

			memcpy(newCstringValue, cstringValue, length);
			pBoundValue->value.cstringValue = newCstringValue;
		    } else {
			pBoundValue->value.cstringValue = NULL;
		    }

		    valuePtr = pBoundValue->value.cstringValue;
		    pBoundValue->length = length;
		    break;
		}
		case CDB2_BLOB: {
		    const unsigned char *blobValue;
		    int length;

		    if (objc != 6) {
			Tcl_AppendResult(interp,
			    "must specify a value for type \"",
			    typeName, "\"\n", NULL);

			code = TCL_ERROR;
			goto done;
		    }

		    blobValue = Tcl_GetByteArrayFromObj(objv[5], &length);

		    if (blobValue != NULL) {
			unsigned char *newBlobValue = malloc(
			    (length + 1) * sizeof(char));

			MAYBE_OUT_OF_MEMORY(newBlobValue);

			memset(newBlobValue, 0,
			    (length + 1) * sizeof(char));

			memcpy(newBlobValue, blobValue, length);
			pBoundValue->value.blobValue = newBlobValue;
		    } else {
			pBoundValue->value.blobValue = NULL;
		    }

		    valuePtr = pBoundValue->value.blobValue;
		    pBoundValue->length = length;
		    break;
		}
		case CDB2_DATETIME: {
		    size_t size = sizeof(cdb2_client_datetime_t);

		    if (objc != 6) {
			Tcl_AppendResult(interp,
			    "must specify a value for type \"",
			    typeName, "\"\n", NULL);

			code = TCL_ERROR;
			goto done;
		    }

		    code = GetValueStructFromObj(interp, pBoundValue->type,
			objv[5], size, &pBoundValue->value.dateTimeValue);

		    if (code != TCL_OK)
			goto done;

		    valuePtr = &pBoundValue->value.dateTimeValue;
		    pBoundValue->length = size;
		    break;
		}
		case CDB2_INTERVALYM: {
		    size_t size = sizeof(cdb2_client_intv_ym_t);

		    if (objc != 6) {
			Tcl_AppendResult(interp,
			    "must specify a value for type \"",
			    typeName, "\"\n", NULL);

			code = TCL_ERROR;
			goto done;
		    }

		    code = GetValueStructFromObj(interp, pBoundValue->type,
			objv[5], size, &pBoundValue->value.intervalYmValue);

		    if (code != TCL_OK)
			goto done;

		    valuePtr = &pBoundValue->value.intervalYmValue;
		    pBoundValue->length = size;
		    break;
		}
		case CDB2_INTERVALDS: {
		    size_t size = sizeof(cdb2_client_intv_ds_t);

		    if (objc != 6) {
			Tcl_AppendResult(interp,
			    "must specify a value for type \"",
			    typeName, "\"\n", NULL);

			code = TCL_ERROR;
			goto done;
		    }

		    code = GetValueStructFromObj(interp, pBoundValue->type,
			objv[5], size, &pBoundValue->value.intervalDsValue);

		    if (code != TCL_OK)
			goto done;

		    valuePtr = &pBoundValue->value.intervalDsValue;
		    pBoundValue->length = size;
		    break;
		}
		case CDB2_DATETIMEUS: {
		    size_t size = sizeof(cdb2_client_datetimeus_t);

		    if (objc != 6) {
			Tcl_AppendResult(interp,
			    "must specify a value for type \"",
			    typeName, "\"\n", NULL);

			code = TCL_ERROR;
			goto done;
		    }

		    code = GetValueStructFromObj(interp, pBoundValue->type,
			objv[5], size, &pBoundValue->value.dateTimeUsValue);

		    if (code != TCL_OK)
			goto done;

		    valuePtr = &pBoundValue->value.dateTimeUsValue;
		    pBoundValue->length = size;
		    break;
		}
		case CDB2_INTERVALDSUS: {
		    size_t size = sizeof(cdb2_client_intv_dsus_t);

		    if (objc != 6) {
			Tcl_AppendResult(interp,
			    "must specify a value for type \"",
			    typeName, "\"\n", NULL);

			code = TCL_ERROR;
			goto done;
		    }

		    code = GetValueStructFromObj(interp, pBoundValue->type,
			objv[5], size, &pBoundValue->value.intervalDsUsValue);

		    if (code != TCL_OK)
			goto done;

		    valuePtr = &pBoundValue->value.intervalDsUsValue;
		    pBoundValue->length = size;
		    break;
		}
		default: {
		    memset(buffer, 0, sizeof(buffer));
		    snprintf(buffer, sizeof(buffer), "%d", pBoundValue->type);

		    Tcl_AppendResult(interp, "unsupported value type ",
			buffer, "\n", NULL);

		    code = TCL_ERROR;
		    goto done;
		}
	    }

	    if (pBoundValue->name != NULL) {
		rc = cdb2_bind_param(pCdb2, pBoundValue->name,
		    pBoundValue->type, valuePtr, pBoundValue->length);
	    } else {
		rc = cdb2_bind_index(pCdb2, pBoundValue->index,
		    pBoundValue->type, valuePtr, pBoundValue->length);
	    }

	    if (rc != CDB2_OK) {
		AppendCdb2ErrorMessage(interp, rc, pCdb2);
		code = TCL_ERROR;
		goto done;
	    }

	    Tcl_ResetResult(interp);
	    break;
	}
	case OPT_CLOSE: {
	    if (objc != 3) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    rc = cdb2_close(pCdb2);

	    if (rc != CDB2_OK) {
		AppendCdb2ErrorMessage(interp, rc, pCdb2);
		code = TCL_ERROR;
		goto done;
	    }

	    code = RemoveCdb2HandleByName(interp, Tcl_GetString(objv[2]));

	    if (code != TCL_OK)
		goto done;

	    FreeParameterValues(interp);
	    Tcl_ResetResult(interp);
	    break;
	}
	case OPT_CNONCE: {
	    const char *cnonce;

	    if (objc != 3) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    cnonce = cdb2_cnonce(pCdb2);

	    if (cnonce == NULL) {
		Tcl_AppendResult(interp, "invalid cnonce\n", NULL);
		code = TCL_ERROR;
		goto done;
	    }

	    valuePtr = Tcl_NewStringObj(cnonce, -1);
	    MAYBE_OUT_OF_MEMORY(valuePtr);

	    Tcl_IncrRefCount(valuePtr);
	    Tcl_SetObjResult(interp, valuePtr);
	    Tcl_DecrRefCount(valuePtr);
	    break;
	}
	case OPT_COLCOUNT: {
	    int colCount;

	    if (objc != 3) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    colCount = cdb2_numcolumns(pCdb2);

	    valuePtr = Tcl_NewIntObj(colCount);
	    MAYBE_OUT_OF_MEMORY(valuePtr);

	    Tcl_IncrRefCount(valuePtr);
	    Tcl_SetObjResult(interp, valuePtr);
	    Tcl_DecrRefCount(valuePtr);
	    break;
	}
	case OPT_COLNAME: {
	    int colIndex;
	    const char *colName;

	    if (objc != 4) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection index");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    GET_CDB2_COLUMN_INDEX_OR_FAIL(objv[3]);
	    colName = cdb2_column_name(pCdb2, colIndex);

	    if (colName == NULL) {
		Tcl_AppendResult(interp, "invalid column name\n", NULL);
		code = TCL_ERROR;
		goto done;
	    }

	    valuePtr = Tcl_NewStringObj(colName, -1);
	    MAYBE_OUT_OF_MEMORY(valuePtr);

	    Tcl_IncrRefCount(valuePtr);
	    Tcl_SetObjResult(interp, valuePtr);
	    Tcl_DecrRefCount(valuePtr);
	    break;
	}
	case OPT_COLSIZE: {
	    int colIndex, colSize;

	    if (objc != 4) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection index");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    GET_CDB2_COLUMN_INDEX_OR_FAIL(objv[3]);
	    colSize = cdb2_column_size(pCdb2, colIndex);

	    valuePtr = Tcl_NewIntObj(colSize);
	    MAYBE_OUT_OF_MEMORY(valuePtr);

	    Tcl_IncrRefCount(valuePtr);
	    Tcl_SetObjResult(interp, valuePtr);
	    Tcl_DecrRefCount(valuePtr);
	    break;
	}
	case OPT_COLTYPE: {
	    int colIndex, colType;
	    const char *colTypeName;

	    if (objc != 4) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection index");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    GET_CDB2_COLUMN_INDEX_OR_FAIL(objv[3]);
	    colType = cdb2_column_type(pCdb2, colIndex);

	    code = GetNameFromValue(interp, colType, aColumnTypes,
		&colTypeName);

	    if (code != TCL_OK)
		goto done;

	    valuePtr = Tcl_NewStringObj(colTypeName, -1);
	    MAYBE_OUT_OF_MEMORY(valuePtr);

	    Tcl_IncrRefCount(valuePtr);
	    Tcl_SetObjResult(interp, valuePtr);
	    Tcl_DecrRefCount(valuePtr);
	    break;
	}
	case OPT_COLVALUE: {
	    int colIndex, colType, colSize;
	    void *pColValue;

	    if (objc != 4) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection index");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    GET_CDB2_COLUMN_INDEX_OR_FAIL(objv[3]);
	    pColValue = cdb2_column_value(pCdb2, colIndex);

	    if (pColValue == NULL) {
		Tcl_AppendResult(interp, "invalid column value\n", NULL);
		code = TCL_ERROR;
		goto done;
	    }

	    colType = cdb2_column_type(pCdb2, colIndex);

	    switch (colType) {
		case CDB2_INTEGER: {
		    Tcl_WideInt wideIntValue = *(Tcl_WideInt *)pColValue;
		    valuePtr = Tcl_NewWideIntObj(wideIntValue);
		    break;
		}
		case CDB2_REAL: {
		    double doubleValue = *(double *)pColValue;
		    valuePtr = Tcl_NewDoubleObj(doubleValue);
		    break;
		}
		case CDB2_CSTRING: {
		    const char *cstringValue = (const char *)pColValue;
		    colSize = cdb2_column_size(pCdb2, colIndex);
		    assert(colSize > 0);
		    valuePtr = Tcl_NewStringObj(cstringValue, colSize - 1);
		    break;
		}
		case CDB2_BLOB: {
		    const unsigned char *blobValue =
			(const unsigned char *)pColValue;

		    colSize = cdb2_column_size(pCdb2, colIndex);
		    valuePtr = Tcl_NewByteArrayObj(blobValue, colSize);
		    break;
		}
		case CDB2_DATETIME: {
		    code = GetListFromValueStruct(interp, colType,
			sizeof(cdb2_client_datetime_t), pColValue,
			sizeof(buffer), buffer);

		    if (code != TCL_OK)
			goto done;

		    valuePtr = Tcl_NewStringObj(buffer, -1);
		    break;
		}
		case CDB2_INTERVALYM: {
		    code = GetListFromValueStruct(interp, colType,
			sizeof(cdb2_client_intv_ym_t), pColValue,
			sizeof(buffer), buffer);

		    if (code != TCL_OK)
			goto done;

		    valuePtr = Tcl_NewStringObj(buffer, -1);
		    break;
		}
		case CDB2_INTERVALDS: {
		    code = GetListFromValueStruct(interp, colType,
			sizeof(cdb2_client_intv_ds_t), pColValue,
			sizeof(buffer), buffer);

		    if (code != TCL_OK)
			goto done;

		    valuePtr = Tcl_NewStringObj(buffer, -1);
		    break;
		}
		case CDB2_DATETIMEUS: {
		    code = GetListFromValueStruct(interp, colType,
			sizeof(cdb2_client_datetimeus_t), pColValue,
			sizeof(buffer), buffer);

		    if (code != TCL_OK)
			goto done;

		    valuePtr = Tcl_NewStringObj(buffer, -1);
		    break;
		}
		case CDB2_INTERVALDSUS: {
		    code = GetListFromValueStruct(interp, colType,
			sizeof(cdb2_client_intv_dsus_t), pColValue,
			sizeof(buffer), buffer);

		    if (code != TCL_OK)
			goto done;

		    valuePtr = Tcl_NewStringObj(buffer, -1);
		    break;
		}
		default: {
		    memset(buffer, 0, sizeof(buffer));
		    snprintf(buffer, sizeof(buffer), "%d", colType);

		    Tcl_AppendResult(interp, "unsupported column type ",
			buffer, "\n", NULL);

		    code = TCL_ERROR;
		    goto done;
		}
	    }

	    MAYBE_OUT_OF_MEMORY(valuePtr);

	    Tcl_IncrRefCount(valuePtr);
	    Tcl_SetObjResult(interp, valuePtr);
	    Tcl_DecrRefCount(valuePtr);
	    break;
	}
	case OPT_CONFIGURE: {
	    const char *config;
	    int useFile = 0, reset = 0;

	    if ((objc < 3) || (objc > 5)) {
		Tcl_WrongNumArgs(interp, 2, objv, "string ?useFile? ?reset?");
		code = TCL_ERROR;
		goto done;
	    }

	    if (objc >= 4) {
		code = Tcl_GetBooleanFromObj(interp, objv[3], &useFile);

		if (code != TCL_OK)
		    goto done;
	    }

	    if (objc >= 5) {
		code = Tcl_GetBooleanFromObj(interp, objv[4], &reset);

		if (code != TCL_OK)
		    goto done;
	    }

	    config = Tcl_GetString(objv[2]);

	    if (useFile) {
		cdb2_set_comdb2db_config(reset ? NULL : config);
	    } else {
		cdb2_set_comdb2db_info(reset ? NULL : config);
	    }

	    Tcl_ResetResult(interp);
	    break;
	}
	case OPT_DEBUG: {
	    if (objc != 3) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    cdb2_set_debug_trace(pCdb2);
	    Tcl_ResetResult(interp);
	    break;
	}
	case OPT_EFFECTS: {
	    cdb2_effects_tp effects;
	    int index;

	    static const char *effectNames[] = {
		"affected", "selected", "updated", "deleted",
		"inserted"
	    };

	    static int *effectIntPtrs[] = {
		NULL, NULL, NULL, NULL, NULL
	    };

	    effectIntPtrs[0] = &effects.num_affected;
	    effectIntPtrs[1] = &effects.num_selected;
	    effectIntPtrs[2] = &effects.num_updated;
	    effectIntPtrs[3] = &effects.num_deleted;
	    effectIntPtrs[4] = &effects.num_inserted;

	    if (objc != 3) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    memset(&effects, 0, sizeof(cdb2_effects_tp));
	    rc = cdb2_get_effects(pCdb2, &effects);

	    if (rc != CDB2_OK) {
		AppendCdb2ErrorMessage(interp, rc, pCdb2);
		code = TCL_ERROR;
		goto done;
	    }

	    listPtr = Tcl_NewObj();
	    MAYBE_OUT_OF_MEMORY(listPtr);
	    Tcl_IncrRefCount(listPtr);

	    assert(COUNT_OF(effectNames) == COUNT_OF(effectIntPtrs));

	    for (index = 0; index < COUNT_OF(effectNames); index++) {
		valuePtr = Tcl_NewStringObj(effectNames[index], -1);
		MAYBE_OUT_OF_MEMORY(valuePtr);
		Tcl_IncrRefCount(valuePtr);

		code = Tcl_ListObjAppendElement(interp, listPtr, valuePtr);

		if (code != TCL_OK)
		    goto done;

		Tcl_DecrRefCount(valuePtr);
		valuePtr = Tcl_NewIntObj(*effectIntPtrs[index]);
		MAYBE_OUT_OF_MEMORY(valuePtr);
		Tcl_IncrRefCount(valuePtr);

		code = Tcl_ListObjAppendElement(interp, listPtr, valuePtr);

		if (code != TCL_OK)
		    goto done;

		Tcl_DecrRefCount(valuePtr);
	    }

	    Tcl_SetObjResult(interp, listPtr);
	    Tcl_DecrRefCount(listPtr);
	    break;
	}
	case OPT_ENCRYPTED: {
	    int isEncrypted;

	    if (objc != 3) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    isEncrypted = cdb2_is_ssl_encrypted(pCdb2);

	    valuePtr = Tcl_NewIntObj(isEncrypted);
	    MAYBE_OUT_OF_MEMORY(valuePtr);

	    Tcl_IncrRefCount(valuePtr);
	    Tcl_SetObjResult(interp, valuePtr);
	    Tcl_DecrRefCount(valuePtr);
	    break;
	}
	case OPT_ERROR: {
	    const char *errStr;

	    if (objc != 3) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    errStr = cdb2_errstr(pCdb2);

	    if (errStr == NULL) {
		Tcl_AppendResult(interp, "there is no error\n", NULL);
		code = TCL_ERROR;
		goto done;
	    }

	    valuePtr = Tcl_NewStringObj(errStr, -1);
	    MAYBE_OUT_OF_MEMORY(valuePtr);

	    Tcl_IncrRefCount(valuePtr);
	    Tcl_SetObjResult(interp, valuePtr);
	    Tcl_DecrRefCount(valuePtr);
	    break;
	}
	case OPT_HINT: {
	    if (objc != 3) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    cdb2_use_hint(pCdb2);
	    Tcl_ResetResult(interp);
	    break;
	}
	case OPT_NEXT: {
	    if (objc != 3) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    rc = cdb2_next_record(pCdb2);

	    switch (rc) {
		case CDB2_OK: {
		    valuePtr = Tcl_NewBooleanObj(1);
		    MAYBE_OUT_OF_MEMORY(valuePtr);
		    break;
		}
		case CDB2_OK_DONE: {
		    valuePtr = Tcl_NewBooleanObj(0);
		    MAYBE_OUT_OF_MEMORY(valuePtr);
		    break;
		}
		default: {
		    AppendCdb2ErrorMessage(interp, rc, pCdb2);
		    code = TCL_ERROR;
		    goto done;
		}
	    }

	    Tcl_IncrRefCount(valuePtr);
	    Tcl_SetObjResult(interp, valuePtr);
	    Tcl_DecrRefCount(valuePtr);
	    break;
	}
	case OPT_OPEN: {
	    cdb2_hndl_tp *pCdb2 = NULL;
	    const char *dbName = NULL;
	    const char *type = "default"; /* TODO: Good default? */
	    int flags = 0;                /* TODO: Good default? */

	    if ((objc < 3) || (objc > 5)) {
		Tcl_WrongNumArgs(interp, 2, objv, "dbName ?type? ?flags?");
		code = TCL_ERROR;
		goto done;
	    }

	    dbName = Tcl_GetString(objv[2]);

	    if (objc >= 4)
		type = Tcl_GetString(objv[3]);

	    if (objc >= 5) {
		code = GetFlagsFromList(interp, objv[4], aOpenFlags, &flags);

		if (code != TCL_OK)
		    goto done;
	    }

	    rc = cdb2_open(&pCdb2, dbName, type, flags);

	    if (rc != CDB2_OK) {
		AppendCdb2ErrorMessage(interp, rc, pCdb2);
		code = TCL_ERROR;
		goto done;
	    }

	    code = AddCdb2HandleByName(interp, pCdb2, buffer);

	    if (code != TCL_OK)
		goto done;

	    Tcl_SetResult(interp, (char *)buffer, TCL_VOLATILE);
	    break;
	}
	case OPT_RUN: {
	    const char *sql;

	    if ((objc != 4) && (objc != 5)) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection sql ?types?");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    sql = Tcl_GetString(objv[3]);

	    if (objc == 5) {
		int index, listObjc;
		Tcl_Obj **listObjv; /* NOTE: Do not free. */

		code = Tcl_ListObjGetElements(interp, objv[4],
		    &listObjc, &listObjv);

		if (code != TCL_OK)
		    goto done;

		types = (int *)attemptckalloc(listObjc * sizeof(int));
		MAYBE_OUT_OF_MEMORY(types);
		memset(types, 0, listObjc * sizeof(int));

		for (index = 0; index < listObjc; index++) {
		    int type;

		    code = GetValueFromName(
			interp, Tcl_GetString(listObjv[index]),
			aColumnTypes, &type);

		    if (code != TCL_OK)
			goto done;

		    types[index] = type;
		}

		rc = cdb2_run_statement_typed(pCdb2, sql, listObjc, types);
	    } else {
		rc = cdb2_run_statement(pCdb2, sql);
	    }

	    if (rc != CDB2_OK) {
		AppendCdb2ErrorMessage(interp, rc, pCdb2);
		code = TCL_ERROR;
		goto done;
	    }

	    Tcl_ResetResult(interp);
	    break;
	}
	case OPT_SOCKPOOL: {
	    int enable = 0;

	    if (objc != 3) {
		Tcl_WrongNumArgs(interp, 2, objv, "enable");
		code = TCL_ERROR;
		goto done;
	    }

	    code = Tcl_GetBooleanFromObj(interp, objv[2], &enable);

	    if (code != TCL_OK)
		goto done;

	    if (enable) {
		cdb2_enable_sockpool();
	    } else {
		cdb2_disable_sockpool();
	    }

	    Tcl_ResetResult(interp);
	    break;
	}
	case OPT_SSL: {
	    int initSsl = 0, initCrypto = 0;

	    if (objc != 4) {
		Tcl_WrongNumArgs(interp, 2, objv, "initSsl initCrypto");
		code = TCL_ERROR;
		goto done;
	    }

	    code = Tcl_GetIntFromObj(interp, objv[2], &initSsl);

	    if (code != TCL_OK)
		goto done;

	    code = Tcl_GetIntFromObj(interp, objv[3], &initCrypto);

	    if (code != TCL_OK)
		goto done;

	    cdb2_init_ssl(initSsl, initCrypto);
	    Tcl_ResetResult(interp);
	    break;
	}
	case OPT_UNBIND: {
	    if (objc != 3) {
		Tcl_WrongNumArgs(interp, 2, objv, "connection");
		code = TCL_ERROR;
		goto done;
	    }

	    GET_CDB2_HANDLE_BY_NAME_OR_FAIL(objv[2]);
	    rc = cdb2_clearbindings(pCdb2);

	    if (rc != CDB2_OK) {
		AppendCdb2ErrorMessage(interp, rc, pCdb2);
		code = TCL_ERROR;
		goto done;
	    }

	    FreeParameterValues(interp);
	    Tcl_ResetResult(interp);
	    break;
	}
	default: {
	    Tcl_AppendResult(interp, "bad option index\n", NULL);
	    code = TCL_ERROR;
	    goto done;
	}
    }

done:
    if (code != TCL_OK) {
	if (hPtr != NULL) {
	    Tcl_DeleteHashEntry(hPtr);
	    hPtr = NULL;
	}

	if (pBoundValue != NULL) {
	    FreeBoundValue(pBoundValue);
	    pBoundValue = NULL;
	}

	if (valuePtr != NULL) {
	    Tcl_DecrRefCount(valuePtr);
	    valuePtr = NULL;
	}

	if (listPtr != NULL) {
	    Tcl_DecrRefCount(listPtr);
	    listPtr = NULL;
	}
    }

    if (types != NULL) {
	ckfree((char *) types);
	types = NULL;
    }

    Tcl_MutexUnlock(&packageMutex);
    return code;
}

/*
 *----------------------------------------------------------------------
 *
 * tclcdb2ObjCmdDeleteProc --
 *
 *	Handles deletion of the command(s) added by this package.
 *	This will cause the saved package data associated with the
 *	Tcl interpreter to be deleted, if it has not been already.
 *
 * Results:
 *	None.
 *
 * Side effects:
 *	None.
 *
 *----------------------------------------------------------------------
 */

static void tclcdb2ObjCmdDeleteProc(
    ClientData clientData)	/* Current Tcl interpreter. */
{
    Tcl_Interp *interp = (Tcl_Interp *) clientData;

    if (!IsValidInterp(interp)) {
	fprintf(stderr, "tclcdb2ObjCmdDeleteProc: bad Tcl interpreter\n");
	return;
    }

    if (Tclcdb2_Unload(interp, TCL_UNLOAD_FROM_CMD_DELETE |
	    TCL_UNLOAD_DETACH_FROM_INTERPRETER) != TCL_OK) {
	fprintf(stderr,
	    "Tclcdb2_Unload: failed via tclcdb2ObjCmdDeleteProc\n");
    }
}
