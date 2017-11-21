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

#ifndef INCLUDED_COMDB2_RCODES_H
#define INCLUDED_COMDB2_RCODES_H

enum comdb2_operations {
    COMDB2_GEN_OP = 0, /**< generic exceptions */
    COMDB2_FND_OP = 10000,
    COMDB2_ADD_OP = 10100,
    COMDB2_DEL_OP = 10200,
    COMDB2_UPD_OP = 10300,
    COMDB2_RNG_OP = 10400,
    COMDB2_CUST_OP = 10500,  /**< store procedures */
    COMDB2_BLK_OP = 10600,   /**< block operation */
    COMDB2_CSTRT_OP = 10700, /**< constraint work */
    COMDB2_QADD_OP = 10800,
    COMDB2_BLOB_OP = 10900
};

enum comdb2_fnd_rc {
    COMDB2_FND_RC_INVL_KEY = 10001,         /**< invalid key. size?idx? */
    COMDB2_FND_RC_INVL_DTA = 10002,         /**< invalid dta. len? */
    COMDB2_FND_RC_INVL_IDX = 10003,         /**< invalid index number */
    COMDB2_FND_RC_UNKN_TBL = 10004,         /**< invalid table */
    COMDB2_FND_RC_INVL_TAG = 10005,         /**< invalid tag */
    COMDB2_FND_RC_INVL_PARTIAL_STR = 10006, /**< invalid partial string */
    COMDB2_FND_RC_ALLOC = 10007,            /**< error on allocation */
    COMDB2_FND_RC_CNVT_DTA = 10008          /**< error converting data TODO */
};

enum comdb2_add_rc {
    COMDB2_ADD_RC_INVL_KEY = 10101,    /**< invalid key. size?idx? */
    COMDB2_ADD_RC_INVL_DTA = 10102,    /**< invalid dta. len? */
    COMDB2_ADD_RC_INVL_IDX = 10103,    /**< invalid index number */
    COMDB2_ADD_RC_INVL_SCHEMA = 10104, /**< invalid schema */
    COMDB2_ADD_RC_CNVT_DTA = 10105,    /**< error converting data */
    COMDB2_ADD_RC_INVL_BLOB = 10106    /**< invalid blob operation */
};

enum comdb2_del_rc {
    COMDB2_DEL_RC_INVL_KEY = 10201,    /**< invalid key. size?idx? */
    COMDB2_DEL_RC_INVL_DTA = 10202,    /**< invalid dta. len? */
    COMDB2_DEL_RC_INVL_IDX = 10203,    /**< invalid index number */
    COMDB2_DEL_RC_INVL_SCHEMA = 10204, /**< invalid schema */
    COMDB2_DEL_RC_UNKN_RRN = 10205,    /**< unknown rrn */
    COMDB2_DEL_RC_UNKN_REC = 10206,    /**< unkrown record */
    COMDB2_DEL_RC_VFY_CSTRT = 10207    /**< error verifying constraints */
};

enum comdb2_upd_rc {
    COMDB2_UPD_RC_INVL_KEY = 10301,    /**< invalid key. size?idx? */
    COMDB2_UPD_RC_INVL_DTA = 10302,    /**< invalid dta. len? */
    COMDB2_UPD_RC_INVL_IDX = 10303,    /**< invalid index number */
    COMDB2_UPD_RC_INVL_SCHEMA = 10304, /**< invalid schema */
    COMDB2_UPD_RC_CNVT_DTA = 10305,    /**< error converting data */
    COMDB2_UPD_RC_UNKN_REC = 10306,    /**< unknown record */
    COMDB2_UPD_RC_VFY_CNSTRT = 10307,  /**< error verifying constraints */
    COMDB2_UPD_RC_CNVT_VREC = 10308,   /**< error converting vrec */
    COMDB2_UPD_RC_INVL_PK = 10309,     /**< invalid primary key */
    COMDB2_UPD_RC_INVL_VLEN = 10310,
    COMDB2_UPD_RC_INVL_NEWLEN = 10311
};

enum comdb2_rng_rc {
    COMDB2_RNG_RC_INVL_KEY = 10401,    /**< invalid key. size?idx? */
    COMDB2_RNG_RC_INVL_TBL = 10402,    /**< invalid table */
    COMDB2_RNG_RC_INVL_TAG = 10403,    /**< invalid tag */
    COMDB2_RNG_RC_INVL_SCHEMA = 10405, /**< invalid schema */
    COMDB2_RNG_RC_BLOBS = 10404,       /**< use of blobs detected */
    COMDB2_RNG_RC_CNVT_KEY = 10405     /**< error converting key */
};

enum comdb2_cust_rc {
    COMDB2_CUST_RC_NAME_SZ = 10501,       /**< invalid operation name size */
    COMDB2_CUST_RC_BAD_BLOB_BUFF = 10502, /**< invalid blob buffer */
    COMDB2_CUST_RC_NB_BLOBS = 10503,      /**< invalid number of blobs */
    COMDB2_CUST_RC_ENV = 10504            /**< no jvm or empty operation */
};

enum comdb2_blk_rc {
    COMDB2_BLK_RC_NB_REQS = 10601,    /**< invalid number of requests */
    COMDB2_BLK_RC_OFFSET = 10602,     /**< wrong offset to databuf */
    COMDB2_BLK_RC_BUF_SZ = 10603,     /**< wrong buffer size */
    COMDB2_BLK_RC_INVL_DBNUM = 10604, /**< invalid database number */
    COMDB2_BLK_RC_UNKN_TAG = 10605,   /**< unknown tag */
    COMDB2_BLK_RC_UNKN_OP = 10606,    /**< unknown block operation */
    COMDB2_BLK_RC_FAIL_COMMIT = 10607 /**< failed to commit block */
};

enum comdb2_cstrt_rc {
    COMDB2_CSTRT_RC_INVL_KEY = 10701,    /**< invalid key. size?idx? */
    COMDB2_CSTRT_RC_INVL_DTA = 10702,    /**< invalid dta. len? */
    COMDB2_CSTRT_RC_INVL_IDX = 10703,    /**< invalid index number */
    COMDB2_CSTRT_RC_INVL_REC = 10704,    /**< invalid record to check */
    COMDB2_CSTRT_RC_INVL_CURSOR = 10705, /**< invalid internal cursor */
    COMDB2_CSTRT_RC_INVL_RRN = 10706,    /**< invalid rrn to chekc */
    COMDB2_CSTRT_RC_INVL_TBL = 10707,    /**< invalid table */
    COMDB2_CSTRT_RC_ALLOC = 10708,       /**< error in allocation */
    COMDB2_CSTRT_RC_DUP = 10709,         /**< detected duplicate key */
    COMDB2_CSTRT_RC_CASCADE = 10710,     /**< error applying CASCADE */
    COMDB2_CSTRT_RC_INVL_TAG = 10711,    /**< invalid tag detected */
    COMDB2_CSTRT_RC_INTL_ERR = 10712,    /**< internal error from berk */
    COMDB2_CSTRT_RC_TRN_TOO_BIG = 10713, /**< transaction too large */
};

enum comdb2_qadd_rc {
    COMDB2_QADD_RC_NAME_SZ = 10801,       /**< invalid operation name size */
    COMDB2_QADD_RC_BAD_BLOB_BUFF = 10802, /**< invalid blob buffer */
    COMDB2_QADD_RC_NB_BLOBS = 10803,      /**< invalid number of blobs */
    COMDB2_QADD_RC_NO_QUEUE = 10804
};

enum comdb2_blob_op {
    COMDB2_BLOB_RC_ALLOC = 10900,
    COMDB2_BLOB_RC_RCV_TOO_LARGE = 10901,
    COMDB2_BLOB_RC_RCV_TOO_MANY = 10902,
    COMDB2_BLOB_RC_RCV_TOO_MUCH = 10903,
    COMDB2_BLOB_RC_RCV_BAD_LENGTH = 10904
};

enum comdb2_schemachange_op {
    COMDB2_SCHEMACHANGE_OK = 11000,
};

#endif
