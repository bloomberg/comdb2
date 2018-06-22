/* Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */
package com.bloomberg.comdb2.jdbc;

/**
 * Constants.
 * 
 * @author Rivers Zhang
 * @author Mohit Khullar
 */
public class Constants {

    public class RequestType {
        public static final int CDB2_REQUEST_CDB2QUERY = 1;
        public static final int CDB2_REQUEST_SQLQUERY = 2;
        public static final int CDB2_REQUEST_DBINFO = 3;
    }

    public class Types {
        public static final int CDB2_INTEGER = 1;
        public static final int CDB2_REAL = 2;
        public static final int CDB2_CSTRING = 3;
        public static final int CDB2_BLOB = 4;
        public static final int CDB2_DATETIME = 6;
        public static final int CDB2_INTERVALYM = 7;
        public static final int CDB2_INTERVALDS = 8;
        public static final int CDB2_DATETIMEUS = 9;
        public static final int CDB2_INTERVALDSUS = 10;
    }

    public class Errors {
        public static final int CDB2_OK = 0;
        public static final int CDB2_OK_DONE = 1;
        public static final int CDB2ERR_CONNECT_ERROR = -1;
        public static final int CDB2ERR_NOTCONNECTED = -2;
        public static final int CDB2ERR_PREPARE_ERROR = -3;
        public static final int CDB2ERR_FSQLPREPARE_ERROR = 1003;
        public static final int CDB2ERR_IO_ERROR = -4;
        public static final int CDB2ERR_INTERNAL = -5;
        public static final int CDB2ERR_NOSTATEMENT = -6;
        public static final int CDB2ERR_BADCOLUMN = -7;
        public static final int CDB2ERR_BADSTATE = -8;
        public static final int CDB2ERR_ASYNCERR = -9;
        public static final int CDB2_OK_ASYNC = -10;

        /* dbfe errors */
        public static final int CDB2ERR_NO_SUCH_DB = -11;
        public static final int CDB2ERR_INVALID_ID = -12;
        public static final int CDB2ERR_RECORD_OUT_OF_RANGE = -13;
        public static final int CDB2ERR_FASTSEED = -14;
        public static final int CDB2ERR_REJECTED = -15;
        public static final int CDB2ERR_STOPPED = -16;
        public static final int CDB2ERR_BADREQ = -17;
        public static final int CDB2ERR_DBCREATE_FAILED = -18;
        public static final int CDB2ERR_DBOP_FAILED = -19;

        /* some error in threadpool code */
        public static final int CDB2ERR_THREADPOOL_INTERNAL = -20;
        /* Trying a write query through read only proxy. */
        public static final int CDB2ERR_READONLY = -21;

        public static final int CDB2ERR_NOMASTER = -101;
        public static final int CDB2ERR_NOTSERIAL = 230;
        public static final int CDB2ERR_CHANGENODE = 402;
        public static final int CDB2ERR_UNTAGGED_DATABASE = -102;
        public static final int CDB2ERR_CONSTRAINTS = -103;

        /* was earlier -104 */
        public static final int CDB2ERR_DEADLOCK = 203;

        /* socksql/readcommited error */
        public static final int CDB2ERR_TRAN_IO_ERROR = -105;
        public static final int CDB2ERR_ACCESS = -106;

        /* linux */
        public static final int CDB2ERR_TRAN_MODE_UNSUPPORTED = -107;

        public static final int CDB2ERR_SCHEMA = -110;

        public static final int CDB2ERR_VERIFY_ERROR = 2;
        public static final int CDB2ERR_FKEY_VIOLATION = 3;
        public static final int CDB2ERR_NULL_CONSTRAINT = 4;

        public static final int CDB2ERR_CONV_FAIL = 113;
        public static final int CDB2ERR_NONKLESS = 114;
        public static final int CDB2ERR_MALLOC = 115;
        public static final int CDB2ERR_NOTSUPPORTED = 116;

        public static final int CDB2ERR_DUPLICATE = 299;
        public static final int CDB2ERR_TZNAME_FAIL = 401;

        public static final int CDB2ERR_UNKNOWN = 300;
    }

    public enum SSL_MODE {
          ALLOW
        , REQUIRE
        , VERIFY_CA
        , VERIFY_HOSTNAME
        , VERIFY_DBNAME
    }

    public enum PEER_SSL_MODE {
          PEER_SSL_UNSUPPORTED
        , PEER_SSL_ALLOW
        , PEER_SSL_REQUIRE
    }
}
/* vim: set sw=4 ts=4 et: */
