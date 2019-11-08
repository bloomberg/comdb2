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

import java.io.*;
import java.util.*;

/**
 * Cdb2Query is an abstraction of the Query to be sent. How a Cdb2Query instance
 * is serialized, depends on the underlying protocol. However, different
 * implementations shall always return the same abstraction, which is a
 * Cdb2Query instance, to upper-level applications.
 * 
 * @author Rivers Zhang
 * @author Mohit Khullar
 */
public class Cdb2Query implements Serializable {

    private static final long serialVersionUID = 5977628400992115303L;

    static class Cdb2Flag {
        int option;
        int value;
    }

    static class Cdb2BindValue {
        String varName;
        int type;
        byte[] value;
    }

    static class Cdb2ReqInfo {
        long timestampus;
        int numretries;
    }

    static class Cdb2ClientInfo {
        int pid; /* Unsupported */
        long tid; /* Unsupported */
        int host_id; /* Unsupported */
        String argv0;
        String stack;
    }

    static class Cdb2SqlQuery {
        String dbName;
        String sqlQuery;
        List<Cdb2Flag> flag;
        boolean littleEndian = false;
        List<Cdb2BindValue> bindVars;
        Cdb2ClientInfo cinfo;
        String tzName;
        List<String> setFlags;
        List<Integer> types;
        String machClass = "unknown";
        byte[] cnonce;
        boolean hasSnapshotInfo = false;
        int file;
        int offset;
        boolean hasSkipRows = false;
        long skipRows;
        boolean hasRetry = false;
        int retry;
        List<Integer> features;
        Cdb2ReqInfo reqInfo;

        public Cdb2SqlQuery(String dbName, String sql) {
            String tz = System.getenv("COMDB2TZ");
            if (tz == null)
                tz = System.getenv("TZ");
            if (tz == null)
                tz = "America/New_York";

            this.tzName = tz;

            this.dbName = dbName;
            this.sqlQuery = sql;

            flag = new ArrayList<Cdb2Flag>();
            bindVars = new ArrayList<Cdb2BindValue>();
            setFlags = new ArrayList<String>();
            types = new ArrayList<Integer>();
            features = new ArrayList<Integer>();
        }

        public Cdb2SqlQuery() {
            this(null, null);
        }
    }

    static class Cdb2DbInfo {
        String dbName;
        boolean littleEndian;

        public Cdb2DbInfo(String dbName) {
            this.dbName = dbName;
            this.littleEndian = false;
        }
    }

    Cdb2DbInfo cdb2DbInfo;
    Cdb2SqlQuery cdb2SqlQuery;
}
/* vim: set sw=4 ts=4 et: */
