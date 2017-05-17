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
 * Cdb2SqlResponse is an abstraction of the Query to be sent. How a Cdb2SqlResponse instance
 * is serialized, depends on the underlying protocol. However, different
 * implementations shall always return the same abstraction, which is a
 * Cdb2SqlResponse instance, to upper-level application level.
 * 
 * @author Rivers Zhang
 * @author Mohit Khullar
 */
public class Cdb2SqlResponse implements Serializable {

    private static final long serialVersionUID = 702572573395132304L;

    static class Column {
        int type;
        byte[] value;

        public Column(int type, byte[] value) {
            this.type = type;
            this.value = value;
        }
    }

    static class Effects {
        int numAffected;
        int numSelected;
        int numUpdated;
        int numDeleted;
        int numInserted;

        public Effects(int numAffected, int numSelected, int numUpdated,
                int numDeleted, int numInserted) {
            super();
            this.numAffected = numAffected;
            this.numSelected = numSelected;
            this.numUpdated = numUpdated;
            this.numDeleted = numDeleted;
            this.numInserted = numInserted;
        }

    }

    List<Column> value;
    Cdb2DbInfoResponse dbInfoResp;
    int errCode;
    String errStr;

    int respType;
    Effects effects;

    boolean hasSnapshotInfo = false;
    int file;
    int offset;

    boolean hasRowId = false;
    long rowId;

    List<Integer> features;
}
/* vim: set sw=4 ts=4 et: */
