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

import java.nio.*;
import java.util.*;
import java.io.*;

class Comdb2LocalHandle extends AbstractConnection {

    private int ncols;
    private String[] names;
    private int[] types;
    private LinkedList<byte[][]> rows = new LinkedList<byte[][]>();
    private byte[][] row;
    private int rowii;

    Comdb2LocalHandle(int n, String[] nms, int[] tps) {
        super(new ProtobufProtocol(), null);
        ncols = n;
        names = nms;
        types = tps;
    }

    /* VVVVVVVVVVVVVV  DO NOTHING  VVVVVVVVVVVV */

    public boolean open() {
        return false;
    }

    public void lookup() throws NoDbHostFoundException {
        /* No lookup should ever be performed on a fake handle. */
    }

    public void clearParameters() {
    }

    public void bindParameter(String name, int type, byte[] data) {
    }

    public void bindParameters(Map<String, Cdb2Query.Cdb2BindValue> bindVars) {
    }

    public int runStatement(String sql) {
        return 1;
    }

    public void runStatementWithException(String sql) {
    }

    public int runStatement(String sql, List<Integer> types) {
        return 1;
    }

    public void runStatementWithException(String sql, List<Integer> types) {
    }

    public int rowsAffected() {
        return 0;
    }

    public int rowsUpdated() {
        return 0;
    }

    public int rowsInserted() {
        return 0;
    }

    public int rowsDeleted() {
        return 0;
    }

    public String errorString() {
        return null;
    }

    public Throwable getLastThrowable() {
        return null;
    }
    /* ^^^^^^^^^^^^^  DO NOTHING  ^^^^^^^^^^^^ */

    /* VVVVVVVVVVVVV  DO SOMETHING  VVVVVVVVVVVV */

    public void close() throws IOException {
        clearResult();
    }

    public boolean nextWithException() {
        return (next() == 0);
    }

    public void clearResult() {
        /* GC?? */
        names = null;
        types = null;
        rows.clear();
    }

    public int next() {
        rows.poll();
        return (rows.peek() == null) ? 1 : 0;
    }

    public int numColumns() {
        return ncols;
    }

    public String columnName(int column) {
        return names[column];
    }

    public int columnType(int column) {
        return types[column];
    }

    public byte[] columnValue(int column) {
        return (rows.peek())[column];
    }

    public int rowsSelected() {
        return rows.size();
    }

    public void beginRow() {
        if (rows.size() == 0)
            rows.add(new byte[0][0]);
        row = new byte[ncols][];
        rowii = 0;
    }

    public void endRow() {
        rows.add(row);
    }

    public void addInteger(Long i) {
        if (i == null)
            row[rowii++] = null;
        else {
            byte[] arr = ByteBuffer.allocate(8).putLong(i).array();
            row[rowii++] = arr;
        }
    }

    public void addReal(Double d) {
        if (d == null)
            row[rowii++] = null;
        else {
            byte[] arr = ByteBuffer.allocate(8).putDouble(d).array();
            row[rowii++] = arr;
        }
    }

    public void addString(String s) {
        if (s == null)
            row[rowii++] = null;
        else {
            // expect nul-terminated string
            byte[] b = s.getBytes();
            byte[] nul = new byte[b.length + 1];
            System.arraycopy(b, 0, nul, 0, b.length);
            row[rowii++] = nul;
        }
    }

    public void addBytes(byte[] b) {
        row[rowii++] = b;
    }

    /* ^^^^^^^^^^^^^  DO SOMETHING  ^^^^^^^^^^^^ */
}
/* vim: set sw=4 ts=4 et: */
