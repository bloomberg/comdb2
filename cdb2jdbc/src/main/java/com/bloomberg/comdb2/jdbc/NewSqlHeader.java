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

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Translated from struct `nwesqlheader' in Mohit's API.
 * 
 * @author Rivers Zhang
 * 
 */
public class NewSqlHeader implements Serializable, ByteArray {
    private static final long serialVersionUID = -4061504948982403787L;

    public static final int BYTES_NEEDED = 16;

    int type;
    int compression;
    int dummy;
    int length;

    public NewSqlHeader(int type, int compression, int dummy, int length) {
        super();
        this.type = type;
        this.compression = compression;
        this.dummy = dummy;
        this.length = length;
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer bb = ByteBuffer.allocate(BYTES_NEEDED);
        return bb.putInt(type).putInt(compression).putInt(dummy).putInt(length)
                .array();
    }

    @Override
    public int capacity() {
        return BYTES_NEEDED;
    }

    @Override
    public boolean reconstructFromBytes(byte[] bytes) {
        if (bytes.length < BYTES_NEEDED)
            return false;

        ByteBuffer bb = ByteBuffer.wrap(bytes);
        type = bb.getInt();
        compression = bb.getInt();
        dummy = bb.getInt();
        length = bb.getInt();
        return true;
    }

    public static NewSqlHeader fromBytes(byte[] bytes) {
        if (bytes.length < BYTES_NEEDED)
            return null;

        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return new NewSqlHeader(bb.getInt(), bb.getInt(), bb.getInt(),
                bb.getInt());
    }

    @Override
    public String toString() {
        return String.format("type=%d, compression=%d, dummy=%d, length=%d",
                type, compression, dummy, length);
    }

    @Override
    public boolean toBool() {
        throw new UnsupportedConversionException("Internal C struct", "boolean");
    }

    @Override
    public long toLong() {
        throw new UnsupportedConversionException("Internal C struct", "long");
    }

    @Override
    public double toDouble() {
        throw new UnsupportedConversionException("Internal C struct", "double");
    }
}
/* vim: set sw=4 ts=4 et: */
