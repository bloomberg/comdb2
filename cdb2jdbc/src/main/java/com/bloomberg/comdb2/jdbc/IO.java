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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * IO provides a universal interface to upper-layer applications. An IO,
 * categorized by its type, can be file-based or network-based. It can be
 * channel-based or port-based, depending on how resources are assigned to
 * requesters. In conclusion, the behaviors of an IO instance, depends on the
 * underlying implementation. However, different implementation always present
 * the identical interface to applications.
 * 
 * @author Rivers Zhang
 */
public interface IO extends Closeable {
    /**
     * Open the "device".
     * 
     * @return true if success.
     */
    boolean open();

    /**
     * Write bytes.
     * 
     * @param bytes
     * @throws IOException
     */
    void write(byte[] bytes) throws IOException;

    /**
     * Write a java string.
     * 
     * @param msg
     * @throws IOException
     */
    void write(String msg) throws IOException;

    /**
     * Flush.
     * 
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * Read output into @bytes.
     * 
     * @param bytes
     * @return number of bytes read.
     * @throws IOException
     */
    int read(byte[] bytes) throws IOException;

    /**
     * Read till '\n'
     */
    String readLine(int max) throws IOException;

    /**
     * Return the {@link OutputStream} object.
     */
    OutputStream getOut();

    /**
     * Return the {@link InputStream} object.
     */
    InputStream getIn();
}
/* vim: set sw=4 ts=4 et: */
