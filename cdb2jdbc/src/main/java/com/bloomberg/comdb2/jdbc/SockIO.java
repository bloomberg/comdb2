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
import java.net.*;

import java.util.logging.Level;
import java.util.logging.Logger;

public class SockIO implements IO {
    private static Logger logger = Logger.getLogger(SockIO.class.getName());

    protected Socket sock;
    protected BufferedOutputStream out;
    protected BufferedInputStream in;

    protected String host;
    protected int port;
    protected int tcpbufsz;
    protected String pmuxrtedb = null;

    protected boolean opened = false;

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
            in = null;
        }
        if (out != null) {
            out.close();
            out = null;
        }
        if (sock != null) {
            sock.close();
            sock = null;
        }
        opened = false;
    }

    public SockIO(String host, int port, int tcpbufsz, String pmuxrtedb) {
        this.host = host;
        this.port = port;
        this.tcpbufsz = tcpbufsz;
        this.pmuxrtedb = pmuxrtedb;
    }

    public SockIO(String host, int port) {
        this(host, port, 0, null);
    }

    public SockIO(String host, int port, String pmuxrtedb) {
        this(host, port, 0, pmuxrtedb);
    }

    public SockIO() {
    }

    public void write(byte[] bytes) throws IOException {
        if (!opened)
            opened = open();
        if (!opened)
            throw new IOException();

        out.write(bytes);
    }

    public void write(String msg) throws IOException {
        write(msg.getBytes());
    }

    public void flush() throws IOException {
        if (!opened)
            opened = open();
        if (!opened)
            throw new IOException();
        out.flush();
    }

    public int read(byte[] res) throws IOException {
        if (!opened)
            opened = open();
        if (!opened)
            throw new IOException();

        /* validate number of bytes read. */
        int total_read, read, res_len;
        for(total_read = 0, read = 0, res_len = res.length;
                (read = in.read(res, total_read, res_len - total_read)) != -1 && read != 0 &&
                (total_read  = (total_read + read)) < res_len; ); // when we hit -1(EOF) or have enough bytes read

        return total_read;
    }

    public String readLine(int max) throws IOException {
        if (!opened)
            opened = open();
        if (!opened)
            throw new IOException();

        byte [] b = new byte[max];
        int i, nr;
        for (i = 0; i < b.length; ++i) {
            nr = in.read(b, i, 1);
            if (nr < 0 || b[i] == '\n') /* EOF or \n */
                return new String(b, 0, i);
        }
        throw new IOException("Unexpectedly long line.");
    }

    @Override
    public boolean open() {
        try {
            close();
            sock = new Socket();

            sock.setSoLinger(true, 0); /* no lingering */
            sock.setSoTimeout(60000); /* timeout 60 seconds */
            sock.setTcpNoDelay(true); /* no nagle */
            if (tcpbufsz > 0)
                sock.setReceiveBufferSize(tcpbufsz);

            sock.bind(new InetSocketAddress(0));
            sock.connect(new InetSocketAddress(host, port), 10000);
            out = new BufferedOutputStream(sock.getOutputStream());
            in = new BufferedInputStream(sock.getInputStream());
            opened = true;

            if (pmuxrtedb != null) {
                String rtepacket = String.format("rte %s/%s/%s\n", "comdb2", "replication", pmuxrtedb);
                /* Errors are handled by the catch block below. */
                out.write(rtepacket.getBytes());
                out.flush();
                readLine(32);
            }
        } catch (IOException e) {
            opened = false;
            logger.log(Level.FINE, "Unable to open socket connection to " + host + ":" + port, e);
            try {
                close();
            } catch (IOException e1) {
                logger.log(Level.FINE, "Unable to close socket connection to " + host + ":" + port, e);
            }
        }
        return opened;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public OutputStream getOut() {
        return out;
    }

    public InputStream getIn() {
        return in;
    }
}
/* vim: set sw=4 ts=4 et: */
