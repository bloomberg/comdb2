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
 * AbstractConnection.
 * 
 * @author Rivers Zhang
 * @author Mohit Khullar
 */
public abstract class AbstractConnection implements DbHandle {

    protected Protocol protocol;
    protected IO io;

    public AbstractConnection(Protocol protocol, IO io) {
        this.protocol = protocol;
        this.io = io;
    }

    public AbstractConnection() {
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public IO getIo() {
        return io;
    }

    public void setIo(IO io) {
        this.io = io;
    }

}
/* vim: set sw=4 ts=4 et: */
