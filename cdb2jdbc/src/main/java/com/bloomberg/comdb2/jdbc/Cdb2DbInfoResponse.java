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
import java.util.Arrays;
import java.util.List;

/**
 * Cdb2DbInfoResponse
 * 
 * @author Rivers Zhang
 * @author Mohit Khullar
 */
public class Cdb2DbInfoResponse implements Serializable{
    private static final long serialVersionUID = -3986582382656884287L;

    static class NodeInfo {
        String name;
        int number;
        int incoherent;
        int room;
        int port;

        NodeInfo(String name, int number, int incoherent, int room,
                int port) {
            super();
            this.name = name;
            this.number = number;
            this.incoherent = incoherent;
            this.room = room;
            this.port = port;
        }

        @Override public String toString() {
            return String.format("name = %s,  number = %d, incoherent = %d, room = %d, port = %d", name, number, incoherent, room, port);
        }
    }

    NodeInfo master;
    List<NodeInfo> nodes;
    Constants.PEER_SSL_MODE peermode;

    @Override public String toString() {
        return master.toString() + "\n" + Arrays.toString(nodes.toArray(new NodeInfo[]{}));
    }
}
/* vim: set sw=4 ts=4 et: */
