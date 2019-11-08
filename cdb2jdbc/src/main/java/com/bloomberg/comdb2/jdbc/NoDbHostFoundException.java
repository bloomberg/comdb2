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
 * NoDbHostFoundException.
 * 
 * @author Rivers Zhang
 */
public class NoDbHostFoundException extends Exception {

    private static final long serialVersionUID = -3336167815219390728L;

    public NoDbHostFoundException(String dbname, Throwable t) {
        super(dbname, t);
    }

    public NoDbHostFoundException(String dbname, String reason, Throwable t) {
        super(dbname + ": " + reason, t);
    }

    public NoDbHostFoundException(String dbname, String reason) {
        this(dbname, reason, null);
    }

    public NoDbHostFoundException(String dbname) {
        super(dbname);
    }

}
/* vim: set sw=4 ts=4 et: */
