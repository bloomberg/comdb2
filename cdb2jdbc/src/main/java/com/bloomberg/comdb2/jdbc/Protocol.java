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

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

public interface Protocol extends Serializable {
    int pack(Cdb2Query query);

    byte[] write(OutputStream ops) throws IOException;

    Cdb2DbInfoResponse unpackDbInfoResp(byte[] bytes) throws IOException;

    Cdb2SqlResponse unpackSqlResp(byte[] bytes) throws IOException;
    
}
/* vim: set sw=4 ts=4 et: */
