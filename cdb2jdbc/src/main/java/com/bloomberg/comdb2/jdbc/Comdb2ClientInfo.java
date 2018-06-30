/* Copyright 2018 Bloomberg Finance L.P.

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

public class Comdb2ClientInfo {
    public static String getCallerClass() {
        String pkg = Comdb2ClientInfo.class.getPackage().getName() + ".";
        StackTraceElement[] stes = Thread.currentThread().getStackTrace();
        for (int i = 1, len = stes.length; i < len; ++i) {
            StackTraceElement ste = stes[i];
            if (!ste.getClassName().startsWith(pkg))
                return ste.getClassName();
        }
        return "Unknown Java class";
    }

    public static String getCallStack(int layers) {
        String pkg = Comdb2ClientInfo.class.getPackage().getName() + ".";
        StringBuilder sb = new StringBuilder("");
        StackTraceElement[] stes = Thread.currentThread().getStackTrace();
        for (int i = 1, len = stes.length; i < len && i < layers ; ++i) {
            StackTraceElement ste = stes[i];
            if (!ste.getClassName().startsWith(pkg)) {
                sb.append(ste.toString());
                if (i < len - 1)
                    sb.append(' ');
            }
        }
        return sb.toString();
    }
}
