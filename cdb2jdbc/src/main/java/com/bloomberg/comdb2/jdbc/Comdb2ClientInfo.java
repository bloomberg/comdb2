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

import java.util.jar.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;

public class Comdb2ClientInfo {
    private static Logger logger = LoggerFactory.getLogger(Comdb2ClientInfo.class);
    
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
    
    public static String getDriverName() {
        String name = Driver.class.getPackage().getImplementationTitle();
        if (name == null) {
            try { 
                JarFile jar = new JarFile(Driver.class.getProtectionDomain().getCodeSource().getLocation().getPath());
                Manifest manifest = new Manifest(jar.getInputStream(jar.getEntry("META-INF/MANIFEST.MF")));
                Attributes attributes = manifest.getMainAttributes();
                name = attributes.getValue("Implementation-Title");
            } catch (IOException e) {
                logger.info("Unable to parse driver class manifest");
            }
        }
        return name == null ? "cdb2jdbc" : name;
    }

    public static String getDriverVersion() {
        String version = Driver.class.getPackage().getImplementationVersion();
        if (version == null) {
            try { 
                JarFile jar = new JarFile(Driver.class.getProtectionDomain().getCodeSource().getLocation().getPath());
                Manifest manifest = new Manifest(jar.getInputStream(jar.getEntry("META-INF/MANIFEST.MF")));
                Attributes attributes = manifest.getMainAttributes();
                version = attributes.getValue("Implementation-Version");
            } catch (IOException e) {
                logger.info("Unable to parse driver class manifest");
            }
        }
        return version == null ? "1.0" : version;
    }

    public static int getDriverMajorVersion() {
        try {
            String version = Comdb2ClientInfo.getDriverVersion();
            String major = version.split("\\.")[0];
            return Integer.parseInt(major);
        } catch (Exception e) {
            return 1;
        }
    }

    public static int getDriverMinorVersion() {
        try {
            String version = Comdb2ClientInfo.getDriverVersion();
            String minor = version.split("\\.")[1];
            return Integer.parseInt(minor);
        } catch (Exception e) {
            return 0;
        }
    }
}
