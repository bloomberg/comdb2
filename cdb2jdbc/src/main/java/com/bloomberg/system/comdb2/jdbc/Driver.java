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
package com.bloomberg.system.comdb2.jdbc;

import java.sql.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Legacy driver class.
 */
public class Driver implements java.sql.Driver {
    private static Logger logger = LoggerFactory.getLogger(Driver.class);
    private com.bloomberg.comdb2.jdbc.Driver drv;

    public Driver() throws SQLException {
        drv = new com.bloomberg.comdb2.jdbc.Driver();
    }

    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            logger.error("Unable to register legacy comdb2 driver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return drv.connect(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return drv.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return drv.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return drv.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return drv.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return drv.jdbcCompliant();
    }

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return drv.getParentLogger();
    }
}
/* vim: set sw=4 ts=4 et: */
