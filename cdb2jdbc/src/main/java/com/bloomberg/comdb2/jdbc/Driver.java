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

import java.sql.*;
import java.util.*;
import java.util.logging.*;
import java.lang.reflect.*;

/**
 * @author Rivers Zhang
 */
public class Driver implements java.sql.Driver {
    private static Logger logger = Logger.getLogger(Driver.class.getName());
    public static final String PREFIX = "jdbc:comdb2:";
    protected HashMap<String, Option> options = new HashMap<String, Option>();
    protected static Driver __instance = new Driver();

    protected static abstract class Option {
        Method m;
        String opt;
        abstract void set(Comdb2Connection conn, String val) throws Throwable;
        public void process(Comdb2Connection conn, String val, Properties info) throws Throwable {
            if (val == null)
                val = info.getProperty(opt);
            if (val != null) {
                logger.log(Level.FINE, String.format("Setting %s to %s\n", opt, val));
                set(conn, val);
            }
        }
    }

    protected static class StringOption extends Option {
        void set(Comdb2Connection conn, String val) throws Throwable {
            m.invoke(conn, val);
        }
        StringOption(String opt, String setter) throws Throwable {
            Class<Comdb2Connection> cls = Comdb2Connection.class;
            m = cls.getMethod("set" + setter, String.class);
            this.opt = opt;
        }
    }

    protected static class IntegerOption extends Option {
        void set(Comdb2Connection conn, String val) throws Throwable {
            int intval = Integer.parseInt(val);
            m.invoke(conn, intval);
        }
        IntegerOption(String opt, String setter) throws Throwable {
            Class<Comdb2Connection> cls = Comdb2Connection.class;
            m = cls.getMethod("set" + setter, Integer.TYPE);
            this.opt = opt;
        }
    }

    protected static class BooleanOption extends Option {
        void set(Comdb2Connection conn, String val) throws Throwable {
            int intval = Integer.parseInt(val);
            if ("true".equalsIgnoreCase(val)
                    || "1".equalsIgnoreCase(val)
                    || "T".equalsIgnoreCase(val)
                    || "on".equalsIgnoreCase(val))
                m.invoke(conn, true);
            else
                m.invoke(conn, false);
        }
        BooleanOption(String opt, String setter) throws Throwable {
            Class<Comdb2Connection> cls = Comdb2Connection.class;
            m = cls.getMethod("set" + setter, Boolean.TYPE);
            this.opt = opt;
        }
    }

    public static Driver getInstance() throws SQLException {
        try {
            __instance.options.put("maxquerytime", new IntegerOption("maxquerytime", "QueryTimeout"));
            __instance.options.put("timeout", new IntegerOption("timeout", "Timeout"));
            __instance.options.put("sotimeout", new IntegerOption("sotimeout", "SoTimeout"));
            __instance.options.put("connect_timeout", new IntegerOption("connect_timeout", "ConnectTimeout"));
            __instance.options.put("comdb2db_timeout", new IntegerOption("comdb2db_timeout", "Comdb2dbTimeout"));
            __instance.options.put("dbinfo_timeout", new IntegerOption("dbinfo_timeout", "DbinfoTimeout"));
            __instance.options.put("user", new StringOption("user", "User"));
            __instance.options.put("password", new StringOption("password", "Password"));
            __instance.options.put("default_type", new StringOption("default_type", "DefaultType"));
            __instance.options.put("room", new StringOption("room", "MachineRoom"));
            __instance.options.put("portmuxport", new IntegerOption("portmuxport", "PortMuxPort"));
            __instance.options.put("comdb2dbname", new StringOption("comdb2dbname", "Comdb2dbName"));
            __instance.options.put("tcpbufsz", new IntegerOption("tcpbufsz", "TcpBufSize"));
            __instance.options.put("dnssuffix", new StringOption("dnssuffix", "DnsSuffix"));
            __instance.options.put("load_balance", new StringOption("load_balance", "Policy"));
            __instance.options.put("microsecond_fraction", new StringOption("microsecond_fraction", "MicroSecond"));
            __instance.options.put("preferred_machine", new StringOption("preferred_machine", "PrefMach"));
            __instance.options.put("comdb2db_max_age", new IntegerOption("comdb2db_max_age", "Comdb2dbMaxAge"));
            __instance.options.put("debug", new BooleanOption("debug", "Debug"));
            __instance.options.put("max_retries", new IntegerOption("max_retries", "MaxRetries"));
            __instance.options.put("ssl_mode", new StringOption("ssl_mode", "SSLMode"));
            __instance.options.put("key_store", new StringOption("key_store", "SSLCrt"));
            __instance.options.put("key_store_password", new StringOption("key_store_password", "SSLCrtPass"));
            __instance.options.put("key_store_type", new StringOption("key_store_type", "SSLCrtType"));
            __instance.options.put("trust_store", new StringOption("trust_store", "SSLCA"));
            __instance.options.put("trust_store_password", new StringOption("trust_store_password", "SSLCAPass"));
            __instance.options.put("trust_store_type", new StringOption("trust_store_type", "SSLCAType"));
            __instance.options.put("crl", new StringOption("crl", "SSLCRL"));
            __instance.options.put("allow_pmux_route", new BooleanOption("allow_pmux_route", "AllowPmuxRoute"));
            __instance.options.put("statement_query_effects",
                    new BooleanOption("statement_query_effects", "StatementQueryEffects"));
            __instance.options.put("verify_retry", new BooleanOption("verify_retry", "VerifyRetry"));
            __instance.options.put("stack_at_open", new BooleanOption("stack_at_open", "StackAtOpen"));
        } catch (Throwable e) {
            throw new SQLException(e);
        }
        return __instance;
    }

    /**
     * Register our driver statically.
     */
    static {
        try {
            DriverManager.registerDriver(Driver.getInstance());
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Unable to register comdb2 driver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url))
            return null;

        /**
         * The format of connection string is `jdbc:comdb2:<dbname>:<cluster>'.
         */

        String[] tokens = url.split("\\/\\s*|\\s+");

        Comdb2Connection ret = null;
        String clusterStr = null;
        String dbStr = null;
        String port = null;
        String attributes = null;

        ArrayList<String> hosts = new ArrayList<String>();
        ArrayList<Integer> ports = new ArrayList<Integer>();

        if (tokens.length < 4) { /* Use legacy format */
            tokens = url.split(":\\s*|\\s+");
            if (tokens.length != 4)
                return null;

            dbStr = tokens[2];
            clusterStr = tokens[3];
        } else {
            /* hosttokens cluster[:port]+ */
            String[] hoststokens = tokens[2].split(",\\s*|\\s+");
            if (hoststokens.length > 1) {
                /* more than 1 hosts. */
                clusterStr = "User-supplied-hosts";
                for (String elem : hoststokens) {
                    String[] hosttokens = elem.split(":\\s*|\\s+");
                    if (hosttokens.length == 1) {
                        /* no port, use -1 as a placeholder */
                        ports.add(-1);
                    } else {
                        try {
                            ports.add(Integer.parseInt(hosttokens[1]));
                        } catch (NumberFormatException e1) {
                            /* invalid input, fall back to -1 */
                            ports.add(-1);
                        }
                    }
                    hosts.add(hosttokens[0]);
                }
            } else {
                /* only 1 host. */
                String[] hosttokens = tokens[2].split(":\\s*|\\s+");
                clusterStr = hosttokens[0];
                /* read url string */
                if (hosttokens.length > 1)
                    port = hosttokens[1];
            }

            /* dbtokens db[?key=value...] */
            String[] dbtokens = tokens[3].split("\\?\\s*|\\s+");
            dbStr = dbtokens[0];
            int indx = url.indexOf('?');
            if (indx > 0 && indx < url.length() - 1)
                attributes = url.substring(indx + 1);
        }

        /* Don't look up. Just set attributes.
           We will look up the database before returning. */
        ret = new Comdb2Connection();
        ret.setDatabase(dbStr);
        ret.setCluster(clusterStr);

        /* add user supplied hosts, if any */
        ret.addHosts(hosts);
        ret.addPorts(ports);

        try {
            /* The custom port isn't a URL parameter but part of the host string.
               Handle it separately. */
            if (port != null) {
                int pval = Integer.parseInt(port);
                logger.log(Level.FINE, String.format("Setting port to %d\n", pval));
                ret.setOverriddenPort(pval);
            }

            /* process attributes */
            if (attributes != null) {
                String[] keyvals = attributes.split("&");
                for (String elem : keyvals) {
                    String[] keyval = elem.split("=");
                    if (keyval.length != 2)
                        continue;

                    Option opt = options.get(keyval[0]);
                    if (opt != null) {
                        opt.process(ret, keyval[1], info);
                    }
                }
            }
        } catch (Throwable t) {
            ret.close();
            throw new SQLException(t);
        }

        try {
            ret.lookup();
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Unable to establish cdb2api connection to " + url, e);
            ret.close();
            throw e;
        }
        return ret;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.toLowerCase().startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[] {};
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

}
/* vim: set sw=4 ts=4 et: */
