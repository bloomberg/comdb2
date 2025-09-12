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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.*;
import java.io.*;

/**
 * @author Rivers Zhang
 */
public class Driver implements java.sql.Driver {
    private static Logger logger = LoggerFactory.getLogger(Driver.class);
    public static final String PREFIX = "jdbc:comdb2:";
    protected HashMap<String, Option> options = new HashMap<String, Option>();
    private static HealthChecker healthChecker;
    private static IdentityCreatorFactory identityCreatorFactory;

    public Driver() throws SQLException {
        populateOptions();
    }

    protected static abstract class Option {
        Method m;
        String opt;
        abstract void set(Comdb2Connection conn, String val) throws Throwable;
        public void process(Comdb2Connection conn, String val, Properties info) throws Throwable {
            if (val == null)
                val = info.getProperty(opt);
            if (val != null) {
                logger.debug(String.format("Setting %s to %s\n", opt, val));
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

    protected void populateOptions() throws SQLException {
        try {
            options.put("allow_pmux_route", new BooleanOption("allow_pmux_route", "AllowPmuxRoute"));
            options.put("autocommit", new BooleanOption("autocommit", "AutoCommit"));
            options.put("maxquerytime", new IntegerOption("maxquerytime", "QueryTimeout"));
            options.put("timeout", new IntegerOption("timeout", "Timeout"));
            options.put("sotimeout", new IntegerOption("sotimeout", "SoTimeout"));
            options.put("connect_timeout", new IntegerOption("connect_timeout", "ConnectTimeout"));
            options.put("comdb2db_timeout", new IntegerOption("comdb2db_timeout", "Comdb2dbTimeout"));
            options.put("dbinfo_timeout", new IntegerOption("dbinfo_timeout", "DbinfoTimeout"));
            options.put("user", new StringOption("user", "User"));
            options.put("password", new StringOption("password", "Password"));
            options.put("default_type", new StringOption("default_type", "DefaultType"));
            options.put("room", new StringOption("room", "MachineRoom"));
            options.put("portmuxport", new IntegerOption("portmuxport", "PortMuxPort"));
            options.put("comdb2dbname", new StringOption("comdb2dbname", "Comdb2dbName"));
            options.put("tcpbufsz", new IntegerOption("tcpbufsz", "TcpBufSize"));
            options.put("dnssuffix", new StringOption("dnssuffix", "DnsSuffix"));
            options.put("load_balance", new StringOption("load_balance", "Policy"));
            options.put("microsecond_fraction", new StringOption("microsecond_fraction", "MicroSecond"));
            options.put("preferred_machine", new StringOption("preferred_machine", "PrefMach"));
            options.put("comdb2db_max_age", new IntegerOption("comdb2db_max_age", "Comdb2dbMaxAge"));
            options.put("debug", new BooleanOption("debug", "Debug"));
            options.put("max_retries", new IntegerOption("max_retries", "MaxRetries"));
            options.put("min_retries", new IntegerOption("min_retries", "MinRetries"));
            options.put("ssl_mode", new StringOption("ssl_mode", "SSLMode"));
            options.put("key_store", new StringOption("key_store", "SSLCrt"));
            options.put("key_store_password", new StringOption("key_store_password", "SSLCrtPass"));
            options.put("key_store_type", new StringOption("key_store_type", "SSLCrtType"));
            options.put("trust_store", new StringOption("trust_store", "SSLCA"));
            options.put("trust_store_password", new StringOption("trust_store_password", "SSLCAPass"));
            options.put("trust_store_type", new StringOption("trust_store_type", "SSLCAType"));
            options.put("crl", new StringOption("crl", "SSLCRL"));
            options.put("statement_query_effects",
                    new BooleanOption("statement_query_effects", "StatementQueryEffects"));
            options.put("verify_retry", new BooleanOption("verify_retry", "VerifyRetry"));
            options.put("stack_at_open", new BooleanOption("stack_at_open", "StackAtOpen"));
            options.put("skip_rs_drain", new BooleanOption("skip_rs_drain", "SkipResultSetDrain"));
            options.put("clear_ack", new BooleanOption("clear_ack", "ClearAck"));
            options.put("use_identity", new StringOption("use_identity", "UseIdentity"));
            options.put("use_txn_for_batch", new BooleanOption("use_txn_for_batch", "UseTxnForBatch"));
        } catch (Throwable e) {
            throw new SQLException(e);
        }
    }

    /**
     * Register our driver statically.
     */
    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            logger.error("Unable to register comdb2 driver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url))
            return null;

        /* Register addons from config. */
        InputStream is = Driver.class.getClassLoader().getResourceAsStream("cdb2jdbc.properties");
        if (is != null) {
            Properties props = new Properties();
            try {
                props.load(is);
                String value = props.getProperty("addons");
                String[] addons = value.split(",");
                for (String addon : addons) {
                    try {
                        Class.forName("com.bloomberg.comdb2.jdbc." + addon);
                    } catch(ClassNotFoundException e) {
                        logger.warn(String.format("Error loading %s addon", addon));
                    }
                }
            } catch (IOException ioe) {
                logger.warn("Error processing properties file\n");
            }
        }

        /**
         * The format of connection string is `jdbc:comdb2:<dbname>:<cluster>'.
         */

        String[] tokens = url.split("\\/\\s*|\\s+");

        Comdb2Connection ret;
        String clusterStr;
        String dbStr;
        String port = null;
        String attributes = null;

        List<String> hosts = new ArrayList<String>();
        List<Integer> ports = new ArrayList<Integer>();
        Set<String> processedOptions = new HashSet<String>();

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
                logger.debug(String.format("Setting port to %d\n", pval));
                ret.setOverriddenPort(pval);
            }

            /* process attributes */
            if (attributes != null) {
                String[] keyvals = attributes.split("&");
                for (String elem : keyvals) {
                    String[] keyval = elem.split("=");
                    if (keyval.length != 2)
                        continue;

                    if (keyval[0].equalsIgnoreCase("addons")) {
                        String[] addons = keyval[1].split(",");
                        for (String addon : addons) {
                            try {
                                Class.forName("com.bloomberg.comdb2.jdbc." + addon);
                            } catch(ClassNotFoundException e) {
                                logger.warn(String.format("Error loading %s addon", addon));
                            }
                        }
                    }

                    Option opt = options.get(keyval[0]);
                    if (opt != null) {
                        opt.process(ret, keyval[1], info);
                        processedOptions.add(opt.opt);
                    }
                }
            }
        } catch (Throwable t) {
            ret.close();
            throw new SQLException(t);
        }

        try {
            for (Object prop : info.keySet()) {
                String propertyName = prop.toString();
                if (processedOptions.contains(propertyName)) {
                    continue;
                }
                Option opt = options.get(propertyName);
                if (opt != null) {
                    opt.process(ret, info.getProperty(propertyName), info);
                    processedOptions.add(propertyName);
                }
            }
        } catch (Throwable t) {
            ret.close();
            throw new SQLException(t);
        }

        try {
            ret.lookup();
        } catch (SQLException e) {
            logger.error("Unable to establish cdb2api connection to {}", url, e);
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
        return Comdb2ClientInfo.getDriverMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return Comdb2ClientInfo.getDriverMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    public static void setHealthChecker(HealthChecker hc) {
        healthChecker = hc;
    }

    public static HealthChecker getHealthChecker() {
        return healthChecker;
    }

    public static void setIdentityCreatorFactory(IdentityCreatorFactory icf) {
        identityCreatorFactory = icf;
    }

    public static IdentityCreatorFactory getIdentityCreatorFactory() {
        return identityCreatorFactory;
    }
}
/* vim: set sw=4 ts=4 et: */
