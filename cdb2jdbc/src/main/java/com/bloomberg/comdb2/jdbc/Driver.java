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

/**
 * @author Rivers Zhang
 */
public class Driver implements java.sql.Driver {
    private static Logger logger = Logger.getLogger(Driver.class.getName());

    public static final String PREFIX = "jdbc:comdb2:";
    public static final String PROPERTY_TIMEOUT = "comdb2_timeout";
    public static final String PROPERTY_USER = "user";
    public static final String PROPERTY_PASSWORD = "password";
    public static final String PROPERTY_DEFAULTTYPE = "default_type";
    public static final String PROPERTY_ROOM = "room";
    public static final String PROPERTY_PORTMUX = "portmuxport";
    public static final String PROPERTY_COMDB2DBNAME = "comdb2dbname";
    public static final String PROPERTY_TCPBUF = "tcpbufsz";
    public static final String PROPERTY_DNSSUFFIX = "dnssuffix";
    public static final String PROPERTY_POLICY = "load_balance";
    public static final String PROPERTY_USEC = "microsecond_fraction";
    public static final String PROPERTY_PREFERMACH = "preferred_machine";
    public static final String PROPERTY_COMDB2DB_MAX_AGE = "comdb2db_max_age";
    public static final String PROPERTY_DEBUG = "debug";
    public static final String PROPERTY_MAXRETRIES = "max_retries";
    public static final String PROPERTY_SSL_MODE = "ssl_mode";
    public static final String PROPERTY_SSL_CRT = "key_store";
    public static final String PROPERTY_SSL_CRTPASS = "key_store_password";
    public static final String PROPERTY_SSL_CRTTYPE = "key_store_type";
    public static final String PROPERTY_SSL_CA = "trust_store";
    public static final String PROPERTY_SSL_CAPASS = "trust_store_password";
    public static final String PROPERTY_SSL_CATYPE = "trust_store_type";
    public static final String PROPERTY_PMUX_RTE = "allow_pmux_route";

    /**
     * Register our driver statically.
     */
    static {
        try {
            DriverManager.registerDriver(new Driver());
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

        /* Oh hi strings */
        Comdb2Connection ret = null;
        String clusterStr = null;
        String dbStr = null;

        String timeout = null;
        String user = null;
        String password = null;
        String default_type = null;
        String room = null;
        String portmux = null;
        String comdb2dbname = null;
        String tcpbufsz = null;
        String dnssuffix = null;
        String port = null;
        String usec = null;
        String prefmach = null;
        String comdb2db_max_age = null;
        String debugmode = null;
        String max_retries = null;
        String sslmode = null;
        String sslcrt = null;
        String sslcrtpass = null;
        String sslcrttype = null;
        String sslca = null;
        String sslcapass = null;
        String sslcatype = null;
        String pmuxrte = null;
        String attributes = null;

        String policy = null;
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

            /* read attributes */
            if (attributes != null) {
                String[] keyvals = attributes.split("&");
                for (String elem : keyvals) {
                    String[] keyval = elem.split("=");
                    if (keyval.length != 2)
                        continue;

                    if (PROPERTY_TIMEOUT.equalsIgnoreCase(keyval[0]))
                        timeout = keyval[1];
                    else if (PROPERTY_USER.equalsIgnoreCase(keyval[0]))
                        user = keyval[1];
                    else if (PROPERTY_PASSWORD.equalsIgnoreCase(keyval[0]))
                        password = keyval[1];
                    else if (PROPERTY_DEFAULTTYPE.equalsIgnoreCase(keyval[0]))
                        default_type = keyval[1];
                    else if (PROPERTY_ROOM.equalsIgnoreCase(keyval[0]))
                        room = keyval[1];
                    else if (PROPERTY_PORTMUX.equalsIgnoreCase(keyval[0]))
                        portmux = keyval[1];
                    else if (PROPERTY_COMDB2DBNAME.equalsIgnoreCase(keyval[0]))
                        comdb2dbname = keyval[1];
                    else if (PROPERTY_TCPBUF.equalsIgnoreCase(keyval[0]))
                        tcpbufsz = keyval[1];
                    else if (PROPERTY_DNSSUFFIX.equalsIgnoreCase(keyval[0]))
                        dnssuffix = keyval[1];
                    else if (PROPERTY_POLICY.equalsIgnoreCase(keyval[0]))
                        policy = keyval[1];
                    else if (PROPERTY_USEC.equalsIgnoreCase(keyval[0]))
                        usec = keyval[1];
                    else if (PROPERTY_PREFERMACH.equalsIgnoreCase(keyval[0]))
                        prefmach = keyval[1];
                    else if (PROPERTY_COMDB2DB_MAX_AGE.equalsIgnoreCase(keyval[0]))
                        comdb2db_max_age = keyval[1];
                    else if (PROPERTY_DEBUG.equalsIgnoreCase(keyval[0]))
                        debugmode = keyval[1];
                    else if (PROPERTY_MAXRETRIES.equalsIgnoreCase(keyval[0]))
                        max_retries = keyval[1];
                    else if (PROPERTY_SSL_MODE.equalsIgnoreCase(keyval[0]))
                        sslmode = keyval[1];
                    else if (PROPERTY_SSL_CRT.equalsIgnoreCase(keyval[0]))
                        sslcrt = keyval[1];
                    else if (PROPERTY_SSL_CRTPASS.equalsIgnoreCase(keyval[0]))
                        sslcrtpass = keyval[1];
                    else if (PROPERTY_SSL_CRTTYPE.equalsIgnoreCase(keyval[0]))
                        sslcrttype = keyval[1];
                    else if (PROPERTY_SSL_CA.equalsIgnoreCase(keyval[0]))
                        sslca = keyval[1];
                    else if (PROPERTY_SSL_CAPASS.equalsIgnoreCase(keyval[0]))
                        sslcapass = keyval[1];
                    else if (PROPERTY_SSL_CATYPE.equalsIgnoreCase(keyval[0]))
                        sslcatype = keyval[1];
                    else if (PROPERTY_PMUX_RTE.equalsIgnoreCase(keyval[0]))
                        pmuxrte = keyval[1];
                }
            }
        }

        ret = new Comdb2Connection(dbStr, clusterStr);
        /* add user supplied hosts, if any */
        ret.addHosts(hosts);
        ret.addPorts(ports);

        /* read properties if absent */
        if (timeout == null)
            timeout = info.getProperty(PROPERTY_TIMEOUT);
        if (user == null)
            user = info.getProperty(PROPERTY_USER);
        if (password == null)
            password = info.getProperty(PROPERTY_PASSWORD);
        if (default_type == null)
            default_type = info.getProperty(PROPERTY_DEFAULTTYPE);
        if (room == null)
            room = info.getProperty(PROPERTY_ROOM);
        if (portmux == null)
            portmux = info.getProperty(PROPERTY_PORTMUX);
        if (comdb2dbname == null)
            comdb2dbname = info.getProperty(PROPERTY_COMDB2DBNAME);
        if (tcpbufsz == null)
            tcpbufsz = info.getProperty(PROPERTY_TCPBUF);
        if (dnssuffix == null)
            dnssuffix = info.getProperty(PROPERTY_DNSSUFFIX);
        if (policy == null)
            policy = info.getProperty(PROPERTY_POLICY);
        if (usec == null)
            usec = info.getProperty(PROPERTY_USEC);
        if (prefmach == null)
            prefmach = info.getProperty(PROPERTY_PREFERMACH);
        if (comdb2db_max_age == null)
            comdb2db_max_age = info.getProperty(PROPERTY_COMDB2DB_MAX_AGE);
        if (debugmode == null)
            debugmode = info.getProperty(PROPERTY_DEBUG);
        if (max_retries == null)
            max_retries = info.getProperty(PROPERTY_MAXRETRIES);
        if (sslmode == null)
            sslmode = info.getProperty(PROPERTY_SSL_MODE);
        if (sslcrt == null)
            sslcrt = info.getProperty(PROPERTY_SSL_CRT);
        if (sslcrtpass == null)
            sslcrtpass = info.getProperty(PROPERTY_SSL_CRTPASS);
        if (sslcrttype == null)
            sslcrttype = info.getProperty(PROPERTY_SSL_CRTTYPE);
        if (sslca == null)
            sslca = info.getProperty(PROPERTY_SSL_CA);
        if (sslcapass == null)
            sslcapass = info.getProperty(PROPERTY_SSL_CAPASS);
        if (sslcatype == null)
            sslcatype = info.getProperty(PROPERTY_SSL_CATYPE);
        if (pmuxrte == null)
            pmuxrte = info.getProperty(PROPERTY_PMUX_RTE);

        try {
            if (port != null) {
                int pval = Integer.parseInt(port);
                logger.log(Level.FINE, String.format("Setting port to %d\n", pval));
                ret.setOverriddenPort(pval);
            }

            if (timeout != null) {
                int toval = Integer.parseInt(timeout);
                logger.log(Level.FINE, String.format("Setting query timeout to %d s\n", toval));
                ret.setQueryTimeout(toval);
            }
			if (user != null) {
				logger.log(Level.FINE, String.format("Setting user to %s\n", user));
				ret.setUser(user);
			}

            if (password != null) {
                logger.log(Level.FINE, String.format("Setting password to %s\n", password));
                ret.setPassword(password);
            }

            if (default_type != null) {
                logger.log(Level.FINE, String.format("Setting default_type to %s\n", default_type));
                ret.setDefaultType(default_type);
            }

            if (room != null) {
                logger.log(Level.FINE, String.format("Setting room to %s\n", room));
                ret.setMachineRoom(room);
            }

            if (portmux != null) {
                int pmval = Integer.parseInt(portmux);
                logger.log(Level.FINE, String.format("Setting pmux port to %d\n", pmval));
                ret.setPortMuxPort(pmval);
            }

            if (comdb2dbname != null) {
                logger.log(Level.FINE, String.format("Setting comdb2dbname to %s\n", comdb2dbname));
                ret.setComdb2dbName(comdb2dbname);
            }

            if (tcpbufsz != null) {
                int tbval = Integer.parseInt(tcpbufsz);
                logger.log(Level.FINE, String.format("Setting TCP recv buffer size to %d\n", tbval));
                ret.setTcpBufSize(tbval);
            }

            if (dnssuffix != null) {
                logger.log(Level.FINE, String.format("Setting DNS suffix to %s\n", dnssuffix));
                ret.setDnsSuffix(dnssuffix);
            }

            if (policy != null) {
                logger.log(Level.FINE, String.format("Setting load balance policy to %s\n", policy));
                ret.setPolicy(policy);
            }

            if (usec != null) {
                logger.log(Level.FINE, String.format("Setting microsecond fraction to %s\n", usec));
                ret.setMicroSecond(usec);
            }

            if (prefmach != null) {
                logger.log(Level.FINE, String.format("Setting preferred machine to %s\n", prefmach));
                ret.setPrefMach(prefmach);
            }

            if (comdb2db_max_age != null) {
                int maxageval = Integer.parseInt(comdb2db_max_age);
                logger.log(Level.FINE, String.format("Setting comdb2db max age to %d second(s)\n", maxageval));
                ret.setComdb2dbMaxAge(maxageval);
            }

            if (debugmode != null) {
                logger.log(Level.FINE, String.format("Setting debug to %s\n", debugmode));
                ret.setDebug(debugmode);
            }

            if (max_retries != null) {
                int maxretriesval = Integer.parseInt(max_retries);
                logger.log(Level.FINE, String.format("Setting max retries to %d\n", maxretriesval));
                ret.setMaxRetries(maxretriesval);
            }

            if (sslmode != null) {
                logger.log(Level.FINE, String.format("Setting ssl mode to %s\n", sslmode));
                ret.setSSLMode(sslmode);
            }

            if (sslcrt != null) {
                logger.log(Level.FINE, String.format("Setting ssl keystore to %s\n", sslcrt));
                ret.setSSLCrt(sslcrt);
            }

            if (sslcrtpass != null) {
                logger.log(Level.FINE, String.format("Setting ssl kspass to %s\n", sslcrtpass));
                ret.setSSLCrtPass(sslcrtpass);
            }

            if (sslcrttype != null) {
                logger.log(Level.FINE, String.format("Setting ssl kstype to %s\n", sslcrttype));
                ret.setSSLCrtType(sslcrttype);
            }

            if (sslca != null) {
                logger.log(Level.FINE, String.format("Setting ssl truststore to %s\n", sslca));
                ret.setSSLCA(sslca);
            }

            if (sslcapass != null) {
                logger.log(Level.FINE, String.format("Setting ssl tspass to %s\n", sslcapass));
                ret.setSSLCAPass(sslcapass);
            }

            if (sslcatype != null) {
                logger.log(Level.FINE, String.format("Setting ssl tstype to %s\n", sslcatype));
                ret.setSSLCAType(sslcatype);
            }

            if (pmuxrte != null) {
                logger.log(Level.FINE, String.format("Setting pmux passthrouth to %s\n", pmuxrte));
                ret.setAllowPmuxRoute(pmuxrte);
            }
        } catch (NumberFormatException e1) {
            logger.log(Level.WARNING, "Incorrect configuration in: " + url, e1);
            ret.close();
            throw new SQLException(e1);
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
