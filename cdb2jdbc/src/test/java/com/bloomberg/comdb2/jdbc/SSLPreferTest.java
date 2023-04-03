package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import java.util.logging.*;
import org.junit.*;
import org.junit.Assert.*;

public class SSLPreferTest {

    String ssldb, nonssldb, cluster, certpath, certpass;

    @Before
    public void setUp() {
        nonssldb = System.getProperty("cdb2jdbc.test.database");
        ssldb = System.getProperty("cdb2jdbc.test.ssldatabase");
        cluster = System.getProperty("cdb2jdbc.test.cluster");
        certpath = System.getProperty("cdb2jdbc.test.sslcertpath");
        certpass = System.getProperty("cdb2jdbc.test.sslcertpass");
    }


    public void prefer_verify_ca(String db) throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(String.format(
                        "jdbc:comdb2://%s/%s?" +
                        "ssl_mode=PREFER_VERIFY_CA&" +
                        "trust_store=%s/truststore&" +
                        "trust_store_password=%s&" +
                        "key_store=%s/keystore&" +
                        "key_store_password=%s",
                        cluster, db, certpath, certpass, certpath, certpass));
            conn.createStatement().executeQuery("SELECT 1");
        } finally {
            if (conn != null)
                conn.close();
        }
    }

    @Test public void prefer_verify_ca() throws SQLException {
        /* Should work against either an SSL or a non-SSL db, since we're asking for 'prefer' */
        prefer_verify_ca(nonssldb);
        prefer_verify_ca(ssldb);
    }


    @Test public void prefer() throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(String.format(
                        "jdbc:comdb2://%s/%s?ssl_mode=PREFER",
                        cluster, nonssldb));
            conn.createStatement().executeQuery("SELECT 1");
        } finally {
            if (conn != null)
                conn.close();
        }
    }
}
