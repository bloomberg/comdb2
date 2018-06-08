package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import java.util.logging.*;
import org.junit.*;
import org.junit.Assert.*;

public class SSLTest {

    String db, cluster, certpath, certpass;

    @Before
    public void setUp() {
        db = System.getProperty("cdb2jdbc.test.ssldatabase");
        cluster = System.getProperty("cdb2jdbc.test.cluster");
        certpath = System.getProperty("cdb2jdbc.test.sslcertpath");
        certpass = System.getProperty("cdb2jdbc.test.sslcertpass");
    }

    @Test public void rejectUnsecureServer() throws SQLException {
        Connection conn = null;
        try {
            String unsecuredb = System.getProperty("cdb2jdbc.test.database");
            conn = DriverManager.getConnection(String.format(
                        "jdbc:comdb2://%s/%s?ssl_mode=REQUIRE", cluster, unsecuredb));
            conn.createStatement().executeQuery("SELECT 1");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Server does not support SSL",
                    sqle.getCause().getMessage().contains("Server does not support SSL"));
        } finally {
            if (conn != null)
                conn.close();
        }
    }

    @Test public void testServerVerifyCA1() throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(String.format(
                        "jdbc:comdb2://%s/%s", cluster, db));
            conn.createStatement().executeQuery("SELECT 1");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Client certificate authentication failed",
                    sqle.getMessage().contains("Client certificate authentication failed"));
        } finally {
            if (conn != null)
                conn.close();
        }
    }

    @Test public void testServerVerifyCA2() throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(String.format(
                        "jdbc:comdb2://%s/%s?" +
                        "key_store=%s/keystore&" +
                        "key_store_password=%s",
                        cluster, db, certpath, certpass));
            conn.createStatement().executeQuery("SELECT 1");
        } finally {
            if (conn != null)
                conn.close();
        }
    }

    @Test
    public void testClientVerifyCA1() throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(String.format(
                        "jdbc:comdb2://%s/%s?" +
                        "ssl_mode=VERIFY_CA&",
                        cluster, db));
            conn.createStatement().executeQuery("SELECT 1");
        } catch (SQLException sqle) {
            Assert.assertTrue("Trust store required for server verification",
                    sqle.getCause().getMessage()
                    .contains("Trust store required for server verification"));
        } finally {
            if (conn != null)
                conn.close();
        }
    }

    @Test
    public void testClientVerifyCA2() throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(String.format(
                        "jdbc:comdb2://%s/%s?" +
                        "ssl_mode=VERIFY_CA&" +
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

    @Test
    public void testClientVerifyHostname() throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(String.format(
                        "jdbc:comdb2://%s/%s?" +
                        "ssl_mode=VERIFY_HOSTNAME&" +
                        "trust_store=%s/truststore&" +
                        "trust_store_password=%s&" +
                        "key_store=%s/keystore&" +
                        "key_store_password=%s",
                        cluster, db, certpath, certpass, certpath, certpass));
            conn.createStatement().executeQuery("SELECT 1");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Certificate does not match host name",
                    sqle.getCause().getMessage()
                    .contains("Certificate does not match host name"));
        } finally {
            if (conn != null)
                conn.close();
        }
    }

    @Test
    public void testClientCRL() throws SQLException {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(String.format(
                        "jdbc:comdb2://%s/%s?" +
                        "ssl_mode=VERIFY_CA&" +
                        "trust_store=%s/truststore&" +
                        "trust_store_password=%s&" +
                        "key_store=%s/keystore&" +
                        "key_store_password=%s&" +
                        "crl=%s/client.crl",
                        cluster, db, certpath, certpass, certpath, certpass, certpath));
            conn.createStatement().executeQuery("SELECT 1");
        } catch (SQLException sqle) {
            Assert.assertTrue("Certificate has been revoked",
                    sqle.getCause().getCause().getMessage()
                    .contains("Certificate has been revoked"));
        } finally {
            if (conn != null)
                conn.close();
        }
    }

    @Test
    public void testSSLSet() throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection(String.format(
                        "jdbc:comdb2://%s/%s", cluster, db));
            stmt = conn.createStatement();
            stmt.executeQuery("SELECT 1");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Client certificate authentication failed",
                    sqle.getMessage().contains("Client certificate authentication failed"));
        }

        try {
            stmt.executeQuery(String.format("SET KEY_STORE %s/keystore", certpath));
            stmt.executeQuery("SELECT 1");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Keystore was tampered with, or password was incorrect",
                    sqle.getCause().getCause().getMessage()
                    .contains("Keystore was tampered with, or password was incorrect"));
        }

        stmt.executeQuery(String.format("SET KEY_STORE_PASSWORD %s", certpass));
        stmt.executeQuery("SELECT 1");
        stmt.executeQuery("SET SSL_MODE VERIFY_CA");

        try {
            stmt.executeQuery("SELECT 1");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Trust store required for server verification",
                    sqle.getCause().getMessage()
                    .contains("Trust store required for server verification"));
        }

        stmt.executeQuery(String.format("SET TRUST_STORE %s/truststore", certpath));
        stmt.executeQuery(String.format("SET TRUST_STORE_PASSWORD %s", certpass));
        stmt.executeQuery("SELECT 1");

        try {
            stmt.executeQuery("SET SSL_MODE VERIFY_HOSTNAME");
            stmt.executeQuery("SELECT 1");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Certificate does not match host name",
                    sqle.getCause().getMessage()
                    .contains("Certificate does not match host name"));
        }

        if (conn != null)
            conn.close();
    }
}
