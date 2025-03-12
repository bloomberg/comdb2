package com.bloomberg.comdb2.jdbc;

import java.io.*;
import java.sql.*;
import java.util.logging.*;
import org.junit.*;
import org.junit.Assert.*;

public class DatabaseDiscoveryTest {

    @Test
    public void testNoDefault() throws IOException, SQLException {
        try {
            LogManager.getLogManager().reset();
            Connection conn = DriverManager.getConnection("jdbc:comdb2://default/db");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Should see correct error message.",
                    sqle.getMessage().contains("No default type configured"));
        }
    }

    @Test
    public void testDNSDown() throws IOException, SQLException {
        try {
            LogManager.getLogManager().reset();
            /* dev-comdb2db is a thing now. make sure that we use an invalid dns server */
            Connection conn = DriverManager.getConnection("jdbc:comdb2://dev/db?dnssuffix=example.com");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Should see correct error message.",
                    sqle.getMessage().contains("Could not find database hosts from DNS and config files"));
        }
    }

    @Test
    public void testPmuxDown() throws IOException, SQLException {
        try {
            LogManager.getLogManager().reset();

            String db = System.getProperty("cdb2jdbc.test.database");
            String cluster = System.getProperty("cdb2jdbc.test.cluster");
            /* This line would succeed, because the driver would use the pmuxport for single-port */
            Connection conn1 = DriverManager.getConnection(
                    String.format("jdbc:comdb2://%s/%s?portmuxport=8888&allow_pmux_route=1", cluster, db));
            conn1.close();
            /* This line would fail, because the driver would attempt to query port 8888 for db port */
            Connection conn2 = DriverManager.getConnection(
                    String.format("jdbc:comdb2://%s/%s?portmuxport=8888&allow_pmux_route=0", cluster, db));
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Should see correct error message.",
                    sqle.getMessage().contains("Could not get database port from user supplied hosts"));
        }
    }

    @Test
    public void testComdb2dbMachineOffline() throws IOException, SQLException {
        try {
            LogManager.getLogManager().reset();
            String fname = "/tmp/comdb2db.jdbc.mvn.test.cfg." + System.currentTimeMillis();
            BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
            writer.write("does_not_exist 0 www.example.com\n");
            writer.close();
            System.setProperty("comdb2db.cfg", fname);
            Connection conn = DriverManager.getConnection("jdbc:comdb2://" +
                    "dev/db?comdb2dbname=does_not_exist&max_retries=1");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Should see correct error message.",
                    sqle.getMessage().contains("A network I/O error occurred"));
        }
    }

    @Test
    public void testComdb2dbNodeDown() throws IOException, SQLException {
        LogManager.getLogManager().reset();
        String fname = "/tmp/comdb2db.jdbc.mvn.test.cfg." + System.currentTimeMillis();
        String cluster = System.getProperty("cdb2jdbc.test.cluster");
        BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
        writer.write("does_not_exist 0 ");
        writer.write(cluster);
        writer.close();
        System.setProperty("comdb2db.cfg", fname);
        try {
            Connection conn = DriverManager.getConnection("jdbc:comdb2://dev/db?comdb2dbname=does_not_exist&allow_pmux_route=0");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Should see correct error message.",
                    sqle.getMessage().contains("Received invalid port from pmux"));
        }
        try {
            Connection conn = DriverManager.getConnection("jdbc:comdb2://dev/db?comdb2dbname=does_not_exist&allow_pmux_route=1");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Should see correct error message.",
                    sqle.getMessage().contains("A network I/O error occurred"));
        }
    }

    @Test
    public void testMalformedComdb2db() throws IOException, SQLException {
        try {
            LogManager.getLogManager().reset();
            String db = System.getProperty("cdb2jdbc.test.database");
            String cluster = System.getProperty("cdb2jdbc.test.cluster");

            Connection conn = DriverManager.getConnection(
                    String.format("jdbc:comdb2://%s/%s", cluster, db));
            try {
                conn.createStatement().execute("DROP TABLE clusters");
            } catch (SQLException sqle) {
            }
            try {
                conn.createStatement().execute("DROP TABLE machines");
            } catch (SQLException sqle) {
            }
            try {
                conn.createStatement().execute("DROP TABLE databases");
            } catch (SQLException sqle) {
            }

            String fname = "/tmp/comdb2db.jdbc.mvn.test.cfg." + System.currentTimeMillis();
            BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
            writer.write(db);
            writer.write(" 0 ");
            writer.write(cluster);
            writer.close();
            System.setProperty("comdb2db.cfg", fname);

            conn = DriverManager.getConnection(
                    String.format("jdbc:comdb2://%s/%s?comdb2dbname=%s", "dev", "does_not_exist", db));

            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Should see correct error message.",
                    sqle.getMessage().contains("Could not query database hosts from"));
        }
    }

    @Test
    public void testDbDown() throws IOException, SQLException {
        LogManager.getLogManager().reset();

        String cluster = System.getProperty("cdb2jdbc.test.cluster");
        String fname = "/tmp/comdb2db.jdbc.mvn.test.cfg." + System.currentTimeMillis();
        BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
        writer.write("does_not_exist");
        writer.write(" 0 ");
        writer.write(cluster);
        writer.close();
        System.setProperty("comdb2db.cfg", fname);

        try {
            Connection conn = DriverManager.getConnection("jdbc:comdb2://dev/does_not_exist?max_retries=1&allow_pmux_route=0");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Should see correct error message.",
                    sqle.getMessage().contains("Received invalid port from pmux."));
        }

        try {
            Connection conn = DriverManager.getConnection("jdbc:comdb2://dev/does_not_exist?max_retries=1&allow_pmux_route=1");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Should see correct error message.",
                    sqle.getMessage().contains("A network I/O error occurred"));
        }
    }

    @Test
    public void testUnregisteredDb() throws IOException, SQLException {
        try {
            LogManager.getLogManager().reset();

            String db = System.getProperty("cdb2jdbc.test.database");
            String cluster = System.getProperty("cdb2jdbc.test.cluster");
            Connection conn = DriverManager.getConnection(
                    String.format("jdbc:comdb2://%s/%s", cluster, db));

            try {
                conn.createStatement().execute("DROP TABLE clusters");
            } catch (SQLException sqle) {
            }
            try {
                conn.createStatement().execute("DROP TABLE machines");
            } catch (SQLException sqle) {
            }
            try {
                conn.createStatement().execute("DROP TABLE databases");
            } catch (SQLException sqle) {
            }

            conn.createStatement().execute("CREATE TABLE clusters (name varchar(8), cluster_name varchar(31), cluster_machs varchar(31))");
            conn.createStatement().execute("CREATE TABLE machines (name varchar(31), cluster varchar(31), room varchar(3))");
            conn.createStatement().execute("CREATE TABLE databases (name varchar(8), dbnum integer)");

            conn.close();

            String fname = "/tmp/comdb2db.jdbc.mvn.test.cfg." + System.currentTimeMillis();
            BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
            writer.write(db);
            writer.write(" 0 ");
            writer.write(cluster);
            writer.close();
            System.setProperty("comdb2db.cfg", fname);
            conn = DriverManager.getConnection("jdbc:comdb2://dev/does_not_exist?comdb2dbname=" + db);

            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Should see correct error message.",
                    sqle.getMessage().contains("No entries of does_not_exist found in"));
        }
    }

    @Test
    public void testShortDbline() throws IOException, SQLException {
        LogManager.getLogManager().reset();

        String db = System.getProperty("cdb2jdbc.test.database");
        String cluster = System.getProperty("cdb2jdbc.test.cluster");

        String fname = "/tmp/comdb2db.jdbc.mvn.test.cfg." + System.currentTimeMillis();
        BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
        writer.write("comdb2db localhost");
        writer.newLine();
        writer.write("comdb3db localhost");
        writer.newLine();
        writer.close();
        System.setProperty("comdb2db.cfg", fname);
        Connection conn = DriverManager.getConnection(String.format("jdbc:comdb2://%s/%s", cluster, db));
        conn.close();
    }

    @After public void tearDown() throws SQLException{
        String db = System.getProperty("cdb2jdbc.test.database");
        String cluster = System.getProperty("cdb2jdbc.test.cluster");
        Connection conn = DriverManager.getConnection(
                String.format("jdbc:comdb2://%s/%s", cluster, db));
        conn.createStatement().execute("DROP TABLE if exists clusters");
        conn.createStatement().execute("DROP TABLE if exists machines");
        conn.createStatement().execute("DROP TABLE if exists databases");
    }

}
