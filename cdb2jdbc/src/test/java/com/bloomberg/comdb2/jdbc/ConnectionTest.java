package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import org.junit.*;
import org.junit.Assert.*;

public class ConnectionTest {

    String db, cluster;
    Connection conn;

    @Before public void setup() throws SQLException {
        db = System.getProperty("cdb2jdbc.test.database");
        cluster = System.getProperty("cdb2jdbc.test.cluster");

        conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db));
        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS t_connection_test");
        stmt.execute("CREATE TABLE t_connection_test (i INTEGER)");

        stmt.close();
        conn.close();
    }

    @Test public void setTransactionIsolationTest() throws SQLException {
        int cnt = -1;
        conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db));

        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();

        stmt.executeUpdate("INSERT INTO t_connection_test VALUES (1)");
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS CNT FROM t_connection_test");
        while (rs.next()) {
            cnt = rs.getInt(1);
        }
        conn.rollback();
        Assert.assertEquals("Should see 1 row", 1, cnt);

        conn.setTransactionIsolation(Comdb2Connection.TRANSACTION_BLOCK);
        stmt.executeUpdate("INSERT INTO t_connection_test VALUES (1)");
        rs = stmt.executeQuery("SELECT COUNT(*) AS CNT FROM t_connection_test");
        while (rs.next()) {
            cnt = rs.getInt(1);
        }
        conn.rollback();
        Assert.assertEquals("Should see 0 row", 0, cnt);

        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        conn.setAutoCommit(true);
        stmt.executeQuery("begin");
        stmt.executeUpdate("INSERT INTO t_connection_test VALUES (1)");
        rs = stmt.executeQuery("SELECT COUNT(*) AS CNT FROM t_connection_test");
        while (rs.next()) {
            cnt = rs.getInt(1);
        }
        stmt.executeQuery("rollback");
        Assert.assertEquals("Should see 1 row", 1, cnt);

        stmt.close();
        conn.close();
    }

    @After public void unsetup() throws SQLException {
        DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db))
            .createStatement().execute("DROP TABLE t_connection_test");
    }
}
