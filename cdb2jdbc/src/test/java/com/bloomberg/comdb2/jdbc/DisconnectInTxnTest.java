package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import org.junit.*;

/* A non-HASQL transaction must not be silently resumed on a new connection
   after a disconnect. The statements sent before the disconnect are lost
   with the old connection, so committing would persist only part of it. */
public class DisconnectInTxnTest {

    String db, cluster;

    @Before public void setup() throws SQLException {
        db = System.getProperty("cdb2jdbc.test.database");
        cluster = System.getProperty("cdb2jdbc.test.cluster");

        Connection conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db));
        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS t_disconnect_in_txn");
        stmt.execute("CREATE TABLE t_disconnect_in_txn (i INTEGER)");
        stmt.close();
        conn.close();
    }

    @Test public void disconnectInTxnFails() throws Exception {
        Comdb2Connection conn = (Comdb2Connection)DriverManager.getConnection(
                String.format("jdbc:comdb2://%s/%s", cluster, db));
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();

        stmt.executeUpdate("INSERT INTO t_disconnect_in_txn VALUES (1)");

        /* Drop the socket out from under the open transaction. */
        conn.dbHandle().close();

        boolean gotError = false;
        try {
            stmt.executeUpdate("INSERT INTO t_disconnect_in_txn VALUES (2)");
            conn.commit();
        } catch (SQLException sqle) {
            gotError = true;
        }
        Assert.assertTrue("Disconnect in transaction should fail.", gotError);

        try {
            conn.rollback();
        } catch (SQLException sqle) {
        }
        stmt.close();
        conn.close();

        Connection conn2 = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db));
        Statement stmt2 = conn2.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT COUNT(*) FROM t_disconnect_in_txn");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("No rows should be committed.", 0, rs.getInt(1));
        rs.close();
        stmt2.close();
        conn2.close();
    }

    @After public void unsetup() throws SQLException {
        Connection conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db));
        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS t_disconnect_in_txn");
        stmt.close();
        conn.close();
    }
}
