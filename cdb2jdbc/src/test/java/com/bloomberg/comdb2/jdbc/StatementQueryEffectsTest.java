package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import org.junit.*;
import org.junit.Assert.*;

public class StatementQueryEffectsTest {

    String db, cluster;
    Connection conn;

    @Before public void setup() throws SQLException {
        db = System.getProperty("cdb2jdbc.test.database");
        cluster = System.getProperty("cdb2jdbc.test.cluster");

        conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db));
        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE IF EXISTS t_stmteffects");
        stmt.execute("CREATE TABLE t_stmteffects (i INTEGER)");
        stmt.execute("INSERT INTO t_stmteffects values (1)");

        stmt.close();
        conn.close();
    }

    /* by default,
       executeUpdate() returns the query effects made by the statement. */
    @Test public void defaultQueryEffects() throws SQLException {
        conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db));
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();

        int nupd;
        nupd = stmt.executeUpdate("UPDATE t_stmteffects SET i = 2 WHERE i = 1");
        Assert.assertEquals("Updated 1 record.", 1, nupd);
        nupd = stmt.executeUpdate("UPDATE t_stmteffects SET i = 4 WHERE i = 3");
        Assert.assertEquals("Updated 0 record.", 0, nupd);

        conn.rollback();
        stmt.close();
        conn.close();
    }

    /* Under statement query effects,
       executeUpdate() returns the query effects made by the statement. */
    @Test public void statementQueryEffects() throws SQLException {
        conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s?statement_query_effects=1", cluster, db));
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();

        int nupd;
        nupd = stmt.executeUpdate("UPDATE t_stmteffects SET i = 2 WHERE i = 1");
        Assert.assertEquals("Updated 1 record.", 1, nupd);
        nupd = stmt.executeUpdate("UPDATE t_stmteffects SET i = 4 WHERE i = 3");
        Assert.assertEquals("Updated 0 record.", 0, nupd);

        conn.rollback();
        stmt.close();
        conn.close();
    }

    /* Under transaction query effects,
       executeUpdate() returns the query effects made by the transaction. */
    @Test public void transactionQueryEffects() throws SQLException {
        conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s?statement_query_effects=0", cluster, db));
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();

        int nupd;
        nupd = stmt.executeUpdate("UPDATE t_stmteffects SET i = 2 WHERE i = 1");
        Assert.assertEquals("Updated 1 record.", 1, nupd);
        nupd = stmt.executeUpdate("UPDATE t_stmteffects SET i = 4 WHERE i = 3");
        Assert.assertEquals("Updated 1 record.", 1, nupd);

        conn.rollback();
        stmt.close();
        conn.close();
    }

    /* Under statement query effects,
       executeBatch() returns the query effects made by each statement. */
    @Test public void statementQueryEffectsBatch() throws SQLException {
        conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s?statement_query_effects=1", cluster, db));
        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement("INSERT INTO t_stmteffects values (?)");
        ps.setInt(1, 100);
        ps.addBatch();
        ps.setInt(1, 200);
        ps.addBatch();
        ps.setInt(1, 300);
        ps.addBatch();
        int[] counts = ps.executeBatch();

        Assert.assertEquals("Inserted 3 rows", 3, counts.length);
        Assert.assertEquals("1", 1, counts[0]);
        Assert.assertEquals("2", 1, counts[1]);
        Assert.assertEquals("3", 1, counts[2]);

        conn.rollback();
        ps.close();
        conn.close();
    }

    /* Under transaction query effects,
       executeBatch() returns the cumulative query effects. */
    @Test public void transactionQueryEffectsBatch() throws SQLException {
        conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s?statement_query_effects=0", cluster, db));
        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement("INSERT INTO t_stmteffects values (?)");
        ps.setInt(1, 100);
        ps.addBatch();
        ps.setInt(1, 200);
        ps.addBatch();
        ps.setInt(1, 300);
        ps.addBatch();
        int[] counts = ps.executeBatch();

        Assert.assertEquals("Inserted 3 rows", 3, counts.length);
        Assert.assertEquals("1", 1, counts[0]);
        Assert.assertEquals("2", 2, counts[1]);
        Assert.assertEquals("3", 3, counts[2]);

        conn.rollback();
        ps.close();
        conn.close();
    }

    @After public void unsetup() throws SQLException {
        conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db));
        Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE t_stmteffects");
    }
}
