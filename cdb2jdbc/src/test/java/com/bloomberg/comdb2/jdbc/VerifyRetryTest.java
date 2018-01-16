package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import org.junit.*;
import org.junit.Assert.*;

public class VerifyRetryTest {

    String db, cluster;
    Connection conn1, conn2;

    @Before public void setup() throws SQLException {
        db = System.getProperty("cdb2jdbc.test.database");
        cluster = System.getProperty("cdb2jdbc.test.cluster");

        conn1 = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db));
        Statement stmt = conn1.createStatement();
        stmt.execute("DROP TABLE IF EXISTS t_verifyretry");
        stmt.execute("CREATE TABLE t_verifyretry (i INTEGER)");
        stmt.execute("INSERT INTO t_verifyretry values (1)");

        stmt.close();
        conn1.close();
    }

    /* One transaction will fail if verifyretry is off. */
    @Test public void verifyRetryOff() throws SQLException {
        conn1 = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db));
        conn1.setAutoCommit(false);
        Statement stmt1 = conn1.createStatement();

        conn2 = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db));
        Statement stmt2 = conn2.createStatement();

        stmt1.executeUpdate("UPDATE t_verifyretry SET i = 2 WHERE 1=1");
        stmt2.executeUpdate("UPDATE t_verifyretry SET i = 3 WHERE 1=1");

        boolean gotError = false;
        try {
            conn1.commit(); /* <-- Should get an exception here */
        } catch (SQLException sqle) {
            gotError = true;
        }

        Assert.assertTrue("Commit should fail.", gotError);

        boolean isThree = false;
        ResultSet rs = stmt1.executeQuery("SELECT i FROM t_verifyretry");
        while (rs.next()) {
            isThree = (rs.getInt(1) == 3);
        }
        Assert.assertTrue("Should get back 3", isThree);

        stmt1.close();
        conn1.close();
        stmt2.close();
        conn2.close();
    }

    /* One transaction will fail if verifyretry is off. */
    @Test public void verifyRetryOn() throws SQLException {
        conn1 = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s?verify_retry=1", cluster, db));
        conn1.setAutoCommit(false);
        Statement stmt1 = conn1.createStatement();

        conn2 = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db));
        Statement stmt2 = conn2.createStatement();

        stmt1.executeUpdate("UPDATE t_verifyretry SET i = 2 WHERE 1=1");
        stmt2.executeUpdate("UPDATE t_verifyretry SET i = 3 WHERE 1=1");

        boolean gotError = false;
        try {
            conn1.commit(); /* <-- Should not get an exception here */
        } catch (SQLException sqle) {
            gotError = true;
        }

        Assert.assertFalse("Commit should succeed.", gotError);

        boolean isTwo = false;
        ResultSet rs = stmt1.executeQuery("SELECT i FROM t_verifyretry");
        while (rs.next()) {
            isTwo = (rs.getInt(1) == 2);
        }

        Assert.assertTrue("Should get back 2", isTwo);

        stmt1.close();
        conn1.close();
        stmt2.close();
        conn2.close();
    }

    @After public void unsetup() throws SQLException {
        DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db))
            .createStatement().execute("DROP TABLE t_verifyretry");
    }
}
