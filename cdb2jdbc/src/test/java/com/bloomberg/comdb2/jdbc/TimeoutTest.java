package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import java.util.logging.*;
import org.junit.*;
import org.junit.Assert.*;

public class TimeoutTest {

    String db, cluster;
    Connection conn1, conn2;

    @Test public void testMaxQueryTimeout() throws SQLException {
        db = System.getProperty("cdb2jdbc.test.database");
        cluster = System.getProperty("cdb2jdbc.test.cluster");

        Connection conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s?maxquerytime=1", cluster, db));
        Statement stmt = conn.createStatement();
        long then = System.currentTimeMillis();
        ResultSet rs = stmt.executeQuery("SELECT SLEEP(5)");
        long duration = System.currentTimeMillis() - then;
        Assert.assertTrue("The query should take 1 to 2 seconds", duration >= 1000 && duration < 3000);
        rs.close();
        stmt.close();
        conn.close();
    }

    @Test public void testOfflineNodeDelay() {
        long then = System.currentTimeMillis();
        try {
            /* Silence our logger.
               The connectivity exception is expected.
               we don't need the extra noise here. */
            LogManager.getLogManager().reset();
            Connection conn = DriverManager.getConnection("jdbc:comdb2://example.com/db");
            conn.close();
        } catch (SQLException sqle) {
            long now = System.currentTimeMillis();
            Assert.assertEquals("Should see a delay of roughly 100 ms", true, (now - then) <= 150);
        }
    }
}
