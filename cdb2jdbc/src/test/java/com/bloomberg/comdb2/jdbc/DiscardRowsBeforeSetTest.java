package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import org.junit.*;
import org.junit.Assert.*;

/* 
 * Description:
 * When using multiple statement handles on the same connection,
 * SET statements may be ignored.
 *
 * Reproduction:
 * See run().
 *
 * Resolution:
 * Discard pending rows before processing the SET statements.
 */

public class DiscardRowsBeforeSetTest {
    String db;
    String cluster;

    Connection conn;

    @Before public void setup() throws ClassNotFoundException, SQLException {
        db = System.getProperty("cdb2jdbc.test.database");
        cluster = System.getProperty("cdb2jdbc.test.cluster");
        conn = DriverManager.getConnection(String.format("jdbc:comdb2://%s/%s", cluster, db));
    }

    @Test public void run() throws SQLException {
        Statement stmt1, stmt2;
        ResultSet rs;
        boolean tz_taking_effect;

        stmt1 = conn.createStatement();
        stmt2 = conn.createStatement();

        stmt1.executeQuery("SET TIMEZONE UTC");
        stmt1.executeQuery("SELECT NOW() AS utctime");
        stmt2.executeQuery("SET TIMEZONE EST");
        rs = stmt2.executeQuery("SELECT NOW() AS esttime");

        Assert.assertEquals("Got rows back from server", true, rs.next());
        String esttime = rs.getString("esttime");
        tz_taking_effect = esttime.contains("EST");
        Assert.assertEquals("Timezone is EST", true, tz_taking_effect);
    }
}
/* vim: set sw=4 ts=4 et: */
