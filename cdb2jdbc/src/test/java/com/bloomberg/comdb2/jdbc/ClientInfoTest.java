package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import org.junit.*;
import org.junit.Assert.*;

public class ClientInfoTest {

    String db, cluster;
    Connection conn;

    @Before public void setup() throws SQLException {
        db = System.getProperty("cdb2jdbc.test.database");
        cluster = System.getProperty("cdb2jdbc.test.cluster");

        conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db));
    }

    @Test public void testClientInfoTest() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeQuery("SELECT 1");
        ResultSet rs = stmt.executeQuery("SELECT stack FROM comdb2_clientstats");
        boolean found = false;
        while (rs.next()) {
            String stack = rs.getString(1);
            if (stack != null && stack.contains("JUnit"))
                found = true;
        }
        Assert.assertTrue("Should find JUnit in the call stack", found);

        rs.close();
        stmt.close();
        conn.close();
    }
}
