package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import org.junit.*;
import org.junit.Assert.*;

public class WithAndGetAreReadTest {
    String db, cluster;
    Connection conn;
    Statement stmt;

    @Before public void setup() throws SQLException {
        db = System.getProperty("cdb2jdbc.test.database");
        cluster = System.getProperty("cdb2jdbc.test.cluster");

        conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s", cluster, db));
        stmt = conn.createStatement();
    }

    @Test public void run() throws SQLException {
        boolean is_read;

        is_read = stmt.execute("GET kw");
        Assert.assertEquals("`GET' is read", true, is_read);

        is_read = stmt.execute("WITH t(i) AS (SELECT 1) SELECT * FROM t");
        Assert.assertEquals("`WITH' is read", true, is_read);
    }

    @After public void unsetup() throws SQLException {
        stmt.close();
        conn.close();
    }
}
