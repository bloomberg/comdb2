package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import java.util.logging.*;
import java.util.*;
import org.junit.*;
import org.junit.Assert.*;

public class CursorStateTest {

    @Test public void CursorStates() throws SQLException {
        String db = System.getProperty("cdb2jdbc.test.database");
        String cluster = System.getProperty("cdb2jdbc.test.cluster");

        Connection conn = DriverManager.getConnection(String.format(
                    "jdbc:comdb2://%s/%s?maxquerytime=1", cluster, db));
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 UNION SELECT 2");

        Assert.assertEquals(true, rs.isBeforeFirst());
        Assert.assertEquals(false, rs.isFirst());
        Assert.assertEquals(false, rs.isAfterLast());

        rs.next();

        Assert.assertEquals(false, rs.isBeforeFirst());
        Assert.assertEquals(true, rs.isFirst());
        Assert.assertEquals(false, rs.isAfterLast());

        rs.next();

        Assert.assertEquals(false, rs.isBeforeFirst());
        Assert.assertEquals(false, rs.isFirst());
        Assert.assertEquals(false, rs.isAfterLast());

        rs.next();

        Assert.assertEquals(false, rs.isBeforeFirst());
        Assert.assertEquals(false, rs.isFirst());
        Assert.assertEquals(true, rs.isAfterLast());

        rs.close();
        stmt.close();
        conn.close();
    }
}
