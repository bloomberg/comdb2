package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import org.junit.*;
import org.junit.Assert.*;

public class BadRte {
    @Test public void testBadRteClient() throws SQLException {
        String db = System.getProperty("cdb2jdbc.test.database");
        String cluster = System.getProperty("cdb2jdbc.test.cluster");
        Connection conn = DriverManager.getConnection(String.format("jdbc:comdb2://%s/%s?allow_pmux_route=1&portmuxport=19000", cluster, db));
        Statement stmt = conn.createStatement();
        stmt.executeQuery("SELECT 1");
        stmt.close();
        conn.close();
    }
}
