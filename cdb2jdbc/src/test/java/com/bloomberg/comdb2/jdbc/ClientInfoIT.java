package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import org.junit.*;
import org.junit.Assert.*;

public class ClientInfoIT {
    @Test public void getDriverInfoTest() {
        String[] expected_driver = { System.getProperty("driverName"), System.getProperty("driverVersion") };
        String[] actual_driver = { Comdb2ClientInfo.getDriverName(), Comdb2ClientInfo.getDriverVersion() };

        for ( String val : actual_driver ) { Assert.assertNotNull("driver value is not null", val); }
        Assert.assertArrayEquals("driver name and version match pom.xml", expected_driver, actual_driver);
    }

    @Test public void getJavaHomeTest() {
        String expected = System.getProperty("java.home");
        String actual = Comdb2ClientInfo.getJavaHome();
        Assert.assertNotNull("java.home should not be null", actual);
        Assert.assertEquals("getJavaHome should return java.home system property", expected, actual);
    }

    @Test public void verifyTaskNameIsJavaHome() throws SQLException {
        String db = System.getProperty("cdb2jdbc.test.database");
        String cluster = System.getProperty("cdb2jdbc.test.cluster");

        Connection conn = DriverManager.getConnection(String.format("jdbc:comdb2://%s/%s", cluster, db));
        Statement stmt = conn.createStatement();
        /* Execute a query so that client info is sent to the server. */
        stmt.executeQuery("SELECT 1");
        ResultSet rs = stmt.executeQuery("SELECT task FROM comdb2_clientstats");
        String javaHome = System.getProperty("java.home");
        boolean found = false;
        while (rs.next()) {
            String task = rs.getString(1);
            if (task != null && task.equals(javaHome))
                found = true;
        }
        Assert.assertTrue("Should find java.home (" + javaHome + ") as task name in comdb2_clientstats", found);

        rs.close();
        stmt.close();
        conn.close();
    }

    @Test public void verifyDriverInfoInDatabase() throws SQLException {
        String db = System.getProperty("cdb2jdbc.test.database");
        String cluster = System.getProperty("cdb2jdbc.test.cluster");

        Connection conn = DriverManager.getConnection(String.format("jdbc:comdb2://%s/%s", cluster, db));
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT comdb2_host()");
        String directcpu = rs.getString(1);
        rs.close();
        stmt.close();
        conn.close();

        conn = DriverManager.getConnection(String.format("jdbc:comdb2://%s/%s", directcpu, db));
        PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM comdb2_api_history WHERE api_driver_name = ? and api_driver_version = ?");
        ps.setString(1, System.getProperty("driverName"));
        ps.setString(2, System.getProperty("driverVersion"));

        rs = ps.executeQuery();
        int cnt = rs.getInt(1);
        Assert.assertTrue(cnt > 0);
        rs.close();
        stmt.close();
        conn.close();
    }
}
