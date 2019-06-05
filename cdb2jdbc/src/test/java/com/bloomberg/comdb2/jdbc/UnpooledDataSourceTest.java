package com.bloomberg.comdb2.jdbc;

import java.sql.*;
import java.util.logging.*;
import org.junit.*;
import org.junit.Assert.*;
import java.util.*;

public class UnpooledDataSourceTest {

    @Test public void testDatabaseInitSqls() throws SQLException {
        String db = System.getProperty("cdb2jdbc.test.database");
        String cluster = System.getProperty("cdb2jdbc.test.cluster");

        UnpooledDataSource ds = new UnpooledDataSource();
        ds.setDriver("com.bloomberg.comdb2.jdbc.Driver");
        ds.setUrl(String.format("jdbc:comdb2://%s/%s?maxquerytime=1", cluster, db));
        ds.setConnectionInitSqls(Arrays.asList("SET TIMEZONE Zulu"));

        Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT CAST(NOW() AS TEXT)");
        String zulu = rs.getString(1);
        Assert.assertTrue("Should get back a time in Zulu", zulu.contains("Zulu"));

        /* Also test URL options in UnpooledDataSource. */
        long then = System.currentTimeMillis();
        rs = stmt.executeQuery("SELECT SLEEP(5)");
        long duration = System.currentTimeMillis() - then;
        Assert.assertTrue("The query should take 1 to 2 seconds", duration >= 1000 && duration < 3000);
        rs.close();
        stmt.close();
        conn.close();


        /* De-register myself from the driver manager to not interfere with other tests. */
        Enumeration<java.sql.Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            java.sql.Driver driver = drivers.nextElement();
            if (!(driver instanceof com.bloomberg.comdb2.jdbc.Driver)) {
                DriverManager.deregisterDriver(driver);
                break;
            }
        }
    }
}
