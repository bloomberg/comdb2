package com.bloomberg.comdb2.jdbc;

import org.junit.*;
import org.junit.Assert.*;

public class ClientInfoIT {
    @Test public void getDriverInfoTest() {
        String[] expected_driver = { System.getProperty("testDriverName"), System.getProperty("testDriverVersion") };
        String[] actual_driver = { Comdb2ClientInfo.getDriverName(), Comdb2ClientInfo.getDriverVersion() };

        for ( String val : actual_driver ) { Assert.assertNotNull("driver value is not null", val); }
        Assert.assertFalse("driver version is not default", actual_driver[1].equals("1.0"));
        Assert.assertArrayEquals("driver name and version match pom.xml", expected_driver, actual_driver);
    }
}
