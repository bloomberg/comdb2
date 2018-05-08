package com.bloomberg.comdb2.jdbc;

import java.io.*;
import java.sql.*;
import java.util.logging.*;
import org.junit.*;
import org.junit.Assert.*;

public class DatabaseDiscoveryTest {

    /* Verify that the driver throws proper exception when comdb2db or bdns is down. */
    @Test public void testComdb2dbNodeDown() throws IOException, SQLException {
        try {
            LogManager.getLogManager().reset();
            String fname = "/tmp/comdb2db.jdbc.mvn.test.cfg." + System.currentTimeMillis();
            BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
            writer.write("\ndoes_not_exist 0 www.example.com\n");
            writer.close();
            System.setProperty("comdb2db.cfg", fname);
            Connection conn = DriverManager.getConnection("jdbc:comdb2://dev/db?comdb2dbname=does_not_exist");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Should see an IO error since the destination is unreachable.",
                    sqle.getMessage().contains("An I/O error occurred"));
        }
    }
}
