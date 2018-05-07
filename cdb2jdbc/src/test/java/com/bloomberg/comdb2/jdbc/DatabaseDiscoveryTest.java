package com.bloomberg.comdb2.jdbc;

import java.io.*;
import java.sql.*;
import java.util.logging.*;
import org.junit.*;
import org.junit.Assert.*;

public class DatabaseDiscoveryTest {

    @Test public void testComdb2dbNodeDown() throws IOException, SQLException {
        try {
            LogManager.getLogManager().reset();
            String fname = "/tmp/comdb2db.jdbc.mvn.test.cfg." + System.currentTimeMillis();
            BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
            writer.write("\ndoes_not_exist 0 localhost\n");
            writer.close();
            System.setProperty("comdb2db.cfg", fname);
            Connection conn = DriverManager.getConnection("jdbc:comdb2://dev/db?comdb2dbname=does_not_exist");
            Assert.assertTrue("Should not reach here", false);
        } catch (SQLException sqle) {
            Assert.assertTrue("Should see sqlexception", true);
        }
    }
}
