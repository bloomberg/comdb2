package com.bloomberg.comdb2.jdbc;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.logging.*;
import org.junit.*;

public class BmsDiscoveryTest {

    @Test
    public void testBmsDisabledViaCfgFile() throws IOException, SQLException {
        LogManager.getLogManager().reset();
        String fname = "/tmp/comdb2db.jdbc.bms.test.cfg." + System.currentTimeMillis();
        BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
        writer.write("comdb2_config:default_type=dev\n");
        writer.write("comdb2_feature:use_bmsd=off\n");
        writer.close();
        System.setProperty("comdb2db.cfg", fname);

        try {
            Connection conn = DriverManager.getConnection(
                    "jdbc:comdb2://dev/testdb?use_bmsd=off&dnssuffix=example.com&max_retries=1");
            Assert.fail("Should not reach here");
        } catch (SQLException sqle) {
            /* BMS is off, so it should fall through to comdb2db/DNS which will fail */
            Assert.assertTrue("Should fail with DNS/comdb2db error, got: " + sqle.getMessage(),
                    sqle.getMessage().contains("Could not find database hosts"));
        } finally {
            new File(fname).delete();
        }
    }

    /*
     * Environment-specific: needs a resolvable comdb2db (external DNS/infra) so
     * discovery reaches the BMS stage. In a plain container it fails earlier at
     * comdb2db host resolution (DatabaseDiscovery ~line 774) with a different
     * message, so it can't run in the open-source CI (.github/workflows/cdb2jdbc.yml).
     */
    @Ignore("Requires comdb2db/DNS infra to reach the BMS discovery stage")
    @Test
    public void testBmsNoFallbackFails() throws IOException, SQLException {
        LogManager.getLogManager().reset();
        String fname = "/tmp/comdb2db.jdbc.bms.test.cfg." + System.currentTimeMillis();
        BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
        writer.write("comdb2_config:default_type=dev\n");
        writer.close();
        System.setProperty("comdb2db.cfg", fname);

        try {
            /* BMS enabled with invalid suffix, fallback disabled — should fail with BMS error */
            Connection conn = DriverManager.getConnection(
                    "jdbc:comdb2://dev/testdb?bmssuffix=invalid.example.test&comdb2db_fallback=off&max_retries=1");
            Assert.fail("Should not reach here");
        } catch (SQLException sqle) {
            Assert.assertTrue("Should fail with BMS error, got: " + sqle.getMessage(),
                    sqle.getMessage().contains("BMS discovery failed"));
        } finally {
            new File(fname).delete();
        }
    }

    @Test
    public void testBmsFallbackToComdb2db() throws IOException, SQLException {
        LogManager.getLogManager().reset();
        String fname = "/tmp/comdb2db.jdbc.bms.test.cfg." + System.currentTimeMillis();
        BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
        writer.write("comdb2_config:default_type=dev\n");
        writer.close();
        System.setProperty("comdb2db.cfg", fname);

        try {
            /* BMS enabled with invalid suffix, fallback enabled — should fail at comdb2db stage */
            Connection conn = DriverManager.getConnection(
                    "jdbc:comdb2://dev/testdb?bmssuffix=invalid.example.test&comdb2db_fallback=on&dnssuffix=example.com&max_retries=1");
            Assert.fail("Should not reach here");
        } catch (SQLException sqle) {
            /* After BMS fails, it falls back to comdb2db which also fails */
            Assert.assertTrue("Should fail at DNS/comdb2db stage, got: " + sqle.getMessage(),
                    sqle.getMessage().contains("Could not find database hosts"));
        } finally {
            new File(fname).delete();
        }
    }

    @Test
    public void testRoomDistanceParsing() throws IOException {
        LogManager.getLogManager().reset();
        String fname = "/tmp/comdb2db.jdbc.bms.test.cfg." + System.currentTimeMillis();
        BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
        writer.write("comdb2_config:default_type=dev\n");
        writer.write("comdb2_feature:room_distance=4:2001,3:0,2:2001,1:2001\n");
        writer.close();
        System.setProperty("comdb2db.cfg", fname);

        Comdb2Handle hndl = new Comdb2Handle();
        hndl.myDbName = "testdb";
        hndl.myDbCluster = "dev";
        hndl.useBmsd = false; /* disable BMS so we don't attempt DNS */

        try {
            DatabaseDiscovery.getDbHosts(hndl, false);
        } catch (NoDbHostFoundException e) {
            /* Expected — we just want to check config parsing */
        }

        Assert.assertNotNull("roomDistance should be parsed", hndl.roomDistance);
        Assert.assertEquals("Room 3 should have distance 0", 0, hndl.roomDistance[3]);
        Assert.assertEquals("Room 4 should have distance 2001", 2001, hndl.roomDistance[4]);
        Assert.assertEquals("Room 1 should have distance 2001", 2001, hndl.roomDistance[1]);
        Assert.assertEquals("Room 2 should have distance 2001", 2001, hndl.roomDistance[2]);
        Assert.assertEquals("Room 0 (unset) should default to MAX_VALUE",
                Integer.MAX_VALUE, hndl.roomDistance[0]);

        new File(fname).delete();
    }

    @Test
    public void testBmsSuffixFromCfgFile() throws IOException {
        LogManager.getLogManager().reset();
        String fname = "/tmp/comdb2db.jdbc.bms.test.cfg." + System.currentTimeMillis();
        BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
        writer.write("comdb2_config:default_type=dev\n");
        writer.write("comdb2_config:bmssuffix=custom.bms.example.com\n");
        writer.close();
        System.setProperty("comdb2db.cfg", fname);

        Comdb2Handle hndl = new Comdb2Handle();
        hndl.myDbName = "testdb";
        hndl.myDbCluster = "dev";
        hndl.useBmsd = false;

        try {
            DatabaseDiscovery.getDbHosts(hndl, false);
        } catch (NoDbHostFoundException e) {
            /* Expected */
        }

        Assert.assertEquals("bmssuffix should be read from config",
                "custom.bms.example.com", hndl.bmssuffix);

        new File(fname).delete();
    }

    @Test
    public void testBmsFeatureFlags() throws IOException {
        LogManager.getLogManager().reset();
        String fname = "/tmp/comdb2db.jdbc.bms.test.cfg." + System.currentTimeMillis();
        BufferedWriter writer = new BufferedWriter(new FileWriter(fname));
        writer.write("comdb2_config:default_type=dev\n");
        writer.write("comdb2_feature:use_bmsd=off\n");
        writer.write("comdb2_feature:comdb2db_fallback=off\n");
        writer.close();
        System.setProperty("comdb2db.cfg", fname);

        Comdb2Handle hndl = new Comdb2Handle();
        hndl.myDbName = "testdb";
        hndl.myDbCluster = "dev";

        try {
            DatabaseDiscovery.getDbHosts(hndl, false);
        } catch (NoDbHostFoundException e) {
            /* Expected */
        }

        Assert.assertFalse("use_bmsd should be off", hndl.useBmsd);
        Assert.assertFalse("comdb2db_fallback should be off", hndl.comdb2dbFallback);

        new File(fname).delete();
    }

    @After
    public void tearDown() {
        System.clearProperty("comdb2db.cfg");
    }
}
