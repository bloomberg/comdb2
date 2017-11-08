package com.bloomberg.comdb2;

import com.bloomberg.comdb2.jdbc.*;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.jcraft.jsch.*;

public class InsertTest
{

    private enum ResultCode {
        UNSET,
        OK,
        FAILED,
        CHECKED,
        RECOVERED,
        LOST,
        UNKNOWN
    }
    ;
    private Connection conn;
    private String cluster;
    private String dbname;
    private boolean debugTrace = false;
    private boolean testTrace = false;
    private boolean selectTest = false;
    private int selectRecords = 10000;
    private int insertsForT1 = 1000;
    private int increment;
    private int runTime;
    private long startTime;
    private int numThreads = 10;
    private Thread threads[];
    private ResultCode results[];
    private int resultsRc[];
    private String resultString[];
    private static ReentrantLock lock;
    private int current;
    private boolean partitionNet = true;
    private ArrayList<String> hosts = null;
    private ArrayList<Integer> ports = null;
    private ArrayList<Boolean> broken = null;

    // Test options
    private boolean isHASql = true;
    private String txnLevel = "serializable";

    private int remoteCommand(String host, String cmd)
        throws JSchException, IOException
    {
        System.out.println("Running " + host + ":" + cmd);
        int exitrc = -1;
        JSch jsch = new JSch();
        Map<String, String> env = System.getenv();
        String user = env.get("CDB2JDBC_TEST_SSH_USER");
        if (user == null) user = System.getProperty("user.name");
        Session session = jsch.getSession(user, host, 22);
        String pw = env.get("CDB2JDBC_TEST_SSH_PASSWORD");
        if (pw != null) session.setPassword(pw);
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect();
        Channel channel = session.openChannel("exec");
        ((ChannelExec)channel).setCommand(cmd);
        channel.setInputStream(null);
        ((ChannelExec)channel).setErrStream(System.err);
        InputStream in = channel.getInputStream();
        channel.connect();
        byte[] tmp = new byte[1024];
        while (true) {
            while (in.available() > 0) {
                int i = in.read(tmp, 0, 1024);
                if (i < 0) break;
                System.out.println(new String(tmp, 0, i));
            }
            if (channel.isClosed()) {
                exitrc = channel.getExitStatus();
                System.out.println("rc=" + exitrc);
                break;
            }
            try {
                Thread.sleep(200);
            } catch (Exception ee) {
                // System.out.println(ee);
            }
        }
        channel.disconnect();
        session.disconnect();
        return exitrc;
    }

    private void fixNet()
    {
        for (int count = 0; count < 2; count++) {
            for (int i = 0; i < this.hosts.size(); i++) {
                String cmd = new String("");
                for (int j = 0; j < this.hosts.size(); j++) {
                    cmd += "sudo iptables -D INPUT -s " + this.hosts.get(j) +
                           " -p tcp --destination-port " + this.ports.get(j) +
                           " -j DROP -w; ";
                    cmd += "sudo iptables -D INPUT -s " + this.hosts.get(j) +
                           " -p udp --destination-port " + this.ports.get(j) +
                           " -j DROP -w; ";
                }
                try {
                    remoteCommand(this.hosts.get(i), cmd);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        }
    }

    private void breakNet()
    {
        if (this.hosts.size() <= 1) {
            System.out.println("Cannot break the net with less than one node");
            return;
        }
        int numPartitioned = 0, count = 0;

        Random rn = new Random(System.currentTimeMillis());
        while (numPartitioned < (this.hosts.size() / 2) && count < 1000) {
            int newidx = rn.nextInt(this.hosts.size());
            if (this.broken.get(newidx) == false) {
                this.broken.set(newidx, true);
                numPartitioned++;
            }
            count++;
        }

        for (int i = 0; i < this.hosts.size(); i++) {
            if (this.broken.get(i)) {
                String cmd = new String("");
                for (int j = 0; j < this.hosts.size(); j++) {
                    if (this.broken.get(j) == false) {

                        System.out.println(
                            "Dereferencing idx " + j + ", hosts.size()=" +
                            this.hosts.size() + ", ports.size()=" +
                            this.ports.size());

                        cmd += "sudo iptables -A INPUT -s " +
                               this.hosts.get(j) +
                               " -p tcp --destination-port " +
                               this.ports.get(j) + " -j DROP -w; ";
                        cmd += "sudo iptables -A INPUT -s " +
                               this.hosts.get(j) +
                               " -p udp --destination-port " +
                               this.ports.get(j) + " -j DROP -w; ";
                    }
                }
                try {
                    remoteCommand(this.hosts.get(i), cmd);
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        }
    }

    private String stateString(ResultCode val)
    {
        switch (val) {
        case UNSET: return new String("UNSET");
        case OK: return new String("OK-NOT-CHECKED");
        case FAILED: return new String("FAILED");
        case CHECKED: return new String("OK");
        case RECOVERED: return new String("RECOVERED");
        case LOST: return new String("LOST");
        case UNKNOWN: return new String("UNKNOWN");
        }
        return new String("**BROKEN** : " + val);
    }

    private void printTestResults()
    {
        int target = current - 1;
        System.out.println("Largest value inserted: " + target);
        int stidx = 0;
        String statestr = stateString(results[stidx]);

        for (int i = 0; i < current; i++) {
            if (results[i] != results[stidx]) {
                target = i - 1;
                System.out.println(stidx + " .. " + target + " : " + statestr);
                stidx = i;
                statestr = stateString(results[i]);
            }
        }

        if (stidx != current) {
            int tg = current - 1;
            System.out.println(stidx + " .. " + tg + " " + statestr);
        }

        System.out.flush();
    }

    private void checkTestResults() throws SQLException, ClassNotFoundException
    {
        Comdb2Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        int curRecord = 0;
        boolean gotResults = false;

        do {
            try {
                Class.forName("com.bloomberg.comdb2.jdbc.Driver");
                conn = (Comdb2Connection)DriverManager.getConnection(
                    String.format("jdbc:comdb2:%s:%s", dbname, cluster));
                if (debugTrace) conn.setDebug("true");
                conn.setMaxRetries(100000);
                stmt = conn.createStatement();
                stmt.execute("set transaction " + txnLevel);
                if (isHASql) {
                    stmt.execute("set hasql on");
                }
                rs = stmt.executeQuery(
                    "select value from jepsen order by value");
                while (rs.next()) {
                    int val = rs.getInt(1);
                    if (val < 0 || val >= current) {
                        System.out.println("Unexpected value " + val);
                    } else if (results[val] != ResultCode.OK) {
                        String state = stateString(results[val]);
                        System.out.println(state + " result for " + val +
                                           " dbrc=" + resultsRc[val] +
                                           " dbErr='" + resultString[val] +
                                           "' changed to RECOVERED");

                        results[val] = ResultCode.RECOVERED;
                    } else {
                        results[val] = ResultCode.CHECKED;
                    }
                }
                gotResults = true;
            } catch (SQLException e) {
                System.out.println("Error retrieving results: " + e +
                                   " retrying");
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
                gotResults = false;
                try {
                    Thread.sleep(1000);
                } catch (Exception ee) {
                }
            }
        } while (gotResults == false);

        for (int i = 0; i < current; i++) {
            if (results[i] == ResultCode.OK) {
                System.out.println("XXX lost value " + i);
                results[i] = ResultCode.LOST;
            }
        }
        rs.close();
        stmt.close();
        conn.close();
    }

    public void testBreakNet()
        throws InterruptedException, SQLException, ClassNotFoundException
    {

        Class.forName("com.bloomberg.comdb2.jdbc.Driver");
        Comdb2Connection conn = (Comdb2Connection)DriverManager.getConnection(
            String.format("jdbc:comdb2:%s:%s", dbname, cluster));
        if (debugTrace) conn.setDebug("true");
        conn.setMaxRetries(100000);
        Statement stmt = conn.createStatement();
        stmt.execute("select 1");

        try {
            int count = 0;
            boolean breakLoop = false;
            do {
                this.hosts = conn.getDbHosts();
                this.ports = conn.getDbPorts();
                if (this.hosts.size() == 0 ||
                    (this.hosts.size() != ports.size())) {
                    System.out.println("hosts.size()=" + this.hosts.size() +
                                       ", ports.size()=" + this.ports.size());
                    System.out.println("hosts=" + this.hosts +
                                       ", ports.size()=" + this.ports);
                    Thread.sleep(1000);
                } else {
                    breakLoop = true;
                }
                if (count++ > 20) {
                    System.out.println("Exiting : hosts.size() is " +
                                       this.hosts.size() + " ports.size() is " +
                                       this.ports.size());
                    System.exit(-1);
                }

            } while (breakLoop == false);

            this.broken = new ArrayList<Boolean>(this.hosts.size());
            for (int i = 0; i < this.hosts.size(); i++) {
                this.broken.add(false);
            }
        } catch (NoDbHostFoundException e) {
            System.out.println(e);
        }

        breakNet();
        Thread.sleep(10000);
        fixNet();
    }

    private void deleteTest()
        throws InterruptedException, SQLException, ClassNotFoundException
    {
        Class.forName("com.bloomberg.comdb2.jdbc.Driver");
        Comdb2Connection conn = (Comdb2Connection)DriverManager.getConnection(
            String.format("jdbc:comdb2:%s:%s", dbname, cluster));
        if (debugTrace) conn.setDebug("true");
        conn.setMaxRetries(100000);
        Comdb2Handle hndl;
        Statement stmt = conn.createStatement();
        stmt.execute("set transaction " + txnLevel);
        if (isHASql) {
            stmt.execute("set hasql on");
        }
        System.out.println("Delete test");
        conn.setAutoCommit(true);
        int rows = 0;
        String sql =
            "delete from t1 where a >= " + selectRecords + " limit 2000";

        try {
            rows = stmt.executeUpdate(sql);
        } catch (SQLException e) {
            conn.rollback();
            System.out.println("XXX td=" + Thread.currentThread().getId() +
                               sql + " returns " + e);
        }
        hndl = conn.dbHandle();
        System.out.println("jdbc-rows reported deleted=" + rows + " rows");
        System.out.println("comdb2Handle rowsAffected=" + hndl.rowsAffected());
        System.out.println("comdb2Handle rowsDeleted=" + hndl.rowsDeleted());
        System.out.println("comdb2Handle rowsInserted=" + hndl.rowsInserted());
        System.out.println("comdb2Handle rowsUpdated=" + hndl.rowsUpdated());
    }

    private void prepareSelectTest()
        throws InterruptedException, SQLException, ClassNotFoundException
    {
        Class.forName("com.bloomberg.comdb2.jdbc.Driver");
        Comdb2Connection conn = (Comdb2Connection)DriverManager.getConnection(
            String.format("jdbc:comdb2:%s:%s", dbname, cluster));
        if (debugTrace) conn.setDebug("true");
        conn.setMaxRetries(100000);
        Statement stmt = conn.createStatement();
        stmt.execute("set transaction " + txnLevel);
        if (isHASql) {
            stmt.execute("set hasql on");
        }

        System.out.println("Preparing select test");

        conn.setAutoCommit(true);

        // Step 1: delete all records above selectRecords
        String sql =
            "delete from t1 where a >= " + selectRecords + " limit 2000";
        boolean keepLooping = true;
        int rows = 0;
        do {
            try {
                rows = stmt.executeUpdate(sql);
            } catch (SQLException e) {
                conn.rollback();
                System.out.println("XXX td=" + Thread.currentThread().getId() +
                                   sql + " returns " + e);
            }
            System.out.println("Deleted " + rows + " rows");
        } while (rows > 0);

        conn.setAutoCommit(false);

        boolean[] haveRecords = new boolean[selectRecords];
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery("select a from t1 order by a");

            while (rs.next()) {
                int val = rs.getInt(1);
                if (val < 0 || val >= selectRecords) {
                    System.out.println(
                        "XXX Unexpected value when filling t1: " + val);
                } else {
                    haveRecords[val] = true;
                }
            }
            rs.close();
            rs = null;
            conn.rollback();
        } catch (SQLException e) {
            System.out.println("XXX td=" + Thread.currentThread().getId() +
                               "'select a from t1 order by a' rCode=" +
                               e.getErrorCode() + " errorString='" +
                               e.getMessage() + "'");

            conn.rollback();
            if (rs != null) {
                rs.close();
                rs = null;
            }
        }

        boolean needInsert = false;
        for (int i = 0; i < selectRecords; i++) {
            if (haveRecords[i] == false) {
                needInsert = true;
                break;
            }
        }

        if (needInsert) {
            int insertedThisTxn = 0;
            try {
                for (int i = 0; i < selectRecords; i++) {
                    if (haveRecords[i] == false) {
                        sql = "insert into t1 (a) values (" + i + ")";
                        stmt.execute(sql);
                        insertedThisTxn++;
                        if (insertedThisTxn >= insertsForT1) {
                            conn.commit();
                            insertedThisTxn = 0;
                        }
                    }
                }
                if (insertedThisTxn > 0) {
                    conn.commit();
                    insertedThisTxn = 0;
                }
            } catch (SQLException e) {
                System.out.println("XXX td=" + Thread.currentThread().getId() +
                                   " sql='" + sql + "' rCode=" +
                                   e.getErrorCode() + " errorString='" +
                                   e.getMessage() + "'");

                conn.rollback();
            }
        }
        conn.close();
        stmt.close();
    }

    public void runInsertTest()
        throws InterruptedException, SQLException, ClassNotFoundException
    {
        threads = new Thread[numThreads];
        Statement stmt;

        Class.forName("com.bloomberg.comdb2.jdbc.Driver");
        Comdb2Connection conn = (Comdb2Connection)DriverManager.getConnection(
            String.format("jdbc:comdb2:%s:%s", dbname, cluster));
        if (debugTrace) conn.setDebug("true");
        conn.setMaxRetries(100000);
        stmt = conn.createStatement();
        stmt.execute("select 1");

        if (selectTest) {
            prepareSelectTest();
        }

        try {
            boolean breakLoop = false;
            int count = 0;
            do {
                this.hosts = conn.getDbHosts();
                this.ports = conn.getDbPorts();
                if (hosts.size() == 0 || (hosts.size() != ports.size())) {
                    System.out.println("hosts.size()=" + this.hosts.size() +
                                       ", ports.size()=" + this.ports.size());
                    System.out.println("hosts=" + this.hosts +
                                       ", ports.size()=" + this.ports);
                    Thread.sleep(1000);
                } else {
                    breakLoop = true;
                }
                if (count++ > 20) {
                    System.out.println("Exiting : hosts.size() is " +
                                       this.hosts.size() + " ports.size() is " +
                                       this.ports.size());
                    System.exit(-1);
                }

            } while (breakLoop == false);

            this.broken = new ArrayList<Boolean>(this.hosts.size());
            for (int i = 0; i < this.hosts.size(); i++) {
                this.broken.add(false);
            }
        } catch (NoDbHostFoundException e) {
            System.out.println(e);
        }

        conn.close();

        if (this.hosts == null && partitionNet) {
            System.err.println("Could not retrieve hosts for test");
            return;
        }

        results = new ResultCode[20 * runTime];
        for (int i = 0; i < 20 * runTime; i++) {
            results[i] = ResultCode.UNSET;
        }
        resultsRc = new int[20 * runTime];
        resultString = new String[20 * runTime];

        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new InsertThread());
        }

        startTime = System.currentTimeMillis();

        for (int i = 0; i < numThreads; i++) {
            threads[i].start();
        }

        if (partitionNet) {
            Thread.sleep(runTime / 3);
            breakNet();
            Thread.sleep(runTime / 3);
            fixNet();
        }

        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }

        Thread.sleep(5000);
        checkTestResults();
        printTestResults();
    }

    public void setTransactionLevel(String txnLevel)
    {
        this.txnLevel = txnLevel;
    }

    public void setThreads(int count)
    {
        this.numThreads = count;
    }

    public void setIncrement(int increment)
    {
        this.increment = increment;
    }

    public void setRuntime(int runTime)
    {
        this.runTime = runTime;
    }

    public InsertTest(String dbname, String cluster, int runTime, int increment)
    {
        this.dbname = dbname;
        this.cluster = cluster;
        this.runTime = runTime;
        this.increment = increment;
        this.current = 0;
        this.lock = new ReentrantLock();
    }

    public InsertTest(String dbname, String cluster)
    {
        this(dbname, cluster, 60000, 1);
    }

    public InsertTest()
    {
        Map<String, String> env = System.getenv();

        this.dbname = env.get("CDB2JDBC_TEST_DBNAME");
        if (this.dbname == null) {
            this.dbname = new String("marktdb");
        }

        this.cluster = env.get("CDB2JDBC_TEST_CLUSTER");
        if (this.cluster == null) {
            this.cluster = new String("dev");
        }

        this.increment = 1;
        String stinc = env.get("CDB2JDBC_TEST_INCREMENT");
        if (stinc != null) {
            this.increment = Integer.parseInt(stinc);
        }

        this.runTime = 60000;
        String ctrt = env.get("CDB2JDBC_TEST_RUNTIME");
        if (ctrt != null) {
            this.runTime = Integer.parseInt(ctrt);
        }

        String sTest = env.get("CDB2JDBC_SELECT_TEST");
        if (sTest != null) {
            this.selectTest = true;
        }

        String dTrace = env.get("CDB2JDBC_DEBUG_TRACE");
        if (dTrace != null) {
            this.debugTrace = true;
        }

        String tTrace = env.get("CDB2JDBC_TEST_TRACE");
        if (tTrace != null) {
            this.testTrace = true;
        }

        this.current = 0;
        this.lock = new ReentrantLock();
    }

    private class InsertThread implements Runnable
    {

        private void insertRecords(Connection conn) throws SQLException
        {
            int start, rcode;

            lock.lock();
            start = current;
            current = current + increment;
            lock.unlock();
            Statement stmt = conn.createStatement();

            for (int i = start; i < (start + increment); i++) {

                String sql = "insert into jepsen(id, value) values (" + i +
                             "," + i + ")";
                if (testTrace)
                    System.out.println("td=" + Thread.currentThread().getId() +
                                       " " + sql);
                try {
                    stmt.execute(sql);
                } catch (SQLException e) {
                    conn.rollback();
                    System.out.println("td=" + Thread.currentThread().getId() +
                                       " Insert for " + i + " returns " + e);
                    results[i] = ResultCode.FAILED;
                    resultsRc[i] = e.getErrorCode();
                    resultString[i] = e.getMessage();
                    stmt.close();
                    return;
                }
            }

            if (selectTest) {
                System.out.println("td=" + Thread.currentThread().getId() +
                                   " select test");
                String sql = "select a from t1 order by a";
                ResultSet rs = null;
                try {
                    stmt.execute(sql);
                    rs = stmt.executeQuery(sql);
                    int expected = 0;
                    while (rs.next()) {
                        int val = rs.getInt(1);
                        if (val != expected) {
                            System.out.println(
                                "XXX td=" + Thread.currentThread().getId() +
                                " unexpected value from select, wanted " +
                                expected + " but got " + val);
                        }
                        expected = (val + 1);
                    }
                    if (expected != selectRecords) {
                        System.out.println(
                            "XXX td=" + Thread.currentThread().getId() +
                            " expected max-record of " + selectRecords +
                            " but got " + expected);
                    }

                } catch (SQLException e) {
                    conn.rollback();
                    System.out.println("XXX td=" +
                                       Thread.currentThread().getId() +
                                       " select-test error:" + e);
                }

                if (rs != null) {
                    rs.close();
                    rs = null;
                }
            }

            try {
                int target = (start + increment) - 1;
                conn.commit();
                if (testTrace)
                    System.out.println("td=" + Thread.currentThread().getId() +
                                       " Executed commit for inserts " + start +
                                       " - " + target);
                for (int i = start; i < (start + increment); i++) {
                    results[i] = ResultCode.OK;
                    resultsRc[i] = 0;
                    resultString[i] = null;
                }
            } catch (SQLException e) {
                System.out.println("td=" + Thread.currentThread().getId() +
                                   " Commit rCode=" + e.getErrorCode() +
                                   " errorString='" + e.getMessage() + "'");
                conn.rollback();
                for (int i = start; i < (start + increment); i++) {
                    resultsRc[i] = e.getErrorCode();
                    if (resultsRc[i] == -109) {
                        results[i] = ResultCode.UNKNOWN;
                    } else {
                        results[i] = ResultCode.FAILED;
                    }
                    resultString[i] = e.getMessage();
                }
            }
            stmt.close();
        }

        private void runInsertThread()
            throws SQLException, ClassNotFoundException
        {
            long target = startTime + runTime, now;
            Comdb2Connection conn;
            Statement stmt;
            Class.forName("com.bloomberg.comdb2.jdbc.Driver");
            conn = (Comdb2Connection)DriverManager.getConnection(
                String.format("jdbc:comdb2:%s:%s", dbname, cluster));
            if (debugTrace) conn.setDebug("true");
            conn.setMaxRetries(100000);
            stmt = conn.createStatement();
            stmt.execute("set transaction " + txnLevel);
            if (isHASql) {
                stmt.execute("set hasql on");
            }
            conn.setAutoCommit(false);
            while ((now = System.currentTimeMillis()) < target) {
                insertRecords(conn);
            }
        }

        public void run()
        {
            try {
                runInsertThread();
            } catch (SQLException e) {
                System.out.println(e);
            } catch (ClassNotFoundException e) {
                System.out.println(e);
            }
        }
    }
}
