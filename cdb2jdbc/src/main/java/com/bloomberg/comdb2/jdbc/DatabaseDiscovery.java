/* Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */
package com.bloomberg.comdb2.jdbc;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.net.*;

import com.bloomberg.comdb2.jdbc.Cdb2DbInfoResponse.NodeInfo;
import com.bloomberg.comdb2.jdbc.Cdb2Query;
import com.bloomberg.comdb2.jdbc.Cdb2Query.Cdb2DbInfo;
import com.bloomberg.comdb2.jdbc.Cdb2Query.Cdb2SqlQuery;

/**
 * DatabaseDiscovery discovers database location from comdb2db or config files.
 * 
 * @author Rivers Zhang
 * @author Mohit Khullar
 */
public class DatabaseDiscovery {
    private static Logger logger = Logger.getLogger(DatabaseDiscovery.class.getName());
    static boolean debug = false;

    /**
     * Comdb2db configuration files.
     */
    static final String CDB2DBCONFIG_PROP = "comdb2db.cfg";
    static final String CDB2DBCONFIG_LOCAL = "/bb/bin/comdb2db.cfg";
    static final String CDB2DBCONFIG_NOBBENV = "/opt/bb/etc/cdb2/config/comdb2db.cfg";
    static final String CDB2DBCONFIG_NOBBENV_PATH = "/opt/bb/etc/cdb2/config.d/";

    /* comdb2lcldb, per jvm. */
    static class TimeAndHosts {
        long ms;
        ArrayList<String> hosts = new ArrayList<String>();
    }
    static HashMap<String, TimeAndHosts> comdb2lcldb = new HashMap<String, TimeAndHosts>();
    /* Make it visible to us only. */
    private static Object lock = new Object();

    /**
     * Reads information from comdb2db cfg file. Returns true if the minimal
     * necessary information has been gathered. Otherwise, returns false.
     * 
     * @param path
     * @param hndl
     * @return
     */
    private static boolean readComdb2dbCfg(String path, Comdb2Handle hndl) {

        boolean ret = true;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(path));

            String line;
            while ((line = br.readLine()) != null) {

                String[] tokens = line.split(":\\s*|=\\s*|,\\s*|\\s+");

                if (tokens.length < 3)
                    continue;

                if (tokens[0].equalsIgnoreCase(hndl.myDbName)) {
                    /**
                     * Gets dunumber and hosts of the actual database.
                     */
                    hndl.myDbNum = Integer.parseInt(tokens[1]);
                    for (int i = 2; i < tokens.length; ++i) {
                        hndl.myDbHosts.add(tokens[i]);
                        hndl.myDbPorts.add(hndl.overriddenPort);
                    }
                } else if (tokens[0].equalsIgnoreCase("comdb2_config")) {

                    if (tokens[1].equalsIgnoreCase("default_type")
                            && hndl.defaultType == null)
                        hndl.defaultType = tokens[2];
                    else if (tokens[1].equalsIgnoreCase("room")
                            && hndl.machineRoom == null)
                        hndl.machineRoom = tokens[2];
                    else if ((tokens[1].equalsIgnoreCase("portmuxport") ||
                              tokens[1].equalsIgnoreCase("pmuxport")) &&
                             !hndl.hasUserPort)
                        hndl.portMuxPort = Integer.parseInt(tokens[2]);
                    else if (tokens[1].equalsIgnoreCase("comdb2dbname")
                            && hndl.comdb2dbName == null)
                        hndl.comdb2dbName = tokens[2];
                    else if (tokens[1].equalsIgnoreCase("tcpbufsz")
                            && !hndl.hasUserTcpSz)
                        try {
                            hndl.tcpbufsz = Integer.parseInt(tokens[2]);
                        } catch (NumberFormatException e) {
                            logger.log(Level.WARNING, "Invalid tcp buffer size.", e);
                        }
                    else if (tokens[1].equalsIgnoreCase("dnssufix")
                            && hndl.dnssuffix == null)
                        hndl.dnssuffix = tokens[2];
                    else if (tokens[1].equalsIgnoreCase("connect_timeout")
                            && !hndl.hasConnectTimeout) {
                        try {
                            hndl.connectTimeout = Integer.parseInt(tokens[2]);
                        } catch (NumberFormatException e) {
                            logger.log(Level.WARNING, "Invalid connect timeout.", e);
                        }
                    }
                    else if (tokens[1].equalsIgnoreCase("comdb2db_timeout")
                            && !hndl.hasComdb2dbTimeout) {
                        try {
                            hndl.comdb2dbTimeout = Integer.parseInt(tokens[2]);
                        } catch (NumberFormatException e) {
                            logger.log(Level.WARNING, "Invalid comdb2db timeout.", e);
                        }
                    }
                    else if (tokens[1].equalsIgnoreCase("stack_at_open")
                            && !hndl.hasSendStack) {
                        hndl.sendStack = tokens[2].equalsIgnoreCase("true");
                    }
                } else if (tokens[0].equalsIgnoreCase(hndl.comdb2dbName)) {
                    /**
                     * Gets dbnumber and hosts of comdb2db.
                     */
                    ret = true;
                    hndl.comdb2dbDbNum = Integer.parseInt(tokens[1]);
                    for (int i = 2; i != tokens.length; ++i)
                        hndl.comdb2dbHosts.add(tokens[i]);
                }
            }

        } catch (IOException e) {
            ret = false;
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                logger.log(Level.WARNING, "Unable to close stream", e);
            }
        }
        return ret;
    }

    private final static String COMDB2DB = "comdb2db";

    /* Get comdb2db hosts from config file or DNS.
       Throws an IOException on error. */
    private static void getComdb2dbHosts(Comdb2Handle hndl,
            boolean just_defaults) throws IOException {
        /*
         * Load conf from path specified in system property
         * CDB2DBCONFIG_PROP (comdb2db.cfg), defaulting to
         * CDB2DBCONFIG_NOBBENV (/opt/bb/etc/cdb2/config/comdb2db.cfg) if the
         * property is not specified.
         */
        String configPath = System.getProperty(CDB2DBCONFIG_PROP, CDB2DBCONFIG_NOBBENV);
        boolean rc = readComdb2dbCfg(configPath, hndl);
        if (!rc) /* fall back to /bb/bin if noenv conf not found */
            rc = readComdb2dbCfg(CDB2DBCONFIG_LOCAL, hndl);
        readComdb2dbCfg(CDB2DBCONFIG_NOBBENV_PATH + hndl.myDbName + ".cfg", hndl);

        if (just_defaults)
            return;

        if (hndl.comdb2dbHosts.size() > 0 || hndl.myDbHosts.size() > 0)
            return;

        String comdb2db_bdns = String.format("%s-%s.%s",
                (hndl.defaultType != null) ? hndl.defaultType : hndl.myDbCluster,
                hndl.comdb2dbName, hndl.dnssuffix);
        InetAddress inetAddress[] = InetAddress.getAllByName(comdb2db_bdns);
        for (int i = 0; i < inetAddress.length; i++)
            hndl.comdb2dbHosts.add(inetAddress[i].getHostAddress());
    }

    /* Returns port number. Throws an IOException on error. */
    private static int getPortMux(String host, int port,
            int soTimeout, int connectTimeout,
            String app, String service, String instance) throws IOException {
        SockIO io = null;
        try {
            String name = String.format("get %s/%s/%s\n", app, service, instance);

            io = new SockIO(host, port, null, soTimeout, connectTimeout);

            io.write(name.getBytes());
            io.flush();

            String line = io.readLine(32);
            return Integer.parseInt(line);
        } finally {
            try {
                if (io != null)
                    io.close();
            } catch (IOException e) {
                /* ignore */
            }
        }
    }

    /* Get dbinfo. Return the index of the master node in validHosts.
       Return -1 if no master. Throw an IOException on error. */
    static int dbInfoQuery(Comdb2Handle hndl,
            Cdb2DbInfoResponse dbInfoResp,
            String dbName, int dbNum, String host, int port,
            List<String> validHosts, List<Integer> validPorts)
        throws IOException {

        SockIO io = null;

        validHosts.clear();
        validPorts.clear();

        try {
            if (dbInfoResp == null) {
                io = new SockIO(host, port, hndl.pmuxrte ? hndl.myDbName : null,
                                hndl.dbinfoTimeout, hndl.connectTimeout);

                io.write("newsql\n");
                io.flush();

                Cdb2Query query = new Cdb2Query();
                query.cdb2DbInfo = new Cdb2DbInfo(dbName);

                Protocol protobuf = new ProtobufProtocol();
                int length = protobuf.pack(query);

                NewSqlHeader nsh = new NewSqlHeader(
                        Constants.RequestType.CDB2_REQUEST_CDB2QUERY,
                        0, 0, length);

                io.write(nsh.toBytes());
                protobuf.write(io.getOut());
                io.flush();

                byte[] res = new byte[nsh.capacity()];
                if (io.read(res) != nsh.capacity())
                    throw new IOException("Received fewer bytes than expected");
                nsh.reconstructFromBytes(res);

                res = null;
                res = new byte[nsh.length];
                if (io.read(res) != nsh.length)
                    throw new IOException("Received fewer bytes than expected");

                dbInfoResp = protobuf.unpackDbInfoResp(res);
                if (dbInfoResp == null)
                    throw new IOException("Received malformed data");
            }

            int master = -1;
            int myroom = 0, hosts_same_room = 0;
            if (debug) {
                System.out.println("dbinfoQuery: dbinfo response " +
                        "returns " + dbInfoResp.nodes);
            }

            /* Add coherent nodes. */
            for (NodeInfo node : dbInfoResp.nodes) {
                if (node.incoherent != 0) {
                    if (debug) {
                        System.out.println("dbinfoQuery: Skipping " +
                                node.name + ": incoherent=" + 
                                node.incoherent + " port=" + node.port);
                    }
                    continue;
                }

                validHosts.add(node.name);
                validPorts.add(node.port);

                if (debug) {
                    System.out.println("Added " + node.name + ":" + 
                            node.port + " to validHosts");
                }

                if (myroom == 0)
                    myroom = (node.room) > 0 ? node.room : -1;

                if (node.name.equalsIgnoreCase(dbInfoResp.master.name))
                    master = validHosts.size() - 1;

                if (myroom == node.room)
                    ++hosts_same_room;
            }

            /* Add incoherent nodes too, but don't count them for same room hosts. */
            for (NodeInfo node : dbInfoResp.nodes) {
                if (node.incoherent == 0)
                    continue;

                validHosts.add(node.name);
                validPorts.add(node.port);

                if (node.name.equalsIgnoreCase(dbInfoResp.master.name))
                    master = validHosts.size() - 1;
            }

            if (validHosts.size() <= 0)
                throw new IOException("Received incomplete dbinfo response");

            if (hndl != null) {
                hndl.masterIndexInMyDbHosts = master;
                hndl.numHostsSameRoom = hosts_same_room;
                hndl.peersslmode = dbInfoResp.peermode;
            }

            return master;

        } finally {
            try {
                if (io != null)
                    io.close();
            } catch (IOException e) {
                /* Ignore */
            }
        }
    }

    /* Gets database's name, number and room by querying a comdb2db server. 
       Throws an IOException on error. */
    private static void queryDbHosts(Comdb2Handle hndl,
            String host, int port) throws IOException {

        String sqlquery = String.format("select M.name, D.dbnum, M.room from machines M join databases D where M.cluster IN "
                + "(select cluster_machs from clusters where name = '%s' and cluster_name = '%s') and D.name = '%s' order by (room = '%s') desc",
                hndl.myDbName, hndl.myDbCluster, hndl.myDbName, hndl.machineRoom);

        SockIO io = null;

        try {
            io = new SockIO(host, port, hndl.pmuxrte ? hndl.myDbName : null,
                            hndl.comdb2dbTimeout, hndl.connectTimeout);

            io.write("newsql\n");
            io.flush();

            Cdb2Query query = new Cdb2Query();
            query.cdb2SqlQuery = new Cdb2SqlQuery(hndl.comdb2dbName, sqlquery);

            Protocol protobuf = new ProtobufProtocol();
            int length = protobuf.pack(query);

            NewSqlHeader nsh = new NewSqlHeader(Constants.RequestType.CDB2_REQUEST_CDB2QUERY, 0, 0, length);

            io.write(nsh.toBytes());
            protobuf.write(io.getOut());
            io.flush();

            byte[] res = new byte[nsh.capacity()];

            do {
                if (io.read(res) != NewSqlHeader.BYTES_NEEDED)
                    throw new IOException("Received fewer bytes than expected");
                nsh = NewSqlHeader.fromBytes(res);
            } while (nsh == null || nsh.length == 0); /* if heartbeat packet, try again */

            nsh.reconstructFromBytes(res);

            res = null;
            res = new byte[nsh.length];

            if (io.read(res) != nsh.length)
                throw new IOException("Received fewer bytes than expected");

            Cdb2SqlResponse sqlResp = protobuf.unpackSqlResp(res);
            if (sqlResp == null || sqlResp.errCode != 0
                    || (sqlResp.respType != 1 && sqlResp.value.size() != 1 &&
                        (sqlResp.value.get(0).type == -1 || sqlResp.value.get(0).type != 3)))
                throw new IOException("Received malformed data");

            hndl.myDbHosts.clear();

            while (sqlResp.respType <= 2) {
                res = null;
                res = new byte[nsh.capacity()];
                if (io.read(res) != nsh.capacity())
                    throw new IOException("Received fewer bytes than expected");
                nsh.reconstructFromBytes(res);

                res = null;
                res = new byte[nsh.length];
                if (io.read(res) != nsh.length)
                    throw new IOException("Received fewer bytes than expected");

                sqlResp = protobuf.unpackSqlResp(res);
                if (sqlResp.errCode != 0)
                    break;
                if (sqlResp.respType == 2 && sqlResp.value != null) {
                    int pos_zero = 0;
                    byte[] valData = sqlResp.value.get(0).value;
                    for (; pos_zero < valData.length && valData[pos_zero] > 0; ++pos_zero)
                        ;

                    hndl.myDbHosts.add(new String(valData, 0, pos_zero));
                    hndl.myDbPorts.add(-1);

                    valData = null;
                }
            }

            /* if the handle requests to cache the information, do it now. */
            if (hndl.age != 0) {
                TimeAndHosts record = new TimeAndHosts();
                record.ms = (hndl.age > 0) ?
                    System.currentTimeMillis() + hndl.age * 1000L :
                    Long.MAX_VALUE;
                record.hosts.addAll(hndl.myDbHosts);
                synchronized(lock) {
                    comdb2lcldb.put(hndl.myDbName + "/" + hndl.myDbCluster, record);
                }
            }
        } finally {
            try {
                if (io != null)
                    io.close();
            } catch (IOException e) {
                /* Ignore. */
            }
        }
    }

    /**
     * Gets the database server host(s) and port(s) of @hndl.
     * 
     * @param hndl
     * @throws NoDbHostFoundException
     */
    static void getDbHosts(Comdb2Handle hndl, boolean refresh) throws NoDbHostFoundException {
        if (debug) { 
            System.out.println("Starting getDbHosts"); 
        }
        if (refresh) {
            /* Clear node info of both the database and comdb2db */
            hndl.comdb2dbHosts.clear();
            hndl.myDbHosts.clear();
            hndl.myDbPorts.clear();
            /* Invalidate db host cache as well. */
            comdb2lcldb.remove(hndl.myDbName + "/" + hndl.myDbCluster);
        }

        if (hndl.myDbCluster.equalsIgnoreCase("local")) {
			/* type is local */
            hndl.isDirectCpu = true;
            hndl.myDbHosts.add("localhost");
            hndl.myDbPorts.add(hndl.overriddenPort);
        } else if ( hndl.myDbCluster.equalsIgnoreCase("default")
                || hndl.myDbCluster.equalsIgnoreCase("dev")
                || hndl.myDbCluster.equalsIgnoreCase("uat")
                || hndl.myDbCluster.equalsIgnoreCase("alpha")
                || hndl.myDbCluster.equalsIgnoreCase("beta")
                || hndl.myDbCluster.equalsIgnoreCase("prod")) {
            hndl.isDirectCpu = false;

            /* if the handle asks for cache, do it now. */
            if (hndl.age != 0) {
                synchronized(lock) {
                    TimeAndHosts record = comdb2lcldb.get(
                            hndl.myDbName + "/" + hndl.myDbCluster);
                    if (record != null) { /* found */
                        long currtm = System.currentTimeMillis();
                        if (currtm <= record.ms) { /* not expired */
                            hndl.myDbHosts.addAll(record.hosts);
                            if (debug) { 
                                System.out.println("Found " + hndl.myDbName + " in hash" 
                                        + record.hosts + " port is overridden:" +hndl.overriddenPort); 
                            }
                            for (int i = 0, len = hndl.myDbHosts.size(); i != len; ++i)
                                hndl.myDbPorts.add(hndl.overriddenPort);
                        }
                    }
                }
            }

            if (hndl.myDbHosts.size() == 0) {
                try {
                    /* get default conf without DNS lookup */
                    getComdb2dbHosts(hndl, true);
                } catch (IOException ioe) {
                    /* Ignore. */
                }

                /* revise comdb2db_name */
                if (hndl.comdb2dbName == null)
                    hndl.comdb2dbName = COMDB2DB;

                /* set cluster type if default */
                if (hndl.myDbHosts.size() == 0 &&
                    hndl.myDbCluster.equalsIgnoreCase("default")) {
                    if (hndl.defaultType == null)
                        throw new NoDbHostFoundException(hndl.myDbName,
                                "No default type configured.");
                    hndl.myDbCluster = hndl.defaultType;
                }

                try {
                    getComdb2dbHosts(hndl, false);
                } catch (IOException ioe) {
                    throw new NoDbHostFoundException(hndl.comdb2dbName,
                            "Could not find database hosts from DNS and config files.",
                            ioe);
                }
            }
        } else if (hndl.myDbHosts.size() == 0) {
            hndl.isDirectCpu = true;
            hndl.myDbHosts.add(hndl.myDbCluster);
            hndl.myDbPorts.add(hndl.overriddenPort);
        }

        Throwable error_during_discovery = null;
        if (hndl.isDirectCpu) {
            boolean atLeastOneValid = false;
            for (int i = 0; i != hndl.myDbPorts.size(); ++i) {
                if (hndl.myDbPorts.get(i) != -1)
                    atLeastOneValid = true;
                else {
                    try {
                        int dbport = getPortMux(hndl.myDbHosts.get(i),
                                hndl.portMuxPort, hndl.soTimeout, hndl.connectTimeout,
                                "comdb2", "replication", hndl.myDbName);
                        if (dbport != -1) {
                            atLeastOneValid = true;
                            hndl.myDbPorts.set(i, dbport);
                        }
                    } catch (IOException ioe) {
                        error_during_discovery = ioe;
                    }
                }
            }
            if (!atLeastOneValid)
                throw new NoDbHostFoundException(hndl.myDbName,
                        "Could not get database port from user supplied hosts.",
                        error_during_discovery);
            return;
        }

        /* If !DIRECT_CPU or no hosts in config file, query comdb2db. */

        ArrayList<String> validHosts = new ArrayList<String>();
        ArrayList<Integer> validPorts = new ArrayList<Integer>();

        boolean found = false;
        error_during_discovery = null;
        int hndlRetries = hndl.maxRetries();

        int retries = 0;

        while (hndl.myDbHosts.size() == 0 && retries < hndlRetries) {
            retries++;
            if (debug) {
                System.out.println("Querying for dbhosts, retries=" + retries);
            }

            /* Get comdb2db dbinfo. */
            int master = -1;
			int comdb2dbPort = -1;
			String connerr = "";
            for (String comdb2dbHost : hndl.comdb2dbHosts) {
                try {
                    comdb2dbPort = getPortMux(comdb2dbHost, hndl.portMuxPort,
                            hndl.soTimeout, hndl.connectTimeout,
                            "comdb2", "replication", hndl.comdb2dbName);

                    if (comdb2dbPort < 0) {
                        connerr = "Received invalid port from pmux.";
                        continue;
                    }

                    master = dbInfoQuery(hndl,
                            null, hndl.comdb2dbName, hndl.comdb2dbDbNum,
                            comdb2dbHost, comdb2dbPort, validHosts, validPorts);
                    found = true;
                    break;
                } catch (IOException ioe) {
                    /* Record last error during database discovery. */
                    error_during_discovery = ioe;
                    connerr = "A network I/O error occurred.";
                }
            }

            if (!found) {
                /* Could not get comdb2db dbinfo. Retry. */
                if (retries < hndlRetries)
                    continue;

                if (hndl.comdb2dbHosts.size() == 0)
                    throw new NoDbHostFoundException(hndl.comdb2dbName,
                            "Could not find database hosts.", error_during_discovery);
                else {
                    /* prepare diagnosis info */
                    String diagnosis = String.format("[pmux=%d][%s=%s@%d] %s",
                            hndl.portMuxPort,
                            hndl.comdb2dbName,
                            hndl.comdb2dbHosts.get(hndl.comdb2dbHosts.size() - 1),
                            comdb2dbPort,
                            connerr);
                    throw new NoDbHostFoundException(hndl.comdb2dbName,
                            diagnosis, error_during_discovery);
                }
			}

            /* Query comdb2db to get a list of machines where the database runs on. */
            found = false;
            error_during_discovery = null;

            for (int i = 0; i != validHosts.size(); ++i) {
                if (master == i && validHosts.size() > 1 || validPorts.get(i) < 0)
                    continue;
                try {
                    queryDbHosts(hndl, validHosts.get(i), validPorts.get(i));
                    found = true;
                    break;
                } catch (IOException ioe) {
                    /* Record last error during database discovery. */
                    error_during_discovery = ioe;
                    connerr = "A network I/O error occurred.";
                }
            }

            if (!found && master >= 0) {
                if (validPorts.get(master) < 0)
                    continue;
                try {
                    queryDbHosts(hndl, validHosts.get(master), validPorts.get(master));
                    found = true;
                } catch (IOException ioe) {
                    /* Record last error during database discovery. */
                    error_during_discovery = ioe;
                    connerr = "A network I/O error occurred.";
                }
            }

            if (!found) {
                /* Could not get machines of the database. Retry. */
                if (retries < hndlRetries)
                    continue;
                throw new NoDbHostFoundException(hndl.myDbName,
                        "Could not query database hosts from " + hndl.comdb2dbName + ": " + connerr,
                        error_during_discovery);
            }

            hndl.comdb2dbHosts.clear();
            hndl.comdb2dbHosts.addAll(validHosts);
        }

        if (hndl.myDbHosts.size() == 0)
            throw new NoDbHostFoundException(hndl.myDbName,
                    String.format("No entries of %s found in %s and config files",
                        hndl.myDbName, hndl.comdb2dbName));

        /* If pmux route is enabled, use pmux port. Otherwise do dbinfo query. */
        if (hndl.pmuxrte) {
            hndl.myDbPorts.clear();
            for (int i = 0; i != hndl.myDbHosts.size(); ++i)
                hndl.myDbPorts.add(hndl.portMuxPort);
            return;
        }

        /* We have a list of machines where the database runs.
           Now get dbinfo from them. */
        found = false;
        error_during_discovery = null;

        int port = -1;
        int increment = hndl.myDbHosts.size();

        if (increment == 0)
            increment = 1;

        String connerr = "";

        for (int retry = 0; (retry < hndlRetries) && (found == false); retry += increment) {
            for (int i = 0; i != hndl.myDbHosts.size(); ++i) {
                try {
                    String host = hndl.myDbHosts.get(i);
                    port = hndl.myDbPorts.get(i);
                    if (port < 0)
                        port = getPortMux(host, hndl.portMuxPort,
                                hndl.soTimeout, hndl.connectTimeout,
                                "comdb2", "replication", hndl.myDbName);

                    if (port < 0) {
                        connerr = "Received invalid port from pmux.";
                        continue;
                    }

                    dbInfoQuery(hndl,
                            null, hndl.myDbName, hndl.myDbNum,
                            host, port, validHosts, validPorts);
                    if (debug) {
                        System.out.println("dbInfoQuery returns hosts=" + validHosts +
                                " ports=" + validPorts + " for " + hndl.myDbName);
                    }
                    found = true;
                    break;
                } catch (IOException ioe) {
                    error_during_discovery = ioe;
                    connerr = "A network I/O error occurred.";
                }
            }

            if (retry > 0 && found == false) {
                int sleepms = (100 * (retry - hndl.myDbHosts.size() + 1));
                if (sleepms > 1000) {
                    sleepms = 1000;

                    logger.log(Level.WARNING, 
                            "Sleeping for 1 second on retry query to dbhosts retry " + retry);
                }
                try {
                    Thread.sleep(sleepms);
                } catch (InterruptedException e)  {
                    // Ignore
                }
            }
        }

        if (!found) {
            String diagnosis = String.format("[pmux=%d][%s=%s@%d] %s",
                    hndl.portMuxPort,
                    hndl.myDbName,
                    hndl.myDbHosts.get(hndl.myDbHosts.size() - 1),
                    port,
                    connerr);
            throw new NoDbHostFoundException(hndl.myDbName, diagnosis, error_during_discovery);
        }

        hndl.myDbHosts.clear();
        hndl.myDbHosts.addAll(validHosts);
        hndl.myDbPorts.clear();
        hndl.myDbPorts.addAll(validPorts);

        if (debug) {
            System.out.println("hndl.myDbHosts is " + hndl.myDbHosts);
            System.out.println("hndl.myDbPorts is " + hndl.myDbPorts);
        }
    }
}
/* vim: set sw=4 ts=4 et: */
