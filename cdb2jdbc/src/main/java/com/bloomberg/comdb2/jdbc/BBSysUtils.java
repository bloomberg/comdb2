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
 * BBSysUtils provides functions that deal with Bloomberg environment stuff.
 * 
 * @author Rivers Zhang
 * @author Mohit Khullar
 */
public class BBSysUtils {
    private static Logger logger = Logger.getLogger(BBSysUtils.class.getName());
    static private boolean debug = false;

    /**
     * Comdb2db configuration files.
     */
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
                    else if (tokens[1].equalsIgnoreCase("portmuxport")
                            && !hndl.hasUserPort)
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
    private final static int COMDB2DB_NUM = 32432;
    private final static String COMDB2DB_DEV = "comdb3db";
    private final static int COMDB2DB_DEV_NUM = 192779;

    /**
     * Returns true if successfully get comdb2dbhosts.
     * 
     * @param hndl
     * @return
     */
    private static boolean getComdb2dbHosts(Comdb2Handle hndl, boolean just_defaults) {
        boolean rc = readComdb2dbCfg(CDB2DBCONFIG_NOBBENV, hndl);
        if (!rc) /* fall back to /bb/bin if noenv conf not found */
            rc = readComdb2dbCfg(CDB2DBCONFIG_LOCAL, hndl);
        readComdb2dbCfg(CDB2DBCONFIG_NOBBENV_PATH + hndl.myDbName + ".cfg", hndl);

        if (just_defaults)
            return true;

        if (hndl.comdb2dbHosts.size() > 0 || hndl.myDbHosts.size() > 0)
            return true;

        String comdb2db_bdns = null;
        try {
            if (hndl.defaultType == null)
                comdb2db_bdns = String.format("%s.%s",
                        hndl.comdb2dbName, hndl.dnssuffix);
            else
                comdb2db_bdns = String.format("%s-%s.%s",
                        hndl.defaultType, hndl.comdb2dbName, hndl.dnssuffix);

            InetAddress inetAddress[] = InetAddress.getAllByName(comdb2db_bdns);
            for (int i = 0; i < inetAddress.length; i++) {
                hndl.comdb2dbHosts.add(inetAddress[i].getHostAddress());
                rc = true;
            }
        } catch (UnknownHostException e) {
            logger.log(Level.SEVERE, "ERROR in getting address " + comdb2db_bdns, e);
        }
        return rc;
    }

    /**
     * Returns port number.
     * 
     * @param host
     * @param port
     * @param app
     * @param service
     * @param instance
     * @return
     */
    private static int getPortMux(String host, int port, String app, String service, String instance) {
        SockIO io = null;
        try {
            String name = String.format("get %s/%s/%s\n", app, service, instance);

            io = new SockIO(host, port, null);

            io.write(name.getBytes());
            io.flush();

            String line = io.readLine(32);
            return Integer.parseInt(line);
        } catch (SocketTimeoutException e) {
            return -1;
        } catch (IOException e) {
            return -1;
        } finally {
            try {
                if (io != null)
                    io.close();
            } catch (IOException e) {
                logger.log(Level.WARNING, "Unable to close portmux connection (" + host + ":" + port + ")", e);
            }
        }
    }

    /**
     * Gets database node information. Returns no-negative value to indicate the
     * master node in @validHosts, and -1 to indicate no master node found.
     * 
     * @param dbName
     *            who I want to know about
     * @param dbNum
     *            its db number
     * @param host
     *            ask who
     * @param port
     *            hndlect to which port
     * @param validHosts
     *            where I want the hosts of @dbName be stored.
     * @param validPorts
     *            where I want the ports of @dbName be stored.
     * @return
     * @throws NoComdb2dbHostFoundException
     */
    static int dbInfoQuery(Comdb2Handle hndl,
            Cdb2DbInfoResponse dbInfoResp,
            String dbName, int dbNum, String host, int port,
            List<String> validHosts, List<Integer> validPorts)
        throws NoDbHostFoundException {

        SockIO io = null;

        validHosts.clear();
        validPorts.clear();

        try {
            if (dbInfoResp == null) {
                io = new SockIO(host, port, hndl.pmuxrte ? hndl.myDbName : null);

                /*********************************
                 * Sending data...
                 *********************************/

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

                /*********************************
                 * Parsing response...
                 *********************************/

                byte[] res = new byte[nsh.capacity()];
                if (io.read(res) != nsh.capacity())
                    throw new NoDbHostFoundException(dbName);
                nsh.reconstructFromBytes(res);

                res = null;
                res = new byte[nsh.length];
                if (io.read(res) != nsh.length)
                    throw new NoDbHostFoundException(dbName);

                dbInfoResp = protobuf.unpackDbInfoResp(res);
                if (dbInfoResp == null)
                    throw new NoDbHostFoundException(dbName);
            }

            int master = -1;
            int myroom = 0, hosts_same_room = 0;
            if (debug) {
                System.out.println("dbinfoQuery: dbinfo response " +
                        "returns " + dbInfoResp.nodes);
            }
            for (NodeInfo node : dbInfoResp.nodes) {
                if (node.incoherent != 0 || node.port < 0) {
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

            if (validHosts.size() <= 0)
                throw new NoDbHostFoundException(dbName);

            if (hndl != null) {
                hndl.masterIndexInMyDbHosts = master;
                hndl.numHostsSameRoom = hosts_same_room;
                hndl.peersslmode = dbInfoResp.peermode;
            }

            return master;

        } catch (IOException e) {
            throw new NoDbHostFoundException(dbName);
        } finally {
            try {
                if (io != null)
                    io.close();
            } catch (IOException e) {
                logger.log(Level.WARNING,
                        "Unable to close dbinfo_query connection ("
                        + host + ":" + port + ")", e);
            }
        }
    }

    /**
     * Gets database's name, number and room by querying a comdb2db server.
     * 
     * @param hndl
     * @param host
     *            host of the comdb2db server.
     * @param port
     *            port of that server.
     * @return
     */
    private static boolean queryDbHosts(Comdb2Handle hndl, String host, int port) {

        String sqlquery = String.format("select M.name, D.dbnum, M.room from machines M join databases D where M.cluster IN "
                + "(select cluster_machs from clusters where name = '%s' and cluster_name = '%s') and D.name = '%s' order by (room = '%s') desc",
                hndl.myDbName, hndl.myDbCluster, hndl.myDbName, hndl.machineRoom);

        SockIO io = null;

        try {
            io = new SockIO(host, port, hndl.pmuxrte ? hndl.myDbName : null);

            /*********************************
             * Sending data...
             *********************************/

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

            /*********************************
             * Parsing response...
             *********************************/

            byte[] res = new byte[nsh.capacity()];

            do {
                if (io.read(res) != NewSqlHeader.BYTES_NEEDED) // unexpected
                                                                // response from
                                                                // server
                    return false;
                nsh = NewSqlHeader.fromBytes(res);
            } while (nsh == null || nsh.length == 0); // if heartbeat packet,
                                                        // try again

            nsh.reconstructFromBytes(res);

            res = null;
            res = new byte[nsh.length];

            if (io.read(res) != nsh.length)
                return false;

            Cdb2SqlResponse sqlResp = protobuf.unpackSqlResp(res);
            if (sqlResp == null || sqlResp.errCode != 0
                    || (sqlResp.respType != 1 && sqlResp.value.size() != 1 && (sqlResp.value.get(0).type == -1 || sqlResp.value.get(0).type != 3)))
                return false;

            hndl.myDbHosts.clear();

            while (sqlResp.respType <= 2) {
                res = null;
                res = new byte[nsh.capacity()];
                if (io.read(res) != nsh.capacity())
                    return false;
                nsh.reconstructFromBytes(res);

                res = null;
                res = new byte[nsh.length];
                if (io.read(res) != nsh.length)
                    return false;

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

            return true;
        } catch (SocketTimeoutException e) {
            return false;
        } catch (IOException e) {
            return false;
        } finally {
            try {
                if (io != null)
                    io.close();
            } catch (IOException e) {
                logger.log(Level.WARNING, "Unable to close get_dbhosts connection (" + host + ":" + port + ")", e);
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
                /* get default conf without DNS lookup */
                getComdb2dbHosts(hndl, true);

                /* start with "comdb2db" */
                String comdb2db_name = COMDB2DB;

                /* revise comdb2db_name */
                if (hndl.comdb2dbName == null)
                    hndl.comdb2dbName = comdb2db_name;

                /* set cluster type if default */
                if (hndl.myDbCluster.equalsIgnoreCase("default")) {
                    if (hndl.defaultType == null)
                        throw new NoDbHostFoundException(hndl.myDbName +
                                "@default. No default type configured?");
                    hndl.myDbCluster = hndl.defaultType;
                }

                if (!getComdb2dbHosts(hndl, false))
                    throw new NoDbHostFoundException(hndl.myDbName +
                            ". Wrong configuration for cluster '" +
                            hndl.myDbCluster + "'.");
            }
        } else if (hndl.myDbHosts.size() == 0) {
            hndl.isDirectCpu = true;
            hndl.myDbHosts.add(hndl.myDbCluster);
            hndl.myDbPorts.add(hndl.overriddenPort);
        }

        if (hndl.isDirectCpu) {
            for (int i = 0; i != hndl.myDbPorts.size(); ++i) {
                if (hndl.myDbPorts.get(i) == -1) {
                    hndl.myDbPorts.set(i, getPortMux(hndl.myDbHosts.get(i),
                                hndl.portMuxPort, "comdb2", "replication", hndl.myDbName));
                }
            }
            return;
        }

        /******************************************
         * If no hosts defined, we have to query comdb2db to get necessary
         * information.
         ******************************************/
        // System.out.printf("Still Looking for Db hosts");

        ArrayList<String> validHosts = new ArrayList<String>();
        ArrayList<Integer> validPorts = new ArrayList<Integer>();

        boolean found = false;

        int retries = 0;

        while (hndl.myDbHosts.size() == 0 && retries < 5) {
            retries++;
            if (debug) {
                System.out.println("Querying for dbhosts, retries=" + retries);
            }

            /*****************************************
             * First, get a list of available comdb2db servers.
             *****************************************/

            int master = -1;
			int comdb2dbPort = -1;
			String connerr = "";
            for (String comdb2dbHost : hndl.comdb2dbHosts) {
                comdb2dbPort = BBSysUtils.getPortMux(comdb2dbHost, hndl.portMuxPort, "comdb2", "replication", hndl.comdb2dbName);
                if (comdb2dbPort < 0)
					connerr = "port error";
                else {
                    try {

                        master = dbInfoQuery(hndl,
                                null, hndl.comdb2dbName, hndl.comdb2dbDbNum,
                                comdb2dbHost, comdb2dbPort, validHosts, validPorts);
                        found = true;
                        break;
                    } catch (NoDbHostFoundException e) {
                        /**
                         * Ignore single exception.
                         */
						connerr = "dbinfo error";
                    }
                }
            }

            /**
             * However, if all dbinfoquery calls failed, then an exception has
             * to be thrown...
             */
            if (!found) {
                if (hndl.comdb2dbHosts.size() == 0)
                    throw new NoDbHostFoundException(hndl.comdb2dbName);
                else {
                    /* prepare diagnosis info */
                    String diagnosis = String.format("[pmux=%d][%s=%s@%d] %s",
                            hndl.portMuxPort,
                            hndl.comdb2dbName,
                            hndl.comdb2dbHosts.get(hndl.comdb2dbHosts.size() - 1),
                            comdb2dbPort,
                            connerr);
                    throw new NoDbHostFoundException(diagnosis);
                }
			}

            /************************************************
             * We have a list of available hosts now. Query the slave nodes
             * one-by-one to get hosts of the database that we want hndlect to.
             ************************************************/

            for (int i = 0; i != validHosts.size(); ++i) {
                if (master == i && validHosts.size() > 1)
                    continue;

                found = queryDbHosts(hndl, validHosts.get(i), validPorts.get(i));
                if (found)
                    break;
            }

            /**********************************************
             * None of slave nodes works. Now try master.
             **********************************************/
            if (!found)
                found = queryDbHosts(hndl, validHosts.get(master), validPorts.get(master));

            if (!found)
                throw new NoDbHostFoundException(hndl.myDbName);

            hndl.comdb2dbHosts.clear();
            hndl.comdb2dbHosts.addAll(validHosts);
        }

        /**********************************************
         * At this stage, we should have a list of hosts stored in @myDbHosts.
         * 
         * Ask them one-by-one to get the host(s) and port(s) of the database we
         * want to hndlect to.
         **********************************************/

        /* If pmux route is enabled, use pmux port. */
        if (hndl.pmuxrte) {
            hndl.myDbPorts.clear();
            for (int i = 0; i != hndl.myDbHosts.size(); ++i)
                hndl.myDbPorts.add(hndl.portMuxPort);
            return;
        }


        found = false;

        int port = -1;
        int hndlRetries = hndl.maxRetries();
        int increment = hndl.myDbHosts.size();

        if (increment == 0)
            increment = 1;

        String connerr = "";

        for (int retry = 0; (retry < hndlRetries) && (found == false); retry += increment) {
            for (int i = 0; i != hndl.myDbHosts.size(); ++i) {
                String host = hndl.myDbHosts.get(i);
                if (hndl.myDbPorts.get(i) >= 0)
                    port = hndl.myDbPorts.get(i);
                else
                    port = getPortMux(host, hndl.portMuxPort, "comdb2", "replication", hndl.myDbName);

                if (port < 0) {
                    connerr = "port error";
                    continue;
                }

                try {
                    dbInfoQuery(hndl,
                            null, hndl.myDbName, hndl.myDbNum,
                            host, port, validHosts, validPorts);
                    if (debug) {
                        System.out.println("dbInfoQuery returns hosts=" + validHosts +
                                " ports=" + validPorts + " for " + hndl.myDbName);
                    }
                    found = true;
                    break;
                } catch (NoDbHostFoundException e) {
                    /**
                     * Ignore single exception.
                     */
                    connerr = "dbinfo error";
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
            if (hndl.myDbHosts.size() == 0)
                throw new NoDbHostFoundException(hndl.myDbName);
            else {
                // diagnosis
                String diagnosis = String.format("[pmux=%d][%s=%s@%d] %s",
                        hndl.portMuxPort,
                        hndl.myDbName,
                        hndl.myDbHosts.get(hndl.myDbHosts.size() - 1),
                        port,
                        connerr);
                throw new NoDbHostFoundException(diagnosis);
            }
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
