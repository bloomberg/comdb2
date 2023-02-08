-- sys.physrep.reset_nodes
--
-- This stored procedure can be run to reset all the nodes that belong to
-- a replication database cluster from the replication metadata tables.
--
-- `hosts` can be a space-separated list of quoted hostnames on which the
-- db nodes are running.
local function main(dbname, hosts, state)
    db:begin()
    for host in string.gmatch(hosts, "%S+") do
        db:exec("DELETE FROM comdb2_physrep_connections WHERE dbname = '" .. dbname .."' AND host = " .. host)
        db:exec("INSERT INTO comdb2_physreps(dbname, host, state) VALUES ('" .. dbname .."', " .. host .. ", '" .. state .. "') " ..
                "    ON CONFLICT (dbname, host) DO UPDATE SET dbname = '" .. dbname .. "', host = " .. host .. ", state = '" .. state .. "'")
    end
    db:commit()
end
