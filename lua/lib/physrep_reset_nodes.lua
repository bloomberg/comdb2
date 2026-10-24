-- sys.physrep.reset_nodes
--
-- This stored procedure can be run to reset all the nodes that belong to
-- a replication database cluster from the replication metadata tables.
--
-- `hosts` can be a space-separated list of quoted hostnames on which the
-- db nodes are running.
local max_verify_retries = 10

local function reset(dbname, hosts, state)
    db:begin()
    for host in string.gmatch(hosts, "%S+") do
        local _, rc = db:exec("DELETE FROM comdb2_physrep_connections WHERE dbname = '" .. dbname .."' AND host = " .. host)
        if rc ~= 0 then
            db:rollback()
            return rc
        end
        _, rc = db:exec("INSERT INTO comdb2_physreps(dbname, host, state) VALUES ('" .. dbname .."', " .. host .. ", '" .. state .. "') " ..
                        "    ON CONFLICT (dbname, host) DO UPDATE SET dbname = '" .. dbname .. "', host = " .. host .. ", state = '" .. state .. "'")
        if rc ~= 0 then
            db:rollback()
            return rc
        end
    end
    return db:commit()
end

local function main(dbname, hosts, state)
    -- Verify errors are routine: a cluster member's keepalive can update a row we are resetting
    local rc
    for i = 1, max_verify_retries do
        rc = reset(dbname, hosts, state)
        if rc ~= db.err_verify then
            break
        end
    end
    if rc ~= 0 then
        return -201, "sys.physrep.reset_nodes failed rc " .. rc .. ": " .. db:error()
    end
end
