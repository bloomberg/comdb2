-- sys.physrep.update_registry

-- Once a physical replicant has successfully connected to a source node,
-- it executes this stored procedure to have the replication metadata tables
-- updated to reflect the same.
local max_verify_retries = 10
local max_downstream_depth = 32
local refused_cycle = -202

-- Is source_dbname@source_hostname the caller itself, or downstream of it?
-- Walk up from the source: each step is an index lookup on (dbname, host).
local function is_downstream(dbname, hostname, source_dbname, source_hostname)
    local rs, rc = db:exec("WITH RECURSIVE upstream (dbname, host, depth) AS " ..
                           "    (SELECT '" .. source_dbname .. "', '" .. source_hostname .. "', 0 " ..
                           "     UNION " ..
                           "     SELECT p.source_dbname, p.source_host, u.depth + 1 " ..
                           "         FROM comdb2_physrep_connections p, upstream u " ..
                           "         WHERE p.dbname = u.dbname AND p.host = u.host " ..
                           "         AND u.depth < " .. max_downstream_depth .. ") " ..
                           "SELECT 1 FROM upstream " ..
                           "    WHERE dbname = '" .. dbname .. "' AND host = '" .. hostname .. "' LIMIT 1")
    if rc ~= 0 then
        return rc, false
    end
    local row = rs:fetch()
    rs:close()
    return 0, row ~= nil
end

local function update(dbname, hostname, source_dbname, source_hostname, members)
    db:begin()

    if source_dbname ~= "NULL" then
        local rc, found = is_downstream(dbname, hostname, source_dbname, source_hostname)
        if rc ~= 0 or found then
            db:rollback()
            return found and refused_cycle or rc
        end
    end

    -- Add all the nodes in the physical replication cluster to comdb2_physreps table
    for host in string.gmatch(members, "%S+") do
        local _, rc = db:exec("INSERT INTO comdb2_physreps(dbname, host, state) VALUES ('" .. dbname .."', " .. host .. ", 'Active') " ..
                              "    ON CONFLICT (dbname, host) DO UPDATE SET dbname = '" ..  dbname .. "', host = " .. host .. ", state = 'Active'")
        if rc ~= 0 then
            db:rollback()
            return rc
        end

        if source_dbname ~= "NULL" then
            _, rc = db:exec("DELETE FROM comdb2_physrep_connections WHERE dbname='" ..  dbname ..  "' AND host=" .. host)
            if rc ~= 0 then
                db:rollback()
                return rc
            end
        end
    end

    if source_dbname ~= "NULL" then
        -- Add the new connection
        local _, rc = db:exec("INSERT INTO comdb2_physrep_connections VALUES ('" .. dbname .. "', '" .. hostname .. "', '" .. source_dbname .. "', '" .. source_hostname .. "')")
        if rc ~= 0 then
            db:rollback()
            return rc
        end
    end

    return db:commit()
end

local function main(dbname, hostname, source_dbname, source_hostname, members)
    local rc
    for i = 1, max_verify_retries do
        rc = update(dbname, hostname, source_dbname, source_hostname, members)
        if rc ~= db.err_verify then
            break
        end
    end
    if rc == refused_cycle then
        return refused_cycle, "sys.physrep.update_registry refused: " .. source_dbname .. "@" .. source_hostname ..
                              " is downstream of " .. dbname .. "@" .. hostname
    end
    if rc ~= 0 then
        return -201, "sys.physrep.update_registry failed rc " .. rc .. ": " .. db:error()
    end
    if source_dbname ~= "NULL" then
        print("lua/update_registry: " .. dbname .. "@" .. hostname .. " is connected to " ..  source_dbname .. "@" .. source_hostname)
    end
end
