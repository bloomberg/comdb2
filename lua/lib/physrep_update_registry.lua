-- sys.physrep.update_registry

-- Once a physical replicant has successfully connected to a source node,
-- it executes this stored procedure to have the replication metadata tables
-- updated to reflect the same.
local max_verify_retries = 10

local function update(dbname, hostname, source_dbname, source_hostname, members)
    db:begin()

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
    if rc ~= 0 then
        return -201, "sys.physrep.update_registry failed rc " .. rc .. ": " .. db:error()
    end
    if source_dbname ~= "NULL" then
        print("lua/update_registry: " .. dbname .. "@" .. hostname .. " is connected to " ..  source_dbname .. "@" .. source_hostname)
    end
end
