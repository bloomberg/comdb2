-- sys.physrep.update_registry

-- Once a physical replicant has successfully connected to a source node,
-- it executes this stored procedure to have the replication metadata tables
-- updated to reflect the same.
local function main(dbname, hostname, source_dbname, source_hostname, members)
    db:begin()

    -- Add all the nodes in the physical replication cluster to comdb2_physreps table
    for host in string.gmatch(members, "%S+") do
        db:exec("INSERT INTO comdb2_physreps(dbname, host, state) VALUES ('" .. dbname .."', " .. host .. ", 'Active') " ..
                "    ON CONFLICT (dbname, host) DO UPDATE SET dbname = '" ..  dbname .. "', host = " .. host .. ", state = 'Active'")

        if source_dbname ~= "NULL" then
            db:exec("DELETE FROM comdb2_physrep_connections WHERE dbname='" ..  dbname ..  "' AND host=" .. host)
        end
    end

    if source_dbname ~= "NULL" then
        -- Add the new connection
        db:exec("INSERT INTO comdb2_physrep_connections VALUES ('" .. dbname .. "', '" .. hostname .. "', '" .. source_dbname .. "', '" .. source_hostname .. "')")
        print("lua/update_registry: " .. dbname .. "@" .. hostname .. " is connected to " ..  source_dbname .. "@" .. source_hostname)
    end

    db:commit()
end
