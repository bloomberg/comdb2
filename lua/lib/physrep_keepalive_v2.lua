-- sys.physrep.keepalive

-- Physical replication source nodes & replicants periodically execute this
-- stored procedure on the replication meta db to update their current LSN.
local function main(dbname, hostname, file, offset, firstfile)
    db:begin()
    db:exec("UPDATE comdb2_physreps SET file = " .. file .. "," ..
            "                           offset = " ..  offset .. "," ..
            "                           firstfile = " ..  firstfile .. "," ..
            "                           last_keepalive = NOW() " ..
            "    WHERE dbname = '" ..  dbname .. "' AND " ..
            "          host = '" .. hostname .. "'")
    db:commit()
end
