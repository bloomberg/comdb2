-- sys.physrep.keepalive_v2

-- Physical replication source nodes & replicants periodically execute this
-- stored procedure on the replication meta db to update their current LSN.
-- Unlike v1, v2 also reports the oldest (first) log file so the registry can
-- range-filter candidate sources.
local function main(dbname, hostname, file, offset, firstfile)
    -- The metadb's comdb2_physreps table may or may not have a firstfile column.
    -- sys.physrep_tunables() reports firstfile == 1 only when the column exists,
    -- so we can update it conditionally and keepalive_v2 works cleanly against
    -- either schema (no error / no need for a schema change to enable v2).
    local tunables = sys.physrep_tunables(dbname)
    local has_firstfile = tunables["firstfile"]

    db:begin()
    if has_firstfile == 1 then
        db:exec("UPDATE comdb2_physreps SET file = " .. file .. "," ..
                "                           offset = " ..  offset .. "," ..
                "                           firstfile = " ..  firstfile .. "," ..
                "                           last_keepalive = NOW() " ..
                "    WHERE dbname = '" ..  dbname .. "' AND " ..
                "          host = '" .. hostname .. "'")
    else
        db:exec("UPDATE comdb2_physreps SET file = " .. file .. "," ..
                "                           offset = " ..  offset .. "," ..
                "                           last_keepalive = NOW() " ..
                "    WHERE dbname = '" ..  dbname .. "' AND " ..
                "          host = '" .. hostname .. "'")
    end
    db:commit()
end
