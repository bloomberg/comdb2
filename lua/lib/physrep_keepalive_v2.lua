-- sys.physrep.keepalive_v2

-- Physical replication source nodes & replicants periodically execute this
-- stored procedure on the replication meta db to update their current LSN.
-- Unlike v1, v2 also reports the oldest (first) log file so the registry can
-- range-filter candidate sources.
local max_verify_retries = 10

local function keepalive(dbname, hostname, file, offset, firstfile, has_firstfile)
    local sql
    if has_firstfile == 1 then
        sql = "UPDATE comdb2_physreps SET file = " .. file .. "," ..
              "                           offset = " ..  offset .. "," ..
              "                           firstfile = " ..  firstfile .. "," ..
              "                           last_keepalive = NOW() " ..
              "    WHERE dbname = '" ..  dbname .. "' AND " ..
              "          host = '" .. hostname .. "'"
    else
        sql = "UPDATE comdb2_physreps SET file = " .. file .. "," ..
              "                           offset = " ..  offset .. "," ..
              "                           last_keepalive = NOW() " ..
              "    WHERE dbname = '" ..  dbname .. "' AND " ..
              "          host = '" .. hostname .. "'"
    end
    db:begin()
    local _, rc = db:exec(sql)
    if rc ~= 0 then
        db:rollback()
        return rc
    end
    return db:commit()
end

local function main(dbname, hostname, file, offset, firstfile)
    -- The metadb's comdb2_physreps table may or may not have a firstfile column.
    -- sys.physrep_tunables() reports firstfile == 1 only when the column exists,
    -- so we can update it conditionally and keepalive_v2 works cleanly against
    -- either schema (no error / no need for a schema change to enable v2).
    local tunables = sys.physrep_tunables(dbname)
    local has_firstfile = tunables["firstfile"]

    local rc
    for i = 1, max_verify_retries do
        rc = keepalive(dbname, hostname, file, offset, firstfile, has_firstfile)
        if rc ~= db.err_verify then
            break
        end
    end
    if rc ~= 0 then
        return -201, "sys.physrep.keepalive_v2 failed rc " .. rc .. ": " .. db:error()
    end
end
