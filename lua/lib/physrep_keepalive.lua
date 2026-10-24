-- sys.physrep.keepalive

-- Physical replication source nodes & replicants periodically execute this
-- stored procedure on the replication meta db to update their current LSN.
local max_verify_retries = 10

local function keepalive(dbname, hostname, file, offset)
    db:begin()
    -- TODO (NC) Instead of NOW(), we should append current time here.
    local _, rc = db:exec("UPDATE comdb2_physreps SET file = " .. file .. "," ..
                          "                           offset = " ..  offset .. "," ..
                          "                           last_keepalive = NOW() " ..
                          "    WHERE dbname = '" ..  dbname .. "' AND " ..
                          "          host = '" .. hostname .. "'")
    if rc ~= 0 then
        db:rollback()
        return rc
    end
    return db:commit()
end

local function main(dbname, hostname, file, offset)
    local rc
    for i = 1, max_verify_retries do
        rc = keepalive(dbname, hostname, file, offset)
        if rc ~= db.err_verify then
            break
        end
    end
    if rc ~= 0 then
        return -201, "sys.physrep.keepalive failed rc " .. rc .. ": " .. db:error()
    end
end
