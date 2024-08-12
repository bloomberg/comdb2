-- sys.physrep.get_revhosts_v2
--
-- This procedure would return a list of nodes that the specified node should
-- attempt to connect to for them to allow querying it back. (see reversesql)
local function main(dbname, hostname, tier, cluster)
    db:begin()

    -- Check whether 'comdb2_physrep_sources' table exists
    local rs, rc = db:exec("SELECT count(*)=1 AS cnt FROM comdb2_tables WHERE tablename = 'comdb2_physrep_sources'")
    local row = rs:fetch()
    if row.cnt == 0 then
        db:commit()
        return
    end

    local sql = ("SELECT dbname, host FROM comdb2_physrep_sources WHERE " ..
                 "source_dbname = '" ..  dbname .. "' AND ( source_host = '" .. hostname .. 
                 "' OR source_host = '" .. tier ..
                 "' OR source_host = '" .. cluster .. "')")

    print ("physrep_get_revhosts_v2: sql = " .. sql)
    local rs, rc = db:exec(sql)
    local row = rs:fetch()
    while row do
        -- target 'host' can be a tier, a cluster, or host
        -- the caller will handle
        db:emit(row)
        row = rs:fetch()
    end

    db:commit()
end

