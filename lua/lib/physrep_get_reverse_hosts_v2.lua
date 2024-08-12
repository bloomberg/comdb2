-- sys.physrep.get_reverse_hosts
--
-- This procedure would return a list of nodes that the specified node should
-- attempt to connect to for them to allow querying it back. (see reversesql)
local function main(dbname, hostname)
    db:begin()

    -- Check whether 'comdb2_physrep_sources' table exists
    local rs, rc = db:exec("SELECT count(*)=1 AS cnt FROM comdb2_tables WHERE tablename = 'comdb2_physrep_sources'")
    local row = rs:fetch()
    if row.cnt == 0 then
        db:commit()
        return
    end

    local rs, rc = db:exec("SELECT dbname, host FROM comdb2_physrep_sources WHERE " ..
                           "source_dbname = '" ..  dbname .. "' AND source_host = '" .. hostname .. "'")
    local row = rs:fetch()
    while row do
        db:emit(row)
        row = rs:fetch()
    end

    db:commit()
end
