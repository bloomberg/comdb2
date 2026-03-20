-- sys.physrep.get_reverse_hosts
--
-- This procedure would return a list of nodes that the specified node should
-- attempt to connect to for them to allow querying it back. (see reversesql)
local function main(dbname, hostname)
    db:begin()

    local rs, rc = db:exec("SELECT dbname, host FROM comdb2_physrep_sources WHERE " ..
                           "source_dbname = '" ..  dbname .. "' AND source_host = '" .. hostname .. "'")
    local row = rs:fetch()
    while row do
        db:emit(row)
        row = rs:fetch()
    end

    db:commit()
end
