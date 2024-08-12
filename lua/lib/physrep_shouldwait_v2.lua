-- sys.physrep.shouldwait_v2
--
-- This stored procedure can be executed by a node to check whether that node
-- needs to wait for a 'reverse-connection' from another node. In particular,
-- this is needed in a situation when there is cross-tier replication and the
-- physical replicant running in a lower tier cannot directly connect to the
-- source node running in a higher tier. In this case, the physical replicant
-- simply waits for a connection from a node in the higher tier, latches on to
-- it to pull physical logs needed for replication. (see `reversesql`)
local function main(dbname, hostname, tier, cluster)
    db:begin()

    local rs, row = db:exec("SELECT count(*)=1 AS cnt FROM comdb2_tables WHERE tablename = 'comdb2_physrep_sources'")
    local row = rs:fetch()

    if row.cnt == 0 then
        db:emit(row)
        db:commit()
        return
    end

    local sql = ("SELECT count(*) as cnt from comdb2_physrep_sources " ..
                 "    WHERE dbname = '" .. dbname .. "' AND " ..
                 "        ( host LIKE '" .. hostname .. "' OR " ..
                 "          host LIKE '" .. tier .. "' OR " ..
                 "          host LIKE '" .. cluster .. "')")

    print ("physrep_shouldwait_v2: sql = " .. sql)
    local rs, row = db:exec(sql)
    local row = rs:fetch()
    db:emit(row)

    db:commit()
end
