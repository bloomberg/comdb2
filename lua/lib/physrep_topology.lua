-- sys.physrep.topology
--
-- Returns the target node, source node and the target node's tier/level.
local function main()
    db:begin()

    local rs, rc = db:exec("WITH RECURSIVE " ..
                           "    physrep_tiers_cte (dbname, host, source_dbname, source_host, tier) AS " ..
                           "        (SELECT p.dbname, p.host, p.source_dbname, p.source_host, t.tier+1 " ..
                           "             FROM comdb2_physrep_connections p, physrep_tiers_cte t " ..
                           "             WHERE p.source_dbname = t.dbname AND p.source_host = t.host) " ..
                           "SELECT * from physrep_tiers_cte " ..
                           "    ORDER BY tier ")
    local row = rs:fetch()
    while row do
        db:emit(row)
        row = rs:fetch()
    end

    db:commit()
end
