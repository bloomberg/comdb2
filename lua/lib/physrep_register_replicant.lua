-- sys.physrep.register_replicant

-- As the first step of registration process, physical replicants execute
-- this stored procedure on the source/replication metadb node in order to
-- register themselves. The node, in return, sends back a list of "potential"
-- nodes that the replicant can connect to to pull and apply physical logs.
local function main(dbname, hostname, lsn, source_dbname, source_hosts)
    db:begin()

    -- Retrieve physrep tunables
    local tunables = sys.physrep_tunables(source_dbname)

    -- The physical replicant is attempting a fresh registration; remove it
    -- from the comdb2_physrep_connections table.
    db:exec("DELETE FROM comdb2_physrep_connections WHERE dbname = '" ..  dbname .. "' AND host = '" .. hostname .. "'")

    -- Try not to allow more than 'physrep_max_pending_replicants' replicant registrations in flight.
    -- The following check is not perfect as it might not work if many requests show up at the same time.
    -- In which case they all could get the same list of potential leader hosts that they can connect
    -- against. And if they have 'physrep_shuffle_host_list' turned off, then the first host in the
    -- list might end up supporting all the replicants.
    -- We try to alleviate this by adding the following sleep for a random duration to spread out
    -- these requests.
    db:exec("SELECT sleep(abs(random()%10))")

    local rs, rc = db:exec("SELECT COUNT(*) AS cnt FROM comdb2_physreps WHERE state='Pending'")
    local row = rs:fetch()
    if row.cnt > tunables["physrep_max_pending_replicants"] then
        db:commit()
        return
    end

    local rs, rc = db:exec("WITH RECURSIVE " ..
                           "    tiers (dbname, host, tier) AS " ..
                           "        (SELECT dbname, host, 0 " ..
                           "             FROM comdb2_physreps " ..
                           "             WHERE dbname = '" .. source_dbname .. "' AND " ..
                           "                   host IN (" .. source_hosts ..") " ..
                           "         UNION ALL " ..
                           "         SELECT p.dbname, p.host, t.tier+1  " ..
                           "             FROM comdb2_physrep_connections p, tiers t " ..
                           "             WHERE p.source_dbname = t.dbname AND p.source_host = t.host), " ..
                           "    child_count (dbname, host, tier, cnt) AS " ..
                           "        (SELECT t.dbname, t.host, t.tier, count (*) " ..
                           "             FROM tiers t LEFT OUTER JOIN comdb2_physrep_connections p " ..
                           "                 ON t.dbname = p.source_dbname AND t.host = p.source_host " ..
                           "             GROUP BY t.dbname, t.host HAVING COUNT(*) < " .. tunables["physrep_fanout"] .. " ) " ..
                           "SELECT c.tier, c.dbname, c.host FROM child_count c, comdb2_physreps p " ..
                           "    WHERE c.dbname = p.dbname AND c.host = p.host AND (p.state IS NULL OR p.state NOT IN ('Pending', 'Inactive'))" ..
                           "    ORDER BY tier, cnt " ..
                           "    LIMIT " .. tunables["physrep_max_candidates"])

    if rs then
        local row = rs:fetch()
        while row do
            db:emit(row)
            row = rs:fetch()
        end
    end

    -- Add this physical replicant requester to the comdb2_physreps table with
    -- its state set to 'Pending'. This information will give an estimate on how
    -- many replicant registrations are currently in progress.
    -- We could deny further requests if there are too many pending requests.
    db:exec("INSERT INTO comdb2_physreps(dbname, host, state) VALUES ('" ..  dbname .. "', '" .. hostname .. "', 'Pending')" ..
            " ON CONFLICT (dbname, host) DO UPDATE SET state = 'Pending'")

    db:commit()
end
