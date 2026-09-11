-- sys.physrep.register_replicant

-- As the first step of registration process, physical replicants execute
-- this stored procedure on the source/replication metadb node in order to
-- register themselves. The node, in return, sends back a list of "potential"
-- nodes that the replicant can connect to to pull and apply physical logs.

-- Note: the 'Pending'/'Inactive' state filter below compares lower(p.state).
-- comdb2_physreps.state is a case-sensitive CSTRING and older builds wrote
-- 'InActive' from start_physrep_threads() while register_self() wrote
-- 'Inactive'.  A case-sensitive NOT IN let the 'InActive' rows through, so a
-- physrep that had restarted but not yet begun replicating was handed out as a
-- viable source.  The replicant then rejected it locally in
-- physrep_allowed_source(), found no candidate leaders, and retried forever.
local function main(dbname, hostname, lsn, source_dbname, source_hosts)

    local pfile, poffset = string.match(lsn, "(%d+):(%d+)")
    pfile = tonumber(pfile)
    poffset = tonumber(poffset)
    print(
        "physrep_register_replicant: dbname = '" .. dbname .. "', hostname = '" .. hostname ..
        "', file = " .. pfile .. ", offset = " .. poffset .. ", source_dbname = '" .. source_dbname .. "', source_hosts = '" ..
        source_hosts .. "'")

    db:begin()

    -- Retrieve physrep tunables
    local tunables = sys.physrep_tunables(source_dbname)

    -- The physical replicant is attempting a fresh registration; remove it
    -- from the comdb2_physrep_connections table.
    db:exec("DELETE FROM comdb2_physrep_connections WHERE dbname = '" ..  dbname .. "' AND host = '" .. hostname .. "'")

    local physrep_fanout = tunables["physrep_fanout"]
    local physrep_max_candidates = tunables["physrep_max_candidates"]
    local firstfile = tunables["firstfile"]
    local cte

    -- Two-tier selection when the metadb tracks firstfile (i.e. comdb2_physreps
    -- has a firstfile column): the primary pass is STRICT -- only sources whose
    -- reported [firstfile, file] range actually covers the replicant's pfile
    -- (firstfile must be non-NULL, so a source that has not reported yet is not
    -- assumed viable). If that yields nothing, the "without fanout" fallback pass
    -- below stays permissive (firstfile IS NULL allowed) so a not-yet-reporting
    -- fleet is never left with zero candidates. When there is no firstfile column
    -- (firstfile == 0) we skip range filtering entirely and lean on the
    -- replicant's connect-time verification (physrep_verify_source_range).
    if firstfile == 1 then
        cte = ("WITH RECURSIVE " ..
                 "    tiers (dbname, host, tier) AS " ..
                 "        (SELECT dbname, host, 0 " ..
                 "             FROM comdb2_physreps " ..
                 "             WHERE dbname = '" .. source_dbname .. "' AND " ..
                 "                   host IN (" .. source_hosts ..") AND " ..
                 "                   (file >= " .. pfile .. " AND " ..
                 "                   firstfile IS NOT NULL AND firstfile <= " .. pfile .. ") " ..
                 "         UNION ALL " ..
                 "         SELECT p.dbname, p.host, t.tier+1  " ..
                 "             FROM comdb2_physrep_connections p, tiers t, comdb2_physreps c" ..
                 "             WHERE p.source_dbname = t.dbname AND p.source_host = t.host " ..
                 "             AND p.dbname = c.dbname AND p.host = c.host " ..
                 "             AND (c.file >= " .. pfile .. " AND " ..
                 "                 c.firstfile IS NOT NULL AND c.firstfile <= " .. pfile ..  ")" ..
                 "             AND (c.last_keepalive > (NOW() - cast (600 as sec)))), " ..
                 "    child_count (dbname, host, tier, cnt) AS " ..
                 "        (SELECT t.dbname, t.host, t.tier, count (*) c" ..
                 "             FROM tiers t LEFT OUTER JOIN comdb2_physrep_connections p " ..
                 "                 ON t.dbname = p.source_dbname AND t.host = p.source_host " ..
                 "             GROUP BY t.dbname, t.host HAVING COUNT(*) < " .. physrep_fanout .. " ) " ..
                 "SELECT c.tier, c.dbname, c.host FROM child_count c, comdb2_physreps p " ..
                 "    WHERE c.dbname = p.dbname AND c.host = p.host AND (p.state IS NULL OR lower(p.state) NOT IN ('pending', 'inactive'))" ..
                 "    ORDER BY tier, cnt, random() " ..
                 "    LIMIT " .. physrep_max_candidates)
    else
        cte = ("WITH RECURSIVE " ..
                 "    tiers (dbname, host, tier) AS " ..
                 "        (SELECT dbname, host, 0 " ..
                 "             FROM comdb2_physreps " ..
                 "             WHERE dbname = '" .. source_dbname .. "' AND " ..
                 "                   host IN (" .. source_hosts ..") " ..
                 "         UNION ALL " ..
                 "         SELECT p.dbname, p.host, t.tier+1  " ..
                 "             FROM comdb2_physrep_connections p, tiers t, comdb2_physreps c" ..
                 "             WHERE p.source_dbname = t.dbname AND p.source_host = t.host" ..
                 "             AND p.dbname = c.dbname AND p.host = c.host " ..
                 "             AND (c.last_keepalive > (NOW() - cast (600 as sec)))), " ..
                 "    child_count (dbname, host, tier, cnt) AS " ..
                 "        (SELECT t.dbname, t.host, t.tier, count (*) " ..
                 "             FROM tiers t LEFT OUTER JOIN comdb2_physrep_connections p " ..
                 "                 ON t.dbname = p.source_dbname AND t.host = p.source_host " ..
                 "             GROUP BY t.dbname, t.host HAVING COUNT(*) < " .. physrep_fanout .. " ) " ..
                 "SELECT c.tier, c.dbname, c.host FROM child_count c, comdb2_physreps p " ..
                 "    WHERE c.dbname = p.dbname AND c.host = p.host AND (p.state IS NULL OR lower(p.state) NOT IN ('pending', 'inactive'))" ..
                 "    ORDER BY tier, random() " ..
                 "    LIMIT " .. physrep_max_candidates)
    end

    print("physrep_register_replicant: sql = " .. cte)
    local rs, rc = db:exec(cte)
    local foundrow = 0

    if (rc == 0) then
        if rs then
            local row = rs:fetch()
            while row do
                foundrow = 1
                db:emit(row)
                row = rs:fetch()
            end
        end

        if (foundrow == 0) then
            print("physrep_register_replicant: found no candidates, running without fanout '" .. source_dbname .. "' source-hosts '" .. source_hosts .. "'")

            if firstfile == 1 then
                cte = ("WITH RECURSIVE " ..
                         "    tiers (dbname, host, tier) AS " ..
                         "        (SELECT dbname, host, 0 " ..
                         "             FROM comdb2_physreps " ..
                         "             WHERE dbname = '" .. source_dbname .. "' AND " ..
                         "                   host IN (" .. source_hosts ..") AND " .. 
                         "                   (file >= " .. pfile .. " AND " ..
                         "                   (firstfile IS NULL OR firstfile <= " .. pfile .. ")) " ..
                         "         UNION ALL " ..
                         "         SELECT p.dbname, p.host, t.tier+1  " ..
                         "             FROM comdb2_physrep_connections p, tiers t, comdb2_physreps c" ..
                         "             WHERE p.source_dbname = t.dbname AND p.source_host = t.host " ..
                         "             AND p.dbname = c.dbname AND p.host = c.host " ..
                         "             AND (c.file >= " .. pfile .. " AND " ..
                         "                 (c.firstfile IS NULL OR c.firstfile <= " .. pfile ..  "))), " ..
                         "    child_count (dbname, host, tier, cnt) AS " ..
                         "        (SELECT t.dbname, t.host, t.tier, count (*) c" ..
                         "             FROM tiers t LEFT OUTER JOIN comdb2_physrep_connections p " ..
                         "                 ON t.dbname = p.source_dbname AND t.host = p.source_host " ..
                         "             GROUP BY t.dbname, t.host) " ..
                         "SELECT c.tier, c.dbname, c.host FROM child_count c, comdb2_physreps p " ..
                         "    WHERE c.dbname = p.dbname AND c.host = p.host AND (p.state IS NULL OR lower(p.state) NOT IN ('pending', 'inactive'))" ..
                         "    ORDER BY tier, cnt, random() " ..
                         "    LIMIT " .. physrep_max_candidates)
            else
                cte = ("WITH RECURSIVE " ..
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
                         "             GROUP BY t.dbname, t.host ) " ..
                         "SELECT c.tier, c.dbname, c.host FROM child_count c, comdb2_physreps p " ..
                         "    WHERE c.dbname = p.dbname AND c.host = p.host AND (p.state IS NULL OR lower(p.state) NOT IN ('pending', 'inactive'))" ..
                         "    ORDER BY tier, random() " ..
                         "    LIMIT " .. physrep_max_candidates)
            end
        
            rs, rc = db:exec(cte)

            foundrow = 0
            if (rc == 0) then
                if rs then
                    local row = rs:fetch()
                    while row do
                        foundrow = 1
                        db:emit(row)
                        row = rs:fetch()
                    end
                end
            end
        end

        if (foundrow == 0) then
            print("physrep_register_replicant: critical error: found no candidates for '" .. source_dbname .. "' source-hosts '" .. source_hosts .. "'" .. " file " .. pfile .. " offset " .. poffset)
        end
        -- Add this physical replicant requester to the comdb2_physreps table with
        -- its state set to 'Pending'. This information will give an estimate on how
        -- many replicant registrations are currently in progress.
        -- We could deny further requests if there are too many pending requests.
        db:exec("INSERT INTO comdb2_physreps(dbname, host, state) VALUES ('" ..  dbname .. "', '" .. hostname .. "', 'Pending')" ..
        " ON CONFLICT (dbname, host) DO UPDATE SET state = 'Pending'")
    end
    db:commit()
end
