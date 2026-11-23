local function main(dbname, node, lsn)
    local cluster = "select 0 as tier, '" .. db:getdbname() .. "' as dbname, host from comdb2_cluster"
    local stmt = db:prepare("select src, host, also from route where name = ?")
    stmt:bind(1, dbname)
    local row = stmt:fetch()
    if row == nil then
        db:exec(cluster):emit()
    elseif row.src ~= "none" then
        local q = "select 1 as tier, '" .. row.src .. "' as dbname, '" .. row.host .. "' as host"
        -- The listed source first, then the cluster
        if row.also == 1 then
            q = q .. " union all " .. cluster
        end
        db:exec(q):emit()
    end
end
