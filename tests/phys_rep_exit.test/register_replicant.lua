local function main(dbname, node, lsn)
    local mydbname = db:getdbname()
    local resultset, rc = db:exec("select 0 as tier, '" .. mydbname .. "' as dbname, host from comdb2_cluster")
    resultset:emit()
end
