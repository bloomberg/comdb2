local function main(dbname, node, lsn)
    local tab, rc = db:table('tmp', {{'tier', 'integer'}, {'dbname', 'string'}, {'host', 'string'}})
    local dbname = db:getdbname()
    local resultset, rc = db:exec("select 0 as tier, " .. dbname .. " as dbname, host from comdb2_cluster")
    resultset:emit()
--    tab:emit()
end
