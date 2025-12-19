local function main(event)
    local tt = db:table("t_trig")
    local tp = event.type
    local row = {}
    if tp == 'add' then
        for k,v in pairs(event.new) do
            row[k] = v
        end
    elseif tp == 'del' then
        for k,v in pairs(event.old) do
            row[k] = v
        end
    end
    return tt:insert(row)
end
