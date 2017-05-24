local function do_update(event)
    local t1_updates = db:table('t1_updates')
    local t1 = db:table('t1')
    local tp = event.type
    local anew, iold
    if tp == 'add' then
        return
    end
    if tp == 'del' then
        return
    end
    anew = event.new.a
    local dtnow = db:now()
    local rc = t1_updates:insert({dt=dtnow, a=anew} ) 
    if rc ~= 0 then
        return rc
    end

    return t1:update({dt=dtnow}, {a=anew})
end

local function main(event)
    --can be called manually via exec procedure upd_date('{"new":{"a":3}}')
    --or via trigger which will pass equivalent object
    local obj = event
    if type(event) == "string" then
        obj = db:json_to_table(event)
    end
    return do_update(obj)   
end
