local function do_update(event)
    local t1_updates = db:table('t1_updates')
    local t1 = db:table('t1')
    local tp = event.type
    local newa, olda
    if tp == 'add' then
        return
    end
    if tp == 'del' then
        return
    end
    --its an update
    olda = event.old.a
    newa = event.new.a

    local changed = 0

    if (olda ~= newa) then
        changed=1
    else if (event.old.b ~= event.new.b) then
        changed=1
    else if (event.new.c ~= nil and (event.old.c == nil or event.old.c ~= event.new.c)) then
        changed=1
    end
    local dtnow = db:now()

    --avoid acting on updates done by this trigger
    if (changed == 0) then
        return 0
    end

    local rc = t1_updates:insert({dt=dtnow,a=newa,delta=db:table_to_json(event)} ) 
    if rc ~= 0 then
        return rc
    end

    local stmt, rc = db:exec("update t1 set dt='"..dtnow.."' where a="..newa)
    if rc ~= 0 then
        db:emit(stmt)  
    end
    return rc
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
