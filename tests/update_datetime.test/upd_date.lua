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

    local change = ''
    if (olda ~= newa) then
        change = change .. " a="..olda.."->"..newa 
    end
    if (event.old.b ~= event.new.b) then
        change = change .. " b="..event.old.b.."->"..event.new.b
    end
    if (event.new.c ~= nil and (event.old.c == nil or event.old.c ~= event.new.c)) then
        change = change .. " c="..event.old.c.."->"..event.new.c
    end
    local dtnow = db:now()

    --avoid acting on updates done by this trigger
    if (change == '') then
        return 0
    end

    local rc = t1_updates:insert({dt=dtnow,a=newa,delta=change} ) 
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
