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

    --bad local res, rc = db:exec( 'insert into t1_updates (a,dt) values(' .. anew .. ','.. dtnow .. ') ' )
    --return rc

    return t1:update({dt='"'..dtnow..'"'}, {a='"'..anew..'"'})
    --bad return db:exec( 'update t1 set dt = ' .. db:now() .. ' where a = ' .. anew)
end

local function main(event)
    local obj = event
    if type(event) == "string" then
        obj = db:json_to_table(event)
    end
    return do_update(obj)   
end
