local function bad_options(err)
    return -200, err
end
local function register_failed()
    return -201, [[failed 'register']]
end
local function begin_failed()
    return -202, [[failed 'begin']]
end
local function commit_failed()
    return -202, [[failed 'commit']]
end

local function define_emit_columns(opt)
    local col
    local cols = {}

    col = {}
    col.name = [[comdb2_event]]
    col.type = [[text]]
    table.insert(cols, col)

    if opt.with_id then
        col = {}
        col.name = [[comdb2_id]]
        col.type = [[blob]]
        table.insert(cols, col)
    end

    if opt.with_tid then
        col = {}
        col.name = [[comdb2_tid]]
        col.type = [[integer]]
        table.insert(cols, col)
    end

    if opt.with_sequence then
        col = {}
        col.name = [[comdb2_sequence]]
        col.type = [[integer]]
        table.insert(cols, col)
    end

    if opt.with_epoch then
        col = {}
        col.name = [[comdb2_epoch]]
        col.type = [[integer]]
        table.insert(cols, col)
    end

    local sql =
    [[SELECT DISTINCT c.columnname name, c.type type ]]..
    [[FROM comdb2_columns c ]]..
    [[JOIN comdb2_triggers t WHERE ]]..
    [[c.tablename = t.tbl_name AND ]]..
    [[c.columnname = t.col AND ]]..
    [[t.name = @name]]
    local stmt = db:prepare(sql)
    stmt:bind([[name]], db:spname())
    local row = stmt:fetch()
    while row do
        table.insert(cols, row)
        row = stmt:fetch()
    end
    stmt:close()

    db:num_columns(#cols)
    for i, row in ipairs(cols) do
        db:column_name(row.name, i)
        db:column_type(row.type, i)
    end
end

local function get_event(consumer, opt)
    if opt.poll_timeout == nil then
        return consumer:get()
    end
    local e = consumer:poll(opt.poll_timeout)
    while e == nil do
        consumer:emit({comdb2_event = [[poll_timeout]]})
        e = consumer:poll(opt.poll_timeout)
    end
    return e
end

local function emit(event, opt, obj, out, type)
    out.comdb2_event = type
    if opt.with_id then
        out.comdb2_id = event.id
    end
    if opt.with_tid then
        out.comdb2_tid = event.tid
    end
    if opt.with_sequence then
        out.comdb2_sequence = event.sequence
    end
    if opt.with_epoch then
        out.comdb2_epoch = event.epoch
    end
    obj:emit(out)
end

local function emit_value(opt, event, obj)
    local type = event.type
    if type == [[upd]] then
        emit(event, opt, db, event.old, [[old]])
        emit(event, opt, obj, event.new, [[new]])
    elseif type == [[del]] then
        emit(event, opt, obj, event.old, type)
    elseif type == [[add]] then
        emit(event, opt, obj, event.new, type)
    end
end

local function validate_options(opt)
    local valid_options = {}
    valid_options.batch_consume = true
    valid_options.consume_count = true --undocumented
    valid_options.emit_timeout = true
    valid_options.poll_timeout = true
    valid_options.register_timeout = true
    valid_options.with_epoch = true
    valid_options.with_id = true --undocumented
    valid_options.with_sequence = true
    valid_options.with_tid = true
    for k, _ in pairs(opt) do
        if valid_options[k] == nil then
            return [[invalid option ']] .. k .. [[']]
        end
    end
end

local function get_options(json)
    local opt
    if json then
        opt = db:json_to_table(json)
        local err = validate_options(opt)
        if err then return nil, err end
    else
        opt = {}
    end
    if opt.with_id == nil then
        opt.with_id = true
    end
    if opt.emit_timeout == nil then
        opt.emit_timeout = 10 * 1000
    end
    return opt, nil
end

local function get_consumer(opt)
    local consumer = db:consumer(opt)
    while consumer == nil do
        if opt.register_timeout == nil then
            return register_failed()
        end
        emit({}, {}, db, {}, [[register_timeout]])
        consumer = db:consumer(opt)
    end
    consumer:emit_timeout(opt.emit_timeout)
    return consumer
end

local function count_consumer(opt, consumer)
    local counter = 0
    while counter < opt.consume_count do
        emit_value(opt, get_event(consumer, opt), consumer)
        consumer:consume()
        counter = counter + 1
    end
end

local function batch_consumer(opt, consumer)
    local rc = db:begin()
    if rc ~= 0 then
        return begin_failed()
    end
    local last = get_event(consumer, opt)
    consumer:next()
    while true do
        local event = consumer:poll(0)
        if event == nil or event.tid ~= last.tid then
            emit_value(opt, last, consumer)
            rc = db:commit()
            if rc ~= 0 then
                return commit_failed()
            end
            rc = db:begin()
            last = get_event(consumer, opt)
            if rc ~= 0 then
                return begin_failed()
            end
        else
            emit_value(opt, last, db)
            last = event
        end
        consumer:next()
    end
end

local function simple_consumer(opt, consumer)
    while true do
        emit_value(opt, get_event(consumer, opt), consumer)
        consumer:consume()
    end
end

local function main(json)
    local opt, err = get_options(json)
    if err then return bad_options(err) end
    define_emit_columns(opt)
    local consumer = get_consumer(opt)
    if opt.consume_count then
        count_consumer(opt, consumer)
    elseif opt.batch_consume then
        batch_consumer(opt, consumer)
    else
        simple_consumer(opt, consumer)
    end
end
