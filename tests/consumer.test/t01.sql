DROP TABLE IF EXISTS t
CREATE TABLE t (i INT);$$
CREATE PROCEDURE next VERSION 'testsuite' {

local function consume(c, get, consume, num, arg)
    if db:begin() ~= 0 then return 0 end
    local total = 0
    for i = 1, num do
        local e = get(c, arg)
        if e ~=nil then
            total = total + 1
            c:emit(e.new.i)
            consume(c)
        end
    end
    if db:commit() ~= 0 then return 0 end
    return total
end

local function main()
    db:num_columns(1)
    db:column_type("int", 1)
    db:column_name("i", 1)

    -- Consume using various methods in different batch sizes
    local c = db:consumer()
    local i = 0
    i = i + consume(c, c.get,  c.next,    1, nil)
    i = i + consume(c, c.poll, c.next,    1, 0)
    i = i + consume(c, c.get,  c.next,    1, nil)
    i = i + consume(c, c.poll, c.next,    1, 0)
    i = i + consume(c, c.get,  c.consume, 1, nil)
    i = i + consume(c, c.poll, c.consume, 1, 0)
    i = i + consume(c, c.get,  c.next,    1, nil)
    i = i + consume(c, c.poll, c.next,    1, 0)
    i = i + consume(c, c.get,  c.consume, 1, nil)
    i = i + consume(c, c.poll, c.consume, 1, 0)
    i = i + consume(c, c.get,  c.next,    10, nil)
    i = i + consume(c, c.poll, c.next,    10, 0)
    i = i + consume(c, c.poll, c.next,    35, 0)
    i = i + consume(c, c.get,  c.next,    35, nil)

    if i ~= 100 then
        return -202, "failed to consume 100 events - events consumed:"..tostring(i)
    end

    local e = c:poll(0)
    if e ~= nil then
        return -201, "failed to consume all rows - poll returned:"..db:table_to_json(e)
    end
end}$$
CREATE LUA CONSUMER next ON (TABLE t FOR INSERT)
INSERT INTO t SELECT * FROM generate_series LIMIT 100
EXEC PROCEDURE next()

CREATE PROCEDURE next VERSION 'testsuite' {
local function main()
    db:num_columns(1)
    db:column_type("int", 1)
    db:column_name("i", 1)
    local c = db:consumer()
    local i = 0
    local e = c:poll(0)
    db:begin()
    while e do
        i = i + 1
        c:next()
        e = c:poll(0)
    end
    if db:commit() == 0 then
        db:emit(i)
    else
        return -201, db:error()
    end
end}$$
INSERT INTO t SELECT * FROM generate_series LIMIT 50000
EXEC PROCEDURE next()
