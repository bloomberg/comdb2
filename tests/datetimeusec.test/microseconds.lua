local function main(t)
    db:settimezone("America/New_York")
    local secs1 = db:cast(61.001, "intervalds")
    local usec1 = secs1.usecs(secs1)
    local secs2 = db:cast(61.246246, "intervalds")
    local usec2 = secs2.usecs(secs2)
    local dt1 = db:cast("2015-02-23T095959.999", "datetime")
    local dt2 = db:cast("2015-02-23T095959.999999", "datetime")
    local msec = dt1["msec"]
    local year = dt2["year"]
    local usec3 = dt2["usec"]
    db:emit(type(secs1))
    db:emit(usec1)
    db:emit(usec2)
    db:emit(type(dt1))
    db:emit(type(dt2))
    db:emit(msec)
    db:emit(year)
    db:emit(usec3)
    db:emit(secs1)
    db:emit(secs2)
    db:emit(dt1)
    db:emit(dt2)
end
