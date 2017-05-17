local function main(t)
    local dtms = db:now("ms")
    local dtus = db:now(6)
    db:emit(dtms, dtus)
end
