local function main(t)
    db:setdatetimeprecision("ms")
    local dtus = db:now()
    db:column_type("datetimeus", 1)
    db:emit(dtus)
end
