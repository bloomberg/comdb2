local function main(t)
    db:setdatetimeprecision("us")
    local dtus = db:now()
    db:column_type("datetime", 1)
    db:emit(dtus)
end
