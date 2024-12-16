CREATE PROCEDURE dormant version '1'{
local function main(event)
    db:sleep(2)
    db:exec("INSERT INTO triggered select * from generate_series(1,49999)")
    return 0
end
}
