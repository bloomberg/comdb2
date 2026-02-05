local function main()
    -- Test 1: Insert and check effects
    db:begin()
    db:exec("INSERT INTO commitsp_with_effects VALUES(1)")
    db:exec("INSERT INTO commitsp_with_effects VALUES(2)")
    db:exec("INSERT INTO commitsp_with_effects VALUES(3)")
    local rc, eff = db:commit({with_effects = true})
    if rc ~= 0 then
        return -200, "commit failed"
    end
    if not eff then
        return -201, "effects not returned"
    end
    if eff.num_inserted ~= 3 then
        return -202, "expected 3 inserts, got " .. tostring(eff.num_inserted)
    end
    if eff.num_affected ~= 3 then
        return -203, "expected num_affected=3, got " .. tostring(eff.num_affected)
    end
    if eff.num_updated ~= 0 then
        return -204, "expected 0 updates after insert test, got " .. tostring(eff.num_updated)
    end
    if eff.num_deleted ~= 0 then
        return -205, "expected 0 deletes after insert test, got " .. tostring(eff.num_deleted)
    end
    db:emit("insert test pass: " .. eff.num_inserted)

    -- Test 1b: Insert again and verify effects are NOT accumulated from Test 1
    db:begin()
    db:exec("INSERT INTO commitsp_with_effects VALUES(4), (5)")
    local rc1b, eff1b = db:commit({with_effects = true})
    if rc1b ~= 0 then
        return -206, "second insert commit failed"
    end
    if eff1b.num_inserted ~= 2 then
        return -207, "expected 2 inserts, got " .. tostring(eff1b.num_inserted) .. " (accumulation bug)"
    end
    if eff1b.num_affected ~= 2 then
        return -208, "expected num_affected=2, got " .. tostring(eff1b.num_affected) .. " (accumulation bug)"
    end
    db:emit("insert accumulation test pass: " .. eff1b.num_inserted)

    -- Test 2: Update and check effects (also verify no accumulation from Test 1)
    db:begin()
    db:exec("UPDATE commitsp_with_effects SET a = a + 10")
    local rc2, eff2 = db:commit({with_effects = true})
    if eff2.num_updated ~= 5 then
        return -209, "expected 5 updates, got " .. tostring(eff2.num_updated)
    end
    if eff2.num_inserted ~= 0 then
        return -210, "expected 0 inserts after update test, got " .. tostring(eff2.num_inserted)
    end
    if eff2.num_deleted ~= 0 then
        return -211, "expected 0 deletes after update test, got " .. tostring(eff2.num_deleted)
    end
    if eff2.num_affected ~= 5 then
        return -212, "expected num_affected=5, got " .. tostring(eff2.num_affected)
    end
    db:emit("update test pass: " .. eff2.num_updated)

    -- Test 3: Delete and check effects
    db:begin()
    db:exec("DELETE FROM commitsp_with_effects WHERE a > 0")
    local rc3, eff3 = db:commit({with_effects = true})
    if eff3.num_deleted ~= 5 then
        return -213, "expected 5 deletes, got " .. tostring(eff3.num_deleted)
    end
    if eff3.num_inserted ~= 0 then
        return -214, "expected 0 inserts after delete test, got " .. tostring(eff3.num_inserted)
    end
    if eff3.num_updated ~= 0 then
        return -215, "expected 0 updates after delete test, got " .. tostring(eff3.num_updated)
    end
    if eff3.num_affected ~= 5 then
        return -216, "expected num_affected=5, got " .. tostring(eff3.num_affected)
    end
    db:emit("delete test pass: " .. eff3.num_deleted)

    -- Test 4: Commit without with_effects still works
    db:begin()
    db:exec("INSERT INTO commitsp_with_effects VALUES(100)")
    local rc4 = db:commit()
    if rc4 ~= 0 then
        return -217, "normal commit failed"
    end
    db:emit("normal commit pass")

    -- Test 5: Select and check effects
    -- Commit data first: socksql SELECTs can only read committed data
    db:begin()
    db:exec("INSERT INTO commitsp_with_effects VALUES(50), (60), (70)")
    db:commit()
    -- Table now has: {100, 50, 60, 70}

    -- Now SELECT the committed rows in a new transaction
    db:begin()
    local stmt2 = db:exec("SELECT * FROM commitsp_with_effects WHERE a >= 50 AND a <= 70")
    local row2 = stmt2:fetch()
    while row2 do
        row2 = stmt2:fetch()
    end
    local rc5, eff5 = db:commit({with_effects = true})
    if rc5 ~= 0 then
        return -218, "select test commit failed"
    end
    if eff5.num_selected ~= 3 then
        return -219, "expected 3 selected, got " .. tostring(eff5.num_selected)
    end
    db:emit("select test pass: selected=" .. eff5.num_selected)

    -- Test 6: Mixed operations (INSERT, UPDATE, DELETE) in one transaction
    db:begin()
    -- INSERT: 5 new rows
    db:exec("INSERT INTO commitsp_with_effects VALUES(200), (300), (400), (500), (600)")
    -- UPDATE: 2 committed rows (50 -> 51, 60 -> 61)
    db:exec("UPDATE commitsp_with_effects SET a = a + 1 WHERE a >= 50 AND a <= 60")
    -- DELETE: 2 committed rows (100 and 70)
    db:exec("DELETE FROM commitsp_with_effects WHERE a = 100 OR a = 70")
    local rc6, eff6 = db:commit({with_effects = true})
    if rc6 ~= 0 then
        return -222, "mixed ops commit failed"
    end
    if eff6.num_inserted ~= 5 then
        return -224, "expected 5 inserts, got " .. tostring(eff6.num_inserted)
    end
    if eff6.num_updated ~= 2 then
        return -225, "expected 2 updates, got " .. tostring(eff6.num_updated)
    end
    if eff6.num_deleted ~= 2 then
        return -226, "expected 2 deletes, got " .. tostring(eff6.num_deleted)
    end
    -- num_affected = num_inserted + num_updated + num_deleted
    if eff6.num_affected ~= 9 then
        return -227, "expected num_affected=9, got " .. tostring(eff6.num_affected)
    end
    db:emit("mixed ops test pass: inserted=" .. eff6.num_inserted ..
            " updated=" .. eff6.num_updated ..
            " deleted=" .. eff6.num_deleted ..
            " affected=" .. eff6.num_affected)

    -- Cleanup
    db:exec("DELETE FROM commitsp_with_effects")

    return 0
end
