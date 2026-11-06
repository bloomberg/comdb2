# Lua Integration

## Summary

Comdb2 integrates **Lua scripting** for user-defined functions, allowing both scalar and aggregate functions to be defined at runtime without recompiling the database.

## Files Modified

- `sqlite/src/main.c` - Lua function registration

## New Files

- `sqlite/src/comdb2lua.c` - Lua function implementation

## How To

### Creating Lua Functions

```sql
-- Create a scalar function
CREATE LUA SCALAR FUNCTION upper_case(s TEXT)
RETURNS TEXT
AS
    return string.upper(s)
END;

-- Use the function
SELECT upper_case(name) FROM users;
```

### Creating Lua Aggregate Functions

```sql
-- Create an aggregate function
CREATE LUA AGGREGATE FUNCTION string_agg(s TEXT, sep TEXT)
RETURNS TEXT
AS
    state = state or ""
    if #state > 0 then
        state = state .. sep
    end
    state = state .. s
    return state
END;

-- Use the aggregate
SELECT string_agg(name, ', ') FROM users GROUP BY department;
```

### Managing Lua Functions

```sql
-- List all Lua functions
SELECT * FROM comdb2_lua_sfuncs;  -- Scalar functions
SELECT * FROM comdb2_lua_afuncs;  -- Aggregate functions

-- Drop a function
DROP LUA FUNCTION upper_case;
```

## Why

### Runtime Extensibility
- Add functions without recompiling database
- Deploy business logic as part of schema
- Customize behavior per database

### Sandboxed Execution
- Lua runs in a controlled environment
- Memory and CPU limits can be enforced
- No direct system access

### Cross-Platform
- Same function works on all nodes
- Schema replication includes functions
- Consistent behavior across cluster

## Implementation Details

### Function Registration
```c
// In main.c
static void register_lua_sfuncs(sqlite3 *db, struct sqlthdstate *thd) {
    listc_t sfuncs;
    get_sfuncs(&sfuncs);
    struct lua_func_t *sfunc;
    LISTC_FOR_EACH(&sfuncs, sfunc, lnk) {
        lua_func_arg_t *arg = malloc(sizeof(lua_func_arg_t));
        arg->thd = thd;
        arg->name = sfunc->name;
        sqlite3_create_function_v2(db, sfunc->name, -1,
            SQLITE_UTF8 | sfunc->flags,
            arg, lua_func, NULL, NULL, free);
    }
}

static void register_lua_afuncs(sqlite3 *db, struct sqlthdstate *thd) {
    listc_t afuncs;
    get_afuncs(&afuncs);
    struct lua_func_t *afunc;
    LISTC_FOR_EACH(&afuncs, afunc, lnk) {
        lua_func_arg_t *arg = malloc(sizeof(lua_func_arg_t));
        arg->thd = thd;
        arg->name = afunc->name;
        sqlite3_create_function_v2(db, afunc->name, -1,
            SQLITE_UTF8, arg,
            NULL, lua_step, lua_final, free);
    }
}
```

### Thread State
```c
typedef struct lua_func_arg {
    struct sqlthdstate *thd;  // Thread-local state
    const char *name;         // Function name
} lua_func_arg_t;
```

### Mutex Protection
```c
// In openDatabase()
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_lock(&mutex);
if( thd != NULL ){
    register_lua_sfuncs(db, thd);
    register_lua_afuncs(db, thd);
}
pthread_mutex_unlock(&mutex);
```

### Index Function Introspection
```c
// Get functions used in table indexes
int sqlite3_table_index_funcs(sqlite3 *db,
    const char *zDbName,
    const char *zTableName,
    char ***pzFuncs,
    int *nFuncs);
```

## Function Types

### Scalar Functions
- Take input values, return single value
- Called for each row
- Examples: `upper_case()`, `format_date()`, `calculate_tax()`

### Aggregate Functions
- Accumulate state across rows
- Called with step/final pattern
- Examples: `string_agg()`, `json_arrayagg()`, `custom_sum()`

## Upgrade Considerations

When upgrading SQLite:
1. **Function Registration API**: sqlite3_create_function_v2 signature stability
2. **Thread Model**: Changes to connection threading may affect registration
3. **Database Opening**: openDatabase() modifications for Lua registration
4. **Value Access**: sqlite3_value_* functions used in Lua bindings
