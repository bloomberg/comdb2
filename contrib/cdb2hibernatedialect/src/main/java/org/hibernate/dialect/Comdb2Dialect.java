/*
 Copied and modified from 
 https://github.com/gwenn/sqlite-dialect/blob/d285e228514e4a70bb18bc3b0de9628f55ce9a43/src/main/java/org/hibernate/dialect/SQLiteDialect.java
 */
 /* Copyright 2015 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */

/*
 * The author disclaims copyright to this source code.  In place of
 * a legal notice, here is a blessing:
 *
 *    May you do good and not evil.
 *    May you find forgiveness for yourself and forgive others.
 *    May you share freely, never taking more than you give.
 *
 */

package org.hibernate.dialect;

import java.sql.*;
import java.util.*;

import org.hibernate.JDBCException;
import org.hibernate.ScrollMode;
import org.hibernate.boot.Metadata;
import org.hibernate.dialect.function.AbstractAnsiTrimEmulationFunction;
import org.hibernate.dialect.function.NoArgSQLFunction;
import org.hibernate.dialect.function.SQLFunction;
import org.hibernate.dialect.function.SQLFunctionTemplate;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.dialect.function.VarArgsSQLFunction;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.dialect.identity.IdentityColumnSupportImpl;
import org.hibernate.dialect.pagination.AbstractLimitHandler;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.dialect.unique.DefaultUniqueDelegate;
import org.hibernate.dialect.unique.UniqueDelegate;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.exception.spi.SQLExceptionConversionDelegate;
import org.hibernate.exception.spi.TemplatedViolatedConstraintNameExtracter;
import org.hibernate.exception.spi.ViolatedConstraintNameExtracter;
import org.hibernate.internal.util.JdbcExceptionHelper;
import org.hibernate.internal.util.StringHelper;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.ForeignKey;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.UniqueKey;
import org.hibernate.type.StandardBasicTypes;

/**
 * An SQL dialect for SQLite 3.
 */
public class Comdb2Dialect extends Dialect {

    public Comdb2Dialect() {
        registerColumnType(Types.INTEGER, "int");
        registerColumnType(Types.BIGINT, "longlong");
        registerColumnType(Types.SMALLINT, "short");
        registerColumnType(Types.TINYINT, "short");
        registerColumnType(Types.BIT, "short");
        registerColumnType(Types.FLOAT, "float");
        registerColumnType(Types.REAL, "double");
        registerColumnType(Types.DOUBLE, "double");
        registerColumnType(Types.DECIMAL, "decimal");
        registerColumnType(Types.CHAR, "char");
        registerColumnType(Types.VARCHAR, "vutf8");
        registerColumnType(Types.LONGVARCHAR, "vutf8");
        registerColumnType(Types.DATE, "datetime");
        registerColumnType(Types.TIME, "datetime");
        registerColumnType(Types.TIMESTAMP, "datetimeus");
        registerColumnType(Types.BINARY, "blob");
        registerColumnType(Types.VARBINARY, "blob");
        registerColumnType(Types.LONGVARBINARY, "blob");
        registerColumnType(Types.BOOLEAN, "short");

        registerFunction("concat", new VarArgsSQLFunction(StandardBasicTypes.STRING, "", "||", ""));
        registerFunction("mod", new SQLFunctionTemplate(StandardBasicTypes.INTEGER, "?1 % ?2"));
        registerFunction("quote", new StandardSQLFunction("quote", StandardBasicTypes.STRING));
        registerFunction("random", new NoArgSQLFunction("random", StandardBasicTypes.INTEGER));
        registerFunction("round", new StandardSQLFunction("round"));
        registerFunction("substr", new StandardSQLFunction("substr", StandardBasicTypes.STRING));
        registerFunction("trim", new AbstractAnsiTrimEmulationFunction() {
            @Override
            protected SQLFunction resolveBothSpaceTrimFunction() {
                return new SQLFunctionTemplate(StandardBasicTypes.STRING, "trim(?1)");
            }

            @Override
            protected SQLFunction resolveBothSpaceTrimFromFunction() {
                return new SQLFunctionTemplate(StandardBasicTypes.STRING, "trim(?2)");
            }

            @Override
            protected SQLFunction resolveLeadingSpaceTrimFunction() {
                return new SQLFunctionTemplate(StandardBasicTypes.STRING, "ltrim(?1)");
            }

            @Override
            protected SQLFunction resolveTrailingSpaceTrimFunction() {
                return new SQLFunctionTemplate(StandardBasicTypes.STRING, "rtrim(?1)");
            }

            @Override
            protected SQLFunction resolveBothTrimFunction() {
                return new SQLFunctionTemplate(StandardBasicTypes.STRING, "trim(?1, ?2)");
            }

            @Override
            protected SQLFunction resolveLeadingTrimFunction() {
                return new SQLFunctionTemplate(StandardBasicTypes.STRING, "ltrim(?1, ?2)");
            }

            @Override
            protected SQLFunction resolveTrailingTrimFunction() {
                return new SQLFunctionTemplate(StandardBasicTypes.STRING, "rtrim(?1, ?2)");
            }
        });
    }

    // IDENTITY support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    private static IdentityColumnSupport IDENTITY_COLUMN_SUPPORT;

    @Override
    public IdentityColumnSupport getIdentityColumnSupport() {
        if (IDENTITY_COLUMN_SUPPORT != null) {
            return IDENTITY_COLUMN_SUPPORT;
        }
        return IDENTITY_COLUMN_SUPPORT = new IdentityColumnSupportImpl();
    }

    // limit/offset support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    private static LimitHandler LIMIT_HANDLER;

    @Override
    public LimitHandler getLimitHandler() {
        if (LIMIT_HANDLER != null) {
            return LIMIT_HANDLER;
        }
        return LIMIT_HANDLER = new AbstractLimitHandler() {
            @Override
            public String processSql(String sql, RowSelection selection) {
                final boolean hasOffset = LimitHelper.hasFirstRow(selection);
                return sql + (hasOffset ? " limit ? offset ?" : " limit ?");
            }

            @Override
            public boolean supportsLimit() {
                return true;
            }

            @Override
            public boolean bindLimitParametersInReverseOrder() {
                return true;
            }
        };
    }

    // lock acquisition support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    @Override
    public boolean supportsLockTimeouts() {
        // may be http://sqlite.org/c3ref/db_mutex.html ?
        return false;
    }

    @Override
    public String getForUpdateString() {
        return "";
    }

    @Override
    public boolean supportsOuterJoinForUpdate() {
        return false;
    }

    // current timestamp support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    @Override
    public boolean supportsCurrentTimestampSelection() {
        return true;
    }

    @Override
    public boolean isCurrentTimestampSelectStringCallable() {
        return false;
    }

    @Override
    public String getCurrentTimestampSelectString() {
        return "select current_timestamp";
    }

    // union subclass support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    // DDL support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    @Override
    public boolean canCreateSchema() {
        return true;
    }

    @Override
    public boolean hasAlterTable() {
        return true;
    }

    @Override
    public boolean dropConstraints() {
        return true;
    }

    @Override
    public boolean qualifyIndexName() {
        return false;
    }

    @Override
    public String getAddColumnString() {
        return "add column";
    }

    @Override
    public String getAddForeignKeyConstraintString(String constraintName, String[] foreignKey, String referencedTable, String[] primaryKey, boolean referencesPrimaryKey) {
        return new StringBuilder().append(" add constraint ")
                .append(constraintName)
                .append(" foreign key (")
                .append(StringHelper.join(", ", foreignKey))
                .append(") references ")
                .append(referencedTable)
                .append(" (")
                .append(StringHelper.join(", ", primaryKey))
                .append(')').toString();
    }

    @Override
    public String getDropForeignKeyString() {
        //We cannot name a forign key, but we can name the index, and dropping the index drops the foreign key
        return " drop foreign key ";
    }

    @Override
    public String getAddPrimaryKeyConstraintString(String constraintName) {
        throw new UnsupportedOperationException("No add primary key syntax supported by Comdb2Dialect");
    }

    @Override
    public boolean supportsCommentOn() {
        return true;
    }

    @Override
    public boolean supportsIfExistsBeforeTableName() {
        return true;
    }

    /* not case insensitive for unicode characters by default (ICU extension needed)
	public boolean supportsCaseInsensitiveLike() {
    return true;
  }
     */
    @Override
    public boolean doesReadCommittedCauseWritersToBlockReaders() {
        return false;
    }

    @Override
    public boolean doesRepeatableReadCauseReadersToBlockWriters() {
        return false;
    }

    @Override
    public boolean supportsTupleDistinctCounts() {
        return false;
    }

    @Override
    public int getInExpressionCountLimit() {
        return 1024;
    }

    private static UniqueDelegate UNIQUE_DELEGATE;

    @Override
    public UniqueDelegate getUniqueDelegate() {
        if (UNIQUE_DELEGATE != null) {
            return UNIQUE_DELEGATE;
        }
        return UNIQUE_DELEGATE = new DefaultUniqueDelegate(this) {
            @Override
            public String getColumnDefinitionUniquenessFragment(Column column) {
                return " unique";
            }

            @Override
            public String getAlterTableToAddUniqueKeyCommand(UniqueKey uniqueKey, Metadata metadata) {
                JdbcEnvironment jdbcEnvironment = metadata.getDatabase().getJdbcEnvironment();

                String tableName = jdbcEnvironment.getQualifiedObjectNameFormatter().format(
                        uniqueKey.getTable().getQualifiedTableName(), dialect
                );

                StringBuilder sb = new StringBuilder();

                sb.append(dialect.getAlterTableString(tableName));
                sb.append(" add unique index ");
                sb.append(dialect.quote(uniqueKey.getName()));
                sb.append(" (");

                List<Column> columns = uniqueKey.getColumns();
                for (int ii = 0, len = columns.size(); ii != len; ++ii) {
                    sb.append(columns.get(ii).getName());
                    if (ii != len - 1)
                        sb.append(", ");
                }

                sb.append(" )");

                return sb.toString();
            }

            @Override
            public String getTableCreationUniqueConstraintsFragment(Table table) {
                StringBuilder sb = new StringBuilder();
                /*
                table.getUniqueKeyIterator().forEachRemaining(uk -> {

                    sb.append(", UNIQUE '")
                            .append(dialect.escapeLiteral(uk.getName()))
                            .append("' (")
                            .append(uk.getColumns().stream().map(c -> c.getName()).collect(Collectors.joining(", ")))
                            .append(")");
                });
                 */

                //COMDB2 Requires a key for each foreign key. This seems like the only place to add the code
                Map<Table.ForeignKeyKey, ForeignKey> fkmap = table.getForeignKeys();
                for (Table.ForeignKeyKey fkk : fkmap.keySet()) {
                    ForeignKey fkv = fkmap.get(fkk);
                    sb.append(", KEY ");
                    sb.append(dialect.quote(fkv.getName()));
                    sb.append(" (");

                    List<Column> columns = fkv.getColumns();
                    for (int ii = 0, len = columns.size(); ii != len; ++ii) {
                        sb.append(columns.get(ii).getName());
                        if (ii != len - 1)
                            sb.append(", ");
                    }

                    sb.append(" )");
                }
                    /*
                    sb.append(", FOREIGN KEY (")
                            .append(v.getColumns().stream().map(c -> c.getName()).collect(Collectors.joining(",")))
                            .append(") REFERENCES ")
                            .append(v.getReferencedTable().getName())
                            .append(" (")
                            .append((v.getReferencedColumns().size() > 0 ? v.getReferencedColumns() : v.getReferencedTable().getPrimaryKey().getColumns()).stream().map(c -> ((Column) c).getName()).collect(Collectors.joining(", ")))
                            .append(")");
                     */
                return sb.toString();
            }
        };
    }

    @Override
    public String getSelectGUIDString() {
        return "select hex(randomblob(16))";
    }

    @Override
    public ScrollMode defaultScrollMode() {
        return ScrollMode.FORWARD_ONLY;
    }

}
