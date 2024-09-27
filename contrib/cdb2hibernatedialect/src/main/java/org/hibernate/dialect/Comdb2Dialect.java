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

import org.hibernate.LockOptions;
import org.hibernate.ScrollMode;
import org.hibernate.boot.model.FunctionContributions;
import org.hibernate.boot.model.TypeContributions;
import org.hibernate.dialect.function.CommonFunctionFactory;
import org.hibernate.dialect.function.CurrentFunction;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.dialect.identity.Comdb2IdentityColumnSupport;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.dialect.pagination.Comdb2LimitHandler;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.unique.Comdb2UniqueDelegate;
import org.hibernate.dialect.unique.UniqueDelegate;
import org.hibernate.query.sqm.produce.function.FunctionParameterType;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.type.BasicType;
import org.hibernate.type.BasicTypeRegistry;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.descriptor.sql.internal.DdlTypeImpl;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;
import org.hibernate.type.spi.TypeConfiguration;

import java.sql.Types;
import java.util.Map;

/**
 * An SQL dialect for comdb2
*/
public class Comdb2Dialect extends Dialect {

    private final UniqueDelegate uniqueDelegate = new Comdb2UniqueDelegate(this);;


    @Override
    protected void registerColumnTypes(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
        super.registerColumnTypes(typeContributions, serviceRegistry);
        final DdlTypeRegistry ddlTypeRegistry = typeContributions.getTypeConfiguration().getDdlTypeRegistry();
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.INTEGER, "int", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.BIGINT, "longlong", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.SMALLINT, "short", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.TINYINT, "short", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.BIT, "short", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.FLOAT, "float", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.REAL, "double", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.DOUBLE, "double", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.DECIMAL, "decimal", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.CHAR, "char", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.VARCHAR, "vutf8", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.LONGVARCHAR, "vutf8", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.DATE, "datetime", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.TIME, "datetime", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.TIMESTAMP, "datetimeus", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.BINARY, "blob", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.VARBINARY, "blob", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.LONGVARBINARY, "blob", this));
        ddlTypeRegistry.addDescriptor(new DdlTypeImpl(Types.BOOLEAN, "short", this));
    }

    @Override
    protected String columnType(int sqlTypeCode) {
        switch (sqlTypeCode) {
            case Types.INTEGER:
            return "int";
            case Types.BIGINT:
            return "longlong";
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.BIT:
            case Types.BOOLEAN:
            return "short";
            case Types.FLOAT:
            return "float";
            case Types.REAL:
            case Types.DOUBLE:
            return "double";
            case Types.DECIMAL:
            return "decimal";
            case Types.CHAR:
            return "char";
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            return "vutf8";
            case Types.DATE:
            case Types.TIME:
            return "datetime";
            case Types.TIMESTAMP:
            return "datetimeus";
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            return "blob";
            default:
            return super.columnType(sqlTypeCode);
        }
    }

    @Override
    public void initializeFunctionRegistry(FunctionContributions functionContributions) {
        super.initializeFunctionRegistry(functionContributions);

        TypeConfiguration typeConfiguration = functionContributions.getTypeConfiguration();
        BasicTypeRegistry basicTypeRegistry = typeConfiguration.getBasicTypeRegistry();
        BasicType<Integer> integerType = basicTypeRegistry.resolve(StandardBasicTypes.INTEGER);

        functionContributions.getFunctionRegistry().register("quote",
                new StandardSQLFunction("quote", StandardBasicTypes.STRING));
        functionContributions.getFunctionRegistry().register("random",
                new CurrentFunction("random", "random", integerType));

        // some of the functions from the common library
        CommonFunctionFactory functionFactory = new CommonFunctionFactory(functionContributions);
        functionFactory.concat_pipeOperator();
        functionFactory.mod_operator();
        functionFactory.round();
        functionFactory.substr();

        // date & time
        functionContributions.getFunctionRegistry()
            .noArgsBuilder("now")
            .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.TIMESTAMP))
            .setUseParenthesesWhenNoArgs(true)
            .register();

        // trim
        functionFactory.trim2();  // covers both 'ltrim' and 'rtrim'
        functionContributions.getFunctionRegistry()
            .namedDescriptorBuilder("trim")
            .setInvariantType(basicTypeRegistry.resolve(StandardBasicTypes.STRING))
            .setArgumentCountBetween(1, 2)
            .setParameterTypes(FunctionParameterType.STRING, FunctionParameterType.STRING)
            .setArgumentListSignature("(STRING string[, STRING characters])")
            .register();
    }

    @Override
    public IdentityColumnSupport getIdentityColumnSupport() {
        return Comdb2IdentityColumnSupport.INSTANCE;
    }

    @Override
    public LimitHandler getLimitHandler() {
        return Comdb2LimitHandler.INSTANCE;
    }

    @Override
    public boolean supportsLockTimeouts() {
        return false;
    }

    @Override
    public String getForUpdateString() {
        return "";
    }

    @Override
    public String applyLocksToSql(String sql, LockOptions aliasedLockOptions, Map<String, String[]> keyColumnNames) {
        return sql.replaceFirst("select", "selectv");
    }

    @Override
    public boolean supportsOuterJoinForUpdate() {
        return false;
    }

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

    @Override
    public boolean qualifyIndexName() {
        return false;
    }

    @Override
    public String getAddForeignKeyConstraintString(String constraintName, String[] foreignKey, String referencedTable, String[] primaryKey, boolean referencesPrimaryKey) {
        return " add constraint " +
                constraintName +
                " foreign key (" +
                String.join(", ", foreignKey) +
                ") references " +
                referencedTable +
                " (" +
                String.join(", ", primaryKey) +
                ')';
    }

    @Override
    public String getDropForeignKeyString() {
        //We cannot name a foreign key, but we can name the index, and dropping the index drops the foreign key
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

    @Override
    public boolean supportsTupleDistinctCounts() {
        return false;
    }

    @Override
    public int getInExpressionCountLimit() {
        return 1024;
    }

    @Override
    public UniqueDelegate getUniqueDelegate() {
        return uniqueDelegate;
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
