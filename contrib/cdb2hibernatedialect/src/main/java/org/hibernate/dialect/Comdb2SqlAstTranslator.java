/*
 Copied and modified from
 https://github.com/hibernate/hibernate-orm/blob/6441c60255d25cbe0fa156f95cbece2fc030c04a/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteSqlAstTranslator.java
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


import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.sqm.ComparisonOperator;
import org.hibernate.sql.ast.spi.AbstractSqlAstTranslator;
import org.hibernate.sql.ast.tree.Statement;
import org.hibernate.sql.ast.tree.cte.CteMaterialization;
import org.hibernate.sql.ast.tree.expression.Any;
import org.hibernate.sql.ast.tree.expression.Every;
import org.hibernate.sql.ast.tree.expression.Expression;
import org.hibernate.sql.ast.tree.expression.Summarization;
import org.hibernate.sql.ast.tree.from.QueryPartTableReference;
import org.hibernate.sql.ast.tree.select.QueryGroup;
import org.hibernate.sql.ast.tree.select.QueryPart;
import org.hibernate.sql.ast.tree.select.QuerySpec;
import org.hibernate.sql.exec.spi.JdbcOperation;

/**
 * A SQL AST translator for SQLite.
 *
 * @author Christian Beikov
 * @author Vlad Mihalcea
 */

public class Comdb2SqlAstTranslator<T extends JdbcOperation> extends AbstractSqlAstTranslator<T> {

    public Comdb2SqlAstTranslator(SessionFactoryImplementor sessionFactory, Statement statement) {
        super( sessionFactory, statement );
    }

    @Override
    protected LockStrategy determineLockingStrategy(
            QuerySpec querySpec,
            ForUpdateClause forUpdateClause,
            Boolean followOnLocking) {
        return LockStrategy.NONE;
    }

    @Override
    protected void renderForUpdateClause(QuerySpec querySpec, ForUpdateClause forUpdateClause) {
        // SQLite does not support the FOR UPDATE clause
    }

    @Override
    public boolean supportsFilterClause() {
        return true;
    }

    @Override
    protected boolean supportsQuantifiedPredicates() {
        return false;
    }

    protected boolean shouldEmulateFetchClause(QueryPart queryPart) {
        // Check if current query part is already row numbering to avoid infinite recursion
        // We also have to emulate this if a fetch clause type other than rows only is used
        return getQueryPartForRowNumbering() != queryPart && !isRowsOnlyFetchClauseType( queryPart );
    }

    @Override
    public void visitQueryGroup(QueryGroup queryGroup) {
        if ( shouldEmulateFetchClause( queryGroup ) ) {
            emulateFetchOffsetWithWindowFunctions( queryGroup, true );
        }
        else {
            super.visitQueryGroup( queryGroup );
        }
    }

    @Override
    public void visitQuerySpec(QuerySpec querySpec) {
        if ( shouldEmulateFetchClause( querySpec ) ) {
            emulateFetchOffsetWithWindowFunctions( querySpec, true );
        }
        else {
            super.visitQuerySpec( querySpec );
        }
    }

    @Override
    public void visitQueryPartTableReference(QueryPartTableReference tableReference) {
        emulateQueryPartTableReferenceColumnAliasing( tableReference );
    }

    @Override
    public void visitOffsetFetchClause(QueryPart queryPart) {
        if ( !isRowNumberingCurrentQueryPart() ) {
            renderLimitOffsetClause( queryPart );
        }
    }

    @Override
    protected void renderComparison(Expression lhs, ComparisonOperator operator, Expression rhs) {
        if ( rhs instanceof Any ) {
            emulateSubQueryRelationalRestrictionPredicate(
                    null,
                    false,
                    ( (Any) rhs ).getSubquery(),
                    lhs,
                    this::renderSelectSimpleComparison,
                    operator
            );
        }
        else if ( rhs instanceof Every ) {
            emulateSubQueryRelationalRestrictionPredicate(
                    null,
                    true,
                    ( (Every) rhs ).getSubquery(),
                    lhs,
                    this::renderSelectSimpleComparison,
                    operator.negated()
            );
        }
        else {
            renderComparisonDistinctOperator( lhs, operator, rhs );
        }
    }

    @Override
    protected void renderPartitionItem(Expression expression) {
        if ( expression instanceof Summarization ) {
            // This could theoretically be emulated by rendering all grouping variations of the query and
            // connect them via union all but that's probably pretty inefficient and would have to happen
            // on the query spec level
            throw new UnsupportedOperationException( "Summarization is not supported by DBMS!" );
        }
        else {
            expression.accept( this );
        }
    }
}

