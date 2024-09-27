/*
 * Copyright 2024 Bloomberg Finance L.P.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hibernate.dialect.unique;

import org.hibernate.boot.Metadata;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.dialect.Dialect;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.Table;
import org.hibernate.mapping.UniqueKey;

import java.util.List;

public class Comdb2UniqueDelegate implements UniqueDelegate {

    private final Dialect dialect;

    public Comdb2UniqueDelegate(Dialect dialect) {
        this.dialect = dialect;
    }

    @Override
    public String getColumnDefinitionUniquenessFragment(Column column, SqlStringGenerationContext context) {
        return getColumnDefinitionUniquenessFragment(column);
    }

    @Override
    public String getTableCreationUniqueConstraintsFragment(Table table, SqlStringGenerationContext context) {
        return "";
    }

    public String getColumnDefinitionUniquenessFragment(Column column) {
        return " unique";
    }

    @Override
    public String getAlterTableToAddUniqueKeyCommand(UniqueKey uniqueKey, Metadata metadata, SqlStringGenerationContext context) {
        final String tableName = context.format(uniqueKey.getTable().getQualifiedTableName());

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
    public String getAlterTableToDropUniqueKeyCommand(UniqueKey uniqueKey, Metadata metadata, SqlStringGenerationContext sqlStringGenerationContext) {
        return "";
    }
}
