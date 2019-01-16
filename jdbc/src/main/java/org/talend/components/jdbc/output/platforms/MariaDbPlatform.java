/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.jdbc.output.platforms;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class MariaDbPlatform extends Platform {

    public static final String MARIADB = "mariadb";

    @Override
    public String name() {
        return MARIADB;
    }

    @Override
    protected String delimiterToken() {
        return "`";
    }

    @Override
    protected String buildQuery(final Table table) {
        // keep the string builder for readability
        final StringBuilder sql = new StringBuilder("CREATE TABLE");
        sql.append(" ");
        sql.append("IF NOT EXISTS");
        sql.append(" ");
        if (table.getSchema() != null && !table.getSchema().isEmpty()) {
            sql.append(identifier(table.getSchema())).append(".");
        }
        sql.append(identifier(table.getName()));
        sql.append("(");
        sql.append(createColumns(table.getColumns()));
        sql.append(createPKs(table.getColumns().stream().filter(Column::isPrimaryKey).collect(Collectors.toList())));
        sql.append(")");
        // todo create index

        log.debug("### create table query ###");
        log.debug(sql.toString());
        return sql.toString();
    }

    @Override
    protected boolean isTableExistsCreationError(Throwable e) {
        return false;
    }

    private String createColumns(final List<Column> columns) {
        return columns.stream().map(this::createColumn).collect(Collectors.joining(","));
    }

    private String createColumn(final Column column) {
        return identifier(column.getName())//
                + " " + toDBType(column)//
                + " " + isRequired(column)//
        ;
    }

    private String toDBType(final Column column) {
        switch (column.getType()) {
        case STRING:
            return column.getSize() <= -1 ? (column.isPrimaryKey() ? "VARCHAR(255)" : "TEXT")
                    : "VARCHAR(" + column.getSize() + ")";
        case BOOLEAN:
            return "BOOLEAN";
        case DOUBLE:
            return "DOUBLE";
        case FLOAT:
            return "FLOAT";
        case LONG:
            return "BIGINT";
        case INT:
            return "INT";
        case BYTES:
            return "BLOB";
        case DATETIME:
            return "DATETIME(6)";
        case RECORD: // todo ??
        case ARRAY: // todo ??
        default:
            throw new IllegalStateException("unsupported type for this database " + column);
        }
    }

}
