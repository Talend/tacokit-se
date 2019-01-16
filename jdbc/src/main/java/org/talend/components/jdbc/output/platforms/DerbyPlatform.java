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

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;

/**
 * syntax detail can be found at <a href="https://dev.mysql.com/doc/refman/8.0/en/create-table.html syntax
 * detail">https://dev.mysql.com/doc/refman/8.0/en/create-table.html</a>
 */
@Slf4j
public class DerbyPlatform extends Platform {

    public static final String DERBY = "derby";

    @Override
    public String name() {
        return DERBY;
    }

    @Override
    protected String delimiterToken() {
        return "\"";
    }

    @Override
    protected String buildQuery(final Table table) {
        // keep the string builder for readability
        final StringBuilder sql = new StringBuilder("CREATE TABLE");
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
    protected boolean isTableExistsCreationError(final Throwable e) {
        return e instanceof SQLException && "X0Y32".equals(((SQLException) e).getSQLState());
    }

    private String createColumns(final List<Column> columns) {
        return columns.stream().map(this::createColumn).collect(joining(","));
    }

    private String createColumn(final Column column) {
        return identifier(column.getName())//
                + " " + toDBType(column)//
                + " " + isRequired(column)//
        ;
    }

    protected String isRequired(final Column column) {
        return column.isNullable() && !column.isPrimaryKey() ? "" : "NOT NULL";
    }

    private String toDBType(final Column column) {
        switch (column.getType()) {
        case STRING:
            return column.getSize() <= -1 ? (column.isPrimaryKey() ? "VARCHAR(255)" : "LONG VARCHAR")
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
            return "TIMESTAMP";
        case RECORD: // todo ??
        case ARRAY: // todo ??
        default:
            throw new IllegalStateException("unsupported type for this database " + column);
        }
    }

}
