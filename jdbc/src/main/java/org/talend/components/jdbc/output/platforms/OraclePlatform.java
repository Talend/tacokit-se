/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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

/**
 * https://docs.oracle.com/cd/B28359_01/server.111/b28310/tables003.htm#ADMIN01503
 */
@Slf4j
public class OraclePlatform extends Platform {

    public static final String NAME = "oracle";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    protected String delimiterToken() {
        return "";
    }

    @Override
    protected String valueQuoteToken() {
        return "";
    }

    @Override
    protected String buildQuery(final Table table) {
        // keep the string builder for readability
        final StringBuilder sql = new StringBuilder("CREATE TABLE");
        sql.append(" ");
        if (table.getSchema() != null && !table.getSchema().isEmpty()) {
            sql.append(table.getSchema()).append(".");
        }
        sql.append(identifier(table.getName()));
        sql.append(" ");
        sql.append(createColumns(table.getColumns()));

        // todo create PK
        // todo create index

        log.debug("### create table query ###");
        log.debug(sql.toString());
        return sql.toString();
    }

    @Override
    protected boolean isTableExistsCreationError(final SQLException e) {
        return "42000".equals(e.getSQLState()) && 955 == e.getErrorCode();
    }

    private String createColumns(final List<Column> columns) {
        return columns.stream().map(this::createColumn).collect(Collectors.joining(",", "(", ")"));
    }

    private String createColumn(final Column column) {
        return identifier(column.getName())//
                + " " + toDBType(column)//
                + " " + isRequired(column)//
                + " " + defaultValue(column) //
                + " " + comment(column);
    }

    private String comment(final Column column) {
        return column.getComment() == null ? "" : "COMMENT " + column.getComment();
    }

    private String isRequired(final Column column) {
        return column.isNullable() ? "NULL" : "NOT NULL";
    }

    private String defaultValue(Column column) {
        return column.getDefaultValue() == null ? "" : "DEFAULT " + column.getDefaultValue();
    }

    private String toDBType(final Column column) {
        switch (column.getType()) {
        case STRING:
            return "VARCHAR(" + column.getSize() + ")";
        case BOOLEAN:
            return "NUMBER(1)";
        case DOUBLE:
        case FLOAT:
        case LONG:
        case INT:
            return "NUMBER";
        case BYTES:
            return "BLOB";
        case DATETIME:
            return "DATE";
        case RECORD: // todo ??
        case ARRAY: // todo ??
        default:
            throw new IllegalStateException("unsupported type for this database " + column);
        }
    }

}
