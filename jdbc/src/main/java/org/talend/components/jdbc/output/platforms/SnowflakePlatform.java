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

import java.util.List;

import static java.util.stream.Collectors.joining;

/**
 * https://docs.snowflake.net/manuals/sql-reference/sql/create-table.html
 */
@Slf4j
public class SnowflakePlatform extends Platform {

    public static final String SNOWFLAKE = "snowflake";

    @Override
    public String name() {
        return SNOWFLAKE;
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
        sql.append("IF NOT EXISTS");
        sql.append(" ");
        if (table.getSchema() != null && !table.getSchema().isEmpty()) {
            sql.append(identifier(table.getSchema())).append(".");
        }
        sql.append(identifier(table.getName()));
        sql.append("(");
        sql.append(createColumns(table.getColumns()));
        sql.append(createPKs(table.getPrimaryKeys()));
        sql.append(")");
        // todo create index

        log.debug("### create table query ###");
        log.debug(sql.toString());
        return sql.toString();
    }

    @Override
    protected boolean isTableExistsCreationError(final Throwable e) {
        return false;
    }

    private String createColumns(final List<Column> columns) {
        return columns.stream().map(this::createColumn).collect(joining(","));
    }

    private String createColumn(final Column column) {
        return identifier(column.getName())//
                + " " + toDBType(column)//
                + " " + isRequired(column)//
                + defaultValue(column);
    }

    private String isRequired(final Column column) {
        return column.isNullable() ? "NULL" : "NOT NULL";
    }

    private String defaultValue(Column column) {
        return column.getDefaultValue() == null ? "" : " DEFAULT " + column.getDefaultValue();
    }

    private String toDBType(final Column column) {
        switch (column.getType()) {
        case STRING:
            /*
             * https://docs.snowflake.net/manuals/sql-reference/data-types-text.html#varchar
             */
            return "VARCHAR";
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
            return "BINARY";
        case DATETIME:
            /*
             * TIMESTAMP_LTZ internally stores UTC time with a specified precision. However, all operations are performed in the
             * current session’s time zone,
             * controlled by the TIMEZONE session parameter.
             * The following data types are aliases for TIMESTAMP_LTZ:
             * TIMESTAMPLTZ
             * TIMESTAMP WITH LOCAL TIME ZONE
             */
            return "TIMESTAMP_LTZ";
        case RECORD:
            return "OBJECT";
        case ARRAY:
            return "ARRAY";
        default:
            throw new IllegalStateException("unsupported type for this database " + column);
        }
    }

}
