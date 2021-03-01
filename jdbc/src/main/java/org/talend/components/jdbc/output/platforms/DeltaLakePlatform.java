/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.talend.components.jdbc.service.I18nMessage;

import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

/**
 * https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-datasource#create-table-delta
 */
@Slf4j
public class DeltaLakePlatform extends Platform {

    public static final String DELTALAKE = "deltalake";

    public DeltaLakePlatform(final I18nMessage i18n) {
        super(i18n);
    }

    @Override
    public String name() {
        return DELTALAKE;
    }

    @Override
    protected String delimiterToken() {
        return "`";
    }

    @Override
    public void addDataSourceProperties(HikariDataSource dataSource) {
        super.addDataSourceProperties(dataSource);
    }

    @Override
    protected String buildQuery(final Connection connection, final Table table) throws SQLException {
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
        sql.append(") USING DELTA");

        log.debug("### create table query ###");
        log.debug(sql.toString());
        return sql.toString();
    }

    @Override
    protected boolean isTableExistsCreationError(final Throwable e) {
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

    protected String isRequired(final Column column) {
        return column.isNullable() && !column.isPrimaryKey() ? "" : "NOT NULL";
    }

    private String toDBType(final Column column) {
        switch (column.getType()) {
        case STRING:
            return "STRING";
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
        case DATETIME:
            return "TIMESTAMP";
        case RECORD:
        case ARRAY:
        case BYTES:
        default:
            throw new IllegalStateException(getI18n().errorUnsupportedType(column.getType().name(), column.getName()));
        }
    }

}
