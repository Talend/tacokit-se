/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.talend.components.jdbc.service.I18nMessage;

import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

/**
 * https://docs.oracle.com/cd/B28359_01/server.111/b28310/tables003.htm#ADMIN01503
 */
@Slf4j
public class OraclePlatform extends Platform {

    public static final String ORACLE = "oracle";

    /*
     * https://docs.oracle.com/cd/B14117_01/server.101/b10758/sqlqr06.htm
     */
    private static final String VARCHAR2_MAX = "4000";

    public OraclePlatform(final I18nMessage i18n) {
        super(i18n);
    }

    @Override
    public String name() {
        return ORACLE;
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
            sql.append(table.getSchema()).append(".");
        }
        sql.append(identifier(table.getName()));
        sql.append("(");
        sql.append(createColumns(table.getColumns()));
        sql.append(createPKs(table.getName(),
                table.getColumns().stream().filter(Column::isPrimaryKey).collect(Collectors.toList())));
        // todo create index
        sql.append(")");

        log.debug("### create table query ###");
        log.debug(sql.toString());
        return sql.toString();
    }

    @Override
    public void addDataSourceProperties(final HikariDataSource dataSource) {
        super.addDataSourceProperties(dataSource);
        dataSource.addDataSourceProperty("oracle.jdbc.J2EE13Compliant", "TRUE");
    }

    @Override
    protected boolean isTableExistsCreationError(final Throwable e) {
        return e instanceof SQLException && "42000".equals(((SQLException) e).getSQLState());
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

    @Override
    protected String pkConstraintName(String table, List<Column> primaryKeys) {
        // Avoid 'ORA-00972: identifier is too long' that limit oracle id to 30 chars max
        final String name = super.pkConstraintName(table, primaryKeys);
        if (name == null || name.length() <= 30) {
            return name;
        }
        return name.substring(0, 30);
    }

    private String toDBType(final Column column) {
        switch (column.getType()) {
        case STRING:
            return column.getSize() <= -1 ? "VARCHAR(" + VARCHAR2_MAX + ")" : "VARCHAR(" + column.getSize() + ")";
        case DOUBLE:
        case FLOAT:
        case LONG:
        case INT:
            return "NUMBER";
        case BYTES:
            return "BLOB";
        case DATETIME:
            return "TIMESTAMP(6)";
        case BOOLEAN:
            throw new IllegalStateException(getI18n().errorUnsupportedBooleanType4Oracle(column.getName()));
        case RECORD:
        case ARRAY:
        default:
            throw new IllegalStateException(getI18n().errorUnsupportedType(column.getType().name(), column.getName()));
        }
    }

}
