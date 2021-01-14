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
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

import org.talend.components.jdbc.configuration.DistributionStrategy;
import org.talend.components.jdbc.configuration.RedshiftSortStrategy;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SQLDWHPlatform extends MSSQLPlatform {

    public static final String SQLDWH = "sqldwh";

    public SQLDWHPlatform(I18nMessage i18n) {
        super(i18n);
    }

    @Override
    public void createTableIfNotExist(final Connection connection, final String name, final List<String> keys,
            final RedshiftSortStrategy sortStrategy, final List<String> sortKeys, final DistributionStrategy distributionStrategy,
            final List<String> distributionKeys, final int varcharLength, final List<Record> records) throws SQLException {
        if (records.isEmpty()) {
            return;
        }
        final Table tableModel = getTableModel(connection, name, keys, null, sortKeys, distributionStrategy, distributionKeys,
                varcharLength, records);
        final String sql = buildQuery(connection, tableModel);

        try (final Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            connection.commit();
        } catch (final Throwable e) {
            if (!isTableExistsCreationError(e)) {
                throw e;
            }

            log.trace("create table issue was ignored. The table and it's name space has been created by an other worker", e);
        }
    }

    @Override
    protected String buildQuery(final Connection connection, final Table table) throws SQLException {
        // keep the string builder for readability
        final StringBuilder sql = new StringBuilder("CREATE TABLE");
        sql.append(" ");
        if (table.getSchema() != null && !table.getSchema().isEmpty()) {
            sql.append(identifier(table.getSchema())).append(".");
        }
        sql.append(identifier(table.getName()));
        sql.append("(");
        sql.append(createColumns(table.getColumns()));
        sql.append(")");

        log.debug("### create table query ###");
        log.debug(sql.toString());
        return sql.toString();
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
            // https://docs.microsoft.com/fr-fr/sql/relational-databases/tables/primary-and-foreign-key-constraints?view=sql-server-2017
            return column.getSize() <= -1 ? (column.isPrimaryKey() ? "VARCHAR(900)" : "VARCHAR(8000)")
                    : "VARCHAR(" + column.getSize() + ")";
        case BOOLEAN:
            return "BIT";
        case DOUBLE:
        case FLOAT:
            return "DECIMAL";
        case LONG:
            return "BIGINT";
        case INT:
            return "INT";
        case BYTES:
            return "VARBINARY(max)";
        case DATETIME:
            return "datetime2";
        case RECORD:
        case ARRAY:
        default:
            throw new IllegalStateException(getI18n().errorUnsupportedType(column.getType().name(), column.getName()));
        }
    }
}
