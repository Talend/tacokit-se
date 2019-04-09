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

import com.zaxxer.hikari.HikariDataSource;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.DistributionStrategy;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.talend.sdk.component.api.record.Schema.Type.STRING;

@Slf4j
@Getter
public abstract class Platform implements Serializable {

    public static final String APPLICATION = "Talend";

    private final I18nMessage i18n;

    protected Platform(I18nMessage i18n) {
        this.i18n = i18n;
    }

    abstract public String name();

    abstract protected String delimiterToken();

    protected abstract String buildQuery(final Table table);

    /**
     * @param e if the exception if a table already exist ignore it. otherwise re throw e
     * @return true if the error is because the table already exist
     */
    protected abstract boolean isTableExistsCreationError(final Throwable e);

    public void createTableIfNotExist(final Connection connection, final String name, final List<String> keys,
            final List<String> sortKeys, final DistributionStrategy distributionStrategy, final List<String> distributionKeys,
            final int varcharLength, final List<Record> records) throws SQLException {
        if (records.isEmpty()) {
            return;
        }

        final String sql = buildQuery(
                getTableModel(connection, name, keys, sortKeys, distributionStrategy, distributionKeys, varcharLength, records));
        try (final Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            connection.commit();
        } catch (final Throwable e) {
            connection.rollback();
            if (!isTableExistsCreationError(e)) {
                throw e;
            }

            log.trace("create table issue was ignored. The table and it's name space has been created by an other worker", e);
        }
    }

    public String identifier(final String name) {
        return name == null || name.isEmpty() ? name : delimiterToken() + name + delimiterToken();
    }

    String createPKs(final String table, final List<Column> primaryKeys) {
        return primaryKeys == null || primaryKeys.isEmpty() ? ""
                : ", CONSTRAINT " + pkConstraintName(table, primaryKeys) + " PRIMARY KEY "
                        + primaryKeys.stream().map(Column::getName).map(this::identifier).collect(joining(",", "(", ")"));
    }

    private String pkConstraintName(String table, List<Column> primaryKeys) {
        final String uuid = UUID.randomUUID().toString();
        return "pk_" + table + "_" + primaryKeys.stream().map(Column::getName).collect(joining("_")) + "_"
                + uuid.substring(0, Math.min(4, uuid.length()));
    }

    protected String isRequired(final Column column) {
        return column.isNullable() && !column.isPrimaryKey() ? "NULL" : "NOT NULL";
    }

    private Table getTableModel(final Connection connection, final String name, final List<String> keys,
            final List<String> sortKeys, DistributionStrategy distributionStrategy, final List<String> distributionKeys,
            final int varcharLength, final List<Record> records) {
        final Table.TableBuilder builder = Table.builder().name(name).distributionStrategy(distributionStrategy);
        try {
            builder.catalog(connection.getCatalog()).schema(connection.getSchema());
        } catch (final SQLException e) {
            log.warn("can't get database catalog or schema", e);
        }
        final List<Schema.Entry> entries = records.stream().flatMap(record -> record.getSchema().getEntries().stream()).distinct()
                .collect(toList());
        return builder.columns(entries.stream()
                .map(entry -> Column.builder().entry(entry).primaryKey(keys.contains(entry.getName()))
                        .sortKey(sortKeys.contains(entry.getName())).distributionKey(distributionKeys.contains(entry.getName()))
                        .size(STRING == entry.getType() ? varcharLength : null).build())
                .collect(toList())).build();
    }

    /**
     * Add platform related properties to jdbc connections
     * @param dataSource the data source object to be configured
     */
    public void addDataSourceProperties(final HikariDataSource dataSource) {
        // to be override by impl
    }
}
