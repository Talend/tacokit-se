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

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Slf4j
public abstract class Platform implements Serializable {

    abstract public String name();

    abstract protected String delimiterToken();

    protected abstract String buildQuery(final Table table);

    /**
     * @param e if the exception if a table allready exist ignore it. otherwise re throw e
     */
    protected abstract boolean isTableExistsCreationError(final Throwable e);

    public void createTableIfNotExist(final Connection connection, final String name, final List<String> keys,
            final List<Record> records) throws SQLException {
        if (records.isEmpty()) {
            return;
        }

        final String sql = buildQuery(getTableModel(connection, name, keys, records));
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

    public String createPKs(final List<Column> primaryKeys) {
        return primaryKeys == null || primaryKeys.isEmpty() ? ""
                : ", PRIMARY KEY "
                        + primaryKeys.stream().map(Column::getName).map(this::identifier).collect(joining(",", "(", ")"));
    }

    private Table getTableModel(final Connection connection, final String name, final List<String> keys,
            final List<Record> records) {
        final Table.TableBuilder builder = Table.builder().name(name);
        try {
            builder.catalog(connection.getCatalog()).schema(connection.getSchema());
        } catch (final SQLException e) {
            log.warn("can't get database catalog or schema", e);
        }
        final List<Schema.Entry> entries = records.stream().flatMap(record -> record.getSchema().getEntries().stream()).distinct()
                .collect(toList());
        return builder
                .columns(entries.stream().map(entry -> Column.builder().entry(entry).size(inferSize(entry, records)).build())
                        .collect(toList()))
                .primaryKeys(keys.stream()
                        .map(k -> Column.builder()
                                .entry(entries.stream().filter(e -> e.getName().equals(k)).findFirst()
                                        .orElseThrow(() -> new IllegalStateException("can't find key {" + k + "}")))
                                .build())
                        .collect(toList()))
                .build();
    }

    private Integer inferSize(Schema.Entry entry, List<Record> records) {
        if (!Schema.Type.STRING.equals(entry.getType())) {
            return null;
        }

        return records.stream().map(record -> record.get(String.class, entry.getName())).filter(Objects::nonNull)
                .mapToInt(String::length).max().orElse(0);
    }

    /**
     * Add platform related properties to jdbc connections
     */
    public void addDataSourceProperties(final HikariDataSource dataSource) {
        // to be override by impl
    }
}
