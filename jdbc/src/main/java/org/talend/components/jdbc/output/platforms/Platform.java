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
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public abstract class Platform implements Serializable {

    abstract public String name();

    abstract protected String delimiterToken();

    abstract protected String valueQuoteToken();

    protected abstract String buildQuery(final Table table);

    /**
     * @param e if the exception if a table allready exist ignore it. otherwise re throw e
     */
    protected abstract boolean isTableExistsCreationError(final SQLException e);

    public void createTableIfNotExist(final Connection connection, final String name, final List<Record> records)
            throws SQLException {
        final String sql = buildQuery(getTableModel(connection, name, records));
        try (final Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (final SQLException e) {
            connection.rollback();
            if (!isTableExistsCreationError(e)) {
                throw e;
            }
        }
        connection.commit();
    }

    String identifier(final String name) {
        return delimiterToken() + name + delimiterToken();
    }

    private Table getTableModel(final Connection connection, final String name, final List<Record> records) {
        final Table.TableBuilder builder = Table.builder().name(name);
        try {
            builder.catalog(connection.getCatalog()).schema(connection.getSchema());
        } catch (final SQLException e) {
            log.warn("can't get database catalog or schema", e);
        }
        return builder.columns(records.stream().flatMap(record -> record.getSchema().getEntries().stream()).distinct()
                .map(entry -> Column.builder().entry(entry).size(inferSize(entry, records)).build()).collect(Collectors.toList()))
                .build();
    }

    private Integer inferSize(final Schema.Entry column, final List<Record> records) {
        switch (column.getType()) {
        case STRING:
            return 5 * records.stream()
                    .filter(record -> record.getSchema().getEntries().stream()
                            .anyMatch(entry -> entry.getName().equals(column.getName())))
                    .map(record -> record.getString(column.getName())).filter(Objects::nonNull).mapToInt(String::length).max()
                    .orElseThrow(
                            () -> new IllegalStateException("can't infer size of {" + column.getName() + "} from incoming data"));
        default:
            return null;
        }

    }
}
