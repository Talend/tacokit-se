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
package org.talend.components.jdbc.output.internal;

import static java.util.stream.Collectors.joining;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.talend.components.jdbc.dataset.OutputDataset;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public abstract class StatementManager implements AutoCloseable {

    private String uuid = UUID.randomUUID().toString();

    private final Connection connection;

    protected final I18nMessage i18n;

    /**
     * Statement indexed by there parameter separated by ,
     */
    private Map<String, PreparedStatement> statements = new HashMap<>();

    public abstract String createQuery(final Record record);

    public abstract Map<Schema.Entry, Integer> getSqlQueryParams(final Record record);

    public static StatementManager get(final OutputDataset dataset, final Connection connection, final I18nMessage i18nMessage) {
        switch (dataset.getActionOnData()) {
        case INSERT:
            return new InsertStatementManager(dataset, connection, i18nMessage);
        case UPDATE:
            return new UpdateStatementManager(dataset, connection, i18nMessage);
        case DELETE:
            return new DeleteStatementManager(dataset, connection, i18nMessage);
        default:
            throw new IllegalStateException(i18nMessage.errorUnsupportedDatabaseAction());
        }
    }

    public void addBatch(final Record record) {
        try {
            final Map<Schema.Entry, Integer> sqlQueryParams = getSqlQueryParams(record);
            PreparedStatement statement = statements
                    .computeIfAbsent(sqlQueryParams.keySet().stream().map(Schema.Entry::getName).collect(joining(",")), k -> {
                        try {
                            return connection.prepareStatement(createQuery(record));
                        } catch (SQLException e) {
                            throw new IllegalStateException(e);
                        }
                    });

            sqlQueryParams.forEach((column, index) -> {
                try {
                    RecordSQLTypes.valueOf(column.getType().name()).setValue(statement, index, column.getName(), record);
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            });

            statement.addBatch();
        } catch (SQLException e) {
            // todo : how to handle record errors ? => just logged for now
            log.error("error with record " + record.toString() + "\nThe record was ignored.", e);
        }
    }

    public void executeBatch() {
        String error = statements.values().stream().map(s -> {
            try {
                s.executeBatch();
                if (!connection.getAutoCommit()) {
                    connection.commit();
                }
                return null;
            } catch (final SQLException e) {
                // fixme : should we transform this component to a processor and emit rejected records
                try {
                    connection.rollback(); // rollback the group
                } catch (final SQLException rollbackError) {
                    log.error("Can't rollback statements", rollbackError);
                }

                final StringBuilder msg = new StringBuilder("[" + e.getErrorCode() + "] " + e.getMessage());
                SQLException batchError = e;
                while (batchError.getNextException() != null) {
                    msg.append("\n- ").append(batchError.getNextException().getLocalizedMessage());
                    batchError = batchError.getNextException();
                }

                return msg.toString();
            }
        }).filter(Objects::nonNull).collect(Collectors.joining("\n"));
        if (!error.isEmpty()) {
            throw new IllegalStateException(error);
        }
    }

    public void clear() {
        statements.forEach((k, s) -> {
            try {
                s.clearParameters();
                s.clearBatch();
            } catch (SQLException e) {
                throw new IllegalStateException(i18n.errorCantClearPreparedStatement());
            }
        });
    }

    @Override
    public void close() {
        log.info("closing statement manager: " + uuid + " from thread: " + Thread.currentThread().getName());
        statements.forEach((k, s) -> {
            try {
                s.close();
            } catch (SQLException e) {
                log.warn(i18n.errorCantClosePreparedStatement(), e);
            }
        });

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn(i18n.errorCantCloseJdbcConnectionProperly(), e);
            }
        }
    }
}
