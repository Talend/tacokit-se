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
package org.talend.components.jdbc.output.statement.operations;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.components.jdbc.output.Reject;
import org.talend.components.jdbc.output.statement.RecordToSQLTypeConverter;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;

@Data
@Slf4j
public abstract class JdbcAction {

    private final OutputConfiguration configuration;

    private final I18nMessage i18n;

    private final DataSource dataSource;

    private final Integer maxRetry = 5;

    private Integer retryCount = 0;

    /**
     * A default retry strategy. We try to detect deadl lock by testing the sql state code.
     * 40001 is the state code used by almost all database to rise a dead lock issue
     */
    private final Function<SQLException, Boolean> retryStrategy = e -> "40001"
            .equals(ofNullable(e.getNextException()).orElse(e).getSQLState());

    protected abstract String buildQuery(List<Record> records);

    protected abstract Map<Integer, Schema.Entry> getQueryParams();

    protected abstract boolean validateQueryParam(Record record);

    public List<Reject> execute(final List<Record> records) throws SQLException {
        if (records.isEmpty()) {
            return emptyList();
        }
        final String query = buildQuery(records);
        final List<Reject> discards = new ArrayList<>();
        try (final Connection connection = dataSource.getConnection()) {
            discards.addAll(processRecords(records, connection, query));
            if (discards.size() != records.size()) {
                connection.commit();
            }
        }

        return discards;
    }

    public static void quiteRollback(final Connection connection) {
        try {
            log.debug("Rollback connection " + connection);
            connection.rollback();
        } catch (SQLException rollbackException) {
            log.error("Can't rollback the connection " + connection, rollbackException);
        }
    }

    private List<Reject> processRecords(final List<Record> records, final Connection connection, final String query)
            throws SQLException {
        final List<Reject> discards = new ArrayList<>();
        do {
            try (final PreparedStatement statement = connection.prepareStatement(query)) {
                discards.addAll(prepareStatement(records, statement));
                try {
                    statement.executeBatch();
                    break;
                } catch (final SQLException e) {
                    statement.clearBatch();
                    if (!retryStrategy.apply(e) || retryCount > maxRetry) {
                        discards.addAll(handleDiscards(records, connection, query, e));
                        break;
                    }

                    retryCount++;
                    log.warn("Deadlock detected. retrying", e);
                    quiteRollback(connection);
                    try {
                        Thread.sleep(retryCount * 100 + 3000); // todo make this configurable (back pressure)
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                    }
                    final List<Record> needRetry = new ArrayList<>(records);
                    needRetry.removeAll(discards.stream().map(Reject::getRecord).collect(toSet()));
                    discards.addAll(processRecords(needRetry, connection, query));
                }
            }
        } while (true);

        return discards;
    }

    private List<Reject> handleDiscards(final List<Record> records, final Connection connection, final String query,
            final SQLException e) throws SQLException {
        if (!(e instanceof BatchUpdateException)) {
            throw e;
        }
        final List<Reject> discards = new ArrayList<>();
        final int[] result = BatchUpdateException.class.cast(e).getUpdateCounts();
        if (result.length == records.size()) {
            /* driver has executed all the batch statements */
            for (int i = 0; i < result.length; i++) {
                switch (result[i]) {
                case Statement.EXECUTE_FAILED:
                    final SQLException error = ofNullable(e.getNextException()).orElse(e);
                    discards.add(new Reject(error.getMessage(), error.getSQLState(), error.getErrorCode(), records.get(i)));
                    break;
                }
            }
        } else {
            /*
             * driver stopped executing batch statements after the failing one
             * all record after failure point need to be reprocessed
             */
            int failurePoint = result.length;
            final SQLException error = ofNullable(e.getNextException()).orElse(e);
            discards.add(new Reject(error.getMessage(), error.getSQLState(), error.getErrorCode(), records.get(failurePoint)));
            discards.addAll(processRecords(records.subList(failurePoint + 1, records.size()), connection, query));
        }

        return discards;
    }

    private List<Reject> prepareStatement(List<Record> records, PreparedStatement statement) throws SQLException {
        final List<Reject> discards = new ArrayList<>();
        for (final Record record : records) {
            statement.clearParameters();
            if (!validateQueryParam(record)) {
                discards.add(new Reject("missing required query param in this record", record));
                continue;
            }
            for (final Map.Entry<Integer, Schema.Entry> entry : getQueryParams().entrySet()) {
                RecordToSQLTypeConverter.valueOf(entry.getValue().getType().name()).setValue(statement, entry.getKey(),
                        entry.getValue(), record);
            }
            statement.addBatch();
        }
        return discards;
    }
}
