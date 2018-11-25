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

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

@Data
@Slf4j
public abstract class JdbcAction {

    private final OutputConfiguration configuration;

    private final I18nMessage i18n;

    private final Supplier<Connection> connection;

    protected abstract String buildQuery(List<Record> records);

    protected abstract Map<Integer, Schema.Entry> getQueryParams();

    protected abstract boolean validateQueryParam(Record record);

    public List<Reject> execute(final List<Record> records) throws SQLException {
        if (records.isEmpty()) {
            return emptyList();
        }

        final String query = buildQuery(records);
        final Connection connection = this.connection.get();
        try (final PreparedStatement statement = connection.prepareStatement(query)) {
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

            try {
                statement.executeBatch();
            } catch (final SQLException e) {
                statement.clearBatch();
                if (!(e instanceof BatchUpdateException)) {
                    throw e;
                }
                final BatchUpdateException batchUpdateException = (BatchUpdateException) e;
                int[] result = batchUpdateException.getUpdateCounts();
                if (result.length == records.size()) {
                    /* driver has executed all the batch statements */
                    for (int i = 0; i < result.length; i++) {
                        switch (result[i]) {
                        case Statement.EXECUTE_FAILED:
                            final SQLException error = (SQLException) batchUpdateException.iterator().next();
                            discards.add(
                                    new Reject(error.getMessage(), error.getSQLState(), error.getErrorCode(), records.get(i)));
                            break;
                        }
                    }
                } else {
                    /*
                     * driver stopped executing batch statements after the failing one
                     * all record after failure point need to be reprocessed
                     */
                    int failurePoint = result.length;
                    final SQLException error = (SQLException) batchUpdateException.iterator().next();
                    discards.add(
                            new Reject(error.getMessage(), error.getSQLState(), error.getErrorCode(), records.get(failurePoint)));
                    discards.addAll(execute(records.subList(failurePoint + 1, records.size())));
                }
            }

            return discards;
        }
    }

}
