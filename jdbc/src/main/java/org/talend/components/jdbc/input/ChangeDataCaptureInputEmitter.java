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
package org.talend.components.jdbc.input;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.InputCaptureDataChangeConfig;
import org.talend.components.jdbc.dataset.ChangeDataCaptureDataset;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.*;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.sql.*;
import java.util.stream.IntStream;

import static java.sql.ResultSetMetaData.columnNoNulls;
import static org.talend.components.jdbc.ErrorFactory.toIllegalStateException;
import static org.talend.sdk.component.api.record.Schema.Type.*;
import static org.talend.sdk.component.api.record.Schema.Type.STRING;

@Slf4j

@Documentation("JDBC input using stream table name")
public class ChangeDataCaptureInputEmitter implements Serializable {

    private final InputCaptureDataChangeConfig inputConfig;

    private RecordBuilderFactory recordBuilderFactory;

    private final JdbcService jdbcDriversService;

    private final I18nMessage i18n;

    protected Connection connection;

    private static Statement statement;

    private Statement statementUpdate;

    private static ResultSet resultSet;

    private static int resultSetSize;

    private static JdbcService.JdbcDatasource dataSource;

    private ChangeDataCaptureDataset cdcDataset;

    private transient Schema schema;

    private static int nbRecords = 0;

    private static int nbIterations = 0;

    private static long lastFetchTime = 0;

    ChangeDataCaptureInputEmitter(@Option("configuration") final InputCaptureDataChangeConfig config,
            final JdbcService jdbcDriversService, final RecordBuilderFactory recordBuilderFactory,
            final I18nMessage i18nMessage) {
        this.inputConfig = config;
        this.recordBuilderFactory = recordBuilderFactory;
        this.jdbcDriversService = jdbcDriversService;
        this.i18n = i18nMessage;
        this.cdcDataset = ((ChangeDataCaptureDataset) config.getDataSet());
    }

    @PostConstruct
    public void init() {
        log.debug("Init is called");
        if (inputConfig.getDataSet().getQuery() == null || inputConfig.getDataSet().getQuery().trim().isEmpty()) {
            throw new IllegalArgumentException(i18n.errorEmptyQuery());
        }
        if (jdbcDriversService.isNotReadOnlySQLQuery(inputConfig.getDataSet().getQuery())) {
            throw new IllegalArgumentException(i18n.errorUnauthorizedQuery());
        }

        String createStreamStatement = this.cdcDataset.createStreamTableIfNotExist();

        try {
            dataSource = jdbcDriversService.createDataSource(inputConfig.getDataSet().getConnection());
            connection = dataSource.getConnection();
            // first statement to create stream table if needed
            statementUpdate = connection.createStatement();
            int result = statementUpdate.executeUpdate(createStreamStatement);
            fetchData();
        } catch (final SQLException e) {
            throw toIllegalStateException(e);
        }
    }

    @Producer
    public Record next() {
        log.debug("Next is called; nbRecords: " + nbRecords + ",nbIterations: " + nbIterations);
        boolean commit = (inputConfig.getChangeOffsetOnRead() == InputCaptureDataChangeConfig.ChangeOffsetOnReadStrategy.YES);
        try {
            boolean resultSetIsNull = (resultSet == null);
            boolean noNext = resultSetIsNull || (resultSetSize == 0) || (resultSet.getRow() == resultSetSize);

            if (noNext && commit) {
                if (resultSet != null)
                    resultSet.close();
                fetchData();
                if (resultSetSize == 0) {
                    return null;
                }
            }

            if (!resultSet.next())
                if (!commit)
                    return null;
            final ResultSetMetaData metaData = resultSet.getMetaData();

            if (schema == null) {
                final Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(RECORD);
                IntStream.rangeClosed(1, metaData.getColumnCount()).forEach(index -> addField(schemaBuilder, metaData, index));
                schema = schemaBuilder.build();
            }

            final Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder(schema);
            IntStream.rangeClosed(1, metaData.getColumnCount()).forEach(index -> addColumn(recordBuilder, metaData, index));

            if (nbRecords == resultSetSize)
                log.info("Last record of series emitted: " + getRowAsString());

            nbRecords++;
            return recordBuilder.build();
        } catch (final SQLException e) {
            log.error("Exception found in next() ", e);
            throw toIllegalStateException(e);
        } catch (final Exception e) {
            log.error("Exception found in next() ", e);
            throw toIllegalStateException(e);
        }
    }

    private int getSize(ResultSet rs) {
        int rowscount = 0;
        try {
            if (rs == null || rs.isClosed())
                return 0;
            while (rs.next()) {
                log.debug("advance cursor");
            }
            rowscount = rs.getRow();
        } catch (final SQLException e) {
            log.error("Exception found in next() ", e);
            return 0;
        }
        return rowscount;
    }

    private void fetchData() {
        ResultSet resultSetCopy;
        boolean commit = (inputConfig.getChangeOffsetOnRead() == InputCaptureDataChangeConfig.ChangeOffsetOnReadStrategy.YES);
        log.info("Fetch data attempt, commmit: " + commit);
        nbIterations++;
        try {
            long time = System.currentTimeMillis();

            if ((time - lastFetchTime < 2000) && (resultSetSize == 0))
                return;

            resultSetSize = 0;

            log.info("------------Fetch data------------------");
            log.info("Fetch data with query: " + inputConfig.getDataSet().getQuery());

            lastFetchTime = time;
            connection.setAutoCommit(false);
            // then read from stream table to compute size
            statement = connection.createStatement();
            statement.setFetchSize(inputConfig.getDataSet().getFetchSize());
            resultSetCopy = statement.executeQuery(inputConfig.getDataSet().getQuery());
            resultSetSize = getSize(resultSetCopy);
            resultSetCopy.close();
            // then read from stream table to compute size
            statement = connection.createStatement();
            statement.setFetchSize(inputConfig.getDataSet().getFetchSize());
            resultSet = statement.executeQuery(inputConfig.getDataSet().getQuery());
            connection.commit();
            connection.setAutoCommit(true);

            // move the offset
            if (commit && (resultSetSize != 0)) {
                log.info("Result set size: " + resultSetSize);
                connection.setAutoCommit(false);
                statementUpdate = connection.createStatement();
                String createStreamCounterStatement = this.cdcDataset.createCounterTableIfNotExist();
                statementUpdate = connection.createStatement();
                int resultCreateCounter = statementUpdate.executeUpdate(createStreamCounterStatement);
                log.debug("Update statement changed records: " + resultCreateCounter);
                String consumeRecordsStreamStatement = this.cdcDataset.createStatementConsumeStreamTable();
                statementUpdate = connection.createStatement();
                int resultConsumeRecordsStream = statementUpdate.executeUpdate(consumeRecordsStreamStatement);
                connection.commit();
                connection.setAutoCommit(true);
            } else {
                log.debug("consumption=no");
            }

        } catch (final SQLException e) {
            log.error("Exception found in next() ", e);
            throw toIllegalStateException(e);
        } catch (final Exception e) {
            log.error("Exception found in next() ", e);
            throw toIllegalStateException(e);
        }
    }

    private String getRowAsString() throws Exception {
        if (resultSet == null) {
            return "Cannot print ResultSet is null";
        }
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int current = resultSet.getRow();
        String res = "Row " + current + ": ";

        for (int index = 1; index <= metaData.getColumnCount(); index++) {
            final String columnName = metaData.getColumnName(index);
            final Object value = resultSet.getObject(index);
            res = res + columnName + ":" + value + ", ";
        }
        return res;
    }

    private void addField(final Schema.Builder builder, final ResultSetMetaData metaData, final int columnIndex) {
        try {
            final String javaType = metaData.getColumnClassName(columnIndex);
            final int sqlType = metaData.getColumnType(columnIndex);
            final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
            entryBuilder.withName(metaData.getColumnName(columnIndex))
                    .withNullable(metaData.isNullable(columnIndex) != columnNoNulls);
            switch (sqlType) {
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
            case java.sql.Types.INTEGER:
                if (javaType.equals(Integer.class.getName())) {
                    builder.withEntry(entryBuilder.withType(INT).build());
                } else {
                    builder.withEntry(entryBuilder.withType(LONG).build());
                }
                break;
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                builder.withEntry(entryBuilder.withType(FLOAT).build());
                break;
            case java.sql.Types.DOUBLE:
                builder.withEntry(entryBuilder.withType(DOUBLE).build());
                break;
            case java.sql.Types.BOOLEAN:
                builder.withEntry(entryBuilder.withType(BOOLEAN).build());
                break;
            case java.sql.Types.TIME:
            case java.sql.Types.DATE:
            case java.sql.Types.TIMESTAMP:
                builder.withEntry(entryBuilder.withType(DATETIME).build());
                break;
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case Types.LONGVARBINARY:
                builder.withEntry(entryBuilder.withType(BYTES).build());
                break;
            case java.sql.Types.BIGINT:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.CHAR:
            default:
                builder.withEntry(entryBuilder.withType(STRING).build());
                break;
            }
        } catch (final SQLException e) {
            throw toIllegalStateException(e);
        }
    }

    private void addColumn(final Record.Builder builder, final ResultSetMetaData metaData, final int columnIndex) {
        try {
            final String javaType = metaData.getColumnClassName(columnIndex);
            final int sqlType = metaData.getColumnType(columnIndex);
            final Object value = resultSet.getObject(columnIndex);
            final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
            entryBuilder.withName(metaData.getColumnName(columnIndex))
                    .withNullable(metaData.isNullable(columnIndex) != columnNoNulls);
            switch (sqlType) {
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
            case java.sql.Types.INTEGER:
                if (value != null) {
                    if (javaType.equals(Integer.class.getName())) {
                        builder.withInt(entryBuilder.withType(INT).build(), (Integer) value);
                    } else {
                        builder.withLong(entryBuilder.withType(LONG).build(), (Long) value);
                    }
                }
                break;
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                if (value != null) {
                    builder.withFloat(entryBuilder.withType(FLOAT).build(), (Float) value);
                }
                break;
            case java.sql.Types.DOUBLE:
                if (value != null) {
                    builder.withDouble(entryBuilder.withType(DOUBLE).build(), (Double) value);
                }
                break;
            case java.sql.Types.BOOLEAN:
                if (value != null) {
                    builder.withBoolean(entryBuilder.withType(BOOLEAN).build(), (Boolean) value);
                }
                break;
            case java.sql.Types.DATE:
                builder.withDateTime(entryBuilder.withType(DATETIME).build(),
                        value == null ? null : new java.util.Date(((java.sql.Date) value).getTime()));
                break;
            case java.sql.Types.TIME:
                builder.withDateTime(entryBuilder.withType(DATETIME).build(),
                        value == null ? null : new java.util.Date(((java.sql.Time) value).getTime()));
                break;
            case java.sql.Types.TIMESTAMP:
                builder.withDateTime(entryBuilder.withType(DATETIME).build(),
                        value == null ? null : new java.util.Date(((java.sql.Timestamp) value).getTime()));
                break;
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case Types.LONGVARBINARY:
                builder.withBytes(entryBuilder.withType(BYTES).build(), value == null ? null : (byte[]) value);
                break;
            case java.sql.Types.BIGINT:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.CHAR:
            default:
                builder.withString(entryBuilder.withType(STRING).build(), value == null ? null : String.valueOf(value));
                break;
            }
        } catch (final SQLException e) {
            throw toIllegalStateException(e);
        }
    }

    @PreDestroy
    public void release() {
        /**
         * Avoid closing resultSet - at least temporary - to be able to have more than 1 record
         */
        /*
         * if (resultSet != null) {
         * try {
         * resultSet.close();
         * } catch (SQLException e) {
         * log.warn(i18n.warnResultSetCantBeClosed(), e);
         * }
         * }
         */
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                log.warn(i18n.warnStatementCantBeClosed(), e);
            }
        }
        if (connection != null) {
            try {
                connection.commit();
            } catch (final SQLException e) {
                log.error(i18n.errorSQL(e.getErrorCode(), e.getMessage()), e);
                try {
                    connection.rollback();
                } catch (final SQLException rollbackError) {
                    log.error(i18n.errorSQL(rollbackError.getErrorCode(), rollbackError.getMessage()), rollbackError);
                }
            }
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn(i18n.warnConnectionCantBeClosed(), e);
            }
        }
        if (dataSource != null) {
            dataSource.close();
        }
    }
}