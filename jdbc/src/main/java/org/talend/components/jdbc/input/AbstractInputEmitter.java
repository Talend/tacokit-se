package org.talend.components.jdbc.input;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.InputConfig;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.sql.*;
import java.util.Date;
import java.util.stream.IntStream;

import static java.sql.ResultSetMetaData.columnNoNulls;
import static org.talend.sdk.component.api.record.Schema.Type.*;

@Slf4j
public abstract class AbstractInputEmitter implements Serializable {

    private final InputConfig inputConfig;

    private RecordBuilderFactory recordBuilderFactory;

    private final JdbcService jdbcDriversService;

    private final I18nMessage i18n;

    protected Connection connection;

    private Statement statement;

    private ResultSet resultSet;

    private JdbcService.JdbcDatasource dataSource;

    AbstractInputEmitter(final InputConfig inputConfig, final JdbcService jdbcDriversService,
            final RecordBuilderFactory recordBuilderFactory, final I18nMessage i18nMessage) {
        this.inputConfig = inputConfig;
        this.recordBuilderFactory = recordBuilderFactory;
        this.jdbcDriversService = jdbcDriversService;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() {
        if (inputConfig.getDataSet().getQuery() == null || inputConfig.getDataSet().getQuery().trim().isEmpty()) {
            throw new IllegalArgumentException(i18n.errorEmptyQuery());
        }
        if (jdbcDriversService.isNotReadOnlySQLQuery(inputConfig.getDataSet().getQuery())) {
            throw new IllegalArgumentException(i18n.errorUnauthorizedQuery());
        }

        try {
            dataSource = jdbcDriversService.createDataSource(inputConfig.getDataSet().getConnection());
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            statement.setFetchSize(inputConfig.getFetchSize());
            resultSet = statement.executeQuery(inputConfig.getDataSet().getQuery());
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Producer
    public Record next() {
        try {
            if (!resultSet.next()) {
                return null;
            }

            final Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder();
            final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
            final ResultSetMetaData metaData = resultSet.getMetaData();
            IntStream.rangeClosed(1, metaData.getColumnCount())
                    .forEach(index -> addColumn(recordBuilder, entryBuilder, metaData, index));
            return recordBuilder.build();

        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private void addColumn(final Record.Builder builder, final Schema.Entry.Builder entryBuilder,
            final ResultSetMetaData metaData, final int columnIndex) {
        try {
            final String javaType = metaData.getColumnClassName(columnIndex);
            final int sqlType = metaData.getColumnType(columnIndex);
            final Object value = resultSet.getObject(columnIndex);
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
                        value == null ? null : new Date(((java.sql.Date) value).getTime()));
                break;
            case java.sql.Types.TIME:
                builder.withDateTime(entryBuilder.withType(DATETIME).build(),
                        value == null ? null : new Date(((java.sql.Time) value).getTime()));
                break;
            case java.sql.Types.TIMESTAMP:
                builder.withDateTime(entryBuilder.withType(DATETIME).build(),
                        value == null ? null : new Date(((java.sql.Timestamp) value).getTime()));
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
            throw new IllegalStateException(e);
        }

    }

    @PreDestroy
    public void release() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                log.warn(i18n.warnResultSetCantBeClosed(), e);
            }
        }
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
