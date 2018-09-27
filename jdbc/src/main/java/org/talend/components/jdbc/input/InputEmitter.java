package org.talend.components.jdbc.input;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Date;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.jdbc.dataset.InputDataset;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "JDBCInput")
@Emitter(name = "Input")
@Documentation("JDBC query input")
public class InputEmitter implements Serializable {

    private final InputDataset queryDataset;

    private RecordBuilderFactory recordBuilderFactory;

    private final JdbcService jdbcDriversService;

    private final I18nMessage i18n;

    private Connection connection;

    private Statement statement;

    private ResultSet resultSet;

    public InputEmitter(@Option("configuration") final InputDataset queryDataSet, final JdbcService jdbcDriversService,
            final RecordBuilderFactory recordBuilderFactory, final I18nMessage i18nMessage) {
        this.queryDataset = queryDataSet;
        this.recordBuilderFactory = recordBuilderFactory;
        this.jdbcDriversService = jdbcDriversService;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() {
        final String query = jdbcDriversService.createQuery(queryDataset);
        try {
            connection = jdbcDriversService.connection(queryDataset.getConnection());
            try {
                connection.setReadOnly(true);
            } catch (final Throwable e) {
                log.warn(i18n.warnReadOnlyOptimisationFailure(), e); // not supported in some database
            }
            if (connection.getMetaData().getDriverName().toLowerCase().contains("mysql")) {
                statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                final Class<?> clazz = statement.getClass();
                try {
                    Method method = clazz.getMethod("enableStreamingResults");
                    if (method != null) { // have to use reflect here
                        method.invoke(statement);
                    }
                } catch (Exception e) { // ignore anything
                }
            } else {
                statement = connection.createStatement();
            }
            resultSet = statement.executeQuery(query);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Producer
    public Record next() {
        try {
            final boolean hasNext = resultSet.next();
            if (hasNext) {
                Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder();
                for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                    final String name = resultSet.getMetaData().getColumnName(i);
                    final int type = resultSet.getMetaData().getColumnType(i);
                    final Object value = resultSet.getObject(i);
                    addColumn(recordBuilder, name, type, value);
                }
                return recordBuilder.build();
            }

            return null;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private void addColumn(final Record.Builder builder, final String name, final int sqlType, final Object value) {
        if (value == null) {
            return;
        }

        switch (sqlType) {
        case java.sql.Types.INTEGER:
        case java.sql.Types.SMALLINT:
        case java.sql.Types.TINYINT:
            builder.withInt(name, (Integer) value);
            break;
        case java.sql.Types.BIGINT:
            builder.withLong(name, (Long) value);
            break;
        case java.sql.Types.FLOAT:
            builder.withFloat(name, (Float) value);
            break;
        case java.sql.Types.DOUBLE:
        case java.sql.Types.REAL:
            builder.withDouble(name, (Double) value);
            break;
        case java.sql.Types.BOOLEAN:
            builder.withBoolean(name, (Boolean) value);
            break;
        case java.sql.Types.DATE:
            builder.withDateTime(name, new Date(((java.sql.Date) value).getTime()));
            break;
        case java.sql.Types.TIME:
            builder.withTimestamp(name, ((java.sql.Time) value).getTime());
            break;
        case java.sql.Types.TIMESTAMP:
            builder.withTimestamp(name, ((java.sql.Timestamp) value).getTime());
            break;
        case java.sql.Types.BINARY:
        case java.sql.Types.VARBINARY:
        case Types.LONGVARBINARY:
            builder.withBytes(name, (byte[]) value);
            break;
        case java.sql.Types.DECIMAL:
        case java.sql.Types.NUMERIC:
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.CHAR:
        default:
            builder.withString(name, String.valueOf(value));
            break;
        }
    }

    @PreDestroy
    public void close() {
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
                connection.close();
            } catch (SQLException e) {
                log.warn(i18n.warnConnectionCantBeClosed(), e);
            }
        }
    }
}
