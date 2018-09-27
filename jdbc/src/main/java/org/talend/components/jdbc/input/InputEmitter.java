package org.talend.components.jdbc.input;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.talend.components.jdbc.JdbcConfiguration;
import org.talend.components.jdbc.dataset.InputDataset;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.configuration.Configuration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "JDBCInput")
@Emitter(name = "Input")
@Documentation("JDBC query input")
public class InputEmitter implements Serializable {

    private final InputDataset queryDataset;

    private JsonBuilderFactory jsonBuilderFactory;

    private final JdbcService jdbcDriversService;

    private final I18nMessage i18n;

    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private final DateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

    private final DateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private Connection connection;

    private Statement statement;

    private ResultSet resultSet;

    public InputEmitter(@Option("configuration") final InputDataset queryDataSet, final JdbcService jdbcDriversService,
            final JsonBuilderFactory jsonBuilderFactory, final I18nMessage i18nMessage) {
        this.queryDataset = queryDataSet;
        this.jsonBuilderFactory = jsonBuilderFactory;
        this.jdbcDriversService = jdbcDriversService;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() {
        final String query = jdbcDriversService.createQuery(queryDataset);
        final String dbType = queryDataset.getConnection().getDbType();

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
    public JsonObject next() {

        try {
            final boolean hasNext = resultSet.next();
            if (hasNext) {
                JsonObjectBuilder recordBuilder = jsonBuilderFactory.createObjectBuilder();
                for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                    final String name = resultSet.getMetaData().getColumnName(i);
                    final int type = resultSet.getMetaData().getColumnType(i);
                    final Object value = resultSet.getObject(i);
                    recordBuilder = addColumn(recordBuilder, name, type, value);
                }
                return recordBuilder.build();
            }

            return null;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private JsonObjectBuilder addColumn(final JsonObjectBuilder builder, final String name, final int sqlType,
            final Object value) {
        switch (sqlType) {
        case java.sql.Types.INTEGER:
        case java.sql.Types.SMALLINT:
        case java.sql.Types.TINYINT:
            return builder.add(name, Integer.class.cast(value));
        case java.sql.Types.BIGINT:
            return builder.add(name, Long.class.cast(value));
        case java.sql.Types.DECIMAL:
        case java.sql.Types.NUMERIC:
        case java.sql.Types.DOUBLE:
        case java.sql.Types.FLOAT:
        case java.sql.Types.REAL:
            return builder.add(name, Double.class.cast(value));
        case java.sql.Types.BOOLEAN:
            return builder.add(name, Boolean.class.cast(value));
        case java.sql.Types.DATE:
            return builder.add(name, dateFormat.format(String.class.cast(value)));
        case java.sql.Types.TIME:
            return builder.add(name, timeFormat.format(String.class.cast(value)));
        case java.sql.Types.TIMESTAMP:
            return builder.add(name, timestampFormat.format(String.class.cast(value)));
        case java.sql.Types.VARCHAR:
        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.CHAR:
        default:
            return builder.add(name, String.class.cast(value));
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
