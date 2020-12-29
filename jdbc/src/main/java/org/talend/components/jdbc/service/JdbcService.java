/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.jdbc.service;

import static java.sql.ResultSetMetaData.columnNoNulls;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static org.talend.components.jdbc.ErrorFactory.toIllegalStateException;
import static org.talend.sdk.component.api.record.Schema.Type.BOOLEAN;
import static org.talend.sdk.component.api.record.Schema.Type.BYTES;
import static org.talend.sdk.component.api.record.Schema.Type.DATETIME;
import static org.talend.sdk.component.api.record.Schema.Type.DOUBLE;
import static org.talend.sdk.component.api.record.Schema.Type.FLOAT;
import static org.talend.sdk.component.api.record.Schema.Type.INT;
import static org.talend.sdk.component.api.record.Schema.Type.LONG;
import static org.talend.sdk.component.api.record.Schema.Type.STRING;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.components.jdbc.configuration.JdbcConfiguration;
import org.talend.components.jdbc.datastore.AuthenticationType;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.dependency.Resolver;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.zaxxer.hikari.HikariDataSource;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class JdbcService {

    private static final String SNOWFLAKE_DATABASE_NAME = "Snowflake";

    private static Pattern READ_ONLY_QUERY_PATTERN = Pattern.compile(
            "^SELECT\\s+((?!((\\bINTO\\b)|(\\bFOR\\s+UPDATE\\b)|(\\bLOCK\\s+IN\\s+SHARE\\s+MODE\\b))).)+$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);

    private final Map<JdbcConfiguration.Driver, URL[]> drivers = new HashMap<>();

    @Service
    private Resolver resolver;

    @Service
    private I18nMessage i18n;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Configuration("jdbc")
    private Supplier<JdbcConfiguration> jdbcConfiguration;

    @Service
    private LocalConfiguration localConfiguration;

    public boolean driverNotDisabled(JdbcConfiguration.Driver driver) {
        return !ofNullable(localConfiguration.get("jdbc.driver." + driver.getId().toLowerCase(Locale.ROOT) + ".skip"))
                .map(Boolean::valueOf).orElse(false);
    }

    /**
     *
     * @param query the query to check
     * @return return false if the sql query is not a read only query, true otherwise
     */
    public boolean isNotReadOnlySQLQuery(final String query) {
        return query != null && !READ_ONLY_QUERY_PATTERN.matcher(query.trim()).matches();
    }

    private JdbcConfiguration.Driver getDriver(final JdbcConnection dataStore) {
        return jdbcConfiguration.get().getDrivers().stream().filter(this::driverNotDisabled)
                .filter(d -> d.getId()
                        .equals(ofNullable(dataStore.getHandler()).filter(h -> !h.isEmpty()).orElse(dataStore.getDbType())))
                .filter(d -> d.getHandlers() == null || d.getHandlers().isEmpty()).findFirst()
                .orElseThrow(() -> new IllegalStateException(i18n.errorDriverNotFound(dataStore.getDbType())));
    }

    public static boolean checkTableExistence(final String tableName, final JdbcService.JdbcDatasource dataSource)
            throws SQLException {
        try (final Connection connection = dataSource.getConnection()) {
            try (final ResultSet resultSet = connection.getMetaData().getTables(connection.getCatalog(), getSchema(connection),
                    tableName, new String[] { "TABLE", "SYNONYM" })) {
                while (resultSet.next()) {
                    if (ofNullable(ofNullable(resultSet.getString("TABLE_NAME")).orElseGet(() -> {
                        try {
                            return resultSet.getString("SYNONYM_NAME");
                        } catch (final SQLException e) {
                            return null;
                        }
                    })).filter(tableName::equals).isPresent()) {
                        return true;
                    }
                }
                return false;
            }
        }
    }

    public JdbcDatasource createDataSource(final JdbcConnection connection) {
        return new JdbcDatasource(i18n, resolver, connection, getDriver(connection), false, false);
    }

    public JdbcDatasource createDataSource(final JdbcConnection connection, final boolean rewriteBatchedStatements) {
        return new JdbcDatasource(i18n, resolver, connection, getDriver(connection), false, rewriteBatchedStatements);
    }

    public JdbcDatasource createDataSource(final JdbcConnection connection, boolean isAutoCommit,
            final boolean rewriteBatchedStatements) {
        final JdbcConfiguration.Driver driver = getDriver(connection);
        return new JdbcDatasource(i18n, resolver, connection, driver, isAutoCommit, rewriteBatchedStatements);
    }

    public static class JdbcDatasource implements AutoCloseable {

        private final Resolver.ClassLoaderDescriptor classLoaderDescriptor;

        private HikariDataSource dataSource;

        public JdbcDatasource(final I18nMessage i18nMessage, final Resolver resolver, final JdbcConnection connection,
                final JdbcConfiguration.Driver driver, final boolean isAutoCommit, final boolean rewriteBatchedStatements) {
            final Thread thread = Thread.currentThread();
            final ClassLoader prev = thread.getContextClassLoader();

            classLoaderDescriptor = resolver.mapDescriptorToClassLoader(driver.getPaths());
            String missingJars = driver.getPaths().stream().filter(p -> classLoaderDescriptor.resolvedDependencies().contains(p))
                    .collect(joining("\n"));
            if (!classLoaderDescriptor.resolvedDependencies().containsAll(driver.getPaths())) {
                throw new IllegalStateException(i18nMessage.errorDriverLoad(driver.getId(), missingJars));
            }

            try {
                thread.setContextClassLoader(classLoaderDescriptor.asClassLoader());
                dataSource = new HikariDataSource();
                if ("MSSQL_JTDS".equals(driver.getId())) {
                    dataSource.setConnectionTestQuery("SELECT 1");
                }
                dataSource.setUsername(connection.getUserId());
                if (SNOWFLAKE_DATABASE_NAME.equals(connection.getDbType())
                        && AuthenticationType.KEY_PAIR == connection.getAuthenticationType()) {
                    dataSource.addDataSourceProperty("privateKey", PrivateKeyUtils.getPrivateKey(connection.getPrivateKey(),
                            connection.getPrivateKeyPassword(), i18nMessage));
                } else {
                    dataSource.setPassword(connection.getPassword());
                }
                dataSource.setDriverClassName(driver.getClassName());
                dataSource.setJdbcUrl(connection.getJdbcUrl());
                dataSource.setAutoCommit(isAutoCommit);
                dataSource.setMaximumPoolSize(1);
                dataSource.setConnectionTimeout(connection.getConnectionTimeOut() * 1000);
                dataSource.setValidationTimeout(connection.getConnectionValidationTimeOut() * 1000);
                PlatformFactory.get(connection, i18nMessage).addDataSourceProperties(dataSource);
                dataSource.addDataSourceProperty("rewriteBatchedStatements", String.valueOf(rewriteBatchedStatements));
                // dataSource.addDataSourceProperty("cachePrepStmts", "true");
                // dataSource.addDataSourceProperty("prepStmtCacheSize", "250");
                // dataSource.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
                // dataSource.addDataSourceProperty("useServerPrepStmts", "true");

                // Security Issues with LOAD DATA LOCAL https://jira.talendforge.org/browse/TDI-42001
                dataSource.addDataSourceProperty("allowLoadLocalInfile", "false"); // MySQL
                dataSource.addDataSourceProperty("allowLocalInfile", "false"); // MariaDB

                driver.getFixedParameters().stream().forEach(kv -> dataSource.addDataSourceProperty(kv.getKey(), kv.getValue()));

            } finally {
                thread.setContextClassLoader(prev);
            }
        }

        public Connection getConnection() throws SQLException {
            final Thread thread = Thread.currentThread();
            final ClassLoader prev = thread.getContextClassLoader();
            try {
                thread.setContextClassLoader(classLoaderDescriptor.asClassLoader());
                return wrap(classLoaderDescriptor.asClassLoader(), dataSource.getConnection(), Connection.class);
            } finally {
                thread.setContextClassLoader(prev);
            }
        }

        @Override
        public void close() {
            final Thread thread = Thread.currentThread();
            final ClassLoader prev = thread.getContextClassLoader();
            try {
                thread.setContextClassLoader(classLoaderDescriptor.asClassLoader());
                dataSource.close();
            } finally {
                thread.setContextClassLoader(prev);
                try {
                    classLoaderDescriptor.close();
                } catch (final Exception e) {
                    log.error("can't close driver classloader properly", e);
                }
            }
        }

        private static <T> T wrap(final ClassLoader classLoader, final Object delegate, final Class<T> api) {
            return api.cast(
                    Proxy.newProxyInstance(classLoader, new Class<?>[] { api }, new ContextualDelegate(delegate, classLoader)));
        }

        @AllArgsConstructor
        private static class ContextualDelegate implements InvocationHandler {

            private final Object delegate;

            private final ClassLoader classLoader;

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                final Thread thread = Thread.currentThread();
                final ClassLoader prev = thread.getContextClassLoader();
                thread.setContextClassLoader(classLoader);
                try {
                    final Object invoked = method.invoke(delegate, args);
                    if (method.getReturnType().getName().startsWith("java.sql.") && method.getReturnType().isInterface()) {
                        return wrap(classLoader, invoked, method.getReturnType());
                    }
                    return invoked;
                } catch (final InvocationTargetException ite) {
                    throw ite.getTargetException();
                } finally {
                    thread.setContextClassLoader(prev);
                }
            }
        }
    }

    public static String getSchema(Connection connection) throws SQLException {
        // Special code for MSSQL JDTS driver
        String schema = null;
        try {

            schema = connection.getSchema();
        } catch (AbstractMethodError e) {
            // ignore
        }
        return schema;
    }

    public void addField(final Schema.Builder builder, final ResultSetMetaData metaData, final int columnIndex) {
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
                if (javaType.equals(Integer.class.getName()) || Short.class.getName().equals(javaType)) {
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

            log.warn("[addField] {} {} {}.", metaData.getColumnName(columnIndex), javaType, sqlType);

        } catch (final SQLException e) {
            throw toIllegalStateException(e);
        }
    }

    public void addColumn(final Record.Builder builder, final ResultSetMetaData metaData, final int columnIndex,
            final ResultSet resultSet) {
        try {
            final int sqlType = metaData.getColumnType(columnIndex);
            final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
            final Object value = resultSet.getObject(columnIndex);
            entryBuilder.withName(metaData.getColumnName(columnIndex))
                    .withNullable(metaData.isNullable(columnIndex) != columnNoNulls);
            switch (sqlType) {
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
            case java.sql.Types.INTEGER:
                if (value != null) {
                    if (value instanceof Integer) {
                        builder.withInt(entryBuilder.withType(INT).build(), (Integer) value);
                    } else if (value instanceof Short) {
                        builder.withInt(entryBuilder.withType(INT).build(), ((Short) value).intValue());
                    } else {
                        builder.withLong(entryBuilder.withType(LONG).build(), Long.valueOf(value.toString()));
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
            throw toIllegalStateException(e);
        }
    }

}
