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
package org.talend.components.jdbc.service;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.JdbcConfiguration;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.dependency.Resolver;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;

@Slf4j
@Service
public class JdbcService {

    private static Pattern READ_ONLY_QUERY_PATTERN = Pattern.compile(
            "^SELECT\\s+((?!((\\bINTO\\b)|(\\bFOR\\s+UPDATE\\b)|(\\bLOCK\\s+IN\\s+SHARE\\s+MODE\\b))).)+$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);

    private final Map<JdbcConfiguration.Driver, URL[]> drivers = new HashMap<>();

    @Service
    private Resolver resolver;

    @Service
    private I18nMessage i18n;

    @Configuration("jdbc")
    private Supplier<JdbcConfiguration> jdbcConfiguration;

    @Service
    private LocalConfiguration localConfiguration;

    public boolean driverNotDisabled(JdbcConfiguration.Driver driver) {
        return !ofNullable(localConfiguration.get("jdbc.driver." + driver.getId().toLowerCase(Locale.ROOT) + ".skip"))
                .map(Boolean::valueOf).orElse(false);
    }

    private URL[] getDriverFiles(final JdbcConfiguration.Driver driver) {
        return drivers.computeIfAbsent(driver, key -> {
            final Collection<File> driverFiles = resolver.resolveFromDescriptor(
                    new ByteArrayInputStream(driver.getPaths().stream().filter(p -> p != null && !p.trim().isEmpty())
                            .collect(joining("\n")).getBytes(StandardCharsets.UTF_8)));
            final String missingJars = driverFiles.stream().filter(f -> !f.exists()).map(File::getAbsolutePath)
                    .collect(joining("\n"));
            if (!missingJars.isEmpty()) {
                throw new IllegalStateException(i18n.errorDriverLoad(driver.getId(), missingJars));
            }
            return driverFiles.stream().map(f -> {
                try {
                    return f.toURI().toURL();
                } catch (MalformedURLException e) {
                    throw new IllegalStateException(e);
                }
            }).toArray(URL[]::new);
        });
    }

    /**
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

    public JdbcDatasource createDataSource(final JdbcConnection connection) {
        final JdbcConfiguration.Driver driver = getDriver(connection);
        return new JdbcDatasource(connection, getDriverFiles(driver), driver, false, false);
    }

    public JdbcDatasource createDataSource(final JdbcConnection connection, final boolean rewriteBatchedStatements) {
        final JdbcConfiguration.Driver driver = getDriver(connection);
        return new JdbcDatasource(connection, getDriverFiles(driver), driver, false, rewriteBatchedStatements);
    }

    public static class JdbcDatasource implements AutoCloseable {

        private final URLClassLoader classLoader;

        private HikariDataSource dataSource;

        JdbcDatasource(final JdbcConnection connection, final URL[] driverFiles, final JdbcConfiguration.Driver driver,
                final boolean isAutoCommit, final boolean rewriteBatchedStatements) {

            final Thread thread = Thread.currentThread();
            final ClassLoader prev = thread.getContextClassLoader();
            this.classLoader = new URLClassLoader(driverFiles, prev);
            try {
                thread.setContextClassLoader(classLoader);
                dataSource = new HikariDataSource();
                dataSource.setPoolName("Hikari-" + thread.getName() + "#" + thread.getId());
                dataSource.setUsername(connection.getUserId());
                dataSource.setPassword(connection.getPassword());
                dataSource.setDriverClassName(driver.getClassName());
                dataSource.setJdbcUrl(connection.getJdbcUrl());
                dataSource.setAutoCommit(isAutoCommit);
                dataSource.setMaximumPoolSize(1);
                // todo : make this configurable
                dataSource.setLeakDetectionThreshold(15 * 60 * 1000);
                dataSource.setConnectionTimeout(30 * 1000);
                dataSource.setValidationTimeout(10 * 1000);
                PlatformFactory.get(connection).addDataSourceProperties(dataSource);
                dataSource.addDataSourceProperty("rewriteBatchedStatements", String.valueOf(rewriteBatchedStatements));
                // dataSource.addDataSourceProperty("cachePrepStmts", "true");
                // dataSource.addDataSourceProperty("prepStmtCacheSize", "250");
                // dataSource.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
                // dataSource.addDataSourceProperty("useServerPrepStmts", "true");
            } finally {
                thread.setContextClassLoader(prev);
            }
        }

        public Connection getConnection() throws SQLException {
            final Thread thread = Thread.currentThread();
            final ClassLoader prev = thread.getContextClassLoader();
            try {
                thread.setContextClassLoader(classLoader);
                return dataSource.getConnection();
            } finally {
                thread.setContextClassLoader(prev);
            }
        }

        @Override
        public void close() {
            try {
                dataSource.close();
            } finally {
                try {
                    classLoader.close();
                } catch (final IOException e) {
                    log.error("can't close driver classloader properly", e);
                }
            }
        }
    }

}
