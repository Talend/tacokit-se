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
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.dependency.Resolver;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
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

    private final Map<JdbcConfiguration.Driver, URLClassLoader> driversClassLoaders = new HashMap<>();

    @Service
    private Resolver resolver;

    @Service
    private I18nMessage i18n;

    @Configuration("jdbc")
    private Supplier<JdbcConfiguration> jdbcConfiguration;

    @PostConstruct
    public void init() {
    }

    /**
     * At this point we will deregister all the drivers that may were registered by this service.
     */
    @PreDestroy
    public void clean() {
        driversClassLoaders.forEach((driver, classLoader) -> {
            try {
                classLoader.close();
            } catch (final IOException e) {
                log.warn(i18n.warnDriverClose(driver.getId()));
            }
        });
    }

    private URLClassLoader getOrCreateDriverClassLoader(final JdbcConfiguration.Driver driver) {
        return driversClassLoaders.computeIfAbsent(driver, key -> {
            final Collection<File> driverFiles = resolver.resolveFromDescriptor(
                    new ByteArrayInputStream(driver.getPaths().stream().filter(p -> p != null && !p.trim().isEmpty())
                            .collect(joining("\n")).getBytes(StandardCharsets.UTF_8)));
            final String missingJars = driverFiles.stream().filter(f -> !f.exists()).map(File::getAbsolutePath)
                    .collect(joining("\n"));
            if (!missingJars.isEmpty()) {
                throw new IllegalStateException(i18n.errorDriverLoad(driver.getId(), missingJars));
            }
            final URL[] urls = driverFiles.stream().map(f -> {
                try {
                    return f.toURI().toURL();
                } catch (MalformedURLException e) {
                    throw new IllegalStateException(e);
                }
            }).toArray(URL[]::new);
            return new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
        });
    }

    /**
     * @return return false if the sql query is not a read only query, true otherwise
     */
    public boolean isNotReadOnlySQLQuery(final String query) {
        return query != null && !READ_ONLY_QUERY_PATTERN.matcher(query.trim()).matches();
    }

    private JdbcConfiguration.Driver getDriver(final JdbcConnection dataStore) {
        return jdbcConfiguration.get().getDrivers().stream()
                .filter(d -> d.getId()
                        .equals(ofNullable(dataStore.getHandler()).filter(h -> !h.isEmpty()).orElse(dataStore.getDbType())))
                .filter(d -> d.getHandlers() == null || d.getHandlers().isEmpty()).findFirst()
                .orElseThrow(() -> new IllegalStateException(i18n.errorDriverNotFound(dataStore.getDbType())));
    }

    public HikariDataSource createDataSourceReadOnly(final JdbcConnection datastore) {
        return createDataSource(datastore, true, true, false);
    }

    public HikariDataSource createDataSource(final JdbcConnection datastore) {
        return createDataSource(datastore, false, false, true);
    }

    public HikariDataSource createDataSource(final JdbcConnection datastore, final boolean rewriteBatchedStatements) {
        return createDataSource(datastore, false, false, rewriteBatchedStatements);
    }

    private HikariDataSource createDataSource(final JdbcConnection connection, final boolean isAutoCommit,
            final boolean isReadOnly, final boolean rewriteBatchedStatements) {
        if (connection.getJdbcUrl() == null || connection.getJdbcUrl().trim().isEmpty()) {
            throw new IllegalArgumentException(i18n.errorEmptyJdbcURL());
        }
        final JdbcConfiguration.Driver driver = getDriver(connection);
        final URLClassLoader driverLoader = getOrCreateDriverClassLoader(driver);
        Thread.currentThread().setContextClassLoader(driverLoader);
        final HikariDataSource hikariDS = new HikariDataSource();
        hikariDS.setUsername(connection.getUserId());
        hikariDS.setPassword(connection.getPassword());
        hikariDS.setDriverClassName(driver.getClassName());
        hikariDS.setJdbcUrl(connection.getJdbcUrl());
        hikariDS.setAutoCommit(isAutoCommit);
        hikariDS.setReadOnly(isReadOnly);
        hikariDS.setMaximumPoolSize(1);
        hikariDS.setLeakDetectionThreshold(10 * 60 * 1000);
        hikariDS.setConnectionTimeout(30 * 1000);
        hikariDS.setValidationTimeout(10 * 1000);
        hikariDS.setPoolName("Hikari-" + Thread.currentThread().getName() + "#" + Thread.currentThread().getId());
        hikariDS.addDataSourceProperty("rewriteBatchedStatements", String.valueOf(rewriteBatchedStatements));
        hikariDS.addDataSourceProperty("cachePrepStmts", "true");
        hikariDS.addDataSourceProperty("prepStmtCacheSize", "250");
        hikariDS.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        hikariDS.addDataSourceProperty("useServerPrepStmts", "true");
        return hikariDS;
    }

}
