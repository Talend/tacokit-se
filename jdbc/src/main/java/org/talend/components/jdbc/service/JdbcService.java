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

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.jdbc.configuration.JdbcConfiguration;
import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.dependency.Resolver;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class JdbcService {

    private static Pattern READ_ONLY_QUERY_PATTERN = Pattern.compile(
            "^SELECT\\s+((?!((\\bINTO\\b)|(\\bFOR\\s+UPDATE\\b)|(\\bLOCK\\s+IN\\s+SHARE\\s+MODE\\b))).)+$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);

    private Enumeration<Driver> initialRegisteredDrivers;

    private final Map<JdbcConfiguration.Driver, URLClassLoader> driversClassLoaders = new HashMap<>();

    @Service
    private Resolver resolver;

    @Service
    private I18nMessage i18n;

    @Configuration("jdbc")
    private Supplier<JdbcConfiguration> jdbcConfiguration;

    @PostConstruct
    public void init() {
        initialRegisteredDrivers = DriverManager.getDrivers();
    }

    /**
     * At this point we will deregister all the drivers that may were registered by this service.
     */
    @PreDestroy
    public void clean() {
        driversClassLoaders.forEach((driver, classLoader) -> {
            while (initialRegisteredDrivers.hasMoreElements()) {
                if (!driver.getClassName().equals(initialRegisteredDrivers.nextElement().getClass().getName())) {
                    deregisterDriver(driver);
                }
            }
        });
        initialRegisteredDrivers = null;
    }

    private void deregisterDriver(final JdbcConfiguration.Driver driver) {
        final URLClassLoader loader = driversClassLoaders.get(driver);
        try {
            Driver d = (Driver) loader.loadClass(driver.getClassName()).newInstance();
            DriverManager.deregisterDriver(d);
        } catch (IllegalAccessException | InstantiationException | SQLException | ClassNotFoundException e) {
            log.error(i18n.errorDriverDeregister(driver.getClassName()), e);
        } finally {
            try {
                loader.close();
            } catch (IOException e) {
                log.warn(i18n.warnDriverClose(driver.getId()));
            }
        }
    }

    private URLClassLoader getDriverClassLoader(final JdbcConfiguration.Driver driver) {
        /*
         * remove invalid driver. when a driver property has changed
         */
        if (!driversClassLoaders.containsKey(driver)) {
            driversClassLoaders.entrySet().stream().filter(entry -> entry.getKey().getId().equals(driver.getId()))
                    .forEach(entry -> {
                        deregisterDriver(entry.getKey());
                        driversClassLoaders.remove(entry.getKey());
                    });
        }

        return driversClassLoaders.computeIfAbsent(driver, key -> {
            final Collection<File> driverFiles = resolver.resolveFromDescriptor(new ByteArrayInputStream(driver.getPaths()
                    .stream().filter(p -> p.getPath() != null && !p.getPath().isEmpty())
                    .map(JdbcConfiguration.Driver.Path::getPath).collect(joining("\n")).getBytes(StandardCharsets.UTF_8)));
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
            return new URLClassLoader(urls, this.getClass().getClassLoader());
        });
    }

    /**
     * @return return false if the sql query is not a read only query, true otherwise
     */
    public boolean isNotReadOnlySQLQuery(final String query) {
        return !READ_ONLY_QUERY_PATTERN.matcher(query.trim()).matches();
    }

    public Connection connection(final BasicDatastore datastore) {
        if (datastore.getJdbcUrl() == null || datastore.getJdbcUrl().trim().isEmpty()) {
            throw new IllegalArgumentException(i18n.errorEmptyJdbcURL());
        }
        final JdbcConfiguration.Driver driver = getDriver(datastore);
        final URLClassLoader driverLoader = getDriverClassLoader(driver);
        try {
            final Driver driverInstance = (Driver) driverLoader.loadClass(driver.getClassName()).newInstance();
            if (!driverInstance.acceptsURL(datastore.getJdbcUrl().trim())) {
                throw new IllegalStateException(i18n.errorUnsupportedSubProtocol());
            }
            final Properties info = new Properties() {

                {
                    setProperty("user", datastore.getUserId());
                    setProperty("password", datastore.getPassword());
                }
            };

            try {
                return driverInstance.connect(datastore.getJdbcUrl().trim(), info);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalStateException(i18n.errorCantLoadDriver(datastore.getDbType()), e);
        } catch (SQLException e) {
            throw new IllegalStateException(i18n.errorSQL(e.getErrorCode(), e.getMessage()), e);
        }
    }

    public boolean isConnectionValid(final Connection connection) throws SQLException {
        return connection.isValid(ofNullable(jdbcConfiguration.get().getConnection().getValidationTimeout()).orElse(5));
    }

    private JdbcConfiguration.Driver getDriver(final BasicDatastore dataStore) {
        return jdbcConfiguration.get().getDrivers().stream().filter(d -> d.getId().equals(dataStore.getDbType())).findFirst()
                .orElseThrow(() -> new IllegalStateException(i18n.errorDriverNotFound(dataStore.getDbType())));
    }
}
