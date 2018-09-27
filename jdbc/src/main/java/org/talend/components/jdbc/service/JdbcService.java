package org.talend.components.jdbc.service;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.bind.Jsonb;

import org.talend.components.jdbc.dataset.InputDataset;
import org.talend.components.jdbc.JdbcConfiguration;
import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.dependency.Resolver;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class JdbcService {

    private static Pattern READ_ONLY_QUERY_PATTERN = Pattern.compile(
            "^SELECT\\s+((?!((\\bINTO\\b)|(\\bFOR\\s+UPDATE\\b)|(\\bLOCK\\s+IN\\s+SHARE\\s+MODE\\b))).)+$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);

    private final ParameterizedType driversType = new ParameterizedType() {

        @Override
        public Type[] getActualTypeArguments() {
            return new Type[] { JdbcConfiguration.class };
        }

        @Override
        public Type getRawType() {
            return List.class;
        }

        @Override
        public Type getOwnerType() {
            return null;
        }
    };

    private Enumeration<Driver> initialRegisteredDrivers;

    private final Map<JdbcConfiguration.Driver, URLClassLoader> driversClassLoaders = new HashMap<>();

    @Service
    private Jsonb jsonb;

    @Service
    private Resolver resolver;

    @Service
    private I18nMessage i18n;

    @PostConstruct
    public void init() {
        initialRegisteredDrivers = DriverManager.getDrivers();
    }

    /**
     * At this point we will deregister all the drivers that may were registered by this service.
     */
    @PreDestroy
    public void clean() {
        final Enumeration<Driver> registeredDrivers = DriverManager.getDrivers();
        driversClassLoaders.forEach((driver, classLoader) -> {
            while (initialRegisteredDrivers.hasMoreElements()) {
                if (!driver.getClassName().equals(initialRegisteredDrivers.nextElement().getClass().getName())) {
                    try {
                        final URLClassLoader loader = driversClassLoaders.get(driver);
                        Driver d = (Driver) loader.loadClass(driver.getClassName()).newInstance();
                        DriverManager.deregisterDriver(d);
                    } catch (IllegalAccessException | InstantiationException | SQLException | ClassNotFoundException e) {
                        log.error(i18n.errorDriverDeregister(driver.getClassName()), e);
                    }
                }
            }
        });
        driversClassLoaders.forEach((k, l) -> {
            try {
                l.close();
            } catch (IOException e) {
                log.warn(i18n.warnDriverClose(k.getId()));
            }
        });
    }

    public URLClassLoader getDriverClassLoader(final JdbcConfiguration.Driver driver) {
        return driversClassLoaders.computeIfAbsent(driver, key -> {
            final Collection<File> driverFiles = resolver.resolveFromDescriptor(new ByteArrayInputStream(driver.getPaths()
                    .stream().filter(p -> p.getPath() != null && !p.getPath().isEmpty())
                    .map(JdbcConfiguration.Driver.Path::getPath).collect(joining("\n")).getBytes(StandardCharsets.UTF_8)));
            final String missingJars = driverFiles.stream().filter(f -> !f.exists()).map(File::getAbsolutePath)
                    .collect(joining("\n"));
            if (!missingJars.isEmpty()) {
                log.error(i18n.errorDriverLoad(driver.getId(), missingJars));
                return null;
            }
            final URL[] urls = driverFiles.stream().filter(File::exists).map(f -> {
                try {
                    return f.toURI().toURL();
                } catch (MalformedURLException e) {
                    throw new IllegalStateException(e);
                }
            }).toArray(URL[]::new);
            return new URLClassLoader(urls, this.getClass().getClassLoader());
        });
    }

    public String createQuery(final InputDataset queryDataset) {
        if (InputDataset.SourceType.TABLE_NAME.equals(queryDataset.getSourceType())) {
            return "select * from " + queryDataset.getTableName();
        } else {
            if (queryDataset.getSqlQuery() == null || queryDataset.getSqlQuery().isEmpty()) {
                throw new IllegalStateException(i18n.errorEmptyQuery());
            }
            if (!READ_ONLY_QUERY_PATTERN.matcher(queryDataset.getSqlQuery().trim()).matches()) {
                throw new UnsupportedOperationException(i18n.errorUnauthorizedQuery());
            }
            return queryDataset.getSqlQuery();
        }
    }

    public Connection connection(final BasicDatastore datastore, final JdbcConfiguration jdbcConfiguration) {
        if (datastore == null) {
            throw new IllegalArgumentException("datastore can't be null");
        }
        if (datastore.getJdbcUrl() == null || datastore.getJdbcUrl().isEmpty()) {
            throw new IllegalArgumentException(i18n.errorEmptyJdbcURL());
        }

        final JdbcConfiguration.Driver driver = getDriver(datastore, jdbcConfiguration);
        final URLClassLoader driverLoader = getDriverClassLoader(driver);
        if (driverLoader == null) {
            throw new IllegalStateException(i18n.errorCantLoadDriver(datastore.getDbType()));
        }
        try {
            final Driver driverInstance = (Driver) driverLoader.loadClass(driver.getClassName()).newInstance();
            if (!driverInstance.acceptsURL(datastore.getJdbcUrl())) {
                throw new IllegalStateException(i18n.errorUnsupportedSubProtocol());
            }
            final Properties info = new Properties() {

                {
                    setProperty("user", datastore.getUserId());
                    setProperty("password", datastore.getPassword());
                }
            };
            try {
                final Connection connection = driverInstance.connect(datastore.getJdbcUrl(), info);
                if (!connection.isValid(30)) {
                    throw new IllegalStateException(i18n.errorInvalidConnection());
                }
                return connection;
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalStateException(i18n.errorCantLoadDriver(datastore.getDbType()));
        } catch (SQLException e) {
            throw new IllegalStateException(i18n.errorSQL(e.getErrorCode(), e.getMessage()));
        }
    }

    private JdbcConfiguration.Driver getDriver(final BasicDatastore datastore, final JdbcConfiguration jdbcConfiguration) {
        return jdbcConfiguration.getDrivers().stream().filter(d -> d.getId().equals(datastore.getDbType())).findFirst()
                .orElseThrow(() -> new IllegalStateException(i18n.errorDriverNotFound(datastore.getDbType())));
    }

}
