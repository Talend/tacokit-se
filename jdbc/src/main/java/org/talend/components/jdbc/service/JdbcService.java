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

import org.talend.components.jdbc.DriverInfo;
import org.talend.components.jdbc.dataset.InputDataset;
import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.dependency.Resolver;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class JdbcService {

    private final static String CONFIG_FILE_lOCATION_KEY = "org.talend.component.jdbc.config.file";

    private static Pattern READ_ONLY_QUERY_PATTERN = Pattern.compile(
            "^SELECT\\s+((?!((\\bINTO\\b)|(\\bFOR\\s+UPDATE\\b)|(\\bLOCK\\s+IN\\s+SHARE\\s+MODE\\b))).)+$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);

    private final ParameterizedType driversType = new ParameterizedType() {

        @Override
        public Type[] getActualTypeArguments() {
            return new Type[] { DriverInfo.class };
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

    @Getter(lazy = true)
    private final Map<String, DriverInfo> drivers = loadtDriversFromConfigurationFile();

    private final Map<String, URLClassLoader> driversClassLoaders = new HashMap<>();

    @Service
    private Jsonb jsonb;

    @Service
    private LocalConfiguration localConfiguration;

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
        getDrivers().entrySet().stream().filter(d -> driversClassLoaders.containsKey(d.getKey())) // a class loader was created
                .filter(d -> {
                    while (initialRegisteredDrivers.hasMoreElements()) {
                        if (d.getValue().getClazz().equals(initialRegisteredDrivers.nextElement().getClass().getName())) {
                            return false; // registered by the jvm
                        }
                    }
                    return true;
                }).filter(d -> {
                    while (registeredDrivers.hasMoreElements()) {
                        if (d.getValue().getClazz().equals(registeredDrivers.nextElement().getClass().getName())) {
                            return true; // need to be cleaned
                        }
                    }
                    return false;
                }).forEach(d -> {
                    try {
                        final URLClassLoader loader = driversClassLoaders.get(d.getKey());
                        Driver driver = (Driver) loader.loadClass(getDrivers().get(d.getKey()).getClazz()).newInstance();
                        DriverManager.deregisterDriver(driver);
                    } catch (IllegalAccessException | InstantiationException | SQLException | ClassNotFoundException e) {
                        log.error(i18n.errorDriverDeregister(d.getValue().getClazz()), e);
                    }
                });

        driversClassLoaders.forEach((k, l) -> {
            try {
                l.close();
            } catch (IOException e) {
                log.warn(i18n.warnDriverClose(k));
            }
        });
    }

    private Map<String, DriverInfo> loadtDriversFromConfigurationFile() {
        final Map<String, DriverInfo> availableDrivers = new HashMap<>();
        InputStream is = null;
        try {
            final String configFile = localConfiguration.get(CONFIG_FILE_lOCATION_KEY);
            if (configFile != null) {// priority to the system property
                try {
                    is = new FileInputStream(configFile);
                } catch (FileNotFoundException e) {
                    throw new IllegalArgumentException(e);
                }
            } else {// then look in the classpath
                is = this.getClass().getClassLoader().getResourceAsStream("jdbc_config.json");
                if (is == null) {// use the default provided configuration file
                    is = this.getClass().getClassLoader().getResourceAsStream("db_type_config.json");
                }
            }
            final List<DriverInfo> info = jsonb.fromJson(is, driversType);
            availableDrivers.putAll(info.stream().collect(toMap(DriverInfo::getId, identity())));
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    // no-op too bad but who care
                }
            }
        }

        return availableDrivers;
    }

    public URLClassLoader getDriverClassLoader(final String driverId) {
        return driversClassLoaders.computeIfAbsent(driverId, key -> {
            final DriverInfo driver = getDrivers().get(driverId);
            final Collection<File> driverFiles = resolver.resolveFromDescriptor(
                    new ByteArrayInputStream(driver.getPaths().stream().filter(p -> p.getPath() != null && !p.getPath().isEmpty())
                            .map(DriverInfo.Path::getPath).collect(joining("\n")).getBytes(StandardCharsets.UTF_8)));
            final String missingJars = driverFiles.stream().filter(f -> !f.exists()).map(File::getAbsolutePath)
                    .collect(joining("\n"));
            if (!missingJars.isEmpty()) {
                log.error(i18n.errorDriverLoad(driverId, missingJars));
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

    public Connection connection(final BasicDatastore datastore) {
        if (datastore == null) {
            throw new IllegalArgumentException("datastore can't be null");
        }
        final DriverInfo driverInfo = getDrivers().get(datastore.getDbType());
        if (driverInfo == null) {
            throw new IllegalStateException(i18n.errorDriverNotFound(datastore.getDbType()));
        }
        final URLClassLoader driverLoader = getDriverClassLoader(driverInfo.getId());
        if (driverLoader == null) {
            throw new IllegalStateException(i18n.errorCantLoadDriver(datastore.getDbType()));
        }
        try {
            final Driver driverInstance = (Driver) driverLoader.loadClass(driverInfo.getClazz()).newInstance();
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

}
