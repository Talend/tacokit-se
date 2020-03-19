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
package org.talend.components.jdbc.service;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;
import org.bouncycastle.util.encoders.Base64;
import org.bouncycastle.util.encoders.DecoderException;
import org.talend.components.jdbc.configuration.JdbcConfiguration;
import org.talend.components.jdbc.datastore.AuthenticationType;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.dependency.Resolver;

import com.zaxxer.hikari.HikariDataSource;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class JdbcService {

    private static Pattern READ_ONLY_QUERY_PATTERN = Pattern.compile(
            "^SELECT\\s+((?!((\\bINTO\\b)|(\\bFOR\\s+UPDATE\\b)|(\\bLOCK\\s+IN\\s+SHARE\\s+MODE\\b))).)+$",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);

    @Service
    private Resolver resolver;

    @Service
    private I18nMessage i18n;

    @Configuration("jdbc")
    private Supplier<JdbcConfiguration> jdbcConfiguration;

    @Service
    private LocalConfiguration localConfiguration;

    static {
        // Define Bouncy Castle Provider for Snowflake Key Pair authentication
        Security.addProvider(new BouncyCastleProvider());
    }

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
            try (final ResultSet resultSet = connection.getMetaData().getTables(connection.getCatalog(), connection.getSchema(),
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
                dataSource.setUsername(connection.getUserId());
                if (AuthenticationType.KEY_PAIR == connection.getAuthenticationType()) {
                    dataSource.addDataSourceProperty("privateKey",
                            getPrivateKey(connection.getPrivateKey(), connection.getPrivateKeyPassword(), i18nMessage));
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

            } finally {
                thread.setContextClassLoader(prev);
            }
        }

        private PrivateKey getPrivateKey(String privateKey, String privateKeyPassword, final I18nMessage i18nMessage) {
            try {
                return privateKey.contains("ENCRYPTED") ? getFromEncrypted(privateKey, privateKeyPassword)
                        : getFromRegular(privateKey);
            } catch (PKCSException pkcse) {
                throw new IllegalArgumentException(i18nMessage.errorPrivateKeyPasswordIncorrect(), pkcse);
            } catch (InvalidKeySpecException | IOException | OperatorCreationException | NoSuchAlgorithmException
                    | DecoderException e) {
                throw new IllegalArgumentException(i18nMessage.errorPrivateKeyIncorrect(), e);
            }
        }

        private PrivateKey getFromEncrypted(String privateKey, String privateKeyPassword)
                throws IOException, OperatorCreationException, PKCSException {
            PKCS8EncryptedPrivateKeyInfo pkcs8EncryptedPrivateKeyInfo = new PKCS8EncryptedPrivateKeyInfo(
                    decodeString(replaceGeneratedExtraString(privateKey, true)));
            InputDecryptorProvider inputDecryptorProvider = new JceOpenSSLPKCS8DecryptorProviderBuilder().setProvider("BC")
                    .build(ofNullable(privateKeyPassword).map(String::toCharArray).orElse(new char[0]));
            PrivateKeyInfo privateKeyInfo = pkcs8EncryptedPrivateKeyInfo.decryptPrivateKeyInfo(inputDecryptorProvider);
            return new JcaPEMKeyConverter().setProvider("BC").getPrivateKey(privateKeyInfo);
        }

        private PrivateKey getFromRegular(String privateKey) throws InvalidKeySpecException, NoSuchAlgorithmException {
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decodeString(replaceGeneratedExtraString(privateKey, false)));
            return KeyFactory.getInstance("RSA").generatePrivate(keySpec);
        }

        private byte[] decodeString(String privateKeyContent) {
            return Base64.decode(privateKeyContent);
        }

        private String replaceGeneratedExtraString(String privateKey, boolean isEncrypted) {
            return isEncrypted
                    ? privateKey.replace("-----BEGIN ENCRYPTED PRIVATE KEY-----", "")
                            .replace("-----END ENCRYPTED PRIVATE KEY-----", "")
                    : privateKey.replace("-----BEGIN RSA PRIVATE KEY-----", "").replace("-----END RSA PRIVATE KEY-----", "");
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

}
