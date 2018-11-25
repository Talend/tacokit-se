package org.talend.components.jdbc;

import org.junit.jupiter.api.extension.*;
import org.talend.components.jdbc.containers.*;
import org.talend.sdk.component.junit.environment.DecoratingEnvironmentProvider;
import org.talend.sdk.component.junit.environment.EnvironmentProvider;
import org.talend.sdk.component.junit.environment.EnvironmentsConfigurationParser;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Locale.ROOT;
import static java.util.Optional.ofNullable;
import static org.talend.components.jdbc.JdbcInvocationContextProvider.SupportedDatabase.getActiveDatabases;

public class JdbcInvocationContextProvider implements TestTemplateInvocationContextProvider, BeforeAllCallback, AfterAllCallback {

    enum SupportedDatabase {
        DERBY,
        MYSQL,
        POSTGRESQL,
        ORACLE,
        MARIADB,
        MSSQL;

        public static Stream<SupportedDatabase> getActiveDatabases() {
            return Boolean.getBoolean("talend.jdbc.it")
                    ? Stream.of(values()).filter(db -> !Boolean.getBoolean(db.name().toLowerCase(ROOT) + ".skip"))
                    : Stream.of(DERBY);
        }
    }

    @Override
    public boolean supportsTestTemplate(ExtensionContext ctx) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext ctx) {
        return getActiveDatabases().flatMap(it -> new EnvironmentsConfigurationParser(ctx.getRequiredTestClass()).stream()
                .map(e -> invocationContext(it, e)));
    }

    private TestTemplateInvocationContext invocationContext(final SupportedDatabase databaseType,
            final EnvironmentProvider provider) {
        return new TestTemplateInvocationContext() {

            @Override
            public String getDisplayName(int invocationIndex) {
                return databaseType + " @ " + DecoratingEnvironmentProvider.class.cast(provider).getName();
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                return asList(new ParameterResolver() {

                    @Override
                    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
                            throws ParameterResolutionException {
                        return JdbcTestContainer.class.equals(parameterContext.getParameter().getType());
                    }

                    @Override
                    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
                            throws ParameterResolutionException {
                        return getStore(extensionContext).get(databaseType);
                    }
                }, (BeforeEachCallback) context -> {
                    final ExtensionContext.Store store = getStore(context);
                    store.put(AutoCloseable.class,
                            provider.start(context.getRequiredTestClass(), context.getRequiredTestClass().getAnnotations()));
                    final JdbcTestContainer container = store.get(databaseType, JdbcTestContainer.class);
                    if (!container.isRunning()) {
                        container.start();
                    }

                }, (AfterEachCallback) context -> {
                    final ExtensionContext.Store store = getStore(context);
                    ofNullable(store.get(AutoCloseable.class, AutoCloseable.class)).ifPresent(c -> {
                        try {
                            c.close();
                        } catch (final Exception e) {
                            throw new IllegalStateException(e);
                        }
                    });
                    ofNullable(store.get(databaseType, JdbcTestContainer.class)).ifPresent(c -> {
                        if (!c.isRunning()) { // can't use is healthy here as it's not implemented in all containers
                            c.stop();
                        }
                    });
                });
            }
        };
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        final ExtensionContext.Store store = getStore(extensionContext);
        getActiveDatabases().forEach(name -> store.put(name, getContainerInstance(name)));
    }

    private JdbcTestContainer getContainerInstance(final SupportedDatabase name) {
        final JdbcTestContainer container;
        switch (name) {
        case MYSQL:
            container = new MySQLTestContainer();
            break;
        case ORACLE:
            container = new OracleTestContainer();
            break;
        case POSTGRESQL:
            container = new PostgresqlTestContainer();
            break;
        case DERBY:
            container = new DerbyTestContainer();
            break;
        case MARIADB:
            container = new MariaDBTestContainer();
            break;
        case MSSQL:
            container = new MSSQLServerTestContainer();
            break;
        default:
            throw new IllegalArgumentException("unsupported database " + name);
        }
        return container;
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        final ExtensionContext.Store store = getStore(extensionContext);
        Stream.of(SupportedDatabase.values()).map(db -> store.remove(db, JdbcTestContainer.class)).filter(Objects::nonNull)
                .forEach(JdbcTestContainer::stop);
    }

    private ExtensionContext.Store getStore(ExtensionContext context) {
        return context.getStore(ExtensionContext.Namespace.create(getClass(), context.getRequiredTestClass()));
    }
}