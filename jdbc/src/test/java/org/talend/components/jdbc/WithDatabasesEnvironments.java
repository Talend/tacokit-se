package org.talend.components.jdbc;

import org.junit.jupiter.api.extension.*;
import org.talend.components.jdbc.containers.*;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.base.junit5.JUnit5InjectionSupport;
import org.talend.sdk.component.junit.environment.DecoratingEnvironmentProvider;
import org.talend.sdk.component.junit.environment.EnvironmentProvider;
import org.talend.sdk.component.junit.environment.EnvironmentsConfigurationParser;
import org.talend.sdk.component.junit5.Injected;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.lang.Boolean.getBoolean;
import static java.util.Arrays.stream;
import static java.util.Collections.singletonList;
import static java.util.Locale.ROOT;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;
import static org.junit.jupiter.api.extension.ConditionEvaluationResult.disabled;
import static org.talend.components.jdbc.Database.ALL;
import static org.talend.components.jdbc.Database.DERBY;

public class WithDatabasesEnvironments implements TestTemplateInvocationContextProvider, BeforeAllCallback, AfterAllCallback {

    @Override
    public boolean supportsTestTemplate(final ExtensionContext ctx) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext ctx) {
        return Database.getActiveDatabases().flatMap(it -> new EnvironmentsConfigurationParser(ctx.getRequiredTestClass())
                .stream().map(e -> invocationContext(it, e)));
    }

    private TestTemplateInvocationContext invocationContext(final Database database, final EnvironmentProvider provider) {
        return new TestTemplateInvocationContext() {

            @Override
            public String getDisplayName(int invocationIndex) {
                final String name = ((DecoratingEnvironmentProvider) provider).getName();
                return database + " @ " + ("direct".equalsIgnoreCase(name) ? "Beam Runner" : name);
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                return singletonList(new WithDatabaseExtension(database, provider));
            }
        };
    }

    private static class WithDatabaseExtension extends BaseComponentsHandler
            implements BeforeEachCallback, AfterEachCallback, ParameterResolver, ExecutionCondition, JUnit5InjectionSupport {

        private final Database database;

        private final EnvironmentProvider provider;

        private static final String ENV_PREFIX = "talend.jdbc.it";

        private static final ConditionEvaluationResult ENABLED = ConditionEvaluationResult.enabled("");

        public WithDatabaseExtension(Database database, EnvironmentProvider provider) {
            this.database = database;
            this.provider = provider;
            this.packageName = "org.talend.components.jdbc";
        }

        @Override
        public void beforeEach(ExtensionContext context) {
            final ExtensionContext.Store store = getStore(context);
            store.put(AutoCloseable.class,
                    provider.start(context.getRequiredTestClass(), context.getRequiredTestClass().getAnnotations()));
            ofNullable(store.get(database, JdbcTestContainer.class)).filter(c -> !c.isRunning())
                    .ifPresent(JdbcTestContainer::start);
            store.put(EmbeddedComponentManager.class, start());
            context.getTestInstance().ifPresent(this::injectServices);

        }

        @Override
        public void afterEach(ExtensionContext context) {
            final ExtensionContext.Store store = getStore(context);
            ofNullable(store.get(AutoCloseable.class, AutoCloseable.class)).ifPresent(c -> {
                try {
                    c.close();
                } catch (final Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            ofNullable(store.get(database, JdbcTestContainer.class)).ifPresent(c -> {
                if (!c.isRunning()) { // can't use is healthy here as it's not implemented in all containers
                    c.stop();
                }
            });

            this.resetState();
            store.get(EmbeddedComponentManager.class, EmbeddedComponentManager.class).close();
        }

        @Override
        public Class<? extends Annotation> injectionMarker() {
            return Injected.class;
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
                throws ParameterResolutionException {
            return JdbcTestContainer.class.equals(parameterContext.getParameter().getType());
        }

        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
                throws ParameterResolutionException {
            return getStore(extensionContext).get(database);
        }

        @Override
        public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
            if (getBoolean(ENV_PREFIX + "." + ALL.name().toLowerCase(ROOT) + ".skip")) {
                return disabled("All test disabled by env var '-D" + ENV_PREFIX + "." + ALL.name().toLowerCase(ROOT) + ".skip'");
            }

            if (!getBoolean(ENV_PREFIX) && !database.equals(DERBY)) {
                return disabled(
                        "Integration test disabled. Only Derby will be used for tests. to activate integration test set env var '-Dtalend.jdbc.it'");
            }

            if (getBoolean(ENV_PREFIX + "." + database.name().toLowerCase(ROOT) + ".skip")) {
                return disabled("Test disabled by env var '-D" + ENV_PREFIX + "." + database.name().toLowerCase(ROOT) + ".skip'");
            }

            return getDisableConditions(context).stream().filter(d -> d.value().equals(ALL) || d.value().equals(database))
                    .findFirst().map(disabled -> disabled(disabled.reason())).orElse(ENABLED);
        }
    }

    @Override
    public void beforeAll(final ExtensionContext ctx) {
        final List<Disabled> disabled = getDisableConditions(ctx);
        if (disabled.stream().anyMatch(d -> ALL.equals(d.value()))) {
            return;
        }
        final ExtensionContext.Store store = getStore(ctx);
        Database.getActiveDatabases().filter(database -> disabled.stream().noneMatch(d -> d.value().equals(database)))
                .forEach(database -> store.put(database, getContainerInstance(database)));
    }

    private static List<Disabled> getDisableConditions(ExtensionContext ctx) {
        return concat(
                ctx.getTestClass().map(clazz -> clazz.getAnnotation(DisabledDatabases.class))
                        .map(annotation -> stream(annotation.value())).orElse(empty()),
                ctx.getTestMethod().map(method -> method.getAnnotation(DisabledDatabases.class))
                        .map(annotation -> stream(annotation.value())).orElse(empty())).collect(toList());
    }

    private static JdbcTestContainer getContainerInstance(final Database name) {
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
        case SNOWFLAKE:
            container = new SnowflakeTestContainer();
            break;
        default:
            throw new IllegalArgumentException("unsupported database " + name);
        }
        return container;
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        final ExtensionContext.Store store = getStore(extensionContext);
        Stream.of(Database.values()).map(database -> store.remove(database, JdbcTestContainer.class)).filter(Objects::nonNull)
                .forEach(JdbcTestContainer::stop);
    }

    private static ExtensionContext.Store getStore(ExtensionContext context) {
        return context
                .getStore(ExtensionContext.Namespace.create(WithDatabasesEnvironments.class, context.getRequiredTestClass()));
    }
}