package org.talend.components.jdbc;

import lombok.Data;
import org.junit.jupiter.api.extension.*;
import org.talend.components.jdbc.containers.*;
import org.talend.sdk.component.junit.environment.DecoratingEnvironmentProvider;
import org.talend.sdk.component.junit.environment.EnvironmentProvider;
import org.talend.sdk.component.junit.environment.EnvironmentsConfigurationParser;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.empty;

public class JdbcInvocationContextProvider implements TestTemplateInvocationContextProvider, BeforeAllCallback, AfterAllCallback {

    @Override
    public boolean supportsTestTemplate(final ExtensionContext ctx) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext ctx) {
        return getTestDatabase(ctx).flatMap(it -> new EnvironmentsConfigurationParser(ctx.getRequiredTestClass()).stream()
                .map(e -> invocationContext(it, e)));
    }

    private TestTemplateInvocationContext invocationContext(final Database database, final EnvironmentProvider provider) {
        return new TestTemplateInvocationContext() {

            @Override
            public String getDisplayName(int invocationIndex) {
                return database + " @ " + ((DecoratingEnvironmentProvider) provider).getName();
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                return asList(new JdbcTestContainerParameterResolver(database),
                        new DatabaseBeforeEachCallback(database, provider), new DatabaseAfterEachCallback(database),
                        new DatabaseExecutionCondition(database));
            }
        };
    }

    @Data
    private static class JdbcTestContainerParameterResolver implements ParameterResolver {

        private final Database database;

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
    }

    @Data
    private static class DatabaseExecutionCondition implements ExecutionCondition {

        private static final ConditionEvaluationResult ENABLED = ConditionEvaluationResult.enabled("");

        private final Database database;

        @Override
        public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
            return getDisableConditions(context).stream()
                    .filter(d -> d.value().equals(Database.ALL) || d.value().equals(database)).findFirst()
                    .map(disabled -> ConditionEvaluationResult.disabled(disabled.reason())).orElse(ENABLED);
        }
    }

    @Data
    private static class DatabaseBeforeEachCallback implements BeforeEachCallback {

        private final Database database;

        private final EnvironmentProvider provider;

        @Override
        public void beforeEach(ExtensionContext context) {
            final ExtensionContext.Store store = getStore(context);
            store.put(AutoCloseable.class,
                    provider.start(context.getRequiredTestClass(), context.getRequiredTestClass().getAnnotations()));
            ofNullable(store.get(database, JdbcTestContainer.class)).filter(c -> !c.isRunning())
                    .ifPresent(JdbcTestContainer::start);

        }
    }

    @Data
    private static class DatabaseAfterEachCallback implements AfterEachCallback {

        private final Database database;

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
        }
    }

    @Override
    public void beforeAll(final ExtensionContext ctx) {
        final ExtensionContext.Store store = getStore(ctx);
        getTestDatabase(ctx).forEach(database -> store.put(database, getContainerInstance(database)));
    }

    private static Stream<Database> getTestDatabase(final ExtensionContext ctx) {
        final List<Database> disables = getDisableConditions(ctx).stream().map(Disabled::value).collect(toList());
        return disables.contains(Database.ALL) ? empty() : Database.getActiveDatabases().filter(d -> !disables.contains(d));
    }

    private static List<Disabled> getDisableConditions(ExtensionContext ctx) {
        List<Disabled> disables = ctx.getTestClass().map(clazz -> clazz.getAnnotation(DisabledDatabases.class))
                .map(annotation -> stream(annotation.value())).orElse(empty()).collect(toList());
        disables.addAll(ctx.getTestMethod().map(method -> method.getAnnotation(DisabledDatabases.class))
                .map(annotation -> stream(annotation.value())).orElse(empty()).collect(toList()));
        return disables;
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
        Stream.of(Database.values()).map(db -> store.remove(db, JdbcTestContainer.class)).filter(Objects::nonNull)
                .forEach(JdbcTestContainer::stop);
    }

    private static ExtensionContext.Store getStore(ExtensionContext context) {
        return context
                .getStore(ExtensionContext.Namespace.create(JdbcInvocationContextProvider.class, context.getRequiredTestClass()));
    }
}