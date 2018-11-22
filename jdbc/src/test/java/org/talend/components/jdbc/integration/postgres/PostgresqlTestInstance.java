package org.talend.components.jdbc.integration.postgres;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.jdbc.BaseTest;
import org.talend.components.jdbc.JdbcContainer;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public interface PostgresqlTestInstance extends JdbcContainer, BaseTest {

    String tableName = "test_" + UUID.randomUUID().toString().substring(0, 10).replace('-', '_');

    JdbcDatabaseContainer container = new PostgreSQLContainer();

    @BeforeAll
    default void before() {
        container.start();
    }

    @AfterAll
    default void after() {
        container.close();
    }

    @BeforeEach
    default void beforeEach() {
        final JdbcConnection datastore = newConnection();
        try (final Connection connection = getJdbcService().connection(datastore)) {
            final String sql = "CREATE TABLE IF NOT EXISTS " + getTestTableName() + "(id INT PRIMARY KEY,"
                    + "t_string VARCHAR(30)," + "t_boolean BOOLEAN DEFAULT true," + "t_float FLOAT NULL, "
                    + "t_double DOUBLE precision, " + "t_bytes bytea, " + "t_date TIMESTAMP, " + "t_long BIGINT)";
            try (final PreparedStatement stm = connection.prepareStatement(sql)) {
                stm.execute();
            }

            try (final PreparedStatement stm = connection.prepareStatement("truncate table " + getTestTableName())) {
                stm.execute();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    default String getDatabaseType() {
        return "Postgresql";
    }

    @Override
    default String getUsername() {
        return container.getUsername();
    }

    @Override
    default String getPassword() {
        return container.getPassword();
    }

    @Override
    default String getJdbcUrl() {
        return container.getJdbcUrl();
    }

    @Override
    default String getTestTableName() {
        return tableName;
    }
}
