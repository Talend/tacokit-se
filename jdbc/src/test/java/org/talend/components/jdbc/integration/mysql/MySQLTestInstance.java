package org.talend.components.jdbc.integration.mysql;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.jdbc.BaseTest;
import org.talend.components.jdbc.JdbcContainer;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public interface MySQLTestInstance extends JdbcContainer, BaseTest {

    String tableName = "test_" + UUID.randomUUID().toString().substring(0, 10).replace('-', '_');

    JdbcDatabaseContainer container = new MySQLContainer();

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
            try (final PreparedStatement stm = connection
                    .prepareStatement("CREATE TABLE IF NOT EXISTS " + getTestTableName() + "(id INT(6) UNSIGNED PRIMARY KEY,"
                            + "t_string VARCHAR(30)," + "t_boolean BOOLEAN DEFAULT true," + "t_float FLOAT(10,2) NULL, "
                            + "t_double DOUBLE(10,2), " + "t_bytes BLOB, " + "t_date TIMESTAMP, " + "t_long BIGINT)")) {
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
        return "MySQL";
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
