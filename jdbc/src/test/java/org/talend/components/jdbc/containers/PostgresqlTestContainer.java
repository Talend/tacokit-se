package org.talend.components.jdbc.containers;

import lombok.experimental.Delegate;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.service.JdbcService;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.talend.components.jdbc.BaseJdbcTest.newConnection;

public class PostgresqlTestContainer implements JdbcTestContainer {

    @Delegate(types = { JdbcDatabaseContainer.class, GenericContainer.class, ContainerState.class })
    private final JdbcDatabaseContainer container = new PostgreSQLContainer();

    @Override
    public void createOrTruncateTable(JdbcService jdbcService) {
        final JdbcConnection datastore = newConnection(this);
        try (final Connection connection = jdbcService.connection(datastore)) {
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
    public String getDatabaseType() {
        return "PostgreSQL";
    }

}
