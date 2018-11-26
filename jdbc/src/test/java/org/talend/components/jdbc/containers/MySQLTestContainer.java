package org.talend.components.jdbc.containers;

import com.zaxxer.hikari.HikariDataSource;
import lombok.experimental.Delegate;
import org.talend.components.jdbc.service.JdbcService;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.talend.components.jdbc.BaseJdbcTest.newConnection;

public class MySQLTestContainer implements JdbcTestContainer {

    @Delegate(types = { JdbcDatabaseContainer.class, GenericContainer.class, ContainerState.class })
    private final MySQLContainer container = new MySQLContainer();

    @Override
    public void createOrTruncateTable(JdbcService jdbcService) {
        try (HikariDataSource dataSource = jdbcService.createDataSource(newConnection(this))) {
            try (final Connection connection = dataSource.getConnection()) {
                try (final PreparedStatement stm = connection
                        .prepareStatement("CREATE TABLE IF NOT EXISTS " + getTestTableName() + "(id INT(7) UNSIGNED PRIMARY KEY,"
                                + "t_string VARCHAR(30)," + "t_boolean BOOLEAN DEFAULT true," + "t_float FLOAT(10,2) NULL, "
                                + "t_double DOUBLE(10,2), " + "t_bytes BLOB, " + "t_date TIMESTAMP, " + "t_long BIGINT)")) {
                    stm.execute();
                } finally {
                    connection.commit();
                }
                try (final PreparedStatement stm = connection.prepareStatement("truncate table " + getTestTableName())) {
                    stm.execute();
                } finally {
                    connection.commit();
                }
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public String getDatabaseType() {
        return "MySQL";
    }

}
