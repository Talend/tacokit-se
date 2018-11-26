package org.talend.components.jdbc.containers;

import com.zaxxer.hikari.HikariDataSource;
import lombok.experimental.Delegate;
import org.talend.components.jdbc.service.JdbcService;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.talend.components.jdbc.BaseJdbcTest.newConnection;

public class MSSQLServerTestContainer implements JdbcTestContainer {

    @Delegate(types = { JdbcDatabaseContainer.class, GenericContainer.class, ContainerState.class })
    private final MSSQLServerContainer container = new MSSQLServerContainer();

    @Override
    public void createOrTruncateTable(JdbcService jdbcService) {
        try (HikariDataSource dataSource = jdbcService.createDataSource(newConnection(this))) {
            try (final Connection connection = dataSource.getConnection()) {
                String sql = "CREATE TABLE " + getTestTableName() + "(id INT PRIMARY KEY," + "t_string VARCHAR(30),"
                        + "t_boolean BIT," + "t_float decimal(10,2) NULL, " + "t_double decimal(10,2), "
                        + "t_bytes varbinary(100), " + "t_date date, " + "t_long BIGINT)";
                System.out.println(sql);
                try (final PreparedStatement stm = connection.prepareStatement(sql)) {
                    stm.execute();
                } catch (final SQLException e) {
                    if (!("S0001".equalsIgnoreCase(e.getSQLState()) && 2714 == e.getErrorCode())) {
                        throw e;
                    }
                    try (final PreparedStatement stm = connection.prepareStatement("truncate table " + getTestTableName())) {
                        stm.execute();
                    }
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
        return "MSSQL";
    }

}
