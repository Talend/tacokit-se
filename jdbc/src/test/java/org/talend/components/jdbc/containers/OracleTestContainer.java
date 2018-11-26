package org.talend.components.jdbc.containers;

import com.zaxxer.hikari.HikariDataSource;
import lombok.experimental.Delegate;
import org.talend.components.jdbc.service.JdbcService;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.talend.components.jdbc.BaseJdbcTest.newConnection;

public class OracleTestContainer implements JdbcTestContainer {

    @Delegate(types = { JdbcDatabaseContainer.class, GenericContainer.class, ContainerState.class })
    private final JdbcDatabaseContainer container = new OracleContainer();

    @Override
    public void createOrTruncateTable(JdbcService jdbcService) {
        try (HikariDataSource dataSource = jdbcService.createDataSource(newConnection(this))) {
            try (final Connection connection = dataSource.getConnection()) {
                final String sql = "CREATE TABLE " + getTestTableName() + "(id NUMBER(7) PRIMARY KEY," + "t_string VARCHAR(30),"
                        + "t_boolean NUMBER(1) default 1," + "t_float NUMBER(10,2), " + "t_double NUMBER(10,2), "
                        + "t_bytes BLOB, " + "t_date DATE, " + "t_long NUMBER(20,2))";
                try (final PreparedStatement stm = connection.prepareStatement(sql)) {
                    stm.execute();
                } catch (final SQLException e) {
                    if (!("42000".equals(e.getSQLState()) && 955 == e.getErrorCode())) {
                        throw e;
                    }

                    try (final PreparedStatement stm = connection.prepareStatement("TRUNCATE TABLE " + getTestTableName())) {
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
        return "Oracle";
    }

    @Override
    public String getTestTableName() {
        return tableName;
    }
}
