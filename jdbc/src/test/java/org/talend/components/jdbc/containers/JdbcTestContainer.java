package org.talend.components.jdbc.containers;

import org.talend.components.jdbc.service.JdbcService;

import java.util.UUID;

public interface JdbcTestContainer extends AutoCloseable {

    String tableName = "test_" + UUID.randomUUID().toString().substring(0, 10).replace('-', '_');

    /**
     * @return Database as defined in the configuration file with en property <code>jdbc.drivers[].id</code>
     */
    String getDatabaseType();

    String getUsername();

    String getPassword();

    String getJdbcUrl();

    void start();

    void stop();

    @Override
    default void close() {
        stop();
    }

    default String getTestTableName() {
        return tableName;
    }

    /**
     * Create a new table. if the table already exist perform a truncate on it
     *
     * @param jdbcService jdbc service to load driver and create connection
     */
    void createOrTruncateTable(final JdbcService jdbcService);

    boolean isRunning();

}
