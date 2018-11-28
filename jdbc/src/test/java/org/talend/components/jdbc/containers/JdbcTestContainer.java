package org.talend.components.jdbc.containers;

public interface JdbcTestContainer extends AutoCloseable {

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

    boolean isRunning();

}
