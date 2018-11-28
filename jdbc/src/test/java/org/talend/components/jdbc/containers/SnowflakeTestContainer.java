package org.talend.components.jdbc.containers;

import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;

public class SnowflakeTestContainer implements JdbcTestContainer {

    private final static String SERVER_ID = "snowflake-for-test";

    private final static String TEST_ACCOUNT = "talend";

    private final static String TEST_WAREHOUSE = "JDBC_IT_WAREHOUSE";

    private final static String TEST_DATABASE = "JDBC_IT_DATABASE";

    private Server server;

    public SnowflakeTestContainer() {
        server = new MavenDecrypter().find(SERVER_ID);
        if (server == null) {
            throw new IllegalArgumentException("cant find snowflake server configuration. Please configure a server with id "
                    + SERVER_ID + " and snowflake username/password for integration test in the maven settings.xml file");
        }
    }

    @Override
    public String getDatabaseType() {
        return "Snowflake";
    }

    @Override
    public String getUsername() {
        return server.getUsername();
    }

    @Override
    public String getPassword() {
        return server.getPassword();
    }

    @Override
    public String getJdbcUrl() {
        return "jdbc:snowflake://" + TEST_ACCOUNT + ".snowflakecomputing.com/?schema=PUBLIC&db=" + TEST_DATABASE + "&warehouse="
                + TEST_WAREHOUSE;
    }

    @Override
    public void start() {
        // no-op cloud env
    }

    @Override
    public void stop() {
        // no-op cloud env
    }

    @Override
    public boolean isRunning() {
        return server != null;
    }
}
