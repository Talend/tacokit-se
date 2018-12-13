package org.talend.components.jdbc.containers;

import lombok.experimental.Delegate;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;

public class OracleTestContainer implements JdbcTestContainer {

    /**
     * image defined in test/resources/testcontainers.properties
     * wnameless/oracle-xe-11g@sha256:825ba799432809fc7200bb1d7ef954973a8991d7702a860c87177fe05301f7da
     */
    @Delegate(types = { JdbcDatabaseContainer.class, GenericContainer.class, ContainerState.class })
    private final JdbcDatabaseContainer container = new OracleContainer();

    @Override
    public String getDatabaseType() {
        return "Oracle";
    }

}
