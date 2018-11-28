package org.talend.components.jdbc.containers;

import lombok.experimental.Delegate;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;

public class OracleTestContainer implements JdbcTestContainer {

    @Delegate(types = { JdbcDatabaseContainer.class, GenericContainer.class, ContainerState.class })
    private final JdbcDatabaseContainer container = new OracleContainer();

    @Override
    public String getDatabaseType() {
        return "Oracle";
    }

}
