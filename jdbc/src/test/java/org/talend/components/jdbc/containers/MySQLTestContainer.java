package org.talend.components.jdbc.containers;

import lombok.experimental.Delegate;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

public class MySQLTestContainer implements JdbcTestContainer {

    @Delegate(types = { JdbcDatabaseContainer.class, GenericContainer.class, ContainerState.class })
    private final MySQLContainer container = new MySQLContainer();

    @Override
    public String getDatabaseType() {
        return "MySQL";
    }

}
