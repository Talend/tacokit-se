package org.talend.components.jdbc.containers;

import lombok.experimental.Delegate;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;

public class MSSQLServerTestContainer implements JdbcTestContainer {

    @Delegate(types = { JdbcDatabaseContainer.class, GenericContainer.class, ContainerState.class })
    private final MSSQLServerContainer container = new MSSQLServerContainer();

    @Override
    public String getDatabaseType() {
        return "MSSQL";
    }

}
