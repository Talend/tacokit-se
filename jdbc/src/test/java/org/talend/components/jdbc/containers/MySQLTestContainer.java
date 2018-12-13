package org.talend.components.jdbc.containers;

import lombok.experimental.Delegate;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;

public class MySQLTestContainer implements JdbcTestContainer {

    @Delegate(types = { JdbcDatabaseContainer.class, GenericContainer.class, ContainerState.class })
    private final MySQLContainer container = (MySQLContainer) new MySQLContainer("mysql:8.0.13")
            // https://github.com/testcontainers/testcontainers-java/issues/736
            .withCommand("--default-authentication-plugin=mysql_native_password");

    @Override
    public String getDatabaseType() {
        return "MySQL";
    }

}
