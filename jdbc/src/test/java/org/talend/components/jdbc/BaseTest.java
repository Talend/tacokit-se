package org.talend.components.jdbc;

import org.talend.components.jdbc.datastore.BasicDatastore;

public class BaseTest {

    public BasicDatastore newConnection(final DerbyExtension.DerbyInfo derbyInfo) {
        final BasicDatastore connection = new BasicDatastore();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("DERBY");
        connection.setJdbcUrl("jdbc:derby://localhost:" + derbyInfo.getPort() + "/" + derbyInfo.getDbName());
        return connection;
    }
}
