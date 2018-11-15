package org.talend.components.jdbc;

import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.BasicDatastore;

public class BaseTest {

    public BasicDatastore newConnection(final DerbyExtension.DerbyInfo derbyInfo) {
        final BasicDatastore connection = new BasicDatastore();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("Derby");
        connection.setJdbcUrl("jdbc:derby://localhost:" + derbyInfo.getPort() + "/" + derbyInfo.getDbName());
        return connection;
    }

    public TableNameDataset newTableNameDataset(final DerbyExtension.DerbyInfo derbyInfo, final String tableName) {
        TableNameDataset dataset = new TableNameDataset();
        dataset.setConnection(newConnection(derbyInfo));
        dataset.setTableName(tableName);
        return dataset;
    }
}
