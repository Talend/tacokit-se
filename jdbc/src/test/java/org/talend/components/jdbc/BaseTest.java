package org.talend.components.jdbc;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.ArrayList;
import java.util.List;

import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.components.jdbc.dataset.SqlQueryDataset;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.runtime.manager.chain.Job;

public abstract class BaseTest {

    @Injected
    protected BaseComponentsHandler componentsHandler;

    public JdbcConnection newConnection(final DerbyExtension.DerbyInfo derbyInfo) {
        final JdbcConnection connection = new JdbcConnection();
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

    public void insertUsers(final DerbyExtension.DerbyInfo derbyInfo, final int rowCount) {
        final OutputConfiguration config = new OutputConfiguration();
        config.setDataset(newTableNameDataset(derbyInfo, "users"));
        config.setActionOnData(OutputConfiguration.ActionOnData.INSERT);
        final String uriParam = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://UserGenerator?" + "config.rowCount=" + rowCount)
                .component("jdbcOutput", "Jdbc://Output?" + uriParam).connections().from("userGenerator").to("jdbcOutput").build()
                .run();
    }

    public List<Record> readAllUsers(final DerbyExtension.DerbyInfo derbyInfo) {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(newConnection(derbyInfo));
        dataset.setSqlQuery("select * from users");
        final String inConfig = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://QueryInput?" + inConfig).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();
        return new ArrayList<>(componentsHandler.getCollectedData(Record.class));
    }
}