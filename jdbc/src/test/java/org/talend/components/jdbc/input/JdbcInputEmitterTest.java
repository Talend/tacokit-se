package org.talend.components.jdbc.input;

import static java.util.stream.Collectors.toList;
import static org.apache.derby.vti.XmlVTI.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.List;

import javax.json.JsonObject;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.DerbyExtension;
import org.talend.components.jdbc.WithDerby;
import org.talend.components.jdbc.dataset.InputDataset;
import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@WithDerby(onStartSQLScript = "derby/create.sql", onShutdownSQLScript = "derby/delete.sql")
@WithComponents("org.talend.components.jdbc") // component package
class JdbcInputEmitterTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Test
    @DisplayName("Execute a valid query")
    void validQuery(final DerbyExtension.DerbyInfo derbyInfo) {
        final BasicDatastore connection = new BasicDatastore();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("DERBY");
        connection.setJdbcUrl("jdbc:derby://localhost:" + derbyInfo.getPort() + "/" + derbyInfo.getDbName());
        final InputDataset dataset = new InputDataset();
        dataset.setConnection(connection);
        dataset.setSourceType(InputDataset.SourceType.QUERY);
        dataset.setSqlQuery("select * from users");

        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://Input?" + config).component("collector", "test://collector").connections()
                .from("jdbcInput").to("collector").build().run();

        final List<JsonObject> records = componentsHandler.getCollectedData(JsonObject.class);
        assertNotNull(records);
        assertEquals(4, records.size());
        assertEquals(asList("user1", "user2", "user3", "user4"),
                records.stream().map(r -> r.getString("NAME")).collect(toList()));
    }

    @Test
    @DisplayName("Execute a not valid query ")
    void invalidQuery(final DerbyExtension.DerbyInfo derbyInfo) {
        final BasicDatastore connection = new BasicDatastore();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("DERBY");
        connection.setJdbcUrl("jdbc:derby://localhost:" + derbyInfo.getPort() + "/" + derbyInfo.getDbName());
        final InputDataset dataset = new InputDataset();
        dataset.setConnection(connection);
        dataset.setSourceType(InputDataset.SourceType.QUERY);
        dataset.setSqlQuery("select from users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://Input?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("Execute a non authorized query (drop table)")
    void unauthorizedDropQuery(final DerbyExtension.DerbyInfo derbyInfo) {
        final BasicDatastore connection = new BasicDatastore();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("DERBY");
        connection.setJdbcUrl("jdbc:derby://localhost:" + derbyInfo.getPort() + "/" + derbyInfo.getDbName());
        final InputDataset dataset = new InputDataset();
        dataset.setConnection(connection);
        dataset.setSourceType(InputDataset.SourceType.QUERY);
        dataset.setSqlQuery("drop table users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(UnsupportedOperationException.class, () -> Job.components().component("jdbcInput", "Jdbc://Input?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("Execute a non authorized query (insert into)")
    void unauthorizedInsertQuery(final DerbyExtension.DerbyInfo derbyInfo) {
        final BasicDatastore connection = new BasicDatastore();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("DERBY");
        connection.setJdbcUrl("jdbc:derby://localhost:" + derbyInfo.getPort() + "/" + derbyInfo.getDbName());
        final InputDataset dataset = new InputDataset();
        dataset.setConnection(connection);
        dataset.setSourceType(InputDataset.SourceType.QUERY);
        dataset.setSqlQuery("INSERT INTO users(id, name) VALUES (1, 'user1')");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(UnsupportedOperationException.class, () -> Job.components().component("jdbcInput", "Jdbc://Input?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("Execute query using valid table name")
    void validTableName(final DerbyExtension.DerbyInfo derbyInfo) {
        final BasicDatastore connection = new BasicDatastore();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("DERBY");
        connection.setJdbcUrl("jdbc:derby://localhost:" + derbyInfo.getPort() + "/" + derbyInfo.getDbName());
        final InputDataset dataset = new InputDataset();
        dataset.setConnection(connection);
        dataset.setSourceType(InputDataset.SourceType.TABLE_NAME);
        dataset.setTableName("users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://Input?" + config).component("collector", "test://collector").connections()
                .from("jdbcInput").to("collector").build().run();

        final List<JsonObject> records = componentsHandler.getCollectedData(JsonObject.class);
        assertNotNull(records);
        assertEquals(4, records.size());
        assertEquals(asList("user1", "user2", "user3", "user4"),
                records.stream().map(r -> r.getString("NAME")).collect(toList()));
    }

    @Test
    @DisplayName("Execute query using invalid table name")
    void invalidTableName(final DerbyExtension.DerbyInfo derbyInfo) {
        final BasicDatastore connection = new BasicDatastore();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("DERBY");
        connection.setJdbcUrl("jdbc:derby://localhost:" + derbyInfo.getPort() + "/" + derbyInfo.getDbName());
        final InputDataset dataset = new InputDataset();
        dataset.setConnection(connection);
        dataset.setSourceType(InputDataset.SourceType.TABLE_NAME);
        dataset.setTableName("xxx");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://Input?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("Execute query using missing driver")
    void missingDriverConfig() {
        final BasicDatastore connection = new BasicDatastore();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("ORACLEXX");
        connection.setJdbcUrl("jdbc:derby://localhost:1234/foo");
        final InputDataset dataset = new InputDataset();
        dataset.setConnection(connection);
        dataset.setSqlQuery("select * from users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://Input?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("Execute query using missing driver file")
    void missingDriverFile() {
        final BasicDatastore connection = new BasicDatastore();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("ORACLE");
        connection.setJdbcUrl("jdbc:derby://localhost:1234/foo");
        final InputDataset dataset = new InputDataset();
        dataset.setConnection(connection);
        dataset.setSqlQuery("select * from users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://Input?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }
}
