package org.talend.components.jdbc.output;

import static java.util.stream.Collectors.toList;
import static org.apache.derby.vti.XmlVTI.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.json.JsonObject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.DerbyExtension;
import org.talend.components.jdbc.JdbcConfiguration;
import org.talend.components.jdbc.WithDerby;
import org.talend.components.jdbc.dataset.InputDataset;
import org.talend.components.jdbc.dataset.OutputDataset;
import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@WithDerby(onStartSQLScript = "derby/output_create.sql", onShutdownSQLScript = "derby/delete.sql")
@WithComponents(value = "org.talend.components.jdbc") // component package
class OutputTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JdbcService jdbcService;

    private JdbcConfiguration jdbcConfiguration;

    @BeforeEach
    void clearTable(final DerbyExtension.DerbyInfo derbyInfo) {
        jdbcConfiguration = new JdbcConfiguration();
        final List<JdbcConfiguration.Driver> drivers = new ArrayList<>();
        drivers.add(new JdbcConfiguration.Driver("DERBY", "org.apache.derby.jdbc.ClientDriver", "",
                Arrays.asList(new JdbcConfiguration.Driver.Path("org.apache.derby:derby:10.12.1.1"))));
        jdbcConfiguration.setDrivers(drivers);

        final BasicDatastore datastore = newConnection(derbyInfo);
        try (final Connection connection = jdbcService.connection(datastore, jdbcConfiguration);) {
            try (final PreparedStatement stm = connection.prepareStatement("truncate table users")) {
                stm.execute();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }

    }

    @Test
    @DisplayName("Execute a valid insert")
    void insert(final DerbyExtension.DerbyInfo derbyInfo) {
        final BasicDatastore connection = newConnection(derbyInfo);
        final OutputDataset dataset = new OutputDataset();
        dataset.setConnection(connection);
        dataset.setActionOnData(OutputDataset.ActionOnData.Insert);
        dataset.setTableName("users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4&config.namePrefix=user")
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("userGenerator").to("jdbcOutput").build()
                .run();

        final InputDataset in = new InputDataset();
        in.setConnection(connection);
        in.setSourceType(InputDataset.SourceType.QUERY);
        in.setSqlQuery("select * from users");

        final String inConfig = configurationByExample().forInstance(in).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://Input?" + inConfig).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();

        final List<JsonObject> records = componentsHandler.getCollectedData(JsonObject.class);
        assertNotNull(records);
        assertEquals(4, records.size());
        assertEquals(asList("user1", "user2", "user3", "user4"),
                records.stream().map(r -> r.getString("NAME")).collect(toList()));
    }

    @Test
    @DisplayName("Execute a valid update")
    void update(final DerbyExtension.DerbyInfo derbyInfo) {
        // insert some initial data
        final BasicDatastore connection = newConnection(derbyInfo);
        final OutputDataset dataset = new OutputDataset();
        dataset.setConnection(connection);
        dataset.setActionOnData(OutputDataset.ActionOnData.Insert);
        dataset.setTableName("users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4&config.namePrefix=user")
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("userGenerator").to("jdbcOutput").build()
                .run();

        // update the inserted data data
        final OutputDataset updateDataset = new OutputDataset();
        updateDataset.setConnection(connection);
        updateDataset.setActionOnData(OutputDataset.ActionOnData.Update);
        updateDataset.setTableName("users");
        final List<OutputDataset.UpdateOperationMapping> updateOperationMapping = new ArrayList<>();
        updateOperationMapping.add(new OutputDataset.UpdateOperationMapping("id", true));
        updateOperationMapping.add(new OutputDataset.UpdateOperationMapping("name", false));
        updateDataset.setUpdateOperationMapping(updateOperationMapping);
        final String updateConfig = configurationByExample().forInstance(updateDataset).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4&config.namePrefix=updatedUser")
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        final InputDataset in = new InputDataset();
        in.setConnection(connection);
        in.setSourceType(InputDataset.SourceType.QUERY);
        in.setSqlQuery("select * from users");
        final String inConfig = configurationByExample().forInstance(in).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://Input?" + inConfig).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();

        final List<JsonObject> records = componentsHandler.getCollectedData(JsonObject.class);
        assertNotNull(records);
        assertEquals(4, records.size());
        assertEquals(asList("updatedUser1", "updatedUser2", "updatedUser3", "updatedUser4"),
                records.stream().map(r -> r.getString("NAME")).collect(toList()));
    }

    @Test
    @DisplayName("Execute a valid update")
    void delete(final DerbyExtension.DerbyInfo derbyInfo) {
        // insert some initial data
        final BasicDatastore connection = newConnection(derbyInfo);
        final OutputDataset dataset = new OutputDataset();
        dataset.setConnection(connection);
        dataset.setActionOnData(OutputDataset.ActionOnData.Insert);
        dataset.setTableName("users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4")
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("userGenerator").to("jdbcOutput").build()
                .run();

        // delete the inserted data data
        final OutputDataset deleteDataset = new OutputDataset();
        deleteDataset.setConnection(connection);
        deleteDataset.setActionOnData(OutputDataset.ActionOnData.Delete);
        deleteDataset.setTableName("users");
        deleteDataset.setDeleteKeys(asList("id"));
        final String updateConfig = configurationByExample().forInstance(deleteDataset).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4")
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        final InputDataset in = new InputDataset();
        in.setConnection(connection);
        in.setSourceType(InputDataset.SourceType.QUERY);
        in.setSqlQuery("select * from users");
        final String inConfig = configurationByExample().forInstance(in).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://Input?" + inConfig).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();

        final List<JsonObject> records = componentsHandler.getCollectedData(JsonObject.class);
        assertNotNull(records);
        assertTrue(records.isEmpty());
    }

    private BasicDatastore newConnection(final DerbyExtension.DerbyInfo derbyInfo) {
        final BasicDatastore connection = new BasicDatastore();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("DERBY");
        connection.setJdbcUrl("jdbc:derby://localhost:" + derbyInfo.getPort() + "/" + derbyInfo.getDbName());
        return connection;
    }

}
