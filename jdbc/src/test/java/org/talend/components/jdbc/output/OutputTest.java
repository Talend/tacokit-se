/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.jdbc.output;

import static java.util.stream.Collectors.toList;
import static org.apache.derby.vti.XmlVTI.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.json.JsonObject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.DerbyExtension;
import org.talend.components.jdbc.WithDerby;
import org.talend.components.jdbc.dataset.InputDataset;
import org.talend.components.jdbc.dataset.OutputDataset;
import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@WithDerby(onStartSQLScript = "derby/output_create.sql", onShutdownSQLScript = "derby/delete.sql")
@WithComponents(value = "org.talend.components.jdbc")
class OutputTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JdbcService jdbcService;

    @Service
    private I18nMessage i18nMessage;

    @BeforeEach
    void clearTable(final DerbyExtension.DerbyInfo derbyInfo) {
        final BasicDatastore datastore = newConnection(derbyInfo);
        try (final Connection connection = jdbcService.connection(datastore);) {
            try (final PreparedStatement stm = connection.prepareStatement("truncate table users")) {
                stm.execute();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }

    }

    @Test
    @DisplayName("Insert - valid query")
    void insert(final DerbyExtension.DerbyInfo derbyInfo) {
        final BasicDatastore connection = newConnection(derbyInfo);
        final OutputDataset dataset = new OutputDataset();
        dataset.setConnection(connection);
        dataset.setActionOnData(OutputDataset.ActionOnData.INSERT);
        dataset.setTableName("users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components()
                .component("userGenerator",
                        "jdbcTest://UserGenerator?config.rowCount=4&config.namePrefix=user&config.nullEvery=2&config.nameIsNull")
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
        assertEquals(1 + 2 + 3 + 4, records.stream().mapToInt(r -> r.getInt("ID")).sum());
        assertEquals(asList("user1", "user3"),
                records.stream().filter(r -> r.containsKey("NAME")).map(r -> r.getString("NAME")).collect(toList()));
    }

    @Test
    @DisplayName("Update - valid query")
    void update(final DerbyExtension.DerbyInfo derbyInfo) {
        // insert some initial data
        final BasicDatastore connection = newConnection(derbyInfo);
        final OutputDataset dataset = new OutputDataset();
        dataset.setConnection(connection);
        dataset.setActionOnData(OutputDataset.ActionOnData.INSERT);
        dataset.setTableName("users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4&config.namePrefix=user")
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("userGenerator").to("jdbcOutput").build()
                .run();

        // update the inserted data data
        final OutputDataset updateDataset = new OutputDataset();
        updateDataset.setConnection(connection);
        updateDataset.setActionOnData(OutputDataset.ActionOnData.UPDATE);
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
    @DisplayName("Update - no keys")
    void updateWithNoKeys(final DerbyExtension.DerbyInfo derbyInfo) {
        final IllegalStateException error = assertThrows(IllegalStateException.class, () -> {
            final OutputDataset updateDataset = new OutputDataset();
            updateDataset.setConnection(newConnection(derbyInfo));
            updateDataset.setActionOnData(OutputDataset.ActionOnData.UPDATE);
            updateDataset.setTableName("users");
            final String updateConfig = configurationByExample().forInstance(updateDataset).configured().toQueryString();
            Job.components()
                    .component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4&config.namePrefix=updatedUser")
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertEquals(i18nMessage.errorNoKeyForUpdateQuery(), error.getMessage());
    }

    @Test
    @DisplayName("Update - missing key in the incoming record")
    void updateWithMissingKeys(final DerbyExtension.DerbyInfo derbyInfo) {
        final IllegalStateException error = assertThrows(IllegalStateException.class, () -> {
            final OutputDataset updateDataset = new OutputDataset();
            updateDataset.setConnection(newConnection(derbyInfo));
            updateDataset.setActionOnData(OutputDataset.ActionOnData.UPDATE);
            updateDataset.setTableName("users");
            final List<OutputDataset.UpdateOperationMapping> updateOperationMapping = new ArrayList<>();
            updateOperationMapping.add(new OutputDataset.UpdateOperationMapping("id", true));
            updateOperationMapping.add(new OutputDataset.UpdateOperationMapping("name", false));
            updateDataset.setUpdateOperationMapping(updateOperationMapping);
            final String updateConfig = configurationByExample().forInstance(updateDataset).configured().toQueryString();
            Job.components()
                    .component("userGenerator",
                            "jdbcTest://UserGenerator?config.rowCount=4&config.nullEvery=2&config.idIsNull=true")
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertEquals(i18nMessage.errorNoFieldForQueryParam("id"), error.getMessage());
    }

    @Test
    @DisplayName("Update - no updatable columns")
    void updateWithNoUpdatableColumn(final DerbyExtension.DerbyInfo derbyInfo) {
        final IllegalStateException error = assertThrows(IllegalStateException.class, () -> {
            final OutputDataset updateDataset = new OutputDataset();
            updateDataset.setConnection(newConnection(derbyInfo));
            updateDataset.setActionOnData(OutputDataset.ActionOnData.UPDATE);
            updateDataset.setTableName("users");
            final List<OutputDataset.UpdateOperationMapping> updateOperationMapping = new ArrayList<>();
            updateOperationMapping.add(new OutputDataset.UpdateOperationMapping("id", true));
            updateDataset.setUpdateOperationMapping(updateOperationMapping);
            final String updateConfig = configurationByExample().forInstance(updateDataset).configured().toQueryString();
            Job.components()
                    .component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4&config.namePrefix=updatedUser")
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertEquals(i18nMessage.errorNoUpdatableColumnWasDefined(), error.getMessage());
    }

    @Test
    @DisplayName("Update - missing updatable column in the incoming recprd")
    void updateWithMissingUpdatableColumn(final DerbyExtension.DerbyInfo derbyInfo) {
        final IllegalStateException error = assertThrows(IllegalStateException.class, () -> {
            final OutputDataset updateDataset = new OutputDataset();
            updateDataset.setConnection(newConnection(derbyInfo));
            updateDataset.setActionOnData(OutputDataset.ActionOnData.UPDATE);
            updateDataset.setTableName("users");
            final List<OutputDataset.UpdateOperationMapping> updateOperationMapping = new ArrayList<>();
            updateOperationMapping.add(new OutputDataset.UpdateOperationMapping("id", true));
            updateOperationMapping.add(new OutputDataset.UpdateOperationMapping("name", false));
            updateDataset.setUpdateOperationMapping(updateOperationMapping);
            final String updateConfig = configurationByExample().forInstance(updateDataset).configured().toQueryString();
            Job.components()
                    .component("userGenerator",
                            "jdbcTest://UserGenerator?config.rowCount=4&config.nameIsNull=true&config.nullEvery=1")
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertEquals(i18nMessage.errorNoFieldForQueryParam("name"), error.getMessage());
    }

    @Test
    @DisplayName("Delete - valid query")
    void delete(final DerbyExtension.DerbyInfo derbyInfo) {
        // insert some initial data
        final BasicDatastore connection = newConnection(derbyInfo);
        final OutputDataset dataset = new OutputDataset();
        dataset.setConnection(connection);
        dataset.setActionOnData(OutputDataset.ActionOnData.INSERT);
        dataset.setTableName("users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4")
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("userGenerator").to("jdbcOutput").build()
                .run();

        // delete the inserted data data
        final OutputDataset deleteDataset = new OutputDataset();
        deleteDataset.setConnection(connection);
        deleteDataset.setActionOnData(OutputDataset.ActionOnData.DELETE);
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

    @Test
    @DisplayName("Delete - No keys")
    void deleteWithNoKeys(final DerbyExtension.DerbyInfo derbyInfo) {
        final IllegalStateException error = assertThrows(IllegalStateException.class, () -> {
            final OutputDataset deleteDataset = new OutputDataset();
            deleteDataset.setConnection(newConnection(derbyInfo));
            deleteDataset.setActionOnData(OutputDataset.ActionOnData.DELETE);
            deleteDataset.setTableName("users");
            final String updateConfig = configurationByExample().forInstance(deleteDataset).configured().toQueryString();
            Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4")
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertEquals(i18nMessage.errorNoKeyForDeleteQuery(), error.getMessage());
    }

    @Test
    @DisplayName("Delete - Missing defined key in incoming record")
    void deleteWithMissingDefinedKeys(final DerbyExtension.DerbyInfo derbyInfo) {
        final IllegalStateException error = assertThrows(IllegalStateException.class, () -> {
            final OutputDataset deleteDataset = new OutputDataset();
            deleteDataset.setConnection(newConnection(derbyInfo));
            deleteDataset.setActionOnData(OutputDataset.ActionOnData.DELETE);
            deleteDataset.setTableName("users");
            deleteDataset.setDeleteKeys(asList("name"));
            final String updateConfig = configurationByExample().forInstance(deleteDataset).configured().toQueryString();
            Job.components()
                    .component("userGenerator",
                            "jdbcTest://UserGenerator?config.rowCount=4&config.nameIsNull=true&config.nullEvery=1")
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertEquals(i18nMessage.errorNoFieldForQueryParam("name"), error.getMessage());
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
