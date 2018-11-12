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

import static java.util.stream.Collectors.toSet;
import static org.apache.derby.vti.XmlVTI.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.BaseTest;
import org.talend.components.jdbc.DerbyExtension;
import org.talend.components.jdbc.WithDerby;
import org.talend.components.jdbc.components.DataCollector;
import org.talend.components.jdbc.dataset.QueryDataset;
import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@WithDerby(onStartSQLScript = "derby/output_create.sql", onShutdownSQLScript = "derby/delete.sql")
@WithComponents(value = "org.talend.components.jdbc")
class OutputTest extends BaseTest {

    @Service
    private JdbcService jdbcService;

    @Service
    private I18nMessage i18nMessage;

    @BeforeEach
    void clearTable(final DerbyExtension.DerbyInfo derbyInfo) {
        DataCollector.reset();
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
        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(newTableNameDataset(derbyInfo, "users"));
        configuration.setActionOnData(OutputConfiguration.ActionOnData.INSERT);

        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components()
                .component("userGenerator",
                        "jdbcTest://UserGenerator?config.rowCount=4&config.namePrefix=user&config.nullEvery=2&config.nameIsNull")
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("userGenerator").to("jdbcOutput").build()
                .run();

        final QueryDataset in = new QueryDataset();
        in.setConnection(newConnection(derbyInfo));
        in.setSqlQuery("select * from users");

        final String inConfig = configurationByExample().forInstance(in).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://QueryInput?" + inConfig)
                .component("collector", "jdbcTest://DataCollector").connections().from("jdbcInput").to("collector").build().run();

        assertEquals(4, DataCollector.getData().size());
        assertEquals(1 + 2 + 3 + 4, DataCollector.getData().stream().mapToInt(r -> r.getInt("ID")).sum());
        assertEquals(Stream.of("user1", "user3").collect(toSet()), DataCollector.getData().stream()
                .filter(r -> r.getString("NAME") != null).map(r -> r.getString("NAME")).collect(Collectors.toSet()));
    }

    @Test
    @DisplayName("Update - valid query")
    void update(final DerbyExtension.DerbyInfo derbyInfo) {
        // insert some initial data
        final BasicDatastore connection = newConnection(derbyInfo);
        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(newTableNameDataset(derbyInfo, "users"));
        configuration.setActionOnData(OutputConfiguration.ActionOnData.INSERT);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4&config.namePrefix=user")
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("userGenerator").to("jdbcOutput").build()
                .run();

        // update the inserted data data
        final OutputConfiguration updateConfiguration = new OutputConfiguration();
        updateConfiguration.setDataset(newTableNameDataset(derbyInfo, "users"));
        updateConfiguration.setActionOnData(OutputConfiguration.ActionOnData.UPDATE);
        final List<OutputConfiguration.UpdateOperationMapping> updateOperationMapping = new ArrayList<>();
        updateOperationMapping.add(new OutputConfiguration.UpdateOperationMapping("id", true));
        updateOperationMapping.add(new OutputConfiguration.UpdateOperationMapping("name", false));
        updateConfiguration.setUpdateOperationMapping(updateOperationMapping);
        final String updateConfig = configurationByExample().forInstance(updateConfiguration).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4&config.namePrefix=updatedUser")
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        final QueryDataset in = new QueryDataset();
        in.setConnection(connection);
        in.setSqlQuery("select * from users");
        final String inConfig = configurationByExample().forInstance(in).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://QueryInput?" + inConfig)
                .component("collector", "jdbcTest://DataCollector").connections().from("jdbcInput").to("collector").build().run();

        assertEquals(4, DataCollector.getData().size());
        assertEquals(Stream.of("updatedUser1", "updatedUser2", "updatedUser3", "updatedUser4").collect(toSet()),
                DataCollector.getData().stream().map(r -> r.getString("NAME")).collect(toSet()));
    }

    @Test
    @DisplayName("Update - no keys")
    void updateWithNoKeys(final DerbyExtension.DerbyInfo derbyInfo) {
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfiguration updateConfiguration = new OutputConfiguration();
            updateConfiguration.setDataset(newTableNameDataset(derbyInfo, "users"));
            updateConfiguration.setActionOnData(OutputConfiguration.ActionOnData.UPDATE);
            final String updateConfig = configurationByExample().forInstance(updateConfiguration).configured().toQueryString();
            Job.components()
                    .component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4&config.namePrefix=updatedUser")
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(i18nMessage.errorNoKeyForUpdateQuery()));
    }

    @Test
    @DisplayName("Update - missing key in the incoming record")
    void updateWithMissingKeys(final DerbyExtension.DerbyInfo derbyInfo) {
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfiguration updateConfiguration = new OutputConfiguration();
            updateConfiguration.setDataset(newTableNameDataset(derbyInfo, "users"));
            updateConfiguration.setActionOnData(OutputConfiguration.ActionOnData.UPDATE);

            final List<OutputConfiguration.UpdateOperationMapping> updateOperationMapping = new ArrayList<>();
            updateOperationMapping.add(new OutputConfiguration.UpdateOperationMapping("id", true));
            updateOperationMapping.add(new OutputConfiguration.UpdateOperationMapping("name", false));
            updateConfiguration.setUpdateOperationMapping(updateOperationMapping);
            final String updateConfig = configurationByExample().forInstance(updateConfiguration).configured().toQueryString();
            Job.components()
                    .component("userGenerator",
                            "jdbcTest://UserGenerator?config.rowCount=4&config.nullEvery=2&config.idIsNull=true")
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(i18nMessage.errorNoFieldForQueryParam("id")));
    }

    @Test
    @DisplayName("Update - no updatable columns")
    void updateWithNoUpdatableColumn(final DerbyExtension.DerbyInfo derbyInfo) {
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfiguration updateConfiguration = new OutputConfiguration();
            updateConfiguration.setActionOnData(OutputConfiguration.ActionOnData.UPDATE);
            updateConfiguration.setDataset(newTableNameDataset(derbyInfo, "users"));

            final List<OutputConfiguration.UpdateOperationMapping> updateOperationMapping = new ArrayList<>();
            updateOperationMapping.add(new OutputConfiguration.UpdateOperationMapping("id", true));
            updateConfiguration.setUpdateOperationMapping(updateOperationMapping);
            final String updateConfig = configurationByExample().forInstance(updateConfiguration).configured().toQueryString();
            Job.components()
                    .component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4&config.namePrefix=updatedUser")
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(i18nMessage.errorNoUpdatableColumnWasDefined()));
    }

    @Test
    @DisplayName("Update - missing updatable column in the incoming record")
    void updateWithMissingUpdatableColumn(final DerbyExtension.DerbyInfo derbyInfo) {
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfiguration updateConfiguration = new OutputConfiguration();
            updateConfiguration.setActionOnData(OutputConfiguration.ActionOnData.UPDATE);
            updateConfiguration.setDataset(newTableNameDataset(derbyInfo, "users"));
            final List<OutputConfiguration.UpdateOperationMapping> updateOperationMapping = new ArrayList<>();
            updateOperationMapping.add(new OutputConfiguration.UpdateOperationMapping("id", true));
            updateOperationMapping.add(new OutputConfiguration.UpdateOperationMapping("name", false));
            updateConfiguration.setUpdateOperationMapping(updateOperationMapping);
            final String updateConfig = configurationByExample().forInstance(updateConfiguration).configured().toQueryString();
            Job.components()
                    .component("userGenerator",
                            "jdbcTest://UserGenerator?config.rowCount=4&config.nameIsNull=true&config.nullEvery=1")
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(i18nMessage.errorNoFieldForQueryParam("name")));
    }

    @Test
    @DisplayName("Delete - valid query")
    void delete(final DerbyExtension.DerbyInfo derbyInfo) {
        // insert some initial data
        final BasicDatastore connection = newConnection(derbyInfo);
        final OutputConfiguration insertConfig = new OutputConfiguration();
        insertConfig.setDataset(newTableNameDataset(derbyInfo, "users"));
        insertConfig.setActionOnData(OutputConfiguration.ActionOnData.INSERT);
        final String config = configurationByExample().forInstance(insertConfig).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4")
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("userGenerator").to("jdbcOutput").build()
                .run();

        // delete the inserted data data
        final OutputConfiguration deleteConfig = new OutputConfiguration();
        deleteConfig.setDataset(newTableNameDataset(derbyInfo, "users"));
        deleteConfig.setActionOnData(OutputConfiguration.ActionOnData.DELETE);
        deleteConfig.setDeleteKeys(asList("id"));
        final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4")
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        final QueryDataset in = new QueryDataset();
        in.setConnection(connection);
        in.setSqlQuery("select * from users");
        final String inConfig = configurationByExample().forInstance(in).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://QueryInput?" + inConfig)
                .component("collector", "jdbcTest://DataCollector").connections().from("jdbcInput").to("collector").build().run();

        assertTrue(DataCollector.getData().isEmpty());
    }

    @Test
    @DisplayName("Delete - No keys")
    void deleteWithNoKeys(final DerbyExtension.DerbyInfo derbyInfo) {
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfiguration deleteConfig = new OutputConfiguration();
            deleteConfig.setDataset(newTableNameDataset(derbyInfo, "users"));
            deleteConfig.setActionOnData(OutputConfiguration.ActionOnData.DELETE);
            final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
            Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=4")
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(i18nMessage.errorNoKeyForDeleteQuery()));
    }

    @Test
    @DisplayName("Delete - Missing defined key in incoming record")
    void deleteWithMissingDefinedKeys(final DerbyExtension.DerbyInfo derbyInfo) {
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfiguration deleteConfig = new OutputConfiguration();
            deleteConfig.setDataset(newTableNameDataset(derbyInfo, "users"));
            deleteConfig.setActionOnData(OutputConfiguration.ActionOnData.DELETE);
            deleteConfig.setDeleteKeys(asList("name"));
            final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
            Job.components()
                    .component("userGenerator",
                            "jdbcTest://UserGenerator?config.rowCount=4&config.nameIsNull=true&config.nullEvery=1")
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(i18nMessage.errorNoFieldForQueryParam("name")));
    }

}
