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

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.BaseTest;
import org.talend.components.jdbc.DerbyExtension;
import org.talend.components.jdbc.WithDerby;
import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@DisplayName("Output [Delete] - Test cases")
@WithDerby(onStartSQLScript = "derby/output_create.sql", onShutdownSQLScript = "derby/delete.sql")
@WithComponents(value = "org.talend.components.jdbc")
class OutputDeleteTest extends BaseTest {

    @Service
    private JdbcService jdbcService;

    @Service
    private I18nMessage i18nMessage;

    @BeforeEach
    void clearTable(final DerbyExtension.DerbyInfo derbyInfo) {
        final JdbcConnection datastore = newConnection(derbyInfo);
        try (final Connection connection = jdbcService.connection(datastore);) {
            try (final PreparedStatement stm = connection.prepareStatement("truncate table users")) {
                stm.execute();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    @DisplayName("Delete - valid query")
    void delete(final DerbyExtension.DerbyInfo derbyInfo) {
        // insert some initial data
        final int rowCount = 4;
        insertUsers(derbyInfo, rowCount);
        // delete the inserted data data
        final OutputConfiguration deleteConfig = new OutputConfiguration();
        deleteConfig.setDataset(newTableNameDataset(derbyInfo, "users"));
        deleteConfig.setActionOnData(OutputConfiguration.ActionOnData.DELETE);
        deleteConfig.setKeys(asList("id"));
        final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=" + rowCount)
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        assertTrue(readAllUsers(derbyInfo).isEmpty());
    }

    @Test
    @DisplayName("Delete - No keys")
    void deleteWithNoKeys(final DerbyExtension.DerbyInfo derbyInfo) {
        final int rowCount = 3;
        insertUsers(derbyInfo, rowCount);
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfiguration deleteConfig = new OutputConfiguration();
            deleteConfig.setDataset(newTableNameDataset(derbyInfo, "users"));
            deleteConfig.setActionOnData(OutputConfiguration.ActionOnData.DELETE);
            final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
            Job.components().component("userGenerator", "jdbcTest://UserGenerator?config.rowCount=" + rowCount)
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(i18nMessage.errorNoKeyForDeleteQuery()));
        assertEquals(rowCount, readAllUsers(derbyInfo).size());
    }

    @Test
    @DisplayName("Delete - Missing defined key in incoming record")
    void deleteWithMissingDefinedKeys(final DerbyExtension.DerbyInfo derbyInfo) {
        // 1) insert some data.
        final int rowCount = 50;
        insertUsers(derbyInfo, rowCount);
        // 2) perform delete test with some record with missing delete key (id)
        final TableNameDataset tableNameDataset = newTableNameDataset(derbyInfo, "users");
        final OutputConfiguration deleteConfig = new OutputConfiguration();
        deleteConfig.setDataset(tableNameDataset);
        deleteConfig.setActionOnData(OutputConfiguration.ActionOnData.DELETE);
        deleteConfig.setKeys(asList("id"));
        final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
        final int nullEvery = 6; // generate corrupted records
        Job.components()
                .component("userGenerator",
                        "jdbcTest://UserGenerator?config.rowCount=" + rowCount + "&config.withNullIds=true" + "&config.nullEvery="
                                + nullEvery)
                .component("jdbcOutput", "Jdbc://Output?configuration.$maxBatchSize=2&" + updateConfig).connections()
                .from("userGenerator").to("jdbcOutput").build().run();

        // 3) check the remaining records
        assertEquals(IntStream.rangeClosed(1, rowCount).filter(r -> r % nullEvery == 0).count(), readAllUsers(derbyInfo).size());
    }
}
