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
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.BaseTest;
import org.talend.components.jdbc.DerbyExtension;
import org.talend.components.jdbc.WithDerby;
import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@DisplayName("Output [Update] - Test cases")
@WithDerby(onStartSQLScript = "derby/output_create.sql", onShutdownSQLScript = "derby/delete.sql")
@WithComponents(value = "org.talend.components.jdbc")
class OutputUpdateTest extends BaseTest {

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
    @DisplayName("Update - valid query")
    void update(final DerbyExtension.DerbyInfo derbyInfo) {
        // insert some initial data
        final int rowCount = 4;
        insertUsers(derbyInfo, rowCount);

        // update the inserted data data
        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(newTableNameDataset(derbyInfo, "users"));
        configuration.setActionOnData(OutputConfiguration.ActionOnData.UPDATE);
        configuration.setKeys(asList("id"));
        final String updateConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components()
                .component("userGenerator",
                        "jdbcTest://UserGenerator?" + "config.rowCount=" + rowCount + "&config.namePrefix=updatedUser")
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        final List<Record> users = readAllUsers(derbyInfo);
        assertEquals(rowCount, users.size());
        assertEquals(IntStream.rangeClosed(1, rowCount).mapToObj(i -> "updatedUser" + i).collect(toSet()),
                users.stream().map(r -> r.getString("NAME")).collect(toSet()));
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
        final int rowCount = 100;
        // 1) insert some data
        insertUsers(derbyInfo, rowCount);
        // 2) try the update
        final int nullEvery = 5;
        final OutputConfiguration updateConfiguration = new OutputConfiguration();
        updateConfiguration.setDataset(newTableNameDataset(derbyInfo, "users"));
        updateConfiguration.setActionOnData(OutputConfiguration.ActionOnData.UPDATE);
        updateConfiguration.setKeys(asList("id"));
        final String updateConfig = configurationByExample().forInstance(updateConfiguration).configured().toQueryString();
        Job.components()
                .component("userGenerator",
                        "jdbcTest://UserGenerator?config.rowCount=" + rowCount + "&config.namePrefix=updatedUser"
                                + "&config.nullEvery=" + nullEvery + "&config.withNullIds=true")
                .component("jdbcOutput", "Jdbc://Output?configuration.$maxBatchSize=10&" + updateConfig).connections()
                .from("userGenerator").to("jdbcOutput").build().run();

        // 3) check for now update
        final long notUpdated = IntStream.rangeClosed(1, rowCount).filter(r -> r % nullEvery == 0).count();
        final List<Record> users = readAllUsers(derbyInfo);
        assertFalse(users.isEmpty());
        assertEquals(rowCount - notUpdated,
                users.stream().filter(user -> user.getString("NAME").startsWith("updatedUser")).count());
        assertEquals(notUpdated, users.stream().filter(user -> user.getString("NAME").startsWith("user")).count());
    }
}
