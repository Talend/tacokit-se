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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.BaseJdbcTest;
import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@DisplayName("Output [Upsert] - Test cases")
@WithComponents(value = "org.talend.components.jdbc")
abstract class UpsertDefaultTest extends BaseJdbcTest {

    @BeforeEach
    void clearTable() {
        final JdbcConnection datastore = newConnection();
        try (final Connection connection = getJdbcService().connection(datastore);) {
            try (final PreparedStatement stm = connection.prepareStatement("truncate table users")) {
                stm.execute();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    @DisplayName("Upsert - valid query")
    void upsert() {
        // insert some initial data
        final int existingRecords = 50;
        insertRows(existingRecords, false, 0, null);

        // update the inserted data data
        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(newTableNameDataset("users"));
        configuration.setActionOnData(OutputConfiguration.ActionOnData.UPSERT);
        configuration.setKeys(asList("id"));
        final String updateConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        final int newRecords = 50;
        Job.components()
                .component("userGenerator",
                        "jdbcTest://RowGenerator?" + "config.rowCount=" + (existingRecords + newRecords)
                                + "&config.namePrefix=updatedUser")
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        final List<Record> users = readAll(getTestTableName());
        assertEquals(existingRecords + newRecords, users.size());
        assertEquals(IntStream.rangeClosed(1, existingRecords + newRecords).mapToObj(i -> "updatedUser" + i).collect(toSet()),
                users.stream().map(r -> r.getString("NAME")).collect(toSet()));
    }

}
