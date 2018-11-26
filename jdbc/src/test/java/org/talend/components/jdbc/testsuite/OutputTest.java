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
package org.talend.components.jdbc.testsuite;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.jdbc.BaseJdbcTest;
import org.talend.components.jdbc.JdbcInvocationContextProvider;
import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.components.jdbc.containers.JdbcTestContainer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit.environment.builtin.beam.DirectRunnerEnvironment;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.*;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@DisplayName("Output")
@WithComponents("org.talend.components.jdbc")
@ExtendWith({ JdbcInvocationContextProvider.class })
@Environment(ContextualEnvironment.class)
@Environment(DirectRunnerEnvironment.class)
class OutputTest extends BaseJdbcTest {

    @TestTemplate
    @DisplayName("Insert - valid use case")
    void insert(final JdbcTestContainer container) {
        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(newTableNameDataset(container.getTestTableName(), container));
        configuration.setActionOnData(OutputConfiguration.ActionOnData.INSERT);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        final int rowCount = getRandomRowCount();
        Job.components().component("rowGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, 0, null))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput").build()
                .run();
        assertEquals(rowCount, countAll(container));
    }

    @TestTemplate
    @DisplayName("Insert - with null values")
    void insertWithNullValues(final JdbcTestContainer container) {
        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(newTableNameDataset(container.getTestTableName(), container));
        configuration.setActionOnData(OutputConfiguration.ActionOnData.INSERT);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        final int rowCount = getRandomRowCount();
        Job.components().component("rowGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, true, 0, null))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput").build()
                .run();
        assertEquals(rowCount, countAll(container));
    }

    /*
     * Duplicated records should be rejected and valid record need to be inserted even if it's done in a one single batch
     * statement
     */
    @TestTemplate
    @DisplayName("Insert - duplicate records")
    void insertDuplicateRecords(final JdbcTestContainer container) {
        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(newTableNameDataset(container.getTestTableName(), container));
        configuration.setActionOnData(OutputConfiguration.ActionOnData.INSERT);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        final int rowCount = 2;
        Job.components().component("rowGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, 0, null))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput").build()
                .run();
        assertEquals(rowCount, readAll(container).size());
        Job.components().component("rowGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount * 2, false, 0, null))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput").build()
                .run();

        long size = countAll(container);
        // some drivers will reject all records and others will reject only duplicated one
        assertTrue(rowCount * 2 == size || rowCount == size);
    }

    @TestTemplate
    @DisplayName("Delete - valid query")
    void delete(final JdbcTestContainer container) {
        // insert some initial data
        final int rowCount = getRandomRowCount();
        insertRows(container, rowCount, false, 0, null);
        // delete the inserted data data
        final OutputConfiguration deleteConfig = new OutputConfiguration();
        deleteConfig.setDataset(newTableNameDataset(container.getTestTableName(), container));
        deleteConfig.setActionOnData(OutputConfiguration.ActionOnData.DELETE);
        deleteConfig.setKeys(singletonList("id"));
        final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://RowGenerator?config.rowCount=" + rowCount)
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        assertEquals(0L, countAll(container));
    }

    @TestTemplate
    @DisplayName("Delete - No keys")
    void deleteWithNoKeys(final JdbcTestContainer container) {
        final long rowCount = 3;
        insertRows(container, rowCount, false, 0, null);
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfiguration deleteConfig = new OutputConfiguration();
            deleteConfig.setDataset(newTableNameDataset(container.getTestTableName(), container));
            deleteConfig.setActionOnData(OutputConfiguration.ActionOnData.DELETE);
            final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
            Job.components().component("userGenerator", "jdbcTest://RowGenerator?config.rowCount=" + rowCount)
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(getI18nMessage().errorNoKeyForDeleteQuery()));
        assertEquals(rowCount, countAll(container));
    }

    @TestTemplate
    @DisplayName("Delete - Missing defined key in incoming record")
    void deleteWithMissingDefinedKeys(final JdbcTestContainer container) {
        // 1) insert some data.
        final int rowCount = 2;
        insertRows(container, rowCount, false, 0, null);
        // 2) perform delete test with some record with missing delete key (id)
        final OutputConfiguration deleteConfig = new OutputConfiguration();
        deleteConfig.setDataset(newTableNameDataset(container.getTestTableName(), container));
        deleteConfig.setActionOnData(OutputConfiguration.ActionOnData.DELETE);
        deleteConfig.setKeys(singletonList("id"));
        final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
        final int missingKeyEvery = 2;
        Job.components()
                .component("userGenerator",
                        "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, missingKeyEvery, null))
                .component("jdbcOutput", "Jdbc://Output?configuration.$maxBatchSize=2&" + updateConfig).connections()
                .from("userGenerator").to("jdbcOutput").build().run();

        // 3) check the remaining records
        assertEquals(IntStream.rangeClosed(1, rowCount).filter(r -> r % missingKeyEvery == 0).count(), readAll(container).size());
    }

    @TestTemplate
    @DisplayName("Update - valid query")
    void update(final JdbcTestContainer container) {
        // insert some initial data
        final int rowCount = getRandomRowCount();
        insertRows(container, rowCount, false, 0, null);
        // update the inserted data data
        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(newTableNameDataset(container.getTestTableName(), container));
        configuration.setActionOnData(OutputConfiguration.ActionOnData.UPDATE);
        configuration.setKeys(singletonList("id"));
        final String updateConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components()
                .component("userGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, 0, "updated"))
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        final List<Record> users = readAll(container);
        assertEquals(rowCount, users.size());
        assertEquals(IntStream.rangeClosed(1, rowCount).mapToObj(i -> "updated" + i).collect(toSet()), users.stream()
                .map(r -> ofNullable(r.getString("T_STRING")).orElseGet(() -> r.getString("t_string"))).collect(toSet()));
    }

    @TestTemplate
    @DisplayName("Update - no keys")
    void updateWithNoKeys(final JdbcTestContainer container) {
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfiguration updateConfiguration = new OutputConfiguration();
            updateConfiguration.setDataset(newTableNameDataset(container.getTestTableName(), container));
            updateConfiguration.setActionOnData(OutputConfiguration.ActionOnData.UPDATE);
            final String updateConfig = configurationByExample().forInstance(updateConfiguration).configured().toQueryString();
            Job.components().component("userGenerator", "jdbcTest://RowGenerator?config.rowCount=4&config.namePrefix=updatedUser")
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(getI18nMessage().errorNoKeyForUpdateQuery()));
    }

    @TestTemplate
    @DisplayName("Upsert - valid query")
    void upsert(final JdbcTestContainer container) {
        // insert some initial data
        final int existingRecords = getRandomRowCount();
        insertRows(container, existingRecords, false, 0, null);
        // update the inserted data data
        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(newTableNameDataset(container.getTestTableName(), container));
        configuration.setActionOnData(OutputConfiguration.ActionOnData.UPSERT);
        configuration.setKeys(singletonList("id"));
        final String updateConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        final int newRecords = existingRecords * 2;
        Job.components()
                .component("rowGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(newRecords, false, 0, "updated"))
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("rowGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        final List<Record> users = readAll(container);
        assertEquals(newRecords, users.size());
        assertEquals(IntStream.rangeClosed(1, newRecords).mapToObj(i -> "updated" + i).collect(toSet()), users.stream()
                .map(r -> ofNullable(r.getString("t_string")).orElseGet(() -> r.getString("T_STRING"))).collect(toSet()));
    }
}
