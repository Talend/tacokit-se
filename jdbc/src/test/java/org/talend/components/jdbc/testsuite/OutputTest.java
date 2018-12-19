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
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.jdbc.BaseJdbcTest;
import org.talend.components.jdbc.Disabled;
import org.talend.components.jdbc.DisabledDatabases;
import org.talend.components.jdbc.WithDatabasesEnvironments;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.containers.JdbcTestContainer;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit.environment.builtin.beam.DirectRunnerEnvironment;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.components.jdbc.Database.SNOWFLAKE;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@DisplayName("Output")
// @Environment(DirectRunnerEnvironment.class)
@Environment(ContextualEnvironment.class)
@ExtendWith(WithDatabasesEnvironments.class)
// @DisabledDatabases({ @Disabled(value = SNOWFLAKE, reason = "Snowflake credentials need to be setup on ci") })
class OutputTest extends BaseJdbcTest {

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @TestTemplate
    @DisplayName("Insert - valid use case")
    void insert(final TestInfo testInfo, final JdbcTestContainer container) {
        final OutputConfig configuration = new OutputConfig();
        final String testTableName = getTestTableName(testInfo);
        configuration.setDataset(newTableNameDataset(testTableName, container));
        configuration.setActionOnData(OutputConfig.ActionOnData.INSERT);
        configuration.setCreateTableIfNotExists(true);
        configuration.setKeys(singletonList("id"));
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();

        final int rowCount = (64 * 1024 * 1024) / recordBuilderFactory.newRecordBuilder().withString("t_string", "data0")
                .withBoolean("t_boolean", true).withLong("t_long", 10000000000L).withDouble("t_double", 1000.85d)
                .withFloat("t_float", 15.50f).withDateTime("t_date", new Date())
                .withBytes("t_bytes", "some data in bytes".getBytes(StandardCharsets.UTF_8)).build().toString().getBytes().length;

        Job.components().component("rowGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, 0, null))
                .component("jdbcOutput", "Jdbc://Output?configuration.$maxBatchSize=" + rowCount + "&" + config).connections()
                .from("rowGenerator").to("jdbcOutput").build().run();
        assertEquals(rowCount, countAll(testTableName, container));
    }

    @TestTemplate
    @DisplayName("Insert - with null values")
    void insertWithNullValues(final TestInfo testInfo, final JdbcTestContainer container) {
        final OutputConfig configuration = new OutputConfig();
        final String testTableName = getTestTableName(testInfo);
        configuration.setDataset(newTableNameDataset(testTableName, container));
        configuration.setActionOnData(OutputConfig.ActionOnData.INSERT);
        configuration.setCreateTableIfNotExists(true);
        configuration.setKeys(singletonList("id"));
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        final int rowCount = 10;
        Job.components().component("rowGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, true, 0, null))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput").build()
                .run();
        assertEquals(rowCount, countAll(testTableName, container));
    }

    @TestTemplate
    @DisplayName("Insert - Bad types handling")
    void insertBadTypes(final TestInfo testInfo, final JdbcTestContainer container) throws ParseException, SQLException {
        final Date date = new Date(new SimpleDateFormat("yyyy-MM-dd").parse("2018-12-6").getTime());
        final Date datetime = new Date();
        final Date time = new Date(1000 * 60 * 60 * 15 + 1000 * 60 * 20 + 39000); // 15:20:39
        final Record record = recordBuilderFactory.newRecordBuilder()
                .withInt(recordBuilderFactory.newEntryBuilder().withType(Schema.Type.INT).withNullable(true).withName("id")
                        .build(), 1)
                .withLong(recordBuilderFactory.newEntryBuilder().withType(Schema.Type.LONG).withNullable(true).withName("t_long")
                        .build(), 10L)
                .withDouble(recordBuilderFactory.newEntryBuilder().withType(Schema.Type.DOUBLE).withNullable(true)
                        .withName("t_double").build(), 20.02d)
                .withFloat(recordBuilderFactory.newEntryBuilder().withType(Schema.Type.FLOAT).withNullable(true)
                        .withName("t_float").build(), 30.03f)
                .withBoolean(recordBuilderFactory.newEntryBuilder().withType(Schema.Type.BOOLEAN).withNullable(true)
                        .withName("t_boolean").build(), false)
                .withBytes("t_bytes", "bytes".getBytes(StandardCharsets.UTF_8)).withString("t_string", "some text")
                .withDateTime("date", date).withDateTime("datetime", datetime).withDateTime("time", time).build();
        // create a table from valid record
        final JdbcConnection dataStore = newConnection(container);
        final String testTableName = getTestTableName(testInfo);
        try (final Connection connection = getJdbcService().createDataSource(dataStore).getConnection()) {
            PlatformFactory.get(dataStore).createTableIfNotExist(connection, testTableName, Collections.emptyList(),
                    Collections.singletonList(record));
        }
        runWithBad("id", "bad id", testTableName, container);
        runWithBad("t_long", "bad long", testTableName, container);
        runWithBad("t_double", "bad double", testTableName, container);
        runWithBad("t_float", "bad float", testTableName, container);
        runWithBad("t_boolean", "bad boolean", testTableName, container);
        runWithBad("date", "bad date", testTableName, container);
        runWithBad("datetime", "bad datetime", testTableName, container);
        runWithBad("time", "bad time", testTableName, container);

        assertEquals(0, countAll(testTableName, container));
    }

    private void runWithBad(final String field, final String value, final String testTableName,
            final JdbcTestContainer container) {
        final Record record = recordBuilderFactory.newRecordBuilder().withString(field, value).build();
        getComponentsHandler().setInputData(IntStream.range(0, 20).mapToObj(i -> record).collect(toList()));
        final OutputConfig configuration = new OutputConfig();
        configuration.setDataset(newTableNameDataset(testTableName, container));
        configuration.setActionOnData(OutputConfig.ActionOnData.INSERT);
        configuration.setCreateTableIfNotExists(false);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        try {
            Job.components().component("emitter", "test://emitter")
                    .component("jdbcOutput", "Jdbc://Output?$configuration.$maxBatchSize=4&" + config).connections()
                    .from("emitter").to("jdbcOutput").build().run();

        } catch (final Throwable e) {
            // those 2 database don't comply with jdbc spec and don't return a batch update exception when there is a batch error.
            if (!"mssql".equalsIgnoreCase(container.getDatabaseType())
                    && !"snowflake".equalsIgnoreCase(container.getDatabaseType())) {
                throw e;
            }
        }

    }

    @TestTemplate
    @DisabledDatabases({ @Disabled(value = SNOWFLAKE, reason = "Snowflake database don't enforce PK and UNIQUE constraint") })
    @DisplayName("Insert - duplicate records")
    void insertDuplicateRecords(final TestInfo testInfo, final JdbcTestContainer container) {
        final String testTableName = getTestTableName(testInfo);
        final long rowCount = 5;
        insertRows(testTableName, container, rowCount, false, 0, null);
        assertEquals(rowCount, countAll(testTableName, container));
        insertRows(testTableName, container, rowCount, false, 0, null);
        assertEquals(rowCount, countAll(testTableName, container));
    }

    @TestTemplate
    @DisplayName("Delete - valid query")
    void delete(final TestInfo testInfo, final JdbcTestContainer container) {
        // insert some initial data
        final int rowCount = 10;
        final String testTableName = getTestTableName(testInfo);
        insertRows(testTableName, container, rowCount, false, 0, null);
        // delete the inserted data data
        final OutputConfig deleteConfig = new OutputConfig();
        deleteConfig.setDataset(newTableNameDataset(testTableName, container));
        deleteConfig.setActionOnData(OutputConfig.ActionOnData.DELETE);
        deleteConfig.setKeys(singletonList("id"));
        final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
        Job.components().component("userGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, 0, null))
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        assertEquals(0L, countAll(testTableName, container));
    }

    @TestTemplate
    @DisplayName("Delete - No keys")
    void deleteWithNoKeys(final TestInfo testInfo, final JdbcTestContainer container) {
        final long rowCount = 3;
        final String testTableName = getTestTableName(testInfo);
        insertRows(testTableName, container, rowCount, false, 0, null);
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfig deleteConfig = new OutputConfig();
            deleteConfig.setDataset(newTableNameDataset(testTableName, container));
            deleteConfig.setActionOnData(OutputConfig.ActionOnData.DELETE);
            final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
            Job.components().component("userGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, 0, null))
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(getI18nMessage().errorNoKeyForDeleteQuery()));
        assertEquals(rowCount, countAll(testTableName, container));
    }

    @TestTemplate
    @DisplayName("Delete - Missing defined key in incoming record")
    void deleteWithMissingDefinedKeys(final TestInfo testInfo, final JdbcTestContainer container) {
        // 1) insert some data.
        final int rowCount = 2;
        final String testTableName = getTestTableName(testInfo);
        insertRows(testTableName, container, rowCount, false, 0, null);
        // 2) perform delete test with some record with missing delete key (id)
        final OutputConfig deleteConfig = new OutputConfig();
        deleteConfig.setDataset(newTableNameDataset(testTableName, container));
        deleteConfig.setActionOnData(OutputConfig.ActionOnData.DELETE);
        deleteConfig.setKeys(singletonList("id"));
        final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
        final int missingKeyEvery = 2;
        Job.components()
                .component("userGenerator",
                        "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, missingKeyEvery, null))
                .component("jdbcOutput", "Jdbc://Output?configuration.$maxBatchSize=2&" + updateConfig).connections()
                .from("userGenerator").to("jdbcOutput").build().run();

        // 3) check the remaining records
        assertEquals(IntStream.rangeClosed(1, rowCount).filter(r -> r % missingKeyEvery == 0).count(),
                readAll(testTableName, container).size());
    }

    @TestTemplate
    @DisplayName("Update - valid query")
    void update(final TestInfo testInfo, final JdbcTestContainer container) {
        // insert some initial data
        final int rowCount = 10;
        final String testTableName = getTestTableName(testInfo);
        insertRows(testTableName, container, rowCount, false, 0, null);
        // update the inserted data data
        final OutputConfig configuration = new OutputConfig();
        configuration.setDataset(newTableNameDataset(testTableName, container));
        configuration.setActionOnData(OutputConfig.ActionOnData.UPDATE);
        configuration.setKeys(singletonList("id"));
        final String updateConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components()
                .component("userGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, 0, "updated"))
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        final List<Record> users = readAll(testTableName, container);
        assertEquals(rowCount, users.size());
        assertEquals(IntStream.rangeClosed(1, rowCount).mapToObj(i -> "updated" + i).collect(toSet()), users.stream()
                .map(r -> ofNullable(r.getString("T_STRING")).orElseGet(() -> r.getString("t_string"))).collect(toSet()));
    }

    @TestTemplate
    @DisplayName("Update - no keys")
    void updateWithNoKeys(final TestInfo testInfo, final JdbcTestContainer container) {
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfig updateConfiguration = new OutputConfig();
            updateConfiguration.setDataset(newTableNameDataset(getTestTableName(testInfo), container));
            updateConfiguration.setActionOnData(OutputConfig.ActionOnData.UPDATE);
            final String updateConfig = configurationByExample().forInstance(updateConfiguration).configured().toQueryString();
            Job.components().component("userGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(1, false, 0, "updated"))
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(getI18nMessage().errorNoKeyForUpdateQuery()));
    }

    @TestTemplate
    @DisplayName("Upsert - valid query")
    void upsert(final TestInfo testInfo, final JdbcTestContainer container) {
        // insert some initial data
        final int existingRecords = 40;
        final String testTableName = getTestTableName(testInfo);
        insertRows(testTableName, container, existingRecords, false, 0, null);
        // update the inserted data data
        final OutputConfig configuration = new OutputConfig();
        configuration.setDataset(newTableNameDataset(testTableName, container));
        configuration.setActionOnData(OutputConfig.ActionOnData.UPSERT);
        configuration.setKeys(singletonList("id"));
        final String updateConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        final int newRecords = existingRecords * 2;
        Job.components()
                .component("rowGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(newRecords, false, 0, "updated"))
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("rowGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        final List<Record> users = readAll(testTableName, container);
        assertEquals(newRecords, users.size());
        assertEquals(IntStream.rangeClosed(1, newRecords).mapToObj(i -> "updated" + i).collect(toSet()), users.stream()
                .map(r -> ofNullable(r.getString("t_string")).orElseGet(() -> r.getString("T_STRING"))).collect(toSet()));
    }

    @TestTemplate
    @DisplayName("Insert - Date type handling")
    void dateTypesTest(final TestInfo testInfo, final JdbcTestContainer container) throws ParseException {
        final Date date = new Date(new SimpleDateFormat("yyyy-MM-dd").parse("2018-12-6").getTime());
        final Date datetime = new Date();
        final Date time = new Date(1000 * 60 * 60 * 15 + 1000 * 60 * 20 + 39000); // 15:20:39
        final Record record = recordBuilderFactory.newRecordBuilder().withDateTime("date", date)
                .withDateTime("datetime", datetime).withDateTime("time", time).build();
        final List<Record> data = new ArrayList<>();
        data.add(record);
        getComponentsHandler().setInputData(data);
        final OutputConfig configuration = new OutputConfig();
        final String testTableName = getTestTableName(testInfo);
        configuration.setDataset(newTableNameDataset(testTableName, container));
        configuration.setActionOnData(OutputConfig.ActionOnData.INSERT);
        configuration.setCreateTableIfNotExists(true);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("emitter", "test://emitter").component("jdbcOutput", "Jdbc://Output?" + config).connections()
                .from("emitter").to("jdbcOutput").build().run();
        List<Record> inserted = readAll(testTableName, container);
        assertEquals(1, inserted.size());
        final Record result = inserted.iterator().next();
        assertEquals(date.getTime(), result.getDateTime("date").toInstant().toEpochMilli());
        assertEquals(time.getTime(), result.getDateTime("time").toInstant().toEpochMilli());
        assertEquals(datetime.getTime(), result.getDateTime("datetime").toInstant().toEpochMilli());
    }

    @TestTemplate
    @DisplayName("Table handling - Not exist and No creation requested")
    void tableNotExistCase(final JdbcTestContainer container) {
        final Record record = recordBuilderFactory.newRecordBuilder().withString("data", "some data").build();
        getComponentsHandler().setInputData(singletonList(record));
        final OutputConfig configuration = new OutputConfig();
        configuration.setDataset(newTableNameDataset("AlienTableThatNeverExist999", container));
        configuration.setActionOnData(OutputConfig.ActionOnData.INSERT);
        configuration.setCreateTableIfNotExists(false);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        final Exception error = assertThrows(Exception.class, () -> Job.components().component("emitter", "test://emitter")
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("emitter").to("jdbcOutput").build().run());

        assertTrue(
                error.getMessage().contains(getI18nMessage().errorTaberDoesNotExists(configuration.getDataset().getTableName())));
    }
}
