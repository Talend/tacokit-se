/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.jdbc.suite;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.talend.components.jdbc.configuration.DistributionStrategy;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.configuration.RedshiftSortStrategy;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.manager.chain.Job;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.derby.vti.XmlVTI.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@DisplayName("Output")
public abstract class OutputTest extends JDBCBaseTest {

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    private boolean withBoolean;

    private boolean withBytes;

    @BeforeEach
    void beforeEach() {
        withBoolean = !this.getContainer().getDatabaseType().equalsIgnoreCase("oracle");
        withBytes = !this.getContainer().getDatabaseType().equalsIgnoreCase("redshift");
    }

    @Test
    @DisplayName("Insert - valid use case")
    void insert(final TestInfo testInfo) {
        final OutputConfig configuration = new OutputConfig();
        final String testTableName = getTestTableName(testInfo);
        configuration.setDataset(newTableNameDataset(testTableName));
        configuration.setActionOnData(OutputConfig.ActionOnData.INSERT.name());
        configuration.setCreateTableIfNotExists(true);
        configuration.setKeys(asList("id"));
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        final int rowCount = 50;
        Job.components()
                .component("rowGenerator",
                        "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, null, withBoolean, withBytes))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput").build()
                .run();
        assertEquals(rowCount, countAll(testTableName));
    }

    @Test
    @DisplayName("Create table - combined primary keys")
    void createTableWithCombinedPrimaryKeys(final TestInfo testInfo) {
        final OutputConfig configuration = new OutputConfig();
        final String testTableName = getTestTableName(testInfo);
        configuration.setDataset(newTableNameDataset(testTableName));
        configuration.setActionOnData(OutputConfig.ActionOnData.INSERT.name());
        configuration.setCreateTableIfNotExists(true);
        configuration.setKeys(asList("id", "string_id"));
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        final int rowCount = 50;
        Job.ExecutorBuilder job = Job.components()
                .component("rowGenerator",
                        "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, null, withBoolean, withBytes))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput").build();
        job.run();
        assertEquals(rowCount, countAll(testTableName));
    }

    @Test
    @DisplayName("Insert - with null values")
    void insertWithNullValues(final TestInfo testInfo) {
        final OutputConfig configuration = new OutputConfig();
        final String testTableName = getTestTableName(testInfo);
        configuration.setDataset(newTableNameDataset(testTableName));
        configuration.setActionOnData(OutputConfig.ActionOnData.INSERT.name());
        configuration.setCreateTableIfNotExists(true);
        configuration.setKeys(singletonList("id"));
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        final int rowCount = 2;
        Job.components()
                .component("rowGenerator",
                        "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, true, null, withBoolean, withBytes))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput").build()
                .run();
        assertEquals(rowCount, countAll(testTableName));
    }

    @Test
    @DisplayName("Insert - Invalid types handling")
    void insertBadTypes(final TestInfo testInfo) throws ParseException, SQLException {
        final Date date = new Date(new SimpleDateFormat("yyyy-MM-dd").parse("2018-12-6").getTime());
        final Date datetime = new Date();
        final Date time = new Date(1000 * 60 * 60 * 15 + 1000 * 60 * 20 + 39000); // 15:20:39
        final Record.Builder builder = recordBuilderFactory.newRecordBuilder()
                .withInt(recordBuilderFactory.newEntryBuilder().withType(Schema.Type.INT).withNullable(true).withName("id")
                        .build(), 1)
                .withLong(recordBuilderFactory.newEntryBuilder().withType(Schema.Type.LONG).withNullable(true).withName("t_long")
                        .build(), 10L)
                .withDouble(recordBuilderFactory.newEntryBuilder().withType(Schema.Type.DOUBLE).withNullable(true)
                        .withName("t_double").build(), 20.02d)
                .withFloat(recordBuilderFactory.newEntryBuilder().withType(Schema.Type.FLOAT).withNullable(true)
                        .withName("t_float").build(), 30.03f)
                .withDateTime("date", date).withDateTime("datetime", datetime).withDateTime("time", time);
        if (!this.getContainer().getDatabaseType().equalsIgnoreCase("oracle")) {
            builder.withBoolean(recordBuilderFactory.newEntryBuilder().withType(Schema.Type.BOOLEAN).withNullable(true)
                    .withName("t_boolean").build(), false);
        }

        // create a table from valid record
        final JdbcConnection dataStore = newConnection();
        final String testTableName = getTestTableName(testInfo);
        try (final Connection connection = getJdbcService().createDataSource(dataStore).getConnection()) {
            PlatformFactory.get(dataStore, getI18nMessage()).createTableIfNotExist(connection, testTableName, emptyList(),
                    RedshiftSortStrategy.COMPOUND, emptyList(), DistributionStrategy.KEYS, emptyList(), -1,
                    Collections.singletonList(builder.build()));
        }
        runWithBad("id", "bad id", testTableName);
        runWithBad("t_long", "bad long", testTableName);
        runWithBad("t_double", "bad double", testTableName);
        runWithBad("t_float", "bad float", testTableName);
        if (!this.getContainer().getDatabaseType().equalsIgnoreCase("oracle")) {
            runWithBad("t_boolean", "bad boolean", testTableName);
        }
        runWithBad("date", "bad date", testTableName);
        runWithBad("datetime", "bad datetime", testTableName);
        runWithBad("time", "bad time", testTableName);

        assertEquals(0, countAll(testTableName));
    }

    private void runWithBad(final String field, final String value, final String testTableName) {
        final Record record = recordBuilderFactory.newRecordBuilder().withString(field, value).build();
        getComponentsHandler().setInputData(Stream.of(record).collect(toList()));
        final OutputConfig configuration = new OutputConfig();
        configuration.setDataset(newTableNameDataset(testTableName));
        configuration.setActionOnData(OutputConfig.ActionOnData.INSERT.name());
        configuration.setCreateTableIfNotExists(false);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        try {
            Job.components().component("emitter", "test://emitter")
                    .component("jdbcOutput", "Jdbc://Output?$configuration.$maxBatchSize=4&" + config).connections()
                    .from("emitter").to("jdbcOutput").build().run();
        } catch (final Throwable e) {
            // those 2 database don't comply with jdbc spec and don't return a batch update exception when there is a batch error.
            final String databaseType = this.getContainer().getDatabaseType();
            if (!"mssql".equalsIgnoreCase(databaseType) && !"snowflake".equalsIgnoreCase(databaseType)) {
                throw e;
            }
        }
    }

    @Test
    // @DisabledDatabases({ @Disabled(value = SNOWFLAKE, reason = "Snowflake database don't enforce PK and UNIQUE constraint") })
    @DisplayName("Insert - duplicate records")
    void insertDuplicateRecords(final TestInfo testInfo) {
        final String testTableName = getTestTableName(testInfo);
        final long rowCount = 5;
        insertRows(testTableName, rowCount, false, null);
        assertEquals(rowCount, countAll(testTableName));
        insertRows(testTableName, rowCount, false, null);
        assertEquals(rowCount, countAll(testTableName));
    }

    @Test
    @DisplayName("Delete - valid query")
    void delete(final TestInfo testInfo) {
        // insert some initial data
        final int rowCount = 10;
        final String testTableName = getTestTableName(testInfo);
        insertRows(testTableName, rowCount, false, null);
        // delete the inserted data data
        final OutputConfig deleteConfig = new OutputConfig();
        deleteConfig.setDataset(newTableNameDataset(testTableName));
        deleteConfig.setActionOnData(OutputConfig.ActionOnData.DELETE.name());
        deleteConfig.setKeys(singletonList("id"));
        final String updateConfig = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
        Job.components()
                .component("userGenerator",
                        "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, null, withBoolean, withBytes))
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        assertEquals(0L, countAll(testTableName));
    }

    @Test
    @DisplayName("Delete - No keys")
    void deleteWithNoKeys(final TestInfo testInfo) {
        final long rowCount = 3;
        final String testTableName = getTestTableName(testInfo);
        insertRows(testTableName, rowCount, false, null);
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfig deleteConfig = new OutputConfig();
            deleteConfig.setDataset(newTableNameDataset(testTableName));
            deleteConfig.setActionOnData(OutputConfig.ActionOnData.DELETE.name());
            final String config = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
            Job.components()
                    .component("userGenerator",
                            "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, null, withBoolean, withBytes))
                    .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(getI18nMessage().errorNoKeyForDeleteQuery()));
        assertEquals(rowCount, countAll(testTableName));
    }

    @Test
    @DisplayName("Delete - Wrong key param")
    void deleteWithNoFieldForQueryParam(final TestInfo testInfo) {
        final long rowCount = 3;
        final String testTableName = getTestTableName(testInfo);
        insertRows(testTableName, rowCount, false, null);
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfig deleteConfig = new OutputConfig();
            deleteConfig.setDataset(newTableNameDataset(testTableName));
            deleteConfig.setActionOnData(OutputConfig.ActionOnData.DELETE.name());
            deleteConfig.setKeys(singletonList("aMissingColumn"));
            final String config = configurationByExample().forInstance(deleteConfig).configured().toQueryString();
            Job.components()
                    .component("userGenerator",
                            "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, null, withBoolean, withBytes))
                    .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(getI18nMessage().errorNoFieldForQueryParam("aMissingColumn")));
        assertEquals(rowCount, countAll(testTableName));
    }

    @Test
    @DisplayName("Update - valid query")
    void update(final TestInfo testInfo) {
        // insert some initial data
        final int rowCount = 20;
        final String testTableName = getTestTableName(testInfo);
        insertRows(testTableName, rowCount, false, null);
        // update the inserted data data
        final OutputConfig configuration = new OutputConfig();
        configuration.setDataset(newTableNameDataset(testTableName));
        configuration.setActionOnData(OutputConfig.ActionOnData.UPDATE.name());
        configuration.setKeys(singletonList("id"));
        final String updateConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components()
                .component("userGenerator",
                        "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, "updated", withBoolean, withBytes))
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        final List<Record> users = readAll(testTableName);
        assertEquals(rowCount, users.size());
        assertEquals(IntStream.rangeClosed(1, rowCount).mapToObj(i -> "updated" + i).collect(toSet()), users.stream()
                .map(r -> ofNullable(r.getString("T_STRING")).orElseGet(() -> r.getString("t_string"))).collect(toSet()));
    }

    @Test
    @DisplayName("Update - no keys")
    void updateWithNoKeys(final TestInfo testInfo) {
        final Exception error = assertThrows(Exception.class, () -> {
            final OutputConfig updateConfiguration = new OutputConfig();
            updateConfiguration.setDataset(newTableNameDataset(getTestTableName(testInfo)));
            updateConfiguration.setActionOnData(OutputConfig.ActionOnData.UPDATE.name());
            final String updateConfig = configurationByExample().forInstance(updateConfiguration).configured().toQueryString();
            Job.components()
                    .component("userGenerator",
                            "jdbcTest://RowGenerator?" + rowGeneratorConfig(1, false, "updated", withBoolean, withBytes))
                    .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator").to("jdbcOutput")
                    .build().run();
        });
        assertTrue(error.getMessage().contains(getI18nMessage().errorNoKeyForUpdateQuery()));
    }

    @Test
    @DisplayName("Upsert - valid query")
    void upsert(final TestInfo testInfo) {
        // insert some initial data
        final int existingRecords = 40;
        final String testTableName = getTestTableName(testInfo);
        insertRows(testTableName, existingRecords, false, null);
        // update the inserted data data
        final OutputConfig configuration = new OutputConfig();
        configuration.setDataset(newTableNameDataset(testTableName));
        configuration.setActionOnData(OutputConfig.ActionOnData.UPSERT.name());
        configuration.setKeys(singletonList("id"));
        final String updateConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        final int newRecords = existingRecords * 2;
        Job.components()
                .component("rowGenerator",
                        "jdbcTest://RowGenerator?" + rowGeneratorConfig(newRecords, false, "updated", withBoolean, withBytes))
                .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("rowGenerator").to("jdbcOutput")
                .build().run();

        // check the update
        final List<Record> users = readAll(testTableName);
        assertEquals(newRecords, users.size());
        assertEquals(IntStream.rangeClosed(1, newRecords).mapToObj(i -> "updated" + i).collect(toSet()), users.stream()
                .map(r -> ofNullable(r.getString("t_string")).orElseGet(() -> r.getString("T_STRING"))).collect(toSet()));
    }

    @Test
    @DisplayName("Insert - Date type handling")
    void dateTypesTest(final TestInfo testInfo) throws ParseException {
        final ZoneId utc = ZoneId.of("UTC");
        final ZonedDateTime date = ZonedDateTime.of(LocalDate.of(2018, 12, 25), LocalTime.of(0, 0, 0), utc);
        final ZonedDateTime datetime = ZonedDateTime.of(2018, 12, 26, 11, 47, 15, 0, utc);
        final ZonedDateTime time = ZonedDateTime.ofInstant(Instant.ofEpochMilli(LocalTime.of(15, 20, 39).toSecondOfDay() * 1000),
                utc); // 15:20:39
        final Record record = recordBuilderFactory.newRecordBuilder().withDateTime("date", date)
                .withDateTime("datetime", datetime).withDateTime("time", time).build();
        final List<Record> data = new ArrayList<>();
        data.add(record);
        getComponentsHandler().setInputData(data);
        final OutputConfig configuration = new OutputConfig();
        final String testTableName = getTestTableName(testInfo);
        configuration.setDataset(newTableNameDataset(testTableName));
        configuration.setActionOnData(OutputConfig.ActionOnData.INSERT.name());
        configuration.setCreateTableIfNotExists(true);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("emitter", "test://emitter").component("jdbcOutput", "Jdbc://Output?" + config).connections()
                .from("emitter").to("jdbcOutput").build().run();
        List<Record> inserted = readAll(testTableName);
        assertEquals(1, inserted.size());
        final Record result = inserted.iterator().next();
        assertEquals(date.toInstant().toEpochMilli(), result.getDateTime("date").toInstant().toEpochMilli());
        assertEquals(datetime.toInstant().toEpochMilli(), result.getDateTime("datetime").toInstant().toEpochMilli());
        assertEquals(time.toInstant().toEpochMilli(), result.getDateTime("time").toInstant().toEpochMilli());
    }

    @Test
    @DisplayName("Table handling - Not exist and No creation requested")
    void tableNotExistCase() {
        final Record record = recordBuilderFactory.newRecordBuilder().withString("data", "some data").build();
        getComponentsHandler().setInputData(singletonList(record));
        final OutputConfig configuration = new OutputConfig();
        configuration.setDataset(newTableNameDataset("AlienTableThatNeverExist999"));
        configuration.setActionOnData(OutputConfig.ActionOnData.INSERT.name());
        configuration.setCreateTableIfNotExists(false);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        final Exception error = assertThrows(Exception.class, () -> Job.components().component("emitter", "test://emitter")
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("emitter").to("jdbcOutput").build().run());

        assertTrue(
                error.getMessage().contains(getI18nMessage().errorTaberDoesNotExists(configuration.getDataset().getTableName())));
    }

    // ParameterizedTest it, but need refactor if that, TODO
    @Test
    @DisplayName("Migration test for old job")
    void testMigration4Old(final TestInfo testInfo) {
        migration(testInfo, true);
    }

    @Test
    @DisplayName("Migration test for new job")
    void testMigration4New(final TestInfo testInfo) {
        migration(testInfo, false);
    }

    void migration(final TestInfo testInfo, boolean old) {
        final OutputConfig configuration = new OutputConfig();
        final String testTableName = getTestTableName(testInfo);
        configuration.setDataset(newTableNameDataset(testTableName));
        configuration.setActionOnData(OutputConfig.ActionOnData.INSERT.name());
        configuration.setCreateTableIfNotExists(true);
        configuration.setKeys(asList("id"));
        final String config = getOldComponentConfigString4MigrationTest(configuration, old);
        final int rowCount = 50;
        Job.components()
                .component("rowGenerator",
                        "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, null, withBoolean, withBytes))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput").build()
                .run();
        assertEquals(rowCount, countAll(testTableName));
    }

    private String getOldComponentConfigString4MigrationTest(OutputConfig configuration, boolean old) {
        String config = configurationByExample().forInstance(configuration).configured().toQueryString() + "&__version=1";
        if (old) {
            config = config.replace("configuration.keys.keys[", "configuration.keys[");
        }
        return config;
    }

}
