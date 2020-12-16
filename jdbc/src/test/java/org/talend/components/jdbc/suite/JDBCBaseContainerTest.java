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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.derby.vti.XmlVTI.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.talend.components.jdbc.configuration.DistributionStrategy;
import org.talend.components.jdbc.configuration.InputQueryConfig;
import org.talend.components.jdbc.configuration.InputTableNameConfig;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.configuration.RedshiftSortStrategy;
import org.talend.components.jdbc.containers.JdbcTestContainer;
import org.talend.components.jdbc.dataset.SqlQueryDataset;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class JDBCBaseContainerTest {

    private JdbcTestContainer container = null;

    public JdbcTestContainer getContainer() {
        if (this.container == null) {
            this.container = this.buildContainer();
        }
        return this.container;
    }

    @BeforeAll
    public void init() {
        JdbcTestContainer container = this.getContainer();
        container.start();
    }

    @AfterAll
    public void release() {
        if (this.container != null) {
            try {
                this.container.close();
            } catch (Exception e) {
            }
            this.container = null;
        }
    }

    public String getTestTableName(final TestInfo info) {
        return info.getTestClass().map(Class::getSimpleName).map(name -> name.substring(0, Math.min(5, name.length())))
                .orElse("TEST") + "_"
                + info.getTestMethod().map(Method::getName).map(name -> name.substring(0, Math.min(10, name.length())))
                        .orElse("TABLE");
    }

    public abstract JdbcTestContainer buildContainer();

    @Nested
    @DisplayName("Platforms")
    @WithComponents("org.talend.components.jdbc")
    public class PlatformTests extends AbstractBaseJDBC {

        private final ZonedDateTime date = ZonedDateTime.of(LocalDateTime.of(2018, 12, 6, 12, 0, 0), ZoneId.systemDefault());

        private final Date datetime = new Date();

        private final Date time = new Date(1000 * 60 * 60 * 15 + 1000 * 60 * 20 + 39000); // 15:20:39

        private List<Record> records;

        @Override
        public JdbcTestContainer getContainer() {
            return JDBCBaseContainerTest.this.getContainer();
        }

        @BeforeEach
        void beforeEach() {
            records = new ArrayList<>();
            Record.Builder recordBuilder = this.getRecordBuilderFactory().newRecordBuilder().withInt("id", 1)
                    .withString("email", "user@talend.com").withString("t_text", RandomStringUtils.randomAlphabetic(300))
                    .withLong("t_long", 10000000000L).withDouble("t_double", 1000.85d).withFloat("t_float", 15.50f)
                    .withDateTime("t_date", date).withDateTime("t_datetime", datetime).withDateTime("t_time", time);

            if (!JDBCBaseContainerTest.this.getContainer().getDatabaseType().equalsIgnoreCase("oracle")) {
                recordBuilder.withBoolean("t_boolean", true);
            }

            if (!JDBCBaseContainerTest.this.getContainer().getDatabaseType().equalsIgnoreCase("redshift")) {
                recordBuilder.withBytes("t_bytes", "some data in bytes".getBytes(StandardCharsets.UTF_8));
            }

            records.add(recordBuilder.build());
        }

        @Test
        @DisplayName("Create table - Single primary key")
        void createTable(final TestInfo testInfo) throws SQLException {
            final String testTable = getTestTableName(testInfo);
            final JdbcConnection dataStore = newConnection();

            try (final JdbcService.JdbcDatasource dataSource = getJdbcService().createDataSource(dataStore)) {
                try (final Connection connection = dataSource.getConnection()) {
                    PlatformFactory.get(dataStore, getI18nMessage()).createTableIfNotExist(connection, testTable, asList("id"),
                            RedshiftSortStrategy.COMPOUND, emptyList(), DistributionStrategy.KEYS, emptyList(), -1, records);
                }
            }
        }

        @Test
        @DisplayName("Create table - Combined primary key")
        void createTableWithCombinedPrimaryKeys(final TestInfo testInfo) throws SQLException {
            final String testTable = getTestTableName(testInfo);
            final JdbcConnection dataStore = newConnection();

            // final DataSource dataSource = this.getDataSource();
            try (final JdbcService.JdbcDatasource dataSource = getJdbcService().createDataSource(dataStore)) {

                try (final Connection connection = dataSource.getConnection()) {
                    PlatformFactory.get(dataStore, getI18nMessage()).createTableIfNotExist(connection, testTable,
                            asList("id", "email"), RedshiftSortStrategy.COMPOUND, emptyList(), DistributionStrategy.KEYS,
                            emptyList(), -1, records);
                }
            }
        }

        @Test
        @DisplayName("Create table - existing table")
        void createExistingTable(final TestInfo testInfo) throws SQLException {
            final String testTable = getTestTableName(testInfo);
            final JdbcConnection dataStore = newConnection();

            try (final JdbcService.JdbcDatasource dataSource = getJdbcService().createDataSource(dataStore)) {
                try (final Connection connection = dataSource.getConnection()) {
                    Platform platform = PlatformFactory.get(dataStore, getI18nMessage());
                    platform.createTableIfNotExist(connection, testTable, asList("id", "email"), RedshiftSortStrategy.COMPOUND,
                            emptyList(), DistributionStrategy.KEYS, emptyList(), -1, records);
                    // recreate the table should not fail
                    platform.createTableIfNotExist(connection, testTable, asList("id", "email"), RedshiftSortStrategy.COMPOUND,
                            emptyList(), DistributionStrategy.KEYS, emptyList(), -1, records);
                }
            }
        }
    }

    @Nested
    @DisplayName("UIActionService")
    @WithComponents("org.talend.components.jdbc")
    public class UIActionServiceTest extends AbstractBaseJDBC {

        @Override
        public JdbcTestContainer getContainer() {
            return JDBCBaseContainerTest.this.getContainer();
        }

        @Test
        @DisplayName("HealthCheck - Valid user")
        void validateBasicDatastore() {
            final HealthCheckStatus status = this.getUiActionService().validateBasicDataStore(newConnection());
            assertNotNull(status);
            assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
        }

        @Test
        @DisplayName("HealthCheck - Bad credentials")
        void healthCheckWithBadCredentials() {
            final JdbcConnection datastore = new JdbcConnection();
            datastore.setDbType(JDBCBaseContainerTest.this.getContainer().getDatabaseType());
            datastore.setJdbcUrl(JDBCBaseContainerTest.this.getContainer().getJdbcUrl());
            datastore.setUserId("bad");
            datastore.setPassword("az");
            final HealthCheckStatus status = this.getUiActionService().validateBasicDataStore(datastore);
            assertNotNull(status);
            assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
        }

        @Test
        @DisplayName("HealthCheck - Bad Database Name")
        void healthCheckWithBadDataBaseName() {
            final JdbcConnection datastore = new JdbcConnection();
            datastore.setDbType(JDBCBaseContainerTest.this.getContainer().getDatabaseType());
            datastore.setJdbcUrl(JDBCBaseContainerTest.this.getContainer().getJdbcUrl() + "DontExistUnlessyouCreatedDB");
            datastore.setUserId("bad");
            datastore.setPassword("az");
            final HealthCheckStatus status = this.getUiActionService().validateBasicDataStore(datastore);
            assertNotNull(status);
            assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
        }

        @Test
        @DisplayName("HealthCheck - Bad Jdbc sub Protocol")
        void healthCheckWithBadSubProtocol() {
            final JdbcConnection datastore = new JdbcConnection();
            datastore.setDbType(JDBCBaseContainerTest.this.getContainer().getDatabaseType());
            datastore.setJdbcUrl("jdbc:darby/DB");
            datastore.setUserId("bad");
            datastore.setPassword("az");
            final HealthCheckStatus status = this.getUiActionService().validateBasicDataStore(datastore);
            assertNotNull(status);
            assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
            assertFalse(status.getComment().isEmpty());
        }

        @Test
        @DisplayName("Get Table list - valid connection")
        void getTableFromDatabase(final TestInfo testInfo) throws SQLException {
            final JdbcConnection datastore = newConnection();
            final String testTableName = getTestTableName(testInfo);
            createTestTable(testTableName, datastore);
            final SuggestionValues values = this.getUiActionService().getTableFromDatabase(datastore);
            assertNotNull(values);
            assertTrue(values.getItems().stream().anyMatch(e -> e.getLabel().equalsIgnoreCase(testTableName)));
        }

        private void createTestTable(String testTableName, JdbcConnection datastore) throws SQLException {
            try (JdbcService.JdbcDatasource dataSource = getJdbcService().createDataSource(datastore, false)) {
                try (final Connection connection = dataSource.getConnection()) {
                    PlatformFactory.get(datastore, getI18nMessage()).createTableIfNotExist(connection, testTableName,
                            singletonList("id"), RedshiftSortStrategy.COMPOUND, emptyList(), DistributionStrategy.KEYS,
                            emptyList(), -1,
                            singletonList(this.getRecordBuilderFactory().newRecordBuilder().withInt("id", 1).build()));
                    connection.commit();
                }
            }
        }

        @Test
        @DisplayName("Get Table list - invalid connection")
        void getTableFromDatabaseWithInvalidConnection() {
            final JdbcConnection datastore = new JdbcConnection();
            datastore.setDbType(JDBCBaseContainerTest.this.getContainer().getDatabaseType());
            datastore.setJdbcUrl(JDBCBaseContainerTest.this.getContainer().getJdbcUrl());
            datastore.setUserId("wrong");
            datastore.setPassword("wrong");
            final SuggestionValues values = this.getUiActionService().getTableFromDatabase(datastore);
            assertNotNull(values);
            assertEquals(0, values.getItems().size());
        }

        @Test
        @DisplayName("Get Table columns list - valid connection")
        void getTableColumnFromDatabase(final TestInfo testInfo) throws SQLException {
            final String testTableName = getTestTableName(testInfo);
            final TableNameDataset tableNameDataset = newTableNameDataset(testTableName);
            createTestTable(testTableName, tableNameDataset.getConnection());
            final SuggestionValues values = this.getUiActionService().getTableColumns(tableNameDataset);
            assertNotNull(values);
            assertEquals(1, values.getItems().size());
            assertEquals(Stream.of("ID").collect(toSet()), values.getItems().stream().map(SuggestionValues.Item::getLabel)
                    .map(l -> l.toUpperCase(Locale.ROOT)).collect(toSet()));
        }

        @Test
        @DisplayName("Get Table Columns list - invalid connection")
        void getTableColumnsFromDatabaseWithInvalidConnection(final TestInfo testInfo) {
            final JdbcConnection datastore = new JdbcConnection();
            datastore.setDbType(JDBCBaseContainerTest.this.getContainer().getDatabaseType());
            datastore.setJdbcUrl(JDBCBaseContainerTest.this.getContainer().getJdbcUrl());
            datastore.setUserId("wrong");
            datastore.setPassword("wrong");
            final TableNameDataset tableNameDataset = new TableNameDataset();
            tableNameDataset.setTableName(getTestTableName(testInfo));
            tableNameDataset.setConnection(datastore);
            final SuggestionValues values = this.getUiActionService().getTableColumns(tableNameDataset);
            assertNotNull(values);
            assertTrue(values.getItems().isEmpty());
        }

        @Test
        @DisplayName(" Get Table Columns list - invalid table name")
        void getTableColumnsFromDatabaseWithInvalidTableName() {
            final JdbcConnection datastore = newConnection();
            final TableNameDataset tableNameDataset = new TableNameDataset();
            tableNameDataset.setTableName("tableNeverExist159");
            tableNameDataset.setConnection(datastore);
            final SuggestionValues values = this.getUiActionService().getTableColumns(tableNameDataset);
            assertNotNull(values);
            assertTrue(values.getItems().isEmpty());
        }

    }

    @Nested
    @DisplayName("Input")
    @WithComponents("org.talend.components.jdbc")
    public class InputTest extends AbstractBaseJDBC {

        @Override
        public JdbcTestContainer getContainer() {
            return JDBCBaseContainerTest.this.getContainer();
        }

        @Test
        @DisplayName("Query - valid select query")
        void validQuery(final TestInfo testInfo) {
            final int rowCount = 50;
            final String testTableName = getTestTableName(testInfo);
            insertRows(testTableName, rowCount, false, null);
            final SqlQueryDataset sqlQueryDataset = new SqlQueryDataset();
            final JdbcConnection connection = newConnection();
            sqlQueryDataset.setConnection(connection);
            sqlQueryDataset.setFetchSize(rowCount / 3);
            sqlQueryDataset
                    .setSqlQuery("select * from " + PlatformFactory.get(connection, getI18nMessage()).identifier(testTableName));
            final InputQueryConfig config = new InputQueryConfig();
            config.setDataSet(sqlQueryDataset);
            final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
            Job.components().component("jdbcInput", "Jdbc://QueryInput?" + configURI).component("collector", "test://collector")
                    .connections().from("jdbcInput").to("collector").build().run();

            final List<Record> collectedData = getComponentsHandler().getCollectedData(Record.class);
            assertEquals(rowCount, collectedData.size());

        }

        @Test
        @DisplayName("Query - unvalid query ")
        void invalidQuery(final TestInfo testInfo) {
            final SqlQueryDataset sqlQueryDataset = new SqlQueryDataset();
            final JdbcConnection connection = newConnection();
            sqlQueryDataset.setConnection(connection);
            sqlQueryDataset.setSqlQuery("select fromm " + getTestTableName(testInfo));
            final InputQueryConfig config = new InputQueryConfig();
            config.setDataSet(sqlQueryDataset);
            final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
            assertThrows(IllegalStateException.class,
                    () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + configURI)
                            .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build()
                            .run());
        }

        @Test
        @DisplayName("Query -  non authorized query (drop table)")
        void unauthorizedDropQuery() {
            final SqlQueryDataset dataset = new SqlQueryDataset();
            dataset.setConnection(newConnection());
            dataset.setSqlQuery("drop table abc");
            final InputQueryConfig config = new InputQueryConfig();
            config.setDataSet(dataset);
            final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
            assertThrows(IllegalArgumentException.class,
                    () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + configURI)
                            .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build()
                            .run());
        }

        @Test
        @DisplayName("Query -  non authorized query (insert into)")
        void unauthorizedInsertQuery() {
            final SqlQueryDataset dataset = new SqlQueryDataset();
            dataset.setConnection(newConnection());
            dataset.setSqlQuery("INSERT INTO users(id, name) VALUES (1, 'user1')");
            final InputQueryConfig config = new InputQueryConfig();
            config.setDataSet(dataset);
            final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
            assertThrows(IllegalArgumentException.class,
                    () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + configURI)
                            .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build()
                            .run());
        }

        @Test
        @DisplayName("TableName - valid table name")
        void validTableName(final TestInfo testInfo) {
            final int rowCount = 50;
            final String testTableName = getTestTableName(testInfo);
            insertRows(testTableName, rowCount, false, null);
            final InputTableNameConfig config = new InputTableNameConfig();
            config.setDataSet(newTableNameDataset(testTableName));
            final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
            Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + configURI)
                    .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run();

            final List<Record> collectedData = getComponentsHandler().getCollectedData(Record.class);
            assertEquals(rowCount, collectedData.size());

        }

        @Test
        @DisplayName("TableName - invalid table name")
        void invalidTableName() {
            final TableNameDataset dataset = new TableNameDataset();
            dataset.setConnection(newConnection());
            dataset.setTableName("xxx");
            final InputTableNameConfig config = new InputTableNameConfig();
            config.setDataSet(dataset);
            final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
            assertThrows(IllegalStateException.class,
                    () -> Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + configURI)
                            .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build()
                            .run());
        }

        @Test
        @DisplayName("TableName - valid table name with null values")
        void validTableNameWithNullValues(final TestInfo testInfo) {
            final int rowCount = 1;
            final String testTableName = getTestTableName(testInfo);
            insertRows(testTableName, rowCount, true, null);
            final InputTableNameConfig config = new InputTableNameConfig();
            config.setDataSet(newTableNameDataset(testTableName));
            final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
            Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + configURI)
                    .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run();

            final List<Record> collectedData = getComponentsHandler().getCollectedData(Record.class);
            assertEquals(rowCount, collectedData.size());

        }

    }

    @Nested
    @DisplayName("Output")
    @WithComponents("org.talend.components.jdbc")
    public class OutputTest extends AbstractBaseJDBC {

        private boolean withBoolean;

        private boolean withBytes;

        @Override
        public JdbcTestContainer getContainer() {
            return JDBCBaseContainerTest.this.getContainer();
        }

        @BeforeEach
        void beforeEach() {
            withBoolean = !JDBCBaseContainerTest.this.getContainer().getDatabaseType().equalsIgnoreCase("oracle");
            withBytes = !JDBCBaseContainerTest.this.getContainer().getDatabaseType().equalsIgnoreCase("redshift");
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
                    .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput")
                    .build().run();
            Assert.assertEquals(rowCount, countAll(testTableName));
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
                    .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput")
                    .build();
            job.run();
            Assert.assertEquals(rowCount, countAll(testTableName));
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
                    .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput")
                    .build().run();
            Assert.assertEquals(rowCount, countAll(testTableName));
        }

        @Test
        @DisplayName("Insert - Invalid types handling")
        void insertBadTypes(final TestInfo testInfo) throws ParseException, SQLException {
            final Date date = new Date(new SimpleDateFormat("yyyy-MM-dd").parse("2018-12-6").getTime());
            final Date datetime = new Date();
            final Date time = new Date(1000 * 60 * 60 * 15 + 1000 * 60 * 20 + 39000); // 15:20:39
            final RecordBuilderFactory recordBuilderFactory = this.getRecordBuilderFactory();
            final Record.Builder builder = recordBuilderFactory.newRecordBuilder()
                    .withInt(recordBuilderFactory.newEntryBuilder().withType(Schema.Type.INT).withNullable(true).withName("id")
                            .build(), 1)
                    .withLong(recordBuilderFactory.newEntryBuilder().withType(Schema.Type.LONG).withNullable(true)
                            .withName("t_long").build(), 10L)
                    .withDouble(recordBuilderFactory.newEntryBuilder().withType(Schema.Type.DOUBLE).withNullable(true)
                            .withName("t_double").build(), 20.02d)
                    .withFloat(recordBuilderFactory.newEntryBuilder().withType(Schema.Type.FLOAT).withNullable(true)
                            .withName("t_float").build(), 30.03f)
                    .withDateTime("date", date).withDateTime("datetime", datetime).withDateTime("time", time);
            if (!JDBCBaseContainerTest.this.getContainer().getDatabaseType().equalsIgnoreCase("oracle")) {
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
            if (!JDBCBaseContainerTest.this.getContainer().getDatabaseType().equalsIgnoreCase("oracle")) {
                runWithBad("t_boolean", "bad boolean", testTableName);
            }
            runWithBad("date", "bad date", testTableName);
            runWithBad("datetime", "bad datetime", testTableName);
            runWithBad("time", "bad time", testTableName);

            Assert.assertEquals(0, countAll(testTableName));
        }

        private void runWithBad(final String field, final String value, final String testTableName) {
            final Record record = this.getRecordBuilderFactory().newRecordBuilder().withString(field, value).build();
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
                // those 2 database don't comply with jdbc spec and don't return a batch update exception when there is a batch
                // error.
                final String databaseType = JDBCBaseContainerTest.this.getContainer().getDatabaseType();
                if (!"mssql".equalsIgnoreCase(databaseType) && !"snowflake".equalsIgnoreCase(databaseType)) {
                    throw e;
                }
            }
        }

        @Test
        @DisplayName("Insert - duplicate records")
        void insertDuplicateRecords(final TestInfo testInfo) {
            final String testTableName = getTestTableName(testInfo);
            final long rowCount = 5;
            insertRows(testTableName, rowCount, false, null);
            Assert.assertEquals(rowCount, countAll(testTableName));
            insertRows(testTableName, rowCount, false, null);
            Assert.assertEquals(rowCount, countAll(testTableName));
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
            Assert.assertEquals(0L, countAll(testTableName));
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
            Assert.assertEquals(rowCount, countAll(testTableName));
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
            Assert.assertEquals(rowCount, countAll(testTableName));
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
            final List<Record> users = readAll(testTableName, this.getComponentsHandler());
            Assert.assertEquals(rowCount, users.size());
            Assert.assertEquals(IntStream.rangeClosed(1, rowCount).mapToObj(i -> "updated" + i).collect(toSet()), users.stream()
                    .map(r -> ofNullable(r.getString("T_STRING")).orElseGet(() -> r.getString("t_string"))).collect(toSet()));
        }

        @Test
        @DisplayName("Update - no keys")
        void updateWithNoKeys(final TestInfo testInfo) {
            final Exception error = assertThrows(Exception.class, () -> {
                final OutputConfig updateConfiguration = new OutputConfig();
                updateConfiguration.setDataset(newTableNameDataset(getTestTableName(testInfo)));
                updateConfiguration.setActionOnData(OutputConfig.ActionOnData.UPDATE.name());
                final String updateConfig = configurationByExample().forInstance(updateConfiguration).configured()
                        .toQueryString();
                Job.components()
                        .component("userGenerator",
                                "jdbcTest://RowGenerator?" + rowGeneratorConfig(1, false, "updated", withBoolean, withBytes))
                        .component("jdbcOutput", "Jdbc://Output?" + updateConfig).connections().from("userGenerator")
                        .to("jdbcOutput").build().run();
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
            final List<Record> users = readAll(testTableName, this.getComponentsHandler());
            Assert.assertEquals(newRecords, users.size());
            Assert.assertEquals(IntStream.rangeClosed(1, newRecords).mapToObj(i -> "updated" + i).collect(toSet()), users.stream()
                    .map(r -> ofNullable(r.getString("t_string")).orElseGet(() -> r.getString("T_STRING"))).collect(toSet()));
        }

        @Test
        @DisplayName("Insert - Date type handling")
        void dateTypesTest(final TestInfo testInfo) {
            final ZoneId utc = ZoneId.of("UTC");
            final ZonedDateTime date = ZonedDateTime.of(LocalDate.of(2018, 12, 25), LocalTime.of(0, 0, 0), utc);
            final ZonedDateTime datetime = ZonedDateTime.of(2018, 12, 26, 11, 47, 15, 0, utc);
            final ZonedDateTime time = ZonedDateTime
                    .ofInstant(Instant.ofEpochMilli(LocalTime.of(15, 20, 39).toSecondOfDay() * 1000), utc); // 15:20:39
            final Record record = this.getRecordBuilderFactory().newRecordBuilder().withDateTime("date", date)
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
            Job.components().component("emitter", "test://emitter").component("jdbcOutput", "Jdbc://Output?" + config)
                    .connections().from("emitter").to("jdbcOutput").build().run();
            List<Record> inserted = readAll(testTableName, this.getComponentsHandler());
            Assert.assertEquals(1, inserted.size());
            final Record result = inserted.iterator().next();
            Assert.assertEquals(date.toInstant().toEpochMilli(), result.getDateTime("date").toInstant().toEpochMilli());
            Assert.assertEquals(datetime.toInstant().toEpochMilli(), result.getDateTime("datetime").toInstant().toEpochMilli());
            Assert.assertEquals(time.toInstant().toEpochMilli(), result.getDateTime("time").toInstant().toEpochMilli());
        }

        @Test
        @DisplayName("Table handling - Not exist and No creation requested")
        void tableNotExistCase() {
            final Record record = this.getRecordBuilderFactory().newRecordBuilder().withString("data", "some data").build();
            getComponentsHandler().setInputData(singletonList(record));
            final OutputConfig configuration = new OutputConfig();
            configuration.setDataset(newTableNameDataset("AlienTableThatNeverExist999"));
            configuration.setActionOnData(OutputConfig.ActionOnData.INSERT.name());
            configuration.setCreateTableIfNotExists(false);
            final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
            final Exception error = assertThrows(Exception.class,
                    () -> Job.components().component("emitter", "test://emitter")
                            .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("emitter").to("jdbcOutput")
                            .build().run());

            final String errorMessage = error.getMessage();
            final String tableName = configuration.getDataset().getTableName();
            final String msglabel = getI18nMessage().errorTaberDoesNotExists(tableName);
            assertTrue(errorMessage.contains(msglabel));
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
                    .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput")
                    .build().run();
            Assert.assertEquals(rowCount, countAll(testTableName));
        }

        private String getOldComponentConfigString4MigrationTest(OutputConfig configuration, boolean old) {
            String config = configurationByExample().forInstance(configuration).configured().toQueryString() + "&__version=1";
            if (old) {
                config = config.replace("configuration.keys.keys[", "configuration.keys[");
            }
            return config;
        }

    }

}
