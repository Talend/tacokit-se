/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
import org.talend.components.jdbc.containers.JdbcTestContainer;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.components.jdbc.service.UIActionService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Locale;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.*;
import static org.talend.components.jdbc.Database.SNOWFLAKE;

@DisplayName("UIActionService")
@Environment(ContextualEnvironment.class)
@ExtendWith({ WithDatabasesEnvironments.class })
@DisabledDatabases({ @Disabled(value = SNOWFLAKE, reason = "Snowflake credentials need to be setup on ci") })
class UIActionServiceTest extends BaseJdbcTest {

    @Service
    private UIActionService uiActionService;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @TestTemplate
    @DisplayName("HealthCheck - Valid user")
    void validateBasicDatastore(final JdbcTestContainer container) {
        final HealthCheckStatus status = uiActionService.validateBasicDataStore(newConnection(container));
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

    @TestTemplate
    @DisplayName("HealthCheck - Bad credentials")
    void healthCheckWithBadCredentials(final JdbcTestContainer container) {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(container.getDatabaseType());
        datastore.setJdbcUrl(container.getJdbcUrl());
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = uiActionService.validateBasicDataStore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @TestTemplate
    @DisplayName("HealthCheck - Bad Database Name")
    void healthCheckWithBadDataBaseName(final JdbcTestContainer container) {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(container.getDatabaseType());
        datastore.setJdbcUrl(container.getJdbcUrl() + "DontExistUnlessyouCreatedDB");
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = uiActionService.validateBasicDataStore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @TestTemplate
    @DisplayName("HealthCheck - Bad Jdbc sub Protocol")
    void healthCheckWithBadSubProtocol(final JdbcTestContainer container) {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(container.getDatabaseType());
        datastore.setJdbcUrl("jdbc:darby/DB");
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = uiActionService.validateBasicDataStore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
        assertFalse(status.getComment().isEmpty());
    }

    @TestTemplate
    @DisplayName("Get Table list - valid connection")
    void getTableFromDatabase(final TestInfo testInfo, final JdbcTestContainer container) throws SQLException {
        final JdbcConnection datastore = newConnection(container);
        final String testTableName = getTestTableName(testInfo);
        createTestTable(testTableName, datastore);
        final SuggestionValues values = uiActionService.getTableFromDatabase(datastore);
        assertNotNull(values);
        assertTrue(values.getItems().stream().anyMatch(e -> e.getLabel().equalsIgnoreCase(testTableName)));
    }

    private void createTestTable(String testTableName, JdbcConnection datastore) throws SQLException {
        try (JdbcService.JdbcDatasource dataSource = getJdbcService().createDataSource(datastore, false)) {
            try (final Connection connection = dataSource.getConnection()) {
                PlatformFactory.get(datastore).createTableIfNotExist(connection, testTableName, singletonList("id"), -1,
                        singletonList(recordBuilderFactory.newRecordBuilder().withInt("id", 1).build()));
                connection.commit();
            }
        }
    }

    @TestTemplate
    @DisplayName("Get Table list - invalid connection")
    void getTableFromDatabaseWithInvalidConnection(final JdbcTestContainer container) {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(container.getDatabaseType());
        datastore.setJdbcUrl(container.getJdbcUrl());
        datastore.setUserId("wrong");
        datastore.setPassword("wrong");
        final SuggestionValues values = uiActionService.getTableFromDatabase(datastore);
        assertNotNull(values);
        assertEquals(0, values.getItems().size());
    }

    @TestTemplate
    @DisplayName("Get Table columns list - valid connection")
    void getTableColumnFromDatabase(final TestInfo testInfo, final JdbcTestContainer container) throws SQLException {
        final String testTableName = getTestTableName(testInfo);
        final TableNameDataset tableNameDataset = newTableNameDataset(testTableName, container);
        createTestTable(testTableName, tableNameDataset.getConnection());
        final SuggestionValues values = uiActionService.getTableColumns(tableNameDataset);
        assertNotNull(values);
        assertEquals(1, values.getItems().size());
        assertEquals(Stream.of("ID").collect(toSet()), values.getItems().stream().map(SuggestionValues.Item::getLabel)
                .map(l -> l.toUpperCase(Locale.ROOT)).collect(toSet()));
    }

    @TestTemplate
    @DisplayName("Get Table Columns list - invalid connection")
    void getTableColumnsFromDatabaseWithInvalidConnection(final TestInfo testInfo, final JdbcTestContainer container) {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(container.getDatabaseType());
        datastore.setJdbcUrl(container.getJdbcUrl());
        datastore.setUserId("wrong");
        datastore.setPassword("wrong");
        final TableNameDataset tableNameDataset = new TableNameDataset();
        tableNameDataset.setTableName(getTestTableName(testInfo));
        tableNameDataset.setConnection(datastore);
        final SuggestionValues values = uiActionService.getTableColumns(tableNameDataset);
        assertNotNull(values);
        assertTrue(values.getItems().isEmpty());
    }

    @TestTemplate
    @DisplayName(" Get Table Columns list - invalid table name")
    void getTableColumnsFromDatabaseWithInvalidTableName(final JdbcTestContainer container) {
        final JdbcConnection datastore = newConnection(container);
        final TableNameDataset tableNameDataset = new TableNameDataset();
        tableNameDataset.setTableName("tableNeverExist159");
        tableNameDataset.setConnection(datastore);
        final SuggestionValues values = uiActionService.getTableColumns(tableNameDataset);
        assertNotNull(values);
        assertTrue(values.getItems().isEmpty());
    }

}
