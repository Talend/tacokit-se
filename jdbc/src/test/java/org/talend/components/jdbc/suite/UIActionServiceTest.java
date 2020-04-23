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
import java.util.Locale;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.talend.components.jdbc.configuration.DistributionStrategy;
import org.talend.components.jdbc.configuration.RedshiftSortStrategy;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.components.jdbc.service.UIActionService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("UIActionService")
public abstract class UIActionServiceTest extends JDBCBaseTest {

    @Service
    private UIActionService uiActionService;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Test
    @DisplayName("HealthCheck - Valid user")
    void validateBasicDatastore() {
        final HealthCheckStatus status = uiActionService.validateBasicDataStore(newConnection());
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

    @Test
    @DisplayName("HealthCheck - Bad credentials")
    void healthCheckWithBadCredentials() {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(this.getContainer().getDatabaseType());
        datastore.setJdbcUrl(this.getContainer().getJdbcUrl());
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = uiActionService.validateBasicDataStore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    @DisplayName("HealthCheck - Bad Database Name")
    void healthCheckWithBadDataBaseName() {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(this.getContainer().getDatabaseType());
        datastore.setJdbcUrl(this.getContainer().getJdbcUrl() + "DontExistUnlessyouCreatedDB");
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = uiActionService.validateBasicDataStore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    @DisplayName("HealthCheck - Bad Jdbc sub Protocol")
    void healthCheckWithBadSubProtocol() {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(this.getContainer().getDatabaseType());
        datastore.setJdbcUrl("jdbc:darby/DB");
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = uiActionService.validateBasicDataStore(datastore);
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
        final SuggestionValues values = uiActionService.getTableFromDatabase(datastore);
        assertNotNull(values);
        assertTrue(values.getItems().stream().anyMatch(e -> e.getLabel().equalsIgnoreCase(testTableName)));
    }

    private void createTestTable(String testTableName, JdbcConnection datastore) throws SQLException {
        try (JdbcService.JdbcDatasource dataSource = getJdbcService().createDataSource(datastore, false)) {
            try (final Connection connection = dataSource.getConnection()) {
                PlatformFactory.get(datastore, getI18nMessage()).createTableIfNotExist(connection, testTableName,
                        singletonList("id"), RedshiftSortStrategy.COMPOUND, emptyList(), DistributionStrategy.KEYS, emptyList(),
                        -1, singletonList(recordBuilderFactory.newRecordBuilder().withInt("id", 1).build()));
                connection.commit();
            }
        }
    }

    @Test
    @DisplayName("Get Table list - invalid connection")
    void getTableFromDatabaseWithInvalidConnection() {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(this.getContainer().getDatabaseType());
        datastore.setJdbcUrl(this.getContainer().getJdbcUrl());
        datastore.setUserId("wrong");
        datastore.setPassword("wrong");
        final SuggestionValues values = uiActionService.getTableFromDatabase(datastore);
        assertNotNull(values);
        assertEquals(0, values.getItems().size());
    }

    @Test
    @DisplayName("Get Table columns list - valid connection")
    void getTableColumnFromDatabase(final TestInfo testInfo) throws SQLException {
        final String testTableName = getTestTableName(testInfo);
        final TableNameDataset tableNameDataset = newTableNameDataset(testTableName);
        createTestTable(testTableName, tableNameDataset.getConnection());
        final SuggestionValues values = uiActionService.getTableColumns(tableNameDataset);
        assertNotNull(values);
        assertEquals(1, values.getItems().size());
        assertEquals(Stream.of("ID").collect(toSet()), values.getItems().stream().map(SuggestionValues.Item::getLabel)
                .map(l -> l.toUpperCase(Locale.ROOT)).collect(toSet()));
    }

    @Test
    @DisplayName("Get Table Columns list - invalid connection")
    void getTableColumnsFromDatabaseWithInvalidConnection(final TestInfo testInfo) {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(this.getContainer().getDatabaseType());
        datastore.setJdbcUrl(this.getContainer().getJdbcUrl());
        datastore.setUserId("wrong");
        datastore.setPassword("wrong");
        final TableNameDataset tableNameDataset = new TableNameDataset();
        tableNameDataset.setTableName(getTestTableName(testInfo));
        tableNameDataset.setConnection(datastore);
        final SuggestionValues values = uiActionService.getTableColumns(tableNameDataset);
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
        final SuggestionValues values = uiActionService.getTableColumns(tableNameDataset);
        assertNotNull(values);
        assertTrue(values.getItems().isEmpty());
    }

}
