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
package org.talend.components.jdbc.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.BaseJdbcTest;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import java.util.Locale;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.*;

public abstract class UIActionServiceTest extends BaseJdbcTest {

    @Service
    private UIActionService myService;

    @Test
    @DisplayName("DynamicValue - Load Drivers")
    void loadSupportedDataBaseTypes() {
        final Values values = myService.loadSupportedDataBaseTypes();
        assertNotNull(values);
        assertEquals(6, values.getItems().size());
        assertEquals(Stream.of("MySQL", "Derby", "Oracle", "Snowflake", "Postgresql", "Redshift").collect(toSet()),
                values.getItems().stream().map(Values.Item::getId).collect(toSet()));
    }

    @Test
    @DisplayName("HealthCheck - Valid user")
    void validateBasicDatastore() {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(getDatabaseType());
        datastore.setJdbcUrl(getJdbcUrl());
        datastore.setUserId(getUsername());
        datastore.setPassword(getPassword());
        final HealthCheckStatus status = myService.validateBasicDataStore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

    @Test
    @DisplayName("HealthCheck - Bad credentials")
    void healthCheckWithBadCredentials() {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(getDatabaseType());
        datastore.setJdbcUrl(getJdbcUrl());
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = myService.validateBasicDataStore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    @DisplayName("HealthCheck - Bad Database Name")
    void healthCheckWithBadDataBaseName() {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(getDatabaseType());
        datastore.setJdbcUrl(getJdbcUrl() + "DontExistUnlessyouCreatedDB");
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = myService.validateBasicDataStore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    @DisplayName("HealthCheck - Bad Jdbc sub Protocol")
    void healthCheckWithBadSubProtocol() {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(getDatabaseType());
        datastore.setJdbcUrl("jdbc:darby/DB");
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = myService.validateBasicDataStore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
        assertFalse(status.getComment().isEmpty());
    }

    @Test
    @DisplayName("Query - Validate select query")
    void validateReadOnlyQuery() {
        assertEquals(ValidationResult.Status.KO, myService.validateReadOnlySQLQuery("update table").getStatus());
        assertEquals(ValidationResult.Status.KO, myService.validateReadOnlySQLQuery("delete table").getStatus());
        assertEquals(ValidationResult.Status.KO, myService.validateReadOnlySQLQuery("insert table").getStatus());
        assertEquals(ValidationResult.Status.KO,
                myService.validateReadOnlySQLQuery("some other command other than select").getStatus());
        assertEquals(ValidationResult.Status.OK, myService.validateReadOnlySQLQuery("select * ").getStatus());
    }

    @Test
    @DisplayName("Get Table list - valid connection")
    void getTableFromDatabase() {
        final JdbcConnection datastore = newConnection();
        final SuggestionValues values = myService.getTableFromDatabase(datastore);
        assertNotNull(values);
        assertEquals(1, values.getItems().size());
        assertEquals(getTestTableName().toLowerCase(), values.getItems().iterator().next().getId().toLowerCase());
    }

    @Test
    @DisplayName("Get Table list - invalid connection")
    void getTableFromDatabaseWithInvalidConnection() {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(getDatabaseType());
        datastore.setJdbcUrl(getJdbcUrl());
        datastore.setUserId("wrong");
        datastore.setPassword("wrong");
        final SuggestionValues values = myService.getTableFromDatabase(datastore);
        assertNotNull(values);
        assertEquals(0, values.getItems().size());
    }

    @Test
    @DisplayName("Get Table columns list - valid connection")
    void getTableColumnFromDatabase() {
        final TableNameDataset tableNameDataset = newTableNameDataset(getTestTableName());
        final SuggestionValues values = myService.getTableColumns(tableNameDataset);
        assertNotNull(values);
        assertEquals(8, values.getItems().size());
        assertEquals(
                Stream.of("T_DOUBLE", "T_LONG", "T_BYTES", "T_FLOAT", "T_BOOLEAN", "T_STRING", "T_DATE", "ID").collect(toSet()),
                values.getItems().stream().map(SuggestionValues.Item::getLabel).map(l -> l.toUpperCase(Locale.ROOT))
                        .collect(toSet()));
    }

    @Test
    @DisplayName("Get Table Columns list - invalid connection")
    void getTableColumnsFromDatabaseWithInvalidConnection() {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(getDatabaseType());
        datastore.setJdbcUrl(getJdbcUrl());
        datastore.setUserId("wrong");
        datastore.setPassword("wrong");
        final TableNameDataset tableNameDataset = new TableNameDataset();
        tableNameDataset.setTableName(getTestTableName());
        tableNameDataset.setConnection(datastore);
        final SuggestionValues values = myService.getTableColumns(tableNameDataset);
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
        final SuggestionValues values = myService.getTableColumns(tableNameDataset);
        assertNotNull(values);
        assertTrue(values.getItems().isEmpty());
    }

}
