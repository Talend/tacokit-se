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

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.DerbyExtension;
import org.talend.components.jdbc.WithDerby;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

@WithDerby(onStartSQLScript = "derby/input_create.sql", onShutdownSQLScript = "derby/delete.sql")
@WithComponents("org.talend.components.jdbc") // component package
class ActionServiceTest {

    @Injected
    private ComponentsHandler componentsHandler;

    @Service
    private ActionService myService;

    @Test
    @DisplayName("DynamicValue - Load Drivers")
    void loadSupportedDataBaseTypes() {
        final Values values = myService.loadSupportedDataBaseTypes();
        assertNotNull(values);
        assertEquals(4, values.getItems().size());
        assertEquals(Stream.of("MySQL", "Derby", "Oracle", "Snowflake").collect(toSet()),
                values.getItems().stream().map(Values.Item::getId).collect(toSet()));
    }

    @Test
    @DisplayName("Datastore HealthCheck - Valid user")
    void validateBasicDatastore(final DerbyExtension.DerbyInfo info) {
        final BasicDatastore datastore = new BasicDatastore();
        datastore.setDbType("Derby");
        datastore.setJdbcUrl("jdbc:derby://localhost:" + info.getPort() + "/" + info.getDbName());
        datastore.setUserId("sa");
        datastore.setPassword("sa");
        final HealthCheckStatus status = myService.validateBasicDataStore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

    @Test
    @DisplayName("Datastore HealthCheck - Bad credentials")
    void healthCheckWithBadCredentials(final DerbyExtension.DerbyInfo info) {
        final BasicDatastore datastore = new BasicDatastore();
        datastore.setDbType("Derby");
        datastore.setJdbcUrl("jdbc:derby://localhost:" + info.getPort() + "/" + info.getDbName());
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = myService.validateBasicDataStore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    @DisplayName("Datastore HealthCheck - Bad Database Name")
    void healthCheckWithBadDataBaseName(final DerbyExtension.DerbyInfo info) {
        final BasicDatastore datastore = new BasicDatastore();
        datastore.setDbType("Derby");
        datastore.setJdbcUrl("jdbc:derby://localhost:" + info.getPort() + "/DontExistUnlessyouCreatedDB");
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = myService.validateBasicDataStore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    @DisplayName("Datastore HealthCheck - Bad Jdbc sub Protocol")
    void healthCheckWithBadSubProtocol() {
        final BasicDatastore datastore = new BasicDatastore();
        datastore.setDbType("Derby");
        datastore.setJdbcUrl("jdbc:darby/DB");
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = myService.validateBasicDataStore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    @DisplayName("Datastore Query - Validate select query")
    void validateReadOnlyQuery() {
        assertEquals(ValidationResult.Status.KO, myService.validateReadOnlySQLQuery("update table").getStatus());
        assertEquals(ValidationResult.Status.KO, myService.validateReadOnlySQLQuery("delete table").getStatus());
        assertEquals(ValidationResult.Status.KO, myService.validateReadOnlySQLQuery("insert table").getStatus());
        assertEquals(ValidationResult.Status.KO,
                myService.validateReadOnlySQLQuery("some other command other than select").getStatus());
        assertEquals(ValidationResult.Status.OK, myService.validateReadOnlySQLQuery("select * ").getStatus());
    }

    @Test
    @DisplayName("Datastore Get Table list - valid connection")
    void getTableFromDatabase(final DerbyExtension.DerbyInfo info) {
        final BasicDatastore datastore = new BasicDatastore();
        datastore.setDbType("Derby");
        datastore.setJdbcUrl("jdbc:derby://localhost:" + info.getPort() + "/" + info.getDbName());
        datastore.setUserId("sa");
        datastore.setPassword("sa");
        final SuggestionValues values = myService.getTableFromDatabase(datastore);
        assertNotNull(values);
        assertEquals(1, values.getItems().size());
        assertEquals("USERS", values.getItems().iterator().next().getId());
    }

    @Test
    @DisplayName("Datastore Get Table list - invalid connection")
    void getTableFromDatabaseWithInvalidConnection(final DerbyExtension.DerbyInfo info) {
        final BasicDatastore datastore = new BasicDatastore();
        datastore.setDbType("Derby");
        datastore.setJdbcUrl("jdbc:derby://localhost:" + info.getPort() + "/" + info.getDbName());
        datastore.setUserId("wrong");
        datastore.setPassword("wrong");
        final SuggestionValues values = myService.getTableFromDatabase(datastore);
        assertNotNull(values);
        assertEquals(0, values.getItems().size());
    }

    @Test
    @DisplayName("Get Table columns list - valid connection")
    void getTableColumnFromDatabase(final DerbyExtension.DerbyInfo info) {
        final BasicDatastore datastore = new BasicDatastore();
        datastore.setDbType("Derby");
        datastore.setJdbcUrl("jdbc:derby://localhost:" + info.getPort() + "/" + info.getDbName());
        datastore.setUserId("sa");
        datastore.setPassword("sa");
        final TableNameDataset tableNameDataset = new TableNameDataset();
        tableNameDataset.setTableName("users");
        tableNameDataset.setConnection(datastore);
        final SuggestionValues values = myService.getTableColumns(tableNameDataset);
        assertNotNull(values);
        assertEquals(2, values.getItems().size());
        assertEquals(Stream.of("ID", "NAME").collect(toSet()),
                values.getItems().stream().map(SuggestionValues.Item::getLabel).collect(toSet()));
    }

    @Test
    @DisplayName("Get Table Columns list - invalid connection")
    void getTableColumnsFromDatabaseWithInvalidConnection(final DerbyExtension.DerbyInfo info) {
        final BasicDatastore datastore = new BasicDatastore();
        datastore.setDbType("Derby");
        datastore.setJdbcUrl("jdbc:derby://localhost:" + info.getPort() + "/" + info.getDbName());
        datastore.setUserId("wrong");
        datastore.setPassword("wrong");
        final TableNameDataset tableNameDataset = new TableNameDataset();
        tableNameDataset.setTableName("users");
        tableNameDataset.setConnection(datastore);
        final SuggestionValues values = myService.getTableColumns(tableNameDataset);
        assertNotNull(values);
        assertTrue(values.getItems().isEmpty());
    }

    @Test
    @DisplayName("Get Table Columns list - invalid table name")
    void getTableColumnsFromDatabaseWithInvalidTableName(final DerbyExtension.DerbyInfo info) {
        final BasicDatastore datastore = new BasicDatastore();
        datastore.setDbType("Derby");
        datastore.setJdbcUrl("jdbc:derby://localhost:" + info.getPort() + "/" + info.getDbName());
        datastore.setUserId("sa");
        datastore.setPassword("sa");
        final TableNameDataset tableNameDataset = new TableNameDataset();
        tableNameDataset.setTableName("tableNeverExist159");
        tableNameDataset.setConnection(datastore);
        final SuggestionValues values = myService.getTableColumns(tableNameDataset);
        assertNotNull(values);
        assertTrue(values.getItems().isEmpty());
    }

}
