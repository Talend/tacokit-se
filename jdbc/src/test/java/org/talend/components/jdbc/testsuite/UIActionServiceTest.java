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
import org.talend.components.jdbc.containers.JdbcTestContainer;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.service.UIActionService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.Locale;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("UIActionService")
@WithComponents("org.talend.components.jdbc")
@ExtendWith({ JdbcInvocationContextProvider.class })
@Environment(ContextualEnvironment.class)
class UIActionServiceTest extends BaseJdbcTest {

    @Service
    private UIActionService myService;

    @TestTemplate
    @DisplayName("HealthCheck - Valid user")
    void validateBasicDatastore(final JdbcTestContainer container) {
        final HealthCheckStatus status = myService.validateBasicDataStore(newConnection(container));
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
        final HealthCheckStatus status = myService.validateBasicDataStore(datastore);
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
        final HealthCheckStatus status = myService.validateBasicDataStore(datastore);
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
        final HealthCheckStatus status = myService.validateBasicDataStore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
        assertFalse(status.getComment().isEmpty());
    }

    @TestTemplate
    @DisplayName("Get Table list - valid connection")
    void getTableFromDatabase(final JdbcTestContainer container) {
        final SuggestionValues values = myService.getTableFromDatabase(newConnection(container));
        assertNotNull(values);
        assertTrue(values.getItems().stream().anyMatch(e -> e.getLabel().equalsIgnoreCase(container.getTestTableName())));
    }

    @TestTemplate
    @DisplayName("Get Table list - invalid connection")
    void getTableFromDatabaseWithInvalidConnection(final JdbcTestContainer container) {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(container.getDatabaseType());
        datastore.setJdbcUrl(container.getJdbcUrl());
        datastore.setUserId("wrong");
        datastore.setPassword("wrong");
        final SuggestionValues values = myService.getTableFromDatabase(datastore);
        assertNotNull(values);
        assertEquals(0, values.getItems().size());
    }

    @TestTemplate
    @DisplayName("Get Table columns list - valid connection")
    void getTableColumnFromDatabase(final JdbcTestContainer container) {
        final TableNameDataset tableNameDataset = newTableNameDataset(container.getTestTableName(), container);
        final SuggestionValues values = myService.getTableColumns(tableNameDataset);
        assertNotNull(values);
        assertEquals(8, values.getItems().size());
        assertEquals(
                Stream.of("T_DOUBLE", "T_LONG", "T_BYTES", "T_FLOAT", "T_BOOLEAN", "T_STRING", "T_DATE", "ID").collect(toSet()),
                values.getItems().stream().map(SuggestionValues.Item::getLabel).map(l -> l.toUpperCase(Locale.ROOT))
                        .collect(toSet()));
    }

    @TestTemplate
    @DisplayName("Get Table Columns list - invalid connection")
    void getTableColumnsFromDatabaseWithInvalidConnection(final JdbcTestContainer container) {
        final JdbcConnection datastore = new JdbcConnection();
        datastore.setDbType(container.getDatabaseType());
        datastore.setJdbcUrl(container.getJdbcUrl());
        datastore.setUserId("wrong");
        datastore.setPassword("wrong");
        final TableNameDataset tableNameDataset = new TableNameDataset();
        tableNameDataset.setTableName(container.getTestTableName());
        tableNameDataset.setConnection(datastore);
        final SuggestionValues values = myService.getTableColumns(tableNameDataset);
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
        final SuggestionValues values = myService.getTableColumns(tableNameDataset);
        assertNotNull(values);
        assertTrue(values.getItems().isEmpty());
    }

}
