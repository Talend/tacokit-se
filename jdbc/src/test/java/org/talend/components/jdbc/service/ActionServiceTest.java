package org.talend.components.jdbc.service;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.DerbyExtension;
import org.talend.components.jdbc.WithDerby;
import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.sdk.component.api.service.Service;
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
        assertEquals(3, values.getItems().size());
        assertEquals(asList("MySQL", "DERBY", "ORACLE"), values.getItems().stream().map(Values.Item::getId).collect(toList()));
    }

    @Test
    @DisplayName("Datastore HealthCheck - Valid user")
    void validateBasicDatastore(final DerbyExtension.DerbyInfo info) {
        final BasicDatastore datastore = new BasicDatastore();
        datastore.setDbType("DERBY");
        datastore.setJdbcUrl("jdbc:derby://localhost:" + info.getPort() + "/" + info.getDbName());
        datastore.setUserId("sa");
        datastore.setPassword("sa");
        final HealthCheckStatus status = myService.validateBasicDatastore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

    @Test
    @DisplayName("Datastore HealthCheck - Bad credentials")
    void healthCheckWithBadCredentials(final DerbyExtension.DerbyInfo info) {
        final BasicDatastore datastore = new BasicDatastore();
        datastore.setDbType("DERBY");
        datastore.setJdbcUrl("jdbc:derby://localhost:" + info.getPort() + "/" + info.getDbName());
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = myService.validateBasicDatastore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    @DisplayName("Datastore HealthCheck - Bad Database Name")
    void healthCheckWithBadDataBaseName(final DerbyExtension.DerbyInfo info) {
        final BasicDatastore datastore = new BasicDatastore();
        datastore.setDbType("DERBY");
        datastore.setJdbcUrl("jdbc:derby://localhost:" + info.getPort() + "/DontExistUnlessyouCreatedDB");
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = myService.validateBasicDatastore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    @DisplayName("Datastore HealthCheck - Bad Jdbc sub Protocol")
    void healthCheckWithBadSubProtocol() {
        final BasicDatastore datastore = new BasicDatastore();
        datastore.setDbType("DERBY");
        datastore.setJdbcUrl("jdbc:darby/DB");
        datastore.setUserId("bad");
        datastore.setPassword("az");
        final HealthCheckStatus status = myService.validateBasicDatastore(datastore);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    @DisplayName("Datastore Get Table list - valid connection")
    void getTableFromDatabase(final DerbyExtension.DerbyInfo info) {
        final BasicDatastore datastore = new BasicDatastore();
        datastore.setDbType("DERBY");
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
        datastore.setDbType("DERBY");
        datastore.setJdbcUrl("jdbc:derby://localhost:" + info.getPort() + "/" + info.getDbName());
        datastore.setUserId("wrong");
        datastore.setPassword("wrong");
        final SuggestionValues values = myService.getTableFromDatabase(datastore);
        assertNotNull(values);
        assertEquals(0, values.getItems().size());
    }

}
