package org.talend.components.salesforce.service;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.salesforce.SfHeaderFilter;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.DecryptedServer;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.http.api.HttpApiHandler;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit.http.junit5.HttpApiInject;
import org.talend.sdk.component.junit.http.junit5.HttpApiName;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.WithMavenServers;
import org.talend.sdk.component.maven.Server;

@WithComponents("org.talend.components.salesforce")
@HttpApi(useSsl = true, headerFilter = SfHeaderFilter.class)
@WithMavenServers //
class UiActionServiceTest {

    static {
        // System.setProperty("talend.junit.http.capture", "true");
    }

    @Service
    private UiActionService service;

    @Service
    private Messages i18n;

    @Service
    private LocalConfiguration configuration;

    @Injected
    private BaseComponentsHandler componentsHandler;

    @HttpApiInject
    private HttpApiHandler<?> httpApiHandler;

    @DecryptedServer(value = "salesforce-password", alwaysTryLookup = false)
    private Server serverWithPassword;

    @DecryptedServer(value = "salesforce-securitykey", alwaysTryLookup = false)
    private Server serverWithSecuritykey;

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Validate connection")
    void validateBasicConnectionOK() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final HealthCheckStatus status = service.validateBasicConnection(datasore, i18n, configuration);
        assertEquals(i18n.healthCheckOk(), status.getComment());
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());

    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Validate connection with bad credentials")
    void validateBasicConnectionFailed() {
        final HealthCheckStatus status = service.validateBasicConnection(new BasicDataStore(), i18n, configuration);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
        assertFalse(status.getComment().isEmpty());
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Load modules")
    void loadModules() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final SuggestionValues modules = service.loadSalesforceModules(datasore);
        assertNotNull(modules);
        assertTrue(modules.isCacheable());
        assertEquals(376, modules.getItems().size());
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Load modules with bad basic credentials")
    void loadModulesWithBadCredentials() {
        assertThrows(IllegalStateException.class, () -> {
            final BasicDataStore datasore = new BasicDataStore();
            datasore.setUserId("basUserName");
            datasore.setPassword("NoPass");
            service.loadSalesforceModules(datasore);
        });
    }

}
