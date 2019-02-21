package org.talend.components.salesforce.input;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.List;

import javax.json.JsonObject;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.salesforce.SfHeaderFilter;
import org.talend.components.salesforce.dataset.QueryDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.DecryptedServer;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.http.api.HttpApiHandler;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit.http.junit5.HttpApiInject;
import org.talend.sdk.component.junit.http.junit5.HttpApiName;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.WithMavenServers;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;
import org.talend.sdk.component.runtime.manager.chain.Job;

@DisplayName("Suite of test for the Salesforce Input component")
@WithComponents("org.talend.components.salesforce")
@HttpApi(useSsl = true, headerFilter = SfHeaderFilter.class)
@WithMavenServers //
class SalesforceInputEmitterTest {

    static {
        // System.setProperty("talend.junit.http.capture", "true");
    }

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
    @DisplayName("Bad credentials case")
    void inputWithBadCredential() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId("badUser");
        datasore.setPassword("badPasswd");
        datasore.setSecurityKey("badSecurityKey");
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setModuleName("account");
        queryDataSet.setSourceType(QueryDataSet.SourceType.MODULE_SELECTION);
        queryDataSet.setDataStore(datasore);
        final String config = configurationByExample().forInstance(queryDataSet).configured().toQueryString();
        final IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://Input?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Module selection case [valid]")
    void inputWithModuleNameValid() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setModuleName("account");
        queryDataSet.setSourceType(QueryDataSet.SourceType.MODULE_SELECTION);
        queryDataSet.setSelectColumnIds(singletonList("Name"));
        queryDataSet.setDataStore(datasore);
        queryDataSet.setCondition("Name Like '%Oil%'");
        final String config = configurationByExample().forInstance(queryDataSet).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://Input?" + config).component("collector", "test://collector")
                .connections().from("salesforce-input").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertEquals(4, res.size());
        assertTrue(res.iterator().next().getString("Name").contains("Oil"));
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Module selection case [invalid]")
    void inputWithModuleNameInvalid() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setModuleName("invalid0");
        queryDataSet.setSourceType(QueryDataSet.SourceType.MODULE_SELECTION);
        queryDataSet.setDataStore(datasore);
        final String config = configurationByExample().forInstance(queryDataSet).configured().toQueryString();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://Input?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Module selection with fields case [invalid]")
    void inputWithModuleNameValidAndInvalidField() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setModuleName("account");
        queryDataSet.setSelectColumnIds(singletonList("InvalidField10x"));
        queryDataSet.setSourceType(QueryDataSet.SourceType.MODULE_SELECTION);
        queryDataSet.setDataStore(datasore);
        final String config = configurationByExample().forInstance(queryDataSet).configured().toQueryString();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://Input?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Soql query selection [valid]")
    void inputWithSoqlQueryValid() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setSourceType(QueryDataSet.SourceType.SOQL_QUERY);
        queryDataSet.setQuery("select Name from account where Name Like  '%Oil%'");
        queryDataSet.setDataStore(datasore);
        final String config = configurationByExample().forInstance(queryDataSet).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://Input?" + config).component("collector", "test://collector")
                .connections().from("salesforce-input").to("collector").build().run();

        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        assertEquals(4, res.size());
        assertTrue(res.iterator().next().getString("Name").contains("Oil"));
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Soql query selection [invalid]")
    void inputWithSoqlQueryInvalid() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setSourceType(QueryDataSet.SourceType.SOQL_QUERY);
        queryDataSet.setQuery("from account");
        queryDataSet.setDataStore(datasore);
        final String config = configurationByExample().forInstance(queryDataSet).configured().toQueryString();
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> Job.components().component("salesforce-input", "Salesforce://Input?" + config)
                        .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build()
                        .run());
    }

    @Test
    @HttpApiName("${class}_${method}")
    @DisplayName("Soql query selection [empty result]")
    void inputWithSoqlQueryEmptyResult() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setUserId(serverWithPassword.getUsername());
        datasore.setPassword(serverWithPassword.getPassword());
        datasore.setSecurityKey(serverWithSecuritykey.getPassword());
        final QueryDataSet queryDataSet = new QueryDataSet();
        queryDataSet.setSourceType(QueryDataSet.SourceType.SOQL_QUERY);
        queryDataSet.setQuery("select  name from account where name = 'this name will never exist $'");
        queryDataSet.setDataStore(datasore);

        final String config = configurationByExample().forInstance(queryDataSet).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://Input?" + config).component("collector", "test://collector")
                .connections().from("salesforce-input").to("collector").build().run();

        final List<JsonObject> records = componentsHandler.getCollectedData(JsonObject.class);
        assertEquals(0, records.size());
    }
}
