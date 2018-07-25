// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo;

import javax.json.JsonBuilderFactory;
import javax.json.JsonReaderFactory;
import javax.json.JsonWriterFactory;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.marketo.dataset.MarketoInputDataSet;
import org.talend.components.marketo.dataset.MarketoOutputDataSet;
import org.talend.components.marketo.datastore.MarketoDataStore;
import org.talend.components.marketo.service.AuthorizationClient;
import org.talend.components.marketo.service.CompanyClient;
import org.talend.components.marketo.service.CustomObjectClient;
import org.talend.components.marketo.service.I18nMessage;
import org.talend.components.marketo.service.LeadClient;
import org.talend.components.marketo.service.ListClient;
import org.talend.components.marketo.service.MarketoService;
import org.talend.sdk.component.api.DecryptedServer;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit.http.junit4.JUnit4HttpApi;
import org.talend.sdk.component.junit.http.junit4.JUnit4HttpApiPerMethodConfigurator;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.WithMavenServers;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;

@HttpApi(useSsl = true)
@WithMavenServers
@WithComponents("org.talend.components.marketo")
public class MarketoBaseTest {

    public static final String MARKETO_CRM_INSTANCE = "https://764-CVE-068.mktorest.com";

    public static final String MARKETO_NOCRM_INSTANCE = "https://586-UXQ-391.mktorest.com";

    public static final String JSON_VALUE_XUNDEFINED_X = "XundefinedX";

    public static String MARKETO_ENDPOINT;

    public static String MARKETO_CLIENT_ID;

    public static String MARKETO_CLIENT_SECRET;

    @Injected
    protected ComponentsHandler handler;

    @ClassRule
    public static final SimpleComponentRule component = new SimpleComponentRule("org.talend.components.marketo");

    @ClassRule
    public static final JUnit4HttpApi API = new JUnit4HttpApi().activeSsl();

    @Rule
    public final JUnit4HttpApiPerMethodConfigurator configurator = new JUnit4HttpApiPerMethodConfigurator(API);

    @DecryptedServer(value = "marketo-nocrm", alwaysTryLookup = false)
    protected Server serverNoCrm;

    @Service
    protected JsonBuilderFactory jsonFactory;

    @Service
    protected JsonReaderFactory jsonReader;

    @Service
    protected JsonWriterFactory jsonWriter;

    @Service
    protected MarketoService marketoService;

    @Service
    protected I18nMessage i18n;

    @Service
    protected AuthorizationClient authorizationClient;

    @Service
    protected LeadClient leadClient;

    @Service
    protected ListClient listClient;

    @Service
    protected CustomObjectClient customObjectClient;

    @Service
    protected CompanyClient companyClient;

    protected MarketoDataStore dataStore; // default dataStore

    protected MarketoDataStore dataStoreWithCRM;

    protected MarketoInputDataSet inputDataSet = new MarketoInputDataSet();

    protected MarketoOutputDataSet outputDataSet = new MarketoOutputDataSet();

    protected transient static final Logger LOG = LoggerFactory.getLogger(MarketoBaseTest.class);

    static {
        try {
            MARKETO_ENDPOINT = MARKETO_NOCRM_INSTANCE;
            final Server serverWithNoCrm = new MavenDecrypter().find("marketo-nocrm");
            MARKETO_CLIENT_ID = serverWithNoCrm.getUsername();
            MARKETO_CLIENT_SECRET = serverWithNoCrm.getPassword();
            System.setProperty("talend.junit.http.capture", "true");
        } catch (Exception e) {
            System.setProperty("talend.junit.http.capture", "false");
        }
    }

    @BeforeClass
    void init() {
        marketoService = component.findService(MarketoService.class);
    }

    @BeforeEach
    protected void setUp() {
        String endpoint = MARKETO_ENDPOINT;
        String clientId = MARKETO_CLIENT_ID;
        String clientSecret = MARKETO_CLIENT_SECRET;
        if (MARKETO_CLIENT_ID == null || MARKETO_CLIENT_ID.isEmpty()) {
            clientId = serverNoCrm.getUsername();
            clientSecret = serverNoCrm.getPassword();
        }
        authorizationClient.base(MARKETO_ENDPOINT);
        dataStore = new MarketoDataStore();
        dataStore.setEndpoint(endpoint);
        dataStore.setClientId(clientId);
        dataStore.setClientSecret(clientSecret);
        inputDataSet.setDataStore(dataStore);
        outputDataSet.setDataStore(dataStore);
    }

}
