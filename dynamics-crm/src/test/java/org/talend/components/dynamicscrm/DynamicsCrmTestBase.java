/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.dynamicscrm;

import java.util.List;
import java.util.UUID;

import javax.naming.AuthenticationException;
import javax.naming.ServiceUnavailableException;

import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetRequest;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.domain.ClientProperty;
import org.apache.olingo.client.api.uri.FilterFactory;
import org.apache.olingo.client.core.uri.FilterFactoryImpl;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.talend.components.dynamicscrm.dataset.DynamicsCrmDataset;
import org.talend.components.dynamicscrm.datastore.AppType;
import org.talend.components.dynamicscrm.datastore.DynamicsCrmConnection;
import org.talend.components.dynamicscrm.service.I18n;
import org.talend.ms.crm.odata.ClientConfiguration;
import org.talend.ms.crm.odata.ClientConfiguration.WebAppPermission;
import org.talend.ms.crm.odata.ClientConfigurationFactory;
import org.talend.ms.crm.odata.DynamicsCRMClient;
import org.talend.ms.crm.odata.QueryOptionConfig;
import org.talend.sdk.component.api.DecryptedServer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.WithMavenServers;
import org.talend.sdk.component.maven.Server;

@WithComponents("org.talend.components.dynamicscrm")
@WithMavenServers
public abstract class DynamicsCrmTestBase {

    protected DynamicsCRMClient client;

    @Injected
    protected BaseComponentsHandler components;

    public final String authEndpoint = "https://login.windows.net/common/oauth2/authorize";

    public final String rootUrl = "https://talend.api.crm.dynamics.com/api/data/v9.0/";

    public final String entitySet = "contacts";

    public final String company = "dchmyga_test" + UUID.randomUUID().toString().replace("-", "");

    @Service
    protected RecordBuilderFactory builderFactory;

    @Service
    protected I18n i18n;

    @DecryptedServer("dynamicscrm.userpwd")
    protected Server userPass;

    @DecryptedServer("dynamicscrm.clientidsecret")
    protected Server clientIdSecret;

    public String getUsername() {
        return userPass.getUsername();
    }

    public String getPassword() {
        return userPass.getPassword();
    }

    public String getClientId() {
        return clientIdSecret.getUsername();
    }

    public String getClientSecret() {
        return clientIdSecret.getPassword();
    }

    public void init() throws AuthenticationException {
        ClientConfiguration clientConfig = ClientConfigurationFactory.buildOAuthWebClientConfiguration(getClientId(),
                getClientSecret(), getUsername(), getPassword(), authEndpoint, WebAppPermission.DELEGATED);
        clientConfig.setTimeout(60);
        clientConfig.setMaxRetry(5, 1000);
        clientConfig.setReuseHttpClient(false);

        client = new DynamicsCRMClient(clientConfig, rootUrl, entitySet);
    }

    public void tearDown(DynamicsCRMClient client) throws ServiceUnavailableException {
        for (ClientEntity entity : getData(client)) {
            ClientProperty property = entity.getProperty("contactid");
            if (property != null && !property.hasNullValue()) {
                client.deleteEntity(property.getValue().toString());
            }
        }
    }

    protected List<ClientEntity> getData(DynamicsCRMClient client) {
        QueryOptionConfig queryOptionConfig = new QueryOptionConfig();
        FilterFactory filterFactory = new FilterFactoryImpl();
        queryOptionConfig.setFilter(filterFactory.eq("company", company).build());
        queryOptionConfig.setReturnEntityProperties(new String[] { "contactid", "annualincome", "assistantname", "business2",
                "callback", "childrensnames", "company", "creditonhold", "_transactioncurrencyid_value", "birthdate" });
        ODataEntitySetRequest<ClientEntitySet> request = client.createEntityRetrieveRequest(queryOptionConfig);
        ODataRetrieveResponse<ClientEntitySet> response = request.execute();
        return response.getBody().getEntities();
    }

    protected DynamicsCrmDataset createDataset() {
        DynamicsCrmConnection connection = new DynamicsCrmConnection();
        connection.setMaxRetries(5);
        connection.setAppType(AppType.WEB);
        connection.setAuthorizationEndpoint(authEndpoint);
        connection.setClientId(getClientId());
        connection.setClientSecret(getClientSecret());
        connection.setUsername(getUsername());
        connection.setPassword(getPassword());
        connection.setServiceRootUrl(rootUrl);
        connection.setTimeout(60);
        DynamicsCrmDataset dataset = new DynamicsCrmDataset();
        dataset.setDatastore(connection);
        dataset.setEntitySet(entitySet);
        return dataset;
    }

    protected ClientEntity createTestEntity(DynamicsCRMClient client) {
        ClientEntity entity = client.newEntity();
        client.addEntityProperty(entity, "annualincome", EdmPrimitiveTypeKind.Decimal, 2.0);
        client.addEntityProperty(entity, "assistantname", EdmPrimitiveTypeKind.String, "assistant");
        client.addEntityProperty(entity, "business2", EdmPrimitiveTypeKind.String, "business2");
        client.addEntityProperty(entity, "callback", EdmPrimitiveTypeKind.String, "callback");
        client.addEntityProperty(entity, "childrensnames", EdmPrimitiveTypeKind.String, "childrensnames");
        client.addEntityProperty(entity, "company", EdmPrimitiveTypeKind.String, company);
        client.addEntityProperty(entity, "creditonhold", EdmPrimitiveTypeKind.Boolean, false);
        return entity;
    }

    protected Record createTestRecord() {
        Schema schema = builderFactory.newSchemaBuilder(Type.RECORD)
                .withEntry(builderFactory.newEntryBuilder().withName("annualincome").withType(Type.FLOAT)
                        .withElementSchema(builderFactory.newSchemaBuilder(Type.FLOAT).build()).build())
                .withEntry(builderFactory.newEntryBuilder().withName("assistantname").withType(Type.STRING)
                        .withElementSchema(builderFactory.newSchemaBuilder(Type.STRING).build()).build())
                .withEntry(builderFactory.newEntryBuilder().withName("business2").withType(Type.STRING)
                        .withElementSchema(builderFactory.newSchemaBuilder(Type.STRING).build()).build())
                .withEntry(builderFactory.newEntryBuilder().withName("callback").withType(Type.STRING)
                        .withElementSchema(builderFactory.newSchemaBuilder(Type.STRING).build()).build())
                .withEntry(builderFactory.newEntryBuilder().withName("childrensnames").withType(Type.STRING)
                        .withElementSchema(builderFactory.newSchemaBuilder(Type.STRING).build()).build())
                .withEntry(builderFactory.newEntryBuilder().withName("company").withType(Type.STRING)
                        .withElementSchema(builderFactory.newSchemaBuilder(Type.STRING).build()).build())
                .withEntry(builderFactory.newEntryBuilder().withName("creditonhold").withType(Type.BOOLEAN)
                        .withElementSchema(builderFactory.newSchemaBuilder(Type.BOOLEAN).build()).build())
                .withEntry(builderFactory.newEntryBuilder().withName("_transactioncurrencyid_value").withType(Type.STRING)
                        .withElementSchema(builderFactory.newSchemaBuilder(Type.STRING).build()).build())
                .withEntry(builderFactory.newEntryBuilder().withName("birthdate").withType(Type.INT)
                        .withElementSchema(builderFactory.newSchemaBuilder(Type.INT).build()).build())
                .build();
        Record record = builderFactory.newRecordBuilder(schema).withFloat("annualincome", 2.0f)
                .withString("assistantname", "assistant").withString("business2", "business2").withString("callback", "callback")
                .withString("childrensnames", "childrensnames").withString("company", company).withBoolean("creditonhold", false)
                .withInt("birthdate", 6720).withString("_transactioncurrencyid_value", "dca1714c-6d1a-e311-a5fb-b4b52f67b688")
                .build();
        return record;
    }

}
