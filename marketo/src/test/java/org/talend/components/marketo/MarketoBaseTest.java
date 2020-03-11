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
package org.talend.components.marketo;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeEach;
import org.talend.components.marketo.dataset.MarketoDataSet;
import org.talend.components.marketo.dataset.MarketoInputConfiguration;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration;
import org.talend.components.marketo.datastore.MarketoDataStore;
import org.talend.components.marketo.service.MarketoService;
import org.talend.sdk.component.api.DecryptedServer;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.WithMavenServers;
import org.talend.sdk.component.maven.Server;

import lombok.Data;

@Data
@WithMavenServers
@WithComponents("org.talend.components.marketo")
public class MarketoBaseTest {

    @ClassRule
    public static final SimpleComponentRule component = new SimpleComponentRule("org.talend.components.marketo");

    @DecryptedServer(value = "marketo-nocrm")
    protected Server serverNoCrm;

    @DecryptedServer(value = "marketo-nocrm-instance")
    protected Server serverNoCrmInstance;

    @Injected
    protected BaseComponentsHandler handler;

    @Service
    protected RecordBuilderFactory recordBuilderFactory;

    @Service
    protected MarketoService service;

    protected MarketoDataStore dataStore;

    protected MarketoDataSet dataSet;

    protected MarketoInputConfiguration inputConfiguration = new MarketoInputConfiguration();

    protected MarketoOutputConfiguration outputConfiguration = new MarketoOutputConfiguration();

    @BeforeClass
    void init() {
        service = component.findService(MarketoService.class);
    }

    @BeforeEach
    protected void setUp() {
        String endpoint = serverNoCrmInstance.getUsername();
        String clientId = serverNoCrm.getUsername();
        String clientSecret = serverNoCrm.getPassword();
        dataStore = new MarketoDataStore();
        dataStore.setEndpoint(endpoint);
        dataStore.setClientId(clientId);
        dataStore.setClientSecret(clientSecret);
        dataSet = new MarketoDataSet();
        dataSet.setDataStore(dataStore);
        inputConfiguration.setDataSet(dataSet);
        outputConfiguration.setDataSet(dataSet);
    }
}
