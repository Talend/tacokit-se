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
 *
 */

package org.talend.components.salesforce.service;

import java.net.URL;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.salesforce")
public class SalesforceServiceTest {

    @Service
    private SalesforceService service;

    @Service
    private LocalConfiguration localConfiguration;

    @Test
    @DisplayName("test get endpoint from config")
    void testGetEndpoint() {
        // 1. if endpoint is not be set, then get endpoint from local configuration file
        // maybe this would be not set in configuration file, use default endpoint would be enough
        final String defualtConfigEndpoint = "https://login.salesforce.com/services/Soap/u/44.0";

        final BasicDataStore ds_1 = new BasicDataStore();
        ds_1.setEndpoint("");
        ds_1.setUserId("userId");
        ds_1.setPassword("password");

        assertEquals(defualtConfigEndpoint, service.getEndpoint(ds_1, localConfiguration));

        // 2. if fill retired endpoint, then replace with active one.
        final String retiredEndpoint = "https://www.salesforce.com/services/Soap/u/43.0";
        final String activeEndpoint = "https://login.salesforce.com/services/Soap/u/43.0";
        final BasicDataStore ds_2 = new BasicDataStore();
        ds_2.setEndpoint(retiredEndpoint);
        ds_2.setUserId("userId");
        ds_2.setPassword("password");

        assertEquals(activeEndpoint, service.getEndpoint(ds_2, localConfiguration));

        // 3. if fill correct endpoint, then return it
        final String endpoint = "https://login.salesforce.com/services/Soap/u/43.0";
        final BasicDataStore ds_3 = new BasicDataStore();
        ds_3.setEndpoint(endpoint);
        ds_3.setUserId("userId");
        ds_3.setPassword("password");

        assertEquals(activeEndpoint, service.getEndpoint(ds_3, localConfiguration));

    }

}
