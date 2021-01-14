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
package org.talend.components.couchbase.testutils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.couchbase.CouchbaseUtilTest;
import org.talend.components.couchbase.datastore.CouchbaseDataStore;
import org.talend.components.couchbase.service.CouchbaseService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.*;

@WithComponents("org.talend.components.couchbase")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Testing of CouchbaseService class")
public class CouchbaseServiceTest extends CouchbaseUtilTest {

    @Service
    private CouchbaseService couchbaseService;

    @Test
    @DisplayName("Test successful connection")
    void couchbaseSuccessfulConnectionTest() {
        assertEquals(HealthCheckStatus.Status.OK, couchbaseService.healthCheck(couchbaseDataStore).getStatus());
    }

    @Test
    @DisplayName("Test unsuccessful connection")
    void couchbaseNotSuccessfulConnectionTest() {
        String wrongPassword = "wrongpass";

        CouchbaseDataStore couchbaseDataStoreWrongPass = new CouchbaseDataStore();
        couchbaseDataStoreWrongPass.setBootstrapNodes(couchbaseDataStore.getBootstrapNodes());
        couchbaseDataStoreWrongPass.setUsername(couchbaseDataStore.getUsername());
        couchbaseDataStoreWrongPass.setPassword(wrongPassword);
        couchbaseDataStoreWrongPass.setConnectTimeout(couchbaseDataStore.getConnectTimeout());

        assertEquals(HealthCheckStatus.Status.KO, couchbaseService.healthCheck(couchbaseDataStoreWrongPass).getStatus());
    }

    @Test
    @DisplayName("Two bootstrap nodes without spaces")
    void resolveAddressesTest() {
        String inputUrl = "192.168.0.1,192.168.0.2";
        String[] resultArrayWithUrls = couchbaseService.resolveAddresses(inputUrl);
        assertEquals("192.168.0.1", resultArrayWithUrls[0], "first expected node");
        assertEquals("192.168.0.2", resultArrayWithUrls[1], "second expected node");
    }

    @Test
    @DisplayName("Two bootstrap nodes with extra spaces")
    void resolveAddressesWithSpacesTest() {
        String inputUrl = " 192.168.0.1, 192.168.0.2";
        String[] resultArrayWithUrls = couchbaseService.resolveAddresses(inputUrl);
        assertEquals("192.168.0.1", resultArrayWithUrls[0], "first expected node");
        assertEquals("192.168.0.2", resultArrayWithUrls[1], "second expected node");
    }
}
