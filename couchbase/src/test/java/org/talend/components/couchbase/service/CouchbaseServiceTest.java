/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.couchbase.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.couchbase.CouchbaseUtilTest;
import org.talend.components.couchbase.datastore.CouchbaseDataStore;
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
        CouchbaseDataStore couchbaseDataStore = new CouchbaseDataStore();
        couchbaseDataStore.setBootstrapNodes(COUCHBASE_CONTAINER.getContainerIpAddress());
        couchbaseDataStore.setUsername(CLUSTER_USERNAME);
        couchbaseDataStore.setPassword(CLUSTER_PASSWORD);
        couchbaseDataStore.setConnectTimeout(DEFAULT_TIMEOUT_IN_SEC);

        assertEquals(HealthCheckStatus.Status.OK, couchbaseService.healthCheck(couchbaseDataStore).getStatus());
    }

    @Test
    @DisplayName("Test unsuccessful connection")
    void couchbaseNotSuccessfulConnectionTest() {
        String wrongPassword = "wrongpass";

        CouchbaseDataStore couchbaseDataStore = new CouchbaseDataStore();
        couchbaseDataStore.setBootstrapNodes(COUCHBASE_CONTAINER.getContainerIpAddress());
        couchbaseDataStore.setUsername(CLUSTER_USERNAME);
        couchbaseDataStore.setPassword(wrongPassword);
        couchbaseDataStore.setConnectTimeout(DEFAULT_TIMEOUT_IN_SEC);

        assertEquals(HealthCheckStatus.Status.KO, couchbaseService.healthCheck(couchbaseDataStore).getStatus());
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

    @Test
    @DisplayName("Test if cluster reachable with single localhost url")
    void isClusterUrlIsReachableTest() {
        CouchbaseDataStore reachableDataStore = new CouchbaseDataStore();
        reachableDataStore.setConnectTimeout(20);
        reachableDataStore.setBootstrapNodes("localhost");

        CouchbaseDataStore notReachableDataStore = new CouchbaseDataStore();
        notReachableDataStore.setConnectTimeout(20);
        notReachableDataStore.setBootstrapNodes("fakeUrl");

        boolean isClusterReachableWithLocalhost = couchbaseService.isClusterReachable(reachableDataStore);
        boolean isClusterReachableWithNotExistLocalhost = couchbaseService.isClusterReachable(notReachableDataStore);

        assertTrue(isClusterReachableWithLocalhost, "cluster is reachable with single URL");
        assertFalse(isClusterReachableWithNotExistLocalhost, "cluster is not reachable with single URL");
    }

    @Test
    @DisplayName("Test if cluster reachable with few localhost urls")
    void isClusterUrlsIsReachableTest() {
        CouchbaseDataStore reachableDataStore = new CouchbaseDataStore();
        reachableDataStore.setConnectTimeout(20);
        reachableDataStore.setBootstrapNodes("localhost, 127.0.0.1, localhost");

        CouchbaseDataStore notReachableDataStore = new CouchbaseDataStore();
        notReachableDataStore.setConnectTimeout(20);
        notReachableDataStore.setBootstrapNodes("wrong, 127.0.0.1, localhost");

        boolean isClusterReachableWithLocalhost = couchbaseService.isClusterReachable(reachableDataStore);
        boolean isClusterReachableWithNotExistLocalhost = couchbaseService.isClusterReachable(notReachableDataStore);

        assertTrue(isClusterReachableWithLocalhost, "cluster is reachable with few URLs");
        assertFalse(isClusterReachableWithNotExistLocalhost, "cluster is not reachable with few URLs");
    }
}
