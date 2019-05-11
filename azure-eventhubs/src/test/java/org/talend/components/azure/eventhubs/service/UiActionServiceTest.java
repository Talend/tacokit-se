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
 *
 */

package org.talend.components.azure.eventhubs.service;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.eventhubs.AzureEventHubsTestBase;
import org.talend.components.azure.eventhubs.dataset.AzureEventHubsDataSet;
import org.talend.components.azure.eventhubs.datastore.AzureEventHubsDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

@Disabled("Azure eventhubs credentials is not ready on ci")
@WithComponents("org.talend.components.azure.eventhubs")
class UiActionServiceTest extends AzureEventHubsTestBase {

    private static final String INVALID_ENDPOINT = "sb://not-exit-ns.servicebus.windows.net";

    // Bad config
    private static final String BAD_SHARED_EVENTHUB_NAME = "not-exist-event-hub";

    private static final String BAD_SASKEY_NAME = "not-exist-sas-key-name";

    private static final String BAD_SASKEY = "zn+KhzbKgnJ7GZJ+jwuFKtHitV7bmHDBjq9YF5g0348=";

    @Service
    private UiActionService service;

    @Service
    private Messages i18n;

    @Test
    @DisplayName("Test endpoint OK [Valid]")
    public void validateConnectionOK() {
        final HealthCheckStatus status = service.checkEndpoint(getDataStore(), i18n);
        assertEquals(i18n.healthCheckOk(), status.getComment());
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());

    }

    @Test
    @DisplayName("Test endpoint Failed [Invalid]")
    public void validateConnectionFailed() {
        final AzureEventHubsDataStore dataStore = new AzureEventHubsDataStore();
        dataStore.setEndpoint(INVALID_ENDPOINT);
        final HealthCheckStatus status = service.checkEndpoint(dataStore, i18n);
        assertNotNull(status);
        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
        assertFalse(status.getComment().isEmpty());
    }

    @Test
    @DisplayName("Test eventhub name OK [Valid]")
    public void checkExistEventHub() {
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setConnection(getDataStore());
        dataSet.setEventHubName(SHARED_EVENTHUB_NAME);
        final ValidationResult status = service.checkEventHub(dataSet.getConnection(), dataSet.getEventHubName(), i18n);
        assertEquals(ValidationResult.Status.OK, status.getStatus());
    }

    @Test
    @DisplayName("Test eventhub name Failed [Invalid]")
    public void checkNotExistEventHub() {
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setConnection(getDataStore());
        dataSet.setEventHubName(BAD_SHARED_EVENTHUB_NAME);
        final ValidationResult status = service.checkEventHub(dataSet.getConnection(), dataSet.getEventHubName(), i18n);
        assertNotNull(status);
        assertEquals(ValidationResult.Status.KO, status.getStatus());
        assertFalse(status.getComment().isEmpty());
    }

    @Test
    @DisplayName("Test bad SAS Key Name [Invalid]")
    public void checkBadSASKeyName() {
        final AzureEventHubsDataStore dataStore = new AzureEventHubsDataStore();
        dataStore.setEndpoint(ENDPOINT);
        dataStore.setSasKeyName(BAD_SASKEY_NAME);
        dataStore.setSasKey(SASKEY);
        final ValidationResult status = service.checkEventHub(dataStore, SHARED_EVENTHUB_NAME, i18n);
        assertNotNull(status);
        assertEquals(ValidationResult.Status.KO, status.getStatus());
        assertFalse(status.getComment().isEmpty());
    }

    @Test
    @DisplayName("Test bad SAS Key [Invalid]")
    public void checkBadSASKey() {
        final AzureEventHubsDataStore dataStore = new AzureEventHubsDataStore();
        dataStore.setEndpoint(ENDPOINT);
        dataStore.setSasKeyName(SASKEY_NAME);
        dataStore.setSasKey(BAD_SASKEY);
        final ValidationResult status = service.checkEventHub(dataStore, SHARED_EVENTHUB_NAME, i18n);
        assertNotNull(status);
        assertEquals(ValidationResult.Status.KO, status.getStatus());
        assertFalse(status.getComment().isEmpty());
    }

    @Test
    @DisplayName("Test check list of partion ids [Valid]")
    public void checkListPartionIds() {
        final AzureEventHubsDataStore dataStore = new AzureEventHubsDataStore();
        dataStore.setEndpoint(ENDPOINT);
        dataStore.setSasKeyName(SASKEY_NAME);
        dataStore.setSasKey(SASKEY);

        AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setConnection(dataStore);
        dataSet.setEventHubName(SHARED_EVENTHUB_NAME);
        final SuggestionValues idValues = service.listPartitionIds(dataSet);
        assertNotNull(idValues);
        assertEquals(4, idValues.getItems().size());
    }

}