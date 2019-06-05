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

package org.talend.components.azure.eventhubs;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;

import org.talend.components.azure.eventhubs.datastore.AzureEventHubsDataStore;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;

import lombok.Data;

@Data
@WithComponents("org.talend.components.azure.eventhubs")
public class AzureEventHubsTestBase implements Serializable {

    public static String ENDPOINT = "sb://comp-test.servicebus.windows.net";

    public static String SASKEY_NAME;

    public static String SASKEY;

    protected static final String SHARED_EVENTHUB_NAME = "eh-test";

    protected static final String CONSUME_GROUP = "consumer-group-1";

    static {
        final MavenDecrypter decrypter = new MavenDecrypter();
        final Server serverSaskey = decrypter.find("azure-eventhubs-saskey");
        SASKEY_NAME = serverSaskey.getUsername();
        SASKEY = serverSaskey.getPassword();
    }

    @Injected
    private BaseComponentsHandler componentsHandler;

    public AzureEventHubsDataStore getDataStore() {
        AzureEventHubsDataStore dataStore = new AzureEventHubsDataStore();
        dataStore.setEndpoint(ENDPOINT);
        dataStore.setSasKeyName(SASKEY_NAME);
        dataStore.setSasKey(SASKEY);
        return dataStore;
    }

    protected String getUniqueID() {
        return Integer.toString(ThreadLocalRandom.current().nextInt(1, 100000));
    }

}