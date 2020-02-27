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
package org.talend.components.netsuite.service;

import org.talend.components.netsuite.datastore.NetSuiteDataStore;
import org.talend.components.netsuite.runtime.NetSuiteEndpoint;
import org.talend.components.netsuite.runtime.client.NetSuiteClientFactory;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.sdk.component.api.service.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class NetSuiteClientConnectionService {

    private NetSuiteClientService<?> connect(NetSuiteDataStore dataStore, Messages i18n) {
        NetSuiteClientFactory<?> netSuiteClientFactory;
        switch (dataStore.getApiVersion()) {
        case V2019_2:
            netSuiteClientFactory = org.talend.components.netsuite.runtime.v2019_2.client.NetSuiteClientFactoryImpl.getFactory();
            break;
        default:
            throw new RuntimeException("Unknown API version: " + dataStore.getApiVersion());
        }
        NetSuiteEndpoint endpoint = new NetSuiteEndpoint(netSuiteClientFactory, i18n, dataStore);
        return endpoint.getClientService();
    }

    public NetSuiteClientService<?> getClientService(NetSuiteDataStore dataStore, Messages i18n) {
        return connect(dataStore, i18n);
    }
}