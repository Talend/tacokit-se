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
package org.talend.components.netsuite.runtime.v2019_2.client;

import org.talend.components.netsuite.runtime.client.NetSuiteClientFactory;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NetSuiteException;

import com.netsuite.webservices.v2019_2.platform.NetSuitePortType;

public class NetSuiteClientFactoryImpl implements NetSuiteClientFactory<NetSuitePortType> {

    public static final NetSuiteClientFactoryImpl INSTANCE = new NetSuiteClientFactoryImpl();

    public static NetSuiteClientFactory<NetSuitePortType> getFactory() {
        return INSTANCE;
    }

    @Override
    public NetSuiteClientService<NetSuitePortType> createClient() throws NetSuiteException {
        return new NetSuiteClientServiceImpl();
    }
}
