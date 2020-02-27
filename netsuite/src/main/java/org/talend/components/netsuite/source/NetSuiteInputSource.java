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
package org.talend.components.netsuite.source;

import java.io.Serializable;

import javax.annotation.PostConstruct;

import org.talend.components.netsuite.dataset.NetSuiteInputProperties;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.search.SearchResultSet;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.runtime.model.TypeDesc;
import org.talend.components.netsuite.service.Messages;
import org.talend.components.netsuite.service.NetSuiteService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

@Documentation("TODO fill the documentation for this source")
public class NetSuiteInputSource implements Serializable {

    private final NetSuiteInputProperties configuration;

    private final RecordBuilderFactory recordBuilderFactory;

    private final Messages i18n;

    private SearchResultSet<?> rs;

    private NsObjectInputTransducer transducer;

    private NetSuiteService netSuiteService;

    private NetSuiteClientService<?> clientService;

    public NetSuiteInputSource(@Option("configuration") final NetSuiteInputProperties configuration,
            final RecordBuilderFactory recordBuilderFactory, final Messages i18n, SearchResultSet<?> rs,
            NetSuiteService netSuiteService, NetSuiteClientService<?> clientService) {
        this.configuration = configuration;
        this.recordBuilderFactory = recordBuilderFactory;
        this.i18n = i18n;
        this.rs = rs;
        this.netSuiteService = netSuiteService;
        this.clientService = clientService;
    }

    @PostConstruct
    public void init() {
        Schema runtimeSchema = netSuiteService.getSchema(configuration.getDataSet(), null, clientService);
        RecordTypeInfo recordTypeInfo = rs.getRecordTypeDesc();
        TypeDesc typeDesc = clientService.getMetaDataSource().getTypeInfo(recordTypeInfo.getName(),
                configuration.getDataSet().isEnableCustomization());
        transducer = new NsObjectInputTransducer(clientService.getBasicMetaData(), i18n, recordBuilderFactory, runtimeSchema,
                typeDesc, configuration.getDataSet().getDataStore().getApiVersion().getVersion());
    }

    @Producer
    public Record next() {
        return rs.hasNext() ? transducer.read(rs::next) : null;
    }
}