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
package org.talend.components.netsuite.source;

import org.talend.components.netsuite.dataset.NetSuiteInputProperties;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.search.SearchResultSet;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.service.Messages;
import org.talend.components.netsuite.service.NetSuiteService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import java.io.Serializable;

@Documentation("TODO fill the documentation for this source")
public class NetSuiteInputSource implements Serializable {

    private final NetSuiteInputProperties configuration;

    private final NetSuiteService service;

    private final RecordBuilderFactory recordBuilderFactory;

    private final Messages i18n;

    private SearchResultSet<?> rs;

    private NsObjectInputTransducer transducer;

    public NetSuiteInputSource(@Option("configuration") final NetSuiteInputProperties configuration,
            final NetSuiteService service, final RecordBuilderFactory recordBuilderFactory, final Messages i18n,
            SearchResultSet<?> rs) {
        this.configuration = configuration;
        this.service = service;
        this.recordBuilderFactory = recordBuilderFactory;
        this.i18n = i18n;
        this.rs = rs;
    }

    @PostConstruct
    public void init() {
        NetSuiteClientService<?> clientService = service.getClientService(configuration.getDataSet().getDataStore());
        Schema runtimeSchema = service.getSchema(configuration.getDataSet(), null);
        RecordTypeInfo recordTypeInfo = rs.getRecordTypeDesc();
        transducer = new NsObjectInputTransducer(clientService, i18n, recordBuilderFactory, runtimeSchema,
                recordTypeInfo.getName(), configuration.getDataSet().getDataStore().getApiVersion().getVersion());
    }

    @Producer
    public Record next() {
        return rs.next() ? transducer.read(rs::get) : null;
    }
}