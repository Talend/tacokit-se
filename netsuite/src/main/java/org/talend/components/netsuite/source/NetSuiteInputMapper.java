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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.talend.components.netsuite.dataset.NetSuiteInputProperties;
import org.talend.components.netsuite.datastore.NetSuiteDataStore;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NsSearchResult;
import org.talend.components.netsuite.runtime.client.search.PageSelection;
import org.talend.components.netsuite.runtime.client.search.SearchResultSet;
import org.talend.components.netsuite.service.Messages;
import org.talend.components.netsuite.service.NetSuiteClientConnectionService;
import org.talend.components.netsuite.service.NetSuiteService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Setter;

@Version(1)
@Icon(value = Icon.IconType.NETSUITE)
@PartitionMapper(name = "Input")
@Documentation("Creates worker emitter with specified configurations")
public class NetSuiteInputMapper implements Serializable {

    private final NetSuiteInputProperties configuration;

    private final NetSuiteService netSuiteService;

    private final NetSuiteClientConnectionService netSuiteClientConnectionService;

    private final RecordBuilderFactory recordBuilderFactory;

    private final Messages i18n;

    @Setter
    private PageSelection pageSelection;

    public NetSuiteInputMapper(@Option("configuration") final NetSuiteInputProperties configuration,
            final RecordBuilderFactory recordBuilderFactory, final Messages i18n, NetSuiteService netSuiteService,
            NetSuiteClientConnectionService netSuiteClientConnectionService) {
        this.configuration = configuration;
        this.recordBuilderFactory = recordBuilderFactory;
        this.i18n = i18n;
        this.netSuiteService = netSuiteService;
        this.netSuiteClientConnectionService = netSuiteClientConnectionService;
    }

    @PostConstruct
    public void init() {
        // rs can be deserialized at worker
        if (pageSelection == null) {
            NetSuiteClientService<?> clientService = netSuiteClientConnectionService
                    .getClientService(configuration.getDataSet().getDataStore(), i18n);
            NetSuiteInputSearcher searcher = new NetSuiteInputSearcher(configuration, clientService);
            NsSearchResult<?> rs = searcher.search();
            // this value is used in case of no splitting
            List<?> recordList = rs.getRecordList();
            if (recordList != null && !recordList.isEmpty()) {
                pageSelection = new PageSelection(rs.getSearchId(), 1, rs.getTotalPages());
            } else {
                pageSelection = PageSelection.createEmpty(rs.getSearchId());
            }
        }
    }

    @Assessor
    public long estimateSize() {
        if (configuration.getDataSet().getDataStore().getLoginType() == NetSuiteDataStore.LoginType.TBA)
            return pageSelection.getPageCount();
        else
            return 0;
    }

    @Split
    public List<NetSuiteInputMapper> split(@PartitionSize final long bundles) {
        if (bundles == 0)
            return Collections.singletonList(this);

        int threads = (int) (pageSelection.getPageCount() / bundles);
        if (threads == 1)
            return Collections.singletonList(this);

        List<NetSuiteInputMapper> res = new LinkedList<>();
        for (int i = 0; i < threads; i++) {
            res.add(createMapper((int) (bundles * i), (int) bundles));
        }
        if (threads * bundles < pageSelection.getPageCount()) {
            res.add(createMapper((int) bundles * threads, pageSelection.getPageCount() - (int) bundles * threads));
        }
        return res;
    }

    private NetSuiteInputMapper createMapper(int offset, int size) {
        NetSuiteInputMapper mapper = new NetSuiteInputMapper(configuration, recordBuilderFactory, i18n, netSuiteService,
                netSuiteClientConnectionService);
        mapper.setPageSelection(new PageSelection(pageSelection.getSearchId(), pageSelection.getFirstPage() + offset, size));
        return mapper;
    }

    @Emitter
    public NetSuiteInputSource createWorker() {
        NetSuiteClientService<?> clientService = netSuiteClientConnectionService
                .getClientService(configuration.getDataSet().getDataStore(), i18n);
        clientService.setBodyFieldsOnly(configuration.isBodyFieldsOnly());
        String recordTypeName = configuration.getDataSet().getRecordType();
        SearchResultSet<?> srs = new SearchResultSet<>(clientService, recordTypeName, pageSelection,
                configuration.getDataSet().isEnableCustomization());
        return new NetSuiteInputSource(configuration, recordBuilderFactory, i18n, srs, netSuiteService, clientService);
    }
}