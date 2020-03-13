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
package org.talend.components.netsuite.processor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.netsuite.dataset.NetSuiteOutputProperties;
import org.talend.components.netsuite.dataset.NetSuiteOutputProperties.DataAction;
import org.talend.components.netsuite.runtime.NsObjectTransducer;
import org.talend.components.netsuite.runtime.client.MetaDataSource;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NsWriteResponse;
import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.runtime.model.TypeDesc;
import org.talend.components.netsuite.runtime.model.beans.Beans;
import org.talend.components.netsuite.service.Messages;
import org.talend.components.netsuite.service.NetSuiteClientConnectionService;
import org.talend.components.netsuite.service.NetSuiteService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.NETSUITE)
@Processor(name = "Output")
@Documentation("Output component processor")
public class NetSuiteOutputProcessor implements Serializable {

    private final NetSuiteOutputProperties configuration;

    private RecordBuilderFactory recordBuilderFactory;

    private Messages i18n;

    private final NetSuiteService netSuiteService;

    private transient NetSuiteClientService<?> clientService;

    private final NetSuiteClientConnectionService netSuiteClientConnectionService;

    private transient NsObjectOutputTransducer transducer;

    private transient Function<List<?>, List<NsWriteResponse<?>>> dataActionFunction;

    private transient List<Record> inputRecordList;

    /**
     * Netsuite output has a limitation of 200 records for an output component
     */
    private static int MAX_OUTPUT_BATCH_SIZE = 100;

    public NetSuiteOutputProcessor(@Option("configuration") final NetSuiteOutputProperties configuration,
            RecordBuilderFactory recordBuilderFactory, Messages i18n, NetSuiteService netSuiteService,
            NetSuiteClientConnectionService netSuiteClientConnectionService) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.configuration = configuration;
        this.i18n = i18n;
        this.netSuiteService = netSuiteService;
        this.netSuiteClientConnectionService = netSuiteClientConnectionService;
    }

    @PostConstruct
    public void init() {
        clientService = netSuiteClientConnectionService.getClientService(configuration.getDataSet().getDataStore(), i18n);
        Schema schema = netSuiteService.getSchema(configuration.getDataSet(), null, clientService);
        referenceDataActionFunction();
        boolean isReference = (configuration.getAction() == DataAction.DELETE);
        MetaDataSource metaDataSource = clientService.getMetaDataSource();
        BasicMetaData basicMetaData = clientService.getBasicMetaData();
        boolean isEnableCustomization = configuration.getDataSet().isEnableCustomization();
        RecordTypeInfo recordTypeInfo = metaDataSource.getRecordType(configuration.getDataSet().getRecordType(),
                isEnableCustomization);
        String typeName;
        if (isReference) {
            // If target NetSuite data object is record ref then
            // we should get descriptor for RecordRef type.
            typeName = recordTypeInfo.getRefType().getTypeName();
        } else {
            typeName = configuration.getDataSet().getRecordType();
        }
        TypeDesc typeDesc = metaDataSource.getTypeInfo(typeName, isEnableCustomization);
        transducer = new NsObjectOutputTransducer(basicMetaData, i18n, typeDesc, schema,
                configuration.getDataSet().getDataStore().getApiVersion().getVersion(), isReference, recordTypeInfo);
        inputRecordList = new ArrayList<>();
    }

    private void referenceDataActionFunction() {
        DataAction data = configuration.getAction();
        switch (data) {
        case ADD:
            dataActionFunction = clientService::addList;
            break;
        case UPDATE:
            dataActionFunction = clientService::updateList;
            break;
        case DELETE:
            dataActionFunction = clientService::deleteList;
            break;
        case UPSERT:
            dataActionFunction = configuration.isUseNativeUpsert() ? clientService::upsertList : this::customUpsert;
            break;
        }
    }

    @BeforeGroup
    public void beforeGroup() {
        inputRecordList.clear();
    }

    @ElementListener
    public void onNext(@Input final Record record) {
        inputRecordList.add(record);
    }

    private void write() {
        int processed = 0;
        while (processed < inputRecordList.size()) {
            List<Object> nsObjectList = inputRecordList.stream().skip(processed).limit(MAX_OUTPUT_BATCH_SIZE)
                    .map(transducer::write).collect(toList());
            processed += nsObjectList.size();
            dataActionFunction.apply(nsObjectList).forEach(this::processWriteResponse);
        }
        inputRecordList.clear();
    }

    private void processWriteResponse(NsWriteResponse<?> response) {
        if (!response.getStatus().isSuccess()) {
            NetSuiteClientService.checkError(response.getStatus());
        }
    }

    @AfterGroup
    public void afterGroup() {
        write();
    }

    @PreDestroy
    public void release() {
        if (!inputRecordList.isEmpty()) {
            write();
        }
    }

    private <T> List<NsWriteResponse<?>> customUpsert(List<T> records) {
        List<T> addList = null;
        List<T> updateList = null;
        for (T nsObject : records) {
            String internalId = (String) Beans.getSimpleProperty(nsObject, NsObjectTransducer.INTERNAL_ID);
            String externalId = (String) Beans.getSimpleProperty(nsObject, NsObjectTransducer.EXTERNAL_ID);
            if (StringUtils.isNotEmpty(internalId) || StringUtils.isNotEmpty(externalId)) {
                if (updateList == null) {
                    updateList = new ArrayList<>();
                }
                updateList.add(nsObject);
            } else {
                if (addList == null) {
                    addList = new ArrayList<>();
                }
                addList.add(nsObject);
            }
        }
        Map<T, NsWriteResponse<?>> responseMap = new HashMap<>(records.size());
        if (addList != null) {
            processOperationResponse(clientService::addList, addList, () -> responseMap);
        }
        if (updateList != null) {
            processOperationResponse(clientService::updateList, updateList, () -> responseMap);
        }

        // Create combined list of write responses with an order as input records list
        return records.stream().map(responseMap::get).collect(toList());
    }

    private <T> void processOperationResponse(Function<List<T>, List<NsWriteResponse<?>>> operation, List<T> recordsToBeProcessed,
            Supplier<Map<T, NsWriteResponse<?>>> supplier) {
        List<NsWriteResponse<?>> responseList = operation.apply(recordsToBeProcessed);
        IntStream.range(0, recordsToBeProcessed.size()).boxed()
                .collect(toMap(recordsToBeProcessed::get, responseList::get, (k1, k2) -> k2, supplier));
    }
}