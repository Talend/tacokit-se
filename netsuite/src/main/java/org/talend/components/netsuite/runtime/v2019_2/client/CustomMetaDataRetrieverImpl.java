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

import com.netsuite.webservices.v2019_2.platform.NetSuitePortType;
import com.netsuite.webservices.v2019_2.platform.core.CustomizationRef;
import com.netsuite.webservices.v2019_2.platform.core.CustomizationType;
import com.netsuite.webservices.v2019_2.platform.core.GetCustomizationIdResult;
import com.netsuite.webservices.v2019_2.platform.core.Record;
import com.netsuite.webservices.v2019_2.platform.core.types.GetCustomizationType;
import com.netsuite.webservices.v2019_2.platform.core.types.RecordType;
import com.netsuite.webservices.v2019_2.platform.messages.GetCustomizationIdRequest;
import com.netsuite.webservices.v2019_2.platform.messages.GetListRequest;
import com.netsuite.webservices.v2019_2.setup.customization.CustomRecordType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.talend.components.netsuite.runtime.NetSuiteErrorCode;
import org.talend.components.netsuite.runtime.client.DefaultCustomMetaDataSource;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.client.NsReadResponse;
import org.talend.components.netsuite.runtime.client.NsRef;
import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.CustomFieldDesc;
import org.talend.components.netsuite.runtime.model.RecordTypeDesc;
import org.talend.components.netsuite.runtime.model.RefType;
import org.talend.components.netsuite.service.Messages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Runtime class for retrieving all required metadata about custom fields.
 */
@Slf4j
public class CustomMetaDataRetrieverImpl implements DefaultCustomMetaDataSource.CustomMetaDataRetriever {

    private NetSuiteClientService<NetSuitePortType> clientService;

    private Messages i18n;

    public CustomMetaDataRetrieverImpl(NetSuiteClientService<NetSuitePortType> clientService, Messages i18n) {
        this.clientService = clientService;
        this.i18n = i18n;
    }

    @Override
    public List<NsRef> retrieveCustomizationIds(final BasicRecordType type) throws NetSuiteException {
        GetCustomizationIdResult result = clientService
                .execute(new NetSuiteClientService.PortOperation<GetCustomizationIdResult, NetSuitePortType>() {

                    @Override
                    public GetCustomizationIdResult execute(NetSuitePortType port) throws Exception {
                        log.debug("Retrieving customization IDs: {}", type.getType());
                        StopWatch stopWatch = new StopWatch();
                        try {
                            stopWatch.start();
                            final GetCustomizationIdRequest request = new GetCustomizationIdRequest();
                            CustomizationType customizationType = new CustomizationType();
                            customizationType.setGetCustomizationType(GetCustomizationType.fromValue(type.getType()));
                            request.setCustomizationType(customizationType);
                            return port.getCustomizationId(request).getGetCustomizationIdResult();
                        } finally {
                            stopWatch.stop();
                            log.debug("Retrieved customization IDs: {}, {}", type.getType(), stopWatch);
                        }
                    }
                });
        if (result.getStatus().getIsSuccess()) {
            List<NsRef> nsRefs;
            if (result.getTotalRecords() > 0) {
                final List<CustomizationRef> refs = result.getCustomizationRefList().getCustomizationRef();
                nsRefs = new ArrayList<>(refs.size());
                for (final CustomizationRef ref : refs) {
                    NsRef nsRef = new NsRef();
                    nsRef.setRefType(RefType.CUSTOMIZATION_REF);
                    nsRef.setScriptId(ref.getScriptId());
                    nsRef.setInternalId(ref.getInternalId());
                    nsRef.setType(ref.getType().value());
                    nsRef.setName(ref.getName());
                    nsRefs.add(nsRef);
                }
            } else {
                nsRefs = Collections.emptyList();
            }
            return nsRefs;
        } else {
            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
                    i18n.cannotRetrieveCustomizationIds(type.name()));
        }
    }

    @Override
    public List<?> retrieveCustomizations(final List<NsRef> nsCustomizationRefs) throws NetSuiteException {
        if (nsCustomizationRefs.isEmpty()) {
            return Collections.emptyList();
        }

        final List<CustomizationRef> customizationRefs = new ArrayList<>(nsCustomizationRefs.size());
        for (NsRef nsCustomizationRef : nsCustomizationRefs) {
            CustomizationRef customizationRef = new CustomizationRef();
            customizationRef.setType(RecordType.fromValue(nsCustomizationRef.getType()));
            customizationRef.setScriptId(nsCustomizationRef.getScriptId());
            customizationRef.setInternalId(nsCustomizationRef.getInternalId());
            customizationRefs.add(customizationRef);
        }

        List<NsReadResponse<Record>> result = clientService
                .execute(new NetSuiteClientService.PortOperation<List<NsReadResponse<Record>>, NetSuitePortType>() {

                    @Override
                    public List<NsReadResponse<Record>> execute(NetSuitePortType port) throws Exception {
                        log.debug("Retrieving customizations: {}", nsCustomizationRefs.size());
                        StopWatch stopWatch = new StopWatch();
                        try {
                            stopWatch.start();
                            final GetListRequest request = new GetListRequest();
                            request.getBaseRef().addAll(customizationRefs);
                            return NetSuiteClientServiceImpl.toNsReadResponseList(port.getList(request).getReadResponseList());
                        } finally {
                            stopWatch.stop();
                            log.debug("Retrieved customizations: {}, {}", nsCustomizationRefs.size(), stopWatch);
                        }
                    }
                });
        if (!result.isEmpty()) {
            List<Record> customizations = new ArrayList<>(result.size());
            for (NsReadResponse<Record> response : result) {
                if (response.getStatus().isSuccess()) {
                    customizations.add(response.getRecord());
                } else {
                    throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR),
                            i18n.cannotRetrieveCustomizations());
                }
            }
            return customizations;
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public Map<String, CustomFieldDesc> retrieveCustomRecordCustomFields(RecordTypeDesc recordType, NsRef nsCustomizationRef)
            throws NetSuiteException {
        List<?> customizationList = retrieveCustomizations(Collections.singletonList(nsCustomizationRef));
        if (customizationList.isEmpty()) {
            return null;
        }
        CustomRecordType customRecordType = (CustomRecordType) customizationList.get(0);
        List<?> customFieldList = customRecordType.getCustomFieldList().getCustomField();
        return DefaultCustomMetaDataSource.createCustomFieldDescMap(clientService, recordType,
                BasicRecordType.getByType(nsCustomizationRef.getType()), customFieldList);
    }

}
