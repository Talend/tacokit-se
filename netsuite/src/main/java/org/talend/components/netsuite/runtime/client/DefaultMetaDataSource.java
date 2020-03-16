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
package org.talend.components.netsuite.runtime.client;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.talend.components.netsuite.runtime.NsObjectTransducer;
import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.CustomFieldDesc;
import org.talend.components.netsuite.runtime.model.CustomRecordTypeInfo;
import org.talend.components.netsuite.runtime.model.FieldDesc;
import org.talend.components.netsuite.runtime.model.RecordTypeDesc;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.runtime.model.SearchRecordTypeDesc;
import org.talend.components.netsuite.runtime.model.TypeDesc;

import static java.util.stream.Collectors.toList;

/**
 * Implementation of <code>MetaDataSource</code> which retrieves customization related meta data
 * from NetSuite web service via <code>NetSuiteClientService</code>.
 *
 * @see NetSuiteClientService
 * @see BasicMetaData
 * @see CustomMetaDataSource
 */
public class DefaultMetaDataSource implements MetaDataSource {

    protected NetSuiteClientService<?> clientService;

    protected CustomMetaDataSource customMetaDataSource;

    public DefaultMetaDataSource(NetSuiteClientService<?> clientService) {
        this.clientService = clientService;

        customMetaDataSource = clientService.createDefaultCustomMetaDataSource();
    }

    @Override
    public BasicMetaData getBasicMetaData() {
        return clientService.getBasicMetaData();
    }

    @Override
    public Collection<RecordTypeInfo> getRecordTypes(boolean customizationEnabled) {

        List<RecordTypeInfo> recordTypes = clientService.getBasicMetaData().getRecordTypes().stream().map(RecordTypeInfo::new)
                .collect(toList());
        if (customizationEnabled) {
            recordTypes.addAll(customMetaDataSource.getCustomRecordTypes());
        }
        return recordTypes;
    }

    @Override
    public TypeDesc getTypeInfo(final String typeName, boolean customizationEnabled) {
        TypeDesc baseTypeDesc;
        String targetTypeName = null;

        RecordTypeInfo recordTypeInfo = getRecordType(typeName, customizationEnabled);
        if (recordTypeInfo instanceof CustomRecordTypeInfo) {
            CustomRecordTypeInfo customRecordTypeInfo = (CustomRecordTypeInfo) recordTypeInfo;
            baseTypeDesc = clientService.getBasicMetaData().getTypeInfo(customRecordTypeInfo.getRecordType().getTypeName());
            targetTypeName = customRecordTypeInfo.getName();
        } else {
            baseTypeDesc = clientService.getBasicMetaData().getTypeInfo(typeName);
            targetTypeName = baseTypeDesc.getTypeName();
        }

        List<FieldDesc> resultFieldDescList = baseTypeDesc.getFields().stream().filter(this::isNotCustomOrNullFieldList)
                .collect(toList());

        if (recordTypeInfo != null && customizationEnabled) {
            customMetaDataSource.getCustomFields(recordTypeInfo).values().stream().forEach(resultFieldDescList::add);
        }

        return new TypeDesc(targetTypeName, baseTypeDesc.getTypeClass(), resultFieldDescList);
    }

    private boolean isNotCustomOrNullFieldList(FieldDesc fieldDesc) {
        String fieldName = fieldDesc.getName();
        return !fieldName.equals(NsObjectTransducer.CUSTOM_FIELD_LIST) && !fieldName.equals(NsObjectTransducer.NULL_FIELD_LIST);
    }

    @Override
    public RecordTypeInfo getRecordType(String typeName, boolean customizationEnabled) {
        RecordTypeDesc recordType = clientService.getBasicMetaData().getRecordType(typeName);
        if (recordType != null) {
            return new RecordTypeInfo(recordType);
        }
        return customizationEnabled ? customMetaDataSource.getCustomRecordType(typeName) : null;
    }

    @Override
    public SearchRecordTypeDesc getSearchRecordType(String recordTypeName, boolean customizationEnabled) {
        SearchRecordTypeDesc searchRecordType = clientService.getBasicMetaData().getSearchRecordType(recordTypeName);
        if (searchRecordType != null) {
            return searchRecordType;
        }
        return Optional.ofNullable(getRecordType(recordTypeName, customizationEnabled)).map(RecordTypeInfo::getRecordType)
                .map(this::getSearchRecordType).orElse(null);
    }

    @Override
    public SearchRecordTypeDesc getSearchRecordType(RecordTypeDesc recordType) {
        if (recordType.getSearchRecordType() != null) {
            return clientService.getBasicMetaData().getSearchRecordType(recordType.getSearchRecordType());
        }
        if (BasicRecordType.CUSTOM_RECORD_TYPE.getType().equals(recordType.getType())) {
            return clientService.getBasicMetaData().getSearchRecordType(BasicRecordType.CUSTOM_RECORD.getType());
        }
        if (BasicRecordType.CUSTOM_TRANSACTION_TYPE.getType().equals(recordType.getType())) {
            return clientService.getBasicMetaData().getSearchRecordType(BasicRecordType.TRANSACTION.getType());
        }
        return null;
    }

    @Override
    public List<String> getSearchRecordCustomFields(String recordTypeName, boolean customizationEnabled) {
        return customMetaDataSource.getCustomFields(getRecordType(recordTypeName, customizationEnabled)).values().stream()
                .filter(CustomFieldDesc::isGlobalSearch).map(CustomFieldDesc::getName).collect(toList());
    }
}
