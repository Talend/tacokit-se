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

import org.talend.components.netsuite.runtime.NsObjectTransducer;
import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.CustomFieldDesc;
import org.talend.components.netsuite.runtime.model.CustomRecordTypeInfo;
import org.talend.components.netsuite.runtime.model.RecordTypeDesc;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.runtime.model.RefType;
import org.talend.components.netsuite.runtime.model.beans.Beans;
import org.talend.components.netsuite.runtime.model.customfield.CustomFieldRefType;
import org.talend.components.netsuite.service.NetSuiteService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Implementation of <code>CustomMetaDataSource</code> which retrieves custom meta data from NetSuite and
 * caches retrieved data.
 */
public class DefaultCustomMetaDataSource<PortT> implements CustomMetaDataSource {

    private static final String LABEL = "label";

    private static final String GLOBAL_SEARCH = "globalSearch";

    protected NetSuiteClientService<PortT> clientService;

    protected Map<String, CustomRecordTypeInfo> customRecordTypeMap = new HashMap<>();

    protected boolean customRecordTypesLoaded = false;

    protected Map<BasicRecordType, List<?>> customFieldMap = new HashMap<>();

    protected Map<String, Map<String, CustomFieldDesc>> recordCustomFieldMap = new HashMap<>();

    protected boolean customFieldsLoaded = false;

    protected Map<String, Map<String, CustomFieldDesc>> customRecordCustomFieldMap = new HashMap<>();

    protected CustomMetaDataRetriever customMetaDataRetriever;

    protected static final List<BasicRecordType> fieldCustomizationTypes = Collections
            .unmodifiableList(Arrays.asList(BasicRecordType.CRM_CUSTOM_FIELD, BasicRecordType.ENTITY_CUSTOM_FIELD,
                    BasicRecordType.ITEM_CUSTOM_FIELD, BasicRecordType.OTHER_CUSTOM_FIELD,
                    BasicRecordType.TRANSACTION_BODY_CUSTOM_FIELD, BasicRecordType.TRANSACTION_COLUMN_CUSTOM_FIELD));

    public DefaultCustomMetaDataSource(NetSuiteClientService<PortT> clientService,
            CustomMetaDataRetriever customMetaDataRetriever) {
        this.clientService = clientService;
        this.customMetaDataRetriever = customMetaDataRetriever;
    }

    @Override
    public Collection<CustomRecordTypeInfo> getCustomRecordTypes() {
        return clientService.executeWithLock(new Function<Void, Collection<CustomRecordTypeInfo>>() {

            @Override
            public Collection<CustomRecordTypeInfo> apply(Void param) {
                retrieveCustomRecordTypes();
                return customRecordTypeMap.values();
            }
        }, null);
    }

    @Override
    public Map<String, CustomFieldDesc> getCustomFields(RecordTypeInfo recordTypeInfo) {
        return clientService.executeWithLock(this::getCustomFieldsImpl, recordTypeInfo);
    }

    @Override
    public CustomRecordTypeInfo getCustomRecordType(String typeName) {
        return clientService.executeWithLock(new Function<String, CustomRecordTypeInfo>() {

            @Override
            public CustomRecordTypeInfo apply(String typeName) {
                retrieveCustomRecordTypes();
                return customRecordTypeMap.get(typeName);
            }
        }, typeName);
    }

    /**
     * Get custom field descriptors for a given record type.
     *
     * @param recordTypeInfo record type info
     * @return custom field descriptors as map
     */
    protected Map<String, CustomFieldDesc> getCustomFieldsImpl(RecordTypeInfo recordTypeInfo) {
        RecordTypeDesc recordType = recordTypeInfo.getRecordType();
        Map<String, CustomFieldDesc> fieldDescMap;
        if (recordTypeInfo instanceof CustomRecordTypeInfo) {
            fieldDescMap = customRecordCustomFieldMap.get(recordTypeInfo.getName());
            if (fieldDescMap == null) {
                retrieveCustomRecordCustomFields((CustomRecordTypeInfo) recordTypeInfo);
                fieldDescMap = customRecordCustomFieldMap.get(recordTypeInfo.getName());
            }
        } else {
            fieldDescMap = recordCustomFieldMap.get(recordType.getType());
            if (fieldDescMap == null) {
                retrieveCustomFields(recordType);
                fieldDescMap = recordCustomFieldMap.get(recordType.getType());
            }
        }
        return fieldDescMap;
    }

    /**
     * Create custom field descriptors.
     *
     * @param recordType record type
     * @param customizationType customization type
     * @param customFieldList list of native NetSuite objects describing custom fields
     * @param <T> type of custom field data objects
     * @return custom field descriptors as map
     */
    public static <T> Map<String, CustomFieldDesc> createCustomFieldDescMap(NetSuiteClientService<?> clientService,
            RecordTypeDesc recordType, BasicRecordType customizationType, List<T> customFieldList) {

        Map<String, CustomFieldDesc> customFieldDescMap = new HashMap<>();

        for (T customField : customFieldList) {
            CustomFieldRefType customFieldRefType = clientService.getBasicMetaData().getCustomFieldRefType(recordType.getType(),
                    customizationType, customField);

            if (customFieldRefType != null) {
                CustomFieldDesc customFieldDesc = new CustomFieldDesc();

                String internalId = (String) Beans.getSimpleProperty(customField, NsObjectTransducer.INTERNAL_ID);
                String scriptId = (String) Beans.getSimpleProperty(customField, NsObjectTransducer.SCRIPT_ID);
                String label = (String) Beans.getSimpleProperty(customField, LABEL);

                NsRef customizationRef = new NsRef();
                customizationRef.setRefType(RefType.CUSTOMIZATION_REF);
                customizationRef.setType(customizationType.getType());
                customizationRef.setName(label);
                customizationRef.setInternalId(internalId);
                customizationRef.setScriptId(scriptId);
                if (Beans.getPropertyInfo(customField, GLOBAL_SEARCH) != null) {
                    customFieldDesc.setGlobalSearch((Boolean) Beans.getSimpleProperty(customField, GLOBAL_SEARCH));
                }
                customFieldDesc.setCustomizationRef(customizationRef);
                customFieldDesc.setName(customizationRef.getScriptId());
                customFieldDesc.setCustomFieldType(customFieldRefType);

                customFieldDesc.setValueType(NetSuiteService.getCustomFieldValueClass(customFieldRefType));
                customFieldDesc.setNullable(true);

                customFieldDescMap.put(customFieldDesc.getName(), customFieldDesc);
            }
        }

        return customFieldDescMap;
    }

    /**
     * Retrieve custom record types from NetSuite web service.
     *
     * @see #customRecordTypeMap
     *
     */
    protected void retrieveCustomRecordTypes() {
        if (customRecordTypesLoaded) {
            return;
        }

        List<NsRef> customTypes = new ArrayList<>();

        List<NsRef> customRecordTypes = customMetaDataRetriever.retrieveCustomizationIds(BasicRecordType.CUSTOM_RECORD_TYPE);
        customTypes.addAll(customRecordTypes);

        List<NsRef> customTransactionTypes = customMetaDataRetriever
                .retrieveCustomizationIds(BasicRecordType.CUSTOM_TRANSACTION_TYPE);
        customTypes.addAll(customTransactionTypes);

        for (NsRef customizationRef : customTypes) {
            String recordType = customizationRef.getType();
            RecordTypeDesc recordTypeDesc = null;
            BasicRecordType basicRecordType = BasicRecordType.getByType(recordType);
            if (basicRecordType != null) {
                recordTypeDesc = clientService.getBasicMetaData()
                        .getRecordType(Beans.toInitialUpper(basicRecordType.getSearchType()));
            }

            CustomRecordTypeInfo customRecordTypeInfo = new CustomRecordTypeInfo(customizationRef.getScriptId(), recordTypeDesc,
                    customizationRef);
            customRecordTypeMap.put(customRecordTypeInfo.getName(), customRecordTypeInfo);
        }

        customRecordTypesLoaded = true;
    }

    /**
     * Retrieve custom fields for a given record type.
     *
     * @param recordType record type
     */
    protected void retrieveCustomFields(RecordTypeDesc recordType) {
        retrieveCustomFields();

        Map<String, CustomFieldDesc> fieldDescMap = new HashMap<>();

        for (BasicRecordType customizationType : fieldCustomizationTypes) {
            List<?> customFieldList = customFieldMap.get(customizationType);
            Map<String, CustomFieldDesc> customFieldDescMap = createCustomFieldDescMap(clientService, recordType,
                    customizationType, customFieldList);
            fieldDescMap.putAll(customFieldDescMap);
        }

        recordCustomFieldMap.put(recordType.getType(), fieldDescMap);
    }

    /**
     * Retrieve custom fields for standard record types from NetSuite web service.
     *
     */
    protected void retrieveCustomFields() {
        if (customFieldsLoaded) {
            return;
        }

        Map<BasicRecordType, List<NsRef>> fieldCustomizationRefs = new HashMap<>(32);
        for (BasicRecordType customizationType : fieldCustomizationTypes) {
            List<NsRef> customizationRefs = customMetaDataRetriever.retrieveCustomizationIds(customizationType);
            fieldCustomizationRefs.put(customizationType, customizationRefs);
        }

        for (BasicRecordType customizationType : fieldCustomizationTypes) {
            List<NsRef> customizationRefs = fieldCustomizationRefs.get(customizationType);
            List<?> fieldCustomizationList = customMetaDataRetriever.retrieveCustomizations(customizationRefs);
            customFieldMap.put(customizationType, fieldCustomizationList);
        }

        customFieldsLoaded = true;
    }

    /**
     * Retrieve custom fields for a given custom record type.
     *
     * @param recordTypeInfo custom record type
     */
    protected void retrieveCustomRecordCustomFields(CustomRecordTypeInfo recordTypeInfo) {
        Map<String, CustomFieldDesc> recordCustomFieldMap = customRecordCustomFieldMap.get(recordTypeInfo.getName());
        if (recordCustomFieldMap != null) {
            return;
        }
        recordCustomFieldMap = customMetaDataRetriever.retrieveCustomRecordCustomFields(recordTypeInfo.getRecordType(),
                recordTypeInfo.getCustomizationRef());
        customRecordCustomFieldMap.put(recordTypeInfo.getName(), recordCustomFieldMap);
    }

    public interface CustomMetaDataRetriever {

        /**
         * Retrieve customization IDs for given customization type.
         *
         * @param type customization type
         * @return list of customization refs
         */
        List<NsRef> retrieveCustomizationIds(final BasicRecordType type);

        /**
         * Retrieve customization for given customization refs.
         *
         * @param nsCustomizationRefs customization refs which to retrieve customization data for
         * @return list of customization records
         */
        List<?> retrieveCustomizations(final List<NsRef> nsCustomizationRefs);

        /**
         * Retrieve custom fields for given custom record type.
         *
         * @param recordType custom record type descriptor
         * @param nsCustomizationRef customization ref for the custom record type
         * @return custom field map which contains <code>(custom field name, custom field descriptor)</code> entries
         */
        Map<String, CustomFieldDesc> retrieveCustomRecordCustomFields(final RecordTypeDesc recordType,
                final NsRef nsCustomizationRef);

    }
}
