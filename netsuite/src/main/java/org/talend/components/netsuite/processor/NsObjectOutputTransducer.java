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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.talend.components.netsuite.runtime.NsObjectTransducer;
import org.talend.components.netsuite.runtime.client.NsRef;
import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.CustomRecordTypeInfo;
import org.talend.components.netsuite.runtime.model.FieldDesc;
import org.talend.components.netsuite.runtime.model.RecordTypeDesc;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.runtime.model.RefType;
import org.talend.components.netsuite.runtime.model.TypeDesc;
import org.talend.components.netsuite.runtime.model.beans.BeanInfo;
import org.talend.components.netsuite.runtime.model.beans.Beans;
import org.talend.components.netsuite.service.Messages;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Responsible for translating of input {@code Record} to output NetSuite data object.
 *
 * <p>
 * Output NetSuite data object can be {@code Record} or {@code RecordRef}.
 */
public class NsObjectOutputTransducer extends NsObjectTransducer {

    /** Specifies whether output NetSuite data object is {@code RecordRef}. */
    private boolean reference;

    /** Descriptor of NetSuite data object type. */
    private TypeDesc typeDesc;

    /** Information about target record type. */
    private RecordTypeInfo recordTypeInfo;

    public NsObjectOutputTransducer(BasicMetaData basicMetaData, Messages i18n, TypeDesc typeDesc, Schema schema,
            String apiVersion, boolean isReference, RecordTypeInfo recordTypeInfo) {
        super(basicMetaData, i18n, apiVersion, schema);
        this.typeDesc = typeDesc;
        this.reference = isReference;
        this.recordTypeInfo = recordTypeInfo;
    }

    /**
     * Translate input {@code Record} to output NetSuite data object.
     *
     * @param record record to be processed
     * @return NetSuite data object
     */
    public Object write(Record record) {
        String targetTypeName;
        if (recordTypeInfo != null && !reference) {
            RecordTypeDesc recordTypeDesc = recordTypeInfo.getRecordType();
            targetTypeName = recordTypeDesc.getTypeName();
        } else {
            targetTypeName = typeDesc.getTypeName();
        }
        Object nsObject = basicMetaData.createInstance(targetTypeName);

        Set<String> nullFieldNames = new HashSet<>();

        writeFields(nsObject, nullFieldNames, record);
        processReference(nsObject, nullFieldNames);
        setNullFields(nsObject, nullFieldNames);

        return nsObject;
    }

    private void writeFields(Object nsObject, Set<String> nullFieldNames, Record record) {
        BeanInfo beanInfo = Beans.getBeanInfo(typeDesc.getTypeClass());
        Map<String, Object> customFieldMap = Collections.emptyMap();
        if (!reference && beanInfo.getProperty(CUSTOM_FIELD_LIST) != null) {
            customFieldMap = new HashMap<>();

            Object customFieldListWrapper = Beans.getSimpleProperty(nsObject, CUSTOM_FIELD_LIST);
            if (customFieldListWrapper != null) {
                List<Object> customFieldList = (List<Object>) Beans.getSimpleProperty(customFieldListWrapper, CUSTOM_FIELD);
                for (Object customField : customFieldList) {
                    String scriptId = (String) Beans.getSimpleProperty(customField, SCRIPT_ID);
                    customFieldMap.put(scriptId, customField);
                }
            }
        }

        Map<String, Entry> targetSchema = new HashMap<>();
        schema.getEntries().forEach(entry -> targetSchema.put(entry.getName(), entry));
        Map<String, FieldDesc> fieldMap = typeDesc.getFieldMap();

        for (Entry entry : record.getSchema().getEntries()) {
            Entry targetEntry = targetSchema.get(entry.getName());
            if (targetEntry == null) {
                continue;
            }
            String nsFieldName = Beans.toInitialLower(entry.getName());

            FieldDesc fieldDesc = fieldMap.get(nsFieldName);
            if (fieldDesc == null) {
                continue;
            }

            Object value;
            if (targetEntry.getType() == Schema.Type.DATETIME && entry.getType() == Schema.Type.INT
                    && record.getInt(entry.getName()) == -1) {
                // int -1 is null ZonedDateTime
                value = null;
            } else if (targetEntry.getType() == Schema.Type.BOOLEAN && entry.getType() == Schema.Type.STRING) {
                // boolean can be coded as string
                value = record.getOptionalString(entry.getName()).map(Boolean::valueOf).orElse(null);
            } else {
                value = record.get(fieldDesc.getRecordValueType(), entry.getName());
            }
            writeField(nsObject, fieldDesc, customFieldMap, nullFieldNames, value);
        }
    }

    private void processReference(Object nsObject, Set<String> nullFieldNames) {
        if (reference) {
            if (recordTypeInfo.getRefType() == RefType.RECORD_REF) {
                FieldDesc recTypeFieldDesc = typeDesc.getField(TYPE);
                RecordTypeDesc recordTypeDesc = recordTypeInfo.getRecordType();
                nullFieldNames.remove(TYPE);
                writeSimpleField(nsObject, recTypeFieldDesc.asSimple(), false, nullFieldNames, recordTypeDesc.getType());

            } else if (recordTypeInfo.getRefType() == RefType.CUSTOM_RECORD_REF) {
                CustomRecordTypeInfo customRecordTypeInfo = (CustomRecordTypeInfo) recordTypeInfo;
                NsRef customizationRef = customRecordTypeInfo.getCustomizationRef();

                FieldDesc typeIdFieldDesc = typeDesc.getField(TYPE_ID);
                nullFieldNames.remove(TYPE_ID);
                writeSimpleField(nsObject, typeIdFieldDesc.asSimple(), false, nullFieldNames, customizationRef.getInternalId());
            }
        } else if (recordTypeInfo != null) {
            RecordTypeDesc recordTypeDesc = recordTypeInfo.getRecordType();
            if (recordTypeDesc.getType().equals(BasicRecordType.CUSTOM_RECORD.getType())) {
                CustomRecordTypeInfo customRecordTypeInfo = (CustomRecordTypeInfo) recordTypeInfo;

                FieldDesc recTypeFieldDesc = typeDesc.getField(REC_TYPE);
                NsRef recordTypeRef = customRecordTypeInfo.getCustomizationRef();

                // Create custom record type ref as JSON to create native RecordRef
                ObjectNode recordRefNode = JsonNodeFactory.instance.objectNode();
                recordRefNode.set(INTERNAL_ID, JsonNodeFactory.instance.textNode(recordTypeRef.getInternalId()));
                recordRefNode.set(TYPE, JsonNodeFactory.instance.textNode(recordTypeDesc.getType()));

                nullFieldNames.remove(REC_TYPE);
                writeSimpleField(nsObject, recTypeFieldDesc.asSimple(), false, nullFieldNames, recordRefNode.toString());
            }
        }
    }

    private void setNullFields(Object nsObject, Set<String> nullFieldNames) {
        BeanInfo beanInfo = Beans.getBeanInfo(typeDesc.getTypeClass());
        if (!nullFieldNames.isEmpty() && beanInfo.getProperty(NULL_FIELD_LIST) != null) {
            Object nullFieldListWrapper = basicMetaData.createInstance(NULL_FIELD);
            Beans.setSimpleProperty(nsObject, NULL_FIELD_LIST, nullFieldListWrapper);
            List<String> nullFields = (List<String>) Beans.getSimpleProperty(nullFieldListWrapper, NAME);
            nullFields.addAll(nullFieldNames);
        }
    }
}