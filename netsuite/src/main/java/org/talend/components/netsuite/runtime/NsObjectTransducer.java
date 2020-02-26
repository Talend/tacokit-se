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
package org.talend.components.netsuite.runtime;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.client.NsRef;
import org.talend.components.netsuite.runtime.converter.ConverterFactory;
import org.talend.components.netsuite.runtime.converter.ConverterFactory.Converter;
import org.talend.components.netsuite.runtime.json.NsTypeResolverBuilder;
import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.CustomFieldDesc;
import org.talend.components.netsuite.runtime.model.FieldDesc;
import org.talend.components.netsuite.runtime.model.SimpleFieldDesc;
import org.talend.components.netsuite.runtime.model.TypeDesc;
import org.talend.components.netsuite.runtime.model.beans.BeanInfo;
import org.talend.components.netsuite.runtime.model.beans.Beans;
import org.talend.components.netsuite.runtime.model.customfield.CustomFieldRefType;
import org.talend.components.netsuite.service.Messages;
import org.talend.sdk.component.api.record.Schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;

import lombok.Data;

/**
 * Responsible for translating of NetSuite data object to/from {@code IndexedRecord}.
 */

@Data
public abstract class NsObjectTransducer {

    private static final String CUSTOM_FIELD_LIST_UPPER_CASE_NAME = "CustomFieldList";

    private static final String CUSTOM_FIELD_LIST_CUSTOM_FIELD = "customFieldList.customField";

    private static final String VALUE = "value";

    public static final String INTERNAL_ID = "internalId";

    public static final String EXTERNAL_ID = "externalId";

    public static final String CUSTOM_FIELD_LIST = "customFieldList";

    public static final String CUSTOM_FIELD = "customField";

    public static final String NAME = "name";

    public static final String TYPE = "type";

    public static final String REC_TYPE = "recType";

    public static final String TYPE_ID = "typeId";

    public static final String SCRIPT_ID = "scriptId";

    public static final String NULL_FIELD_LIST = "nullFieldList";

    public static final String NULL_FIELD = "NullField";

    protected Messages i18n;

    /** XML data type factory used. */
    protected final DatatypeFactory datatypeFactory;

    /** JSON-Object mapper used. */
    protected final ObjectMapper objectMapper;

    /** Cached value converters by value class. */
    protected Map<Class<?>, Converter<?, ?>> valueConverterCache = new HashMap<>();

    private String apiVersion;

    protected BasicMetaData basicMetaData;

    protected Schema schema;

    /**
     * Creates instance of transducer using given NetSuite client.
     */
    protected NsObjectTransducer(BasicMetaData basicMetaData, Messages i18n, String apiVersion, Schema schema) {
        this.basicMetaData = basicMetaData;
        this.i18n = i18n;
        this.apiVersion = apiVersion;
        this.schema = schema;
        try {
            datatypeFactory = DatatypeFactory.newInstance();
        } catch (DatatypeConfigurationException e) {
            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.OPERATION_NOT_SUPPORTED),
                    i18n.cannotGetDataTypeFactory(), e);
        }

        objectMapper = new ObjectMapper();

        // Customize typing of JSON objects.
        objectMapper.setDefaultTyping(new NsTypeResolverBuilder(basicMetaData));

        // Register JAXB annotation module to perform mapping of data model objects to/from JSON.
        objectMapper.registerModule(new JaxbAnnotationModule());
    }

    /**
     * Build and get map of field values by names, including custom fields.
     *
     * <p>
     * Custom fields in data model object are stored in separate {@code customFieldList} field
     * as list of {@code CustomFieldRef} objects.
     *
     * @param nsObject NetSuite data model object which to extract field values from
     * @param schema target schema
     * @param typeDesc type descriptor
     * @return table of fields' values by field names
     */
    protected Map<String, Object> getMapView(Object nsObject, Schema schema, TypeDesc typeDesc) {
        Map<String, Object> valueMap = new HashMap<>();
        Map<String, FieldDesc> fieldMap = typeDesc.getFieldMap();
        Map<String, CustomFieldDesc> customFieldMap = new HashMap<>();

        // Extract normal fields
        for (Schema.Entry entry : schema.getEntries()) {
            // Get actual name of the field
            String nsFieldName = Beans.toInitialLower(entry.getName());
            FieldDesc fieldDesc = fieldMap.get(nsFieldName);

            if (fieldDesc == null) {
                continue;
            }

            if (fieldDesc instanceof CustomFieldDesc) {
                // It's custom field, we will extract it in next stage.
                customFieldMap.put(nsFieldName, (CustomFieldDesc) fieldDesc);
            } else {
                Object value = Beans.getSimpleProperty(nsObject, fieldDesc.getName());
                valueMap.put(nsFieldName, value);
            }
        }

        // Extract custom fields
        BeanInfo beanInfo = Beans.getBeanInfo(typeDesc.getTypeClass());
        if (!customFieldMap.isEmpty() && beanInfo.getProperty(CUSTOM_FIELD_LIST) != null) {
            List<?> customFieldList = (List<?>) Beans.getProperty(nsObject, CUSTOM_FIELD_LIST_CUSTOM_FIELD);
            if (customFieldList != null && !customFieldList.isEmpty()) {
                // Traverse all received custom fields and extract fields specified in schema
                for (Object customField : customFieldList) {
                    String scriptId = (String) Beans.getSimpleProperty(customField, SCRIPT_ID);
                    CustomFieldDesc customFieldInfo = customFieldMap.get(scriptId);
                    if (customFieldInfo != null) {
                        String fieldName = customFieldInfo.getName();
                        valueMap.put(fieldName, customField);
                    }
                }
            }
        }

        return valueMap;
    }

    /**
     * Read a value from a field.
     *
     * @param valueMap map containing raw values by names
     * @param fieldDesc field descriptor
     * @return value of a field or <code>null</code>
     */
    protected Object readField(Map<String, Object> valueMap, FieldDesc fieldDesc) {
        String fieldName = fieldDesc.getName();
        Converter valueConverter = getValueConverter(fieldDesc);
        if (fieldDesc instanceof CustomFieldDesc) {
            Object customField = valueMap.get(fieldName);
            if (customField != null) {
                Object value = Beans.getSimpleProperty(customField, VALUE);
                return valueConverter.convertToRecordType(value);
            }
            return null;
        } else {
            Object value = valueMap.get(fieldName);
            return valueConverter.convertToRecordType(value);
        }
    }

    /**
     * Write a value to a field.
     *
     * @param nsObject target NetSuite data model object which to write field value to
     * @param fieldDesc field descriptor
     * @param customFieldMap map of native custom field objects by names
     * @param nullFieldNames collection to register null'ed fields
     * @param value value to be written, can be <code>null</code>
     */
    protected void writeField(Object nsObject, FieldDesc fieldDesc, Map<String, Object> customFieldMap,
            Collection<String> nullFieldNames, Object value) {
        writeField(nsObject, fieldDesc, customFieldMap, true, nullFieldNames, value);
    }

    /**
     * Write a value to a field.
     *
     * @param nsObject target NetSuite data model object which to write field value to
     * @param fieldDesc field descriptor
     * @param customFieldMap map of native custom field objects by names
     * @param replace specifies whether to forcibly replace a field's value
     * @param nullFieldNames collection to register null'ed fields
     * @param value value to be written, can be <code>null</code>
     */
    protected void writeField(Object nsObject, FieldDesc fieldDesc, Map<String, Object> customFieldMap, boolean replace,
            Collection<String> nullFieldNames, Object value) {
        if (fieldDesc instanceof CustomFieldDesc) {
            writeCustomField(nsObject, fieldDesc.asCustom(), customFieldMap, replace, nullFieldNames, value);
        } else {
            writeSimpleField(nsObject, fieldDesc.asSimple(), replace, nullFieldNames, value);
        }
    }

    /**
     * Write a custom field which is not defined by NetSuite standard data model.
     *
     * @param nsObject target NetSuite data model object which to write field value to
     * @param fieldDesc field descriptor
     * @param customFieldMap map of native custom field objects by names
     * @param replace specifies whether to forcibly replace a field's value
     * @param nullFieldNames collection to register null'ed fields
     * @param value value to be written, can be <code>null</code>
     */
    protected void writeCustomField(Object nsObject, CustomFieldDesc fieldDesc, Map<String, Object> customFieldMap,
            boolean replace, Collection<String> nullFieldNames, Object value) {

        NsRef ref = fieldDesc.getCustomizationRef();
        CustomFieldRefType customFieldRefType = fieldDesc.getCustomFieldType();

        // Create custom field list wrapper if required
        Object customFieldListWrapper = Beans.getSimpleProperty(nsObject, CUSTOM_FIELD_LIST);
        if (customFieldListWrapper == null) {
            customFieldListWrapper = basicMetaData.createInstance(CUSTOM_FIELD_LIST_UPPER_CASE_NAME);
            Beans.setSimpleProperty(nsObject, CUSTOM_FIELD_LIST, customFieldListWrapper);
        }
        List<Object> customFieldList = (List<Object>) Beans.getSimpleProperty(customFieldListWrapper, CUSTOM_FIELD);

        Object customField = customFieldMap.get(ref.getScriptId());
        Converter valueConverter = getValueConverter(fieldDesc);

        Object targetValue = valueConverter.convertToDatum(value);

        if (targetValue == null) {
            if (replace && customField != null && customFieldList != null) {
                customFieldList.remove(customField);
                nullFieldNames.add(fieldDesc.getName());
            }
        } else {
            if (customField == null) {
                // Custom field instance doesn't exist,
                // create new instance and set identifiers
                customField = basicMetaData.createInstance(customFieldRefType.getTypeName());
                Beans.setSimpleProperty(customField, SCRIPT_ID, ref.getScriptId());
                Beans.setSimpleProperty(customField, INTERNAL_ID, ref.getInternalId());

                customFieldList.add(customField);
                customFieldMap.put(ref.getScriptId(), customField);
            }

            Beans.setSimpleProperty(customField, VALUE, targetValue);
        }
    }

    /**
     * Write a value to a simple field which is defined by NetSuite standard data model.
     *
     * @param nsObject target NetSuite data model object which to write field value to
     * @param fieldDesc field descriptor
     * @param replace specifies whether to forcibly replace a field's value
     * @param nullFieldNames collection to register null'ed fields
     * @param value value to be written, can be <code>null</code>
     */
    protected void writeSimpleField(Object nsObject, SimpleFieldDesc fieldDesc, boolean replace,
            Collection<String> nullFieldNames, Object value) {

        Converter valueConverter = getValueConverter(fieldDesc);

        Object targetValue = valueConverter.convertToDatum(value);

        if (targetValue == null && replace) {
            Beans.setSimpleProperty(nsObject, fieldDesc.getPropertyName(), null);
            nullFieldNames.add(fieldDesc.getName());
        } else {
            Beans.setSimpleProperty(nsObject, fieldDesc.getPropertyName(), targetValue);
        }
    }

    /**
     * Determine value class for given custom field type.
     *
     * @param customFieldRefType custom field type
     * @return value class or {@code null} for
     * {@link CustomFieldRefType#SELECT} and {@link CustomFieldRefType#MULTI_SELECT} types
     */
    protected Class<?> getCustomFieldValueConverterTargetClass(CustomFieldRefType customFieldRefType) {
        switch (customFieldRefType) {
        case BOOLEAN:
            return Boolean.class;
        case STRING:
            return String.class;
        case LONG:
            return Long.class;
        case DOUBLE:
            return Double.class;
        case DATE:
            return XMLGregorianCalendar.class;
        case SELECT:
            return getPicklistClass();
        case MULTI_SELECT:
        default:
            return null;
        }
    }

    /**
     * Get value converter for given field descriptor.
     *
     * @param fieldDesc field descriptor
     * @return value converter
     */
    public Converter<?, ?> getValueConverter(FieldDesc fieldDesc) {
        Class<?> valueClass;
        if (fieldDesc instanceof CustomFieldDesc) {
            CustomFieldDesc customFieldDesc = (CustomFieldDesc) fieldDesc;
            CustomFieldRefType customFieldRefType = customFieldDesc.getCustomFieldType();
            valueClass = getCustomFieldValueConverterTargetClass(customFieldRefType);
        } else {
            valueClass = fieldDesc.getValueType();
        }

        return valueClass != null ? getValueConverter(valueClass) : getValueConverter((Class<?>) null);
    }

    public Class<?> getPicklistClass() {
        try {
            return Class.forName("com.netsuite.webservices.v" + apiVersion.replace('.', '_') + ".platform.core.ListOrRecordRef");
        } catch (ClassNotFoundException e) {
            // ignore
        }
        return null;
    }

    /**
     * Get value converter for given class.
     *
     * <p>
     * Converters are created on demand and cached.
     *
     * @param valueClass value class
     * @return value converter or {@code null}
     */
    public Converter<?, ?> getValueConverter(Class<?> valueClass) {
        return valueConverterCache.computeIfAbsent(valueClass, this::createValueConverter);
    }

    /**
     * Create new instance of value converter for given class.
     *
     * @param valueClass value class
     * @return value converter or {@code null}
     */
    protected Converter<?, ?> createValueConverter(Class<?> valueClass) {
        return ConverterFactory.getValueConverter(valueClass, objectMapper);
    }

}
