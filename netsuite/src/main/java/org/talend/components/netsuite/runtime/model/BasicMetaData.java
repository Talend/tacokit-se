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
package org.talend.components.netsuite.runtime.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.talend.components.netsuite.runtime.NetSuiteErrorCode;
import org.talend.components.netsuite.runtime.NsObjectTransducer;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.model.beans.BeanInfo;
import org.talend.components.netsuite.runtime.model.beans.Beans;
import org.talend.components.netsuite.runtime.model.beans.PropertyInfo;
import org.talend.components.netsuite.runtime.model.customfield.CrmCustomFieldAdapter;
import org.talend.components.netsuite.runtime.model.customfield.CustomFieldAdapter;
import org.talend.components.netsuite.runtime.model.customfield.CustomFieldRefType;
import org.talend.components.netsuite.runtime.model.customfield.DefaultCustomFieldAdapter;
import org.talend.components.netsuite.runtime.model.customfield.EntityCustomFieldAdapter;
import org.talend.components.netsuite.runtime.model.customfield.ItemCustomFieldAdapter;
import org.talend.components.netsuite.runtime.model.customfield.ItemOptionCustomFieldAdapter;
import org.talend.components.netsuite.runtime.model.customfield.OtherCustomFieldAdapter;
import org.talend.components.netsuite.runtime.model.customfield.TransactionBodyCustomFieldAdapter;
import org.talend.components.netsuite.runtime.model.customfield.TransactionColumnCustomFieldAdapter;
import org.talend.components.netsuite.runtime.model.search.SearchBooleanFieldAdapter;
import org.talend.components.netsuite.runtime.model.search.SearchBooleanFieldOperator;
import org.talend.components.netsuite.runtime.model.search.SearchDateFieldAdapter;
import org.talend.components.netsuite.runtime.model.search.SearchDoubleFieldAdapter;
import org.talend.components.netsuite.runtime.model.search.SearchEnumMultiSelectFieldAdapter;
import org.talend.components.netsuite.runtime.model.search.SearchFieldAdapter;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorName;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorType;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorTypeDesc;
import org.talend.components.netsuite.runtime.model.search.SearchFieldType;
import org.talend.components.netsuite.runtime.model.search.SearchLongFieldAdapter;
import org.talend.components.netsuite.runtime.model.search.SearchMultiSelectFieldAdapter;
import org.talend.components.netsuite.runtime.model.search.SearchStringFieldAdapter;
import org.talend.components.netsuite.runtime.model.search.SearchTextNumberFieldAdapter;
import org.talend.components.netsuite.service.Messages;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

/**
 * Provides information about NetSuite standard data model.
 */
public abstract class BasicMetaData {

    protected Map<String, Class<?>> typeMap = new HashMap<>();

    protected Map<String, Class<?>> searchFieldMap = new HashMap<>();

    protected Map<SearchFieldOperatorType, SearchFieldOperatorTypeDesc<?>> searchFieldOperatorTypeMap = new HashMap<>();

    /** Table of search field adapters by search field types. */
    protected Map<SearchFieldType, SearchFieldAdapter<?>> searchFieldAdapterMap = new HashMap<>();

    /** Table of custom field adapters by custom field types. */
    protected Map<BasicRecordType, CustomFieldAdapter<?>> customFieldAdapterMap = new HashMap<>();

    private Messages i18n;

    protected BasicMetaData() {
        bindCustomFieldAdapters();
    }

    public void setI18n(Messages i18n) {
        this.i18n = i18n;
    }

    /**
     * Bind data object types to type names for given type hierarchy.
     *
     * @param baseClass base class of type hierarchy
     */
    protected void bindTypeHierarchy(Class<?> baseClass) {
        Set<Class<?>> classes = new HashSet<>();
        TypeUtils.collectXmlTypes(baseClass, baseClass, classes);
        for (Class<?> clazz : classes) {
            bindType(clazz, null);
        }
    }

    /**
     * Bind given data type class to type name.
     *
     * @param typeClass data type class
     * @param typeName type name
     */
    protected void bindType(Class<?> typeClass, String typeName) {
        String typeNameToRegister = typeName != null ? typeName : typeClass.getSimpleName();
        if (typeMap.containsKey(typeNameToRegister)) {
            Class<?> clazz = typeMap.get(typeNameToRegister);
            if (clazz == typeClass) {
                return;
            } else {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR), i18n
                        .bindingTypeError(typeNameToRegister, typeClass.getName(), typeMap.get(typeNameToRegister).getName()));
            }
        }
        typeMap.put(typeNameToRegister, typeClass);
    }

    /**
     * Bind search field classes to search field type names.
     *
     * @param searchFieldClasses search field classes to be registered and bound
     */
    protected void bindSearchFields(Collection<Class<?>> searchFieldClasses) {
        for (Class<?> entry : searchFieldClasses) {
            String searchFieldTypeName = entry.getSimpleName();

            searchFieldMap.put(searchFieldTypeName, entry);

            // Register an adapter for this search field type.
            bindSearchFieldAdapter(searchFieldTypeName);
        }
    }

    /**
     * Bind search operator type descriptors.
     *
     * @param searchFieldOperatorTypes search operator type descriptors to be registered and bound
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void bindSearchFieldOperatorTypes(Collection<SearchFieldOperatorTypeDesc> searchFieldOperatorTypes) {

        searchFieldOperatorTypes.stream().forEach(type -> searchFieldOperatorTypeMap.put(type.getOperatorType(), type));
        searchFieldOperatorTypeMap.put(SearchFieldOperatorType.BOOLEAN,
                new SearchFieldOperatorTypeDesc(SearchFieldOperatorType.BOOLEAN, SearchBooleanFieldOperator.class, null, null));

    }

    /**
     * Bind search field adapter for given search field type.
     *
     * @param searchFieldTypeName name of search field type which to bind adapter for
     */
    protected void bindSearchFieldAdapter(String searchFieldTypeName) {
        SearchFieldType searchFieldType = SearchFieldType.getByFieldTypeName(searchFieldTypeName);
        bindSearchFieldAdapter(searchFieldType);
    }

    /**
     * Bind search field adapter for given search field type.
     *
     * @param searchFieldType search field type which to bind adapter for
     */
    protected void bindSearchFieldAdapter(final SearchFieldType searchFieldType) {
        Class<?> fieldClass = getSearchFieldClass(searchFieldType.getFieldTypeName());
        SearchFieldAdapter<?> fieldAdapter;
        switch (searchFieldType) {
        case BOOLEAN:
        case CUSTOM_BOOLEAN:
            fieldAdapter = new SearchBooleanFieldAdapter<>(this, searchFieldType, fieldClass);
            break;
        case STRING:
        case CUSTOM_STRING:
            fieldAdapter = new SearchStringFieldAdapter<>(this, searchFieldType, fieldClass);
            break;
        case TEXT_NUMBER:
            fieldAdapter = new SearchTextNumberFieldAdapter<>(this, searchFieldType, fieldClass);
            break;
        case LONG:
        case CUSTOM_LONG:
            fieldAdapter = new SearchLongFieldAdapter<>(this, searchFieldType, fieldClass);
            break;
        case DOUBLE:
        case CUSTOM_DOUBLE:
            fieldAdapter = new SearchDoubleFieldAdapter<>(this, searchFieldType, fieldClass);
            break;
        case DATE:
        case CUSTOM_DATE:
            fieldAdapter = new SearchDateFieldAdapter<>(this, searchFieldType, fieldClass);
            break;
        case MULTI_SELECT:
        case CUSTOM_MULTI_SELECT:
            fieldAdapter = new SearchMultiSelectFieldAdapter<>(this, searchFieldType, fieldClass);
            break;
        case SELECT:
        case CUSTOM_SELECT:
            fieldAdapter = new SearchEnumMultiSelectFieldAdapter<>(this, searchFieldType, fieldClass);
            break;
        default:
            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR),
                    i18n.invalidSearchType(searchFieldType.getFieldTypeName()));
        }
        searchFieldAdapterMap.put(searchFieldType, fieldAdapter);
    }

    /**
     * Bind search field adapters for custom field types.
     */
    protected void bindCustomFieldAdapters() {
        bindCustomFieldAdapter(new CrmCustomFieldAdapter<>());
        bindCustomFieldAdapter(new EntityCustomFieldAdapter<>());
        bindCustomFieldAdapter(new ItemCustomFieldAdapter<>());
        bindCustomFieldAdapter(new ItemOptionCustomFieldAdapter<>());
        bindCustomFieldAdapter(new OtherCustomFieldAdapter<>());
        bindCustomFieldAdapter(new TransactionBodyCustomFieldAdapter<>());
        bindCustomFieldAdapter(new TransactionColumnCustomFieldAdapter<>());
        bindCustomFieldAdapter(new DefaultCustomFieldAdapter<>(BasicRecordType.CUSTOM_LIST, false));
        bindCustomFieldAdapter(new DefaultCustomFieldAdapter<>(BasicRecordType.CUSTOM_RECORD, true));
        bindCustomFieldAdapter(new DefaultCustomFieldAdapter<>(BasicRecordType.CUSTOM_RECORD_TYPE, true));
        bindCustomFieldAdapter(new DefaultCustomFieldAdapter<>(BasicRecordType.CUSTOM_TRANSACTION_TYPE, true));
        bindCustomFieldAdapter(new DefaultCustomFieldAdapter<>(BasicRecordType.ITEM_NUMBER_CUSTOM_FIELD, false));
    }

    /**
     * Bind a custom field adapter.
     *
     * @param adapter custom field adapter to be registered and bound to search field type
     */
    protected void bindCustomFieldAdapter(CustomFieldAdapter<?> adapter) {
        customFieldAdapterMap.put(adapter.getType(), adapter);
    }

    /**
     * Get data object type class for given name of type.
     *
     * @param typeName name of type
     * @return class for type name or {@code null} if type was not found
     */
    public Class<?> getTypeClass(String typeName) {
        // First, look for type in table of types.
        Class<?> clazz = typeMap.get(typeName);
        if (clazz != null) {
            return clazz;
        }
        // Then, look to record types.
        RecordTypeDesc recordType = getRecordType(typeName);
        if (recordType != null) {
            return recordType.getRecordClass();
        }
        return null;
    }

    /**
     * Get type descriptor for given name of type.
     *
     * @param typeName name of type
     * @return type descriptor for type name or {@code null} if type was not found
     */
    public TypeDesc getTypeInfo(String typeName) {
        Class<?> clazz = getTypeClass(typeName);
        return Optional.ofNullable(clazz).map(this::getTypeInfo)
                .orElseThrow(() -> new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.OPERATION_NOT_SUPPORTED),
                        i18n.recordTypeNotFound(typeName)));
    }

    /**
     * Get type descriptor for given type class.
     *
     * @param clazz class of type
     * @return type descriptor
     */
    public TypeDesc getTypeInfo(Class<?> clazz) {
        BeanInfo beanInfo = Beans.getBeanInfo(clazz);
        List<PropertyInfo> propertyInfos = beanInfo.getProperties();

        List<FieldDesc> fields = new ArrayList<>(propertyInfos.size());

        for (PropertyInfo propertyInfo : propertyInfos) {
            String fieldName = propertyInfo.getName();
            Class<?> fieldValueType = propertyInfo.getReadType();

            // Skip 'class' property
            if ((propertyInfo.getName().equals("class") && fieldValueType == Class.class)) {
                continue;
            }

            boolean isKey = isKeyField(propertyInfo);
            SimpleFieldDesc fieldDesc = new SimpleFieldDesc(fieldName, fieldValueType, isKey, true);
            fieldDesc.setPropertyName(propertyInfo.getName());
            fields.add(fieldDesc);
        }

        return new TypeDesc(clazz.getSimpleName(), clazz, fields);
    }

    /**
     * Get record type descriptor for given record type name.
     *
     * <p>
     * Implementation is provided by concrete version of NetSuite runtime.
     *
     * @param typeName name of record type
     * @return record type descriptor or {@code null} if given type doesn't match any known type
     */
    public abstract RecordTypeDesc getRecordType(String typeName);

    /**
     * Get record type descriptors of all available record types.
     *
     * @return record type descriptors
     */
    public abstract Collection<RecordTypeDesc> getRecordTypes();

    /**
     * Get search record type descriptor for given search record type name.
     *
     * @param searchRecordType search record type name
     * @return search record type descriptor or {@code null} if given type doesn't match any known type
     */
    public abstract SearchRecordTypeDesc getSearchRecordType(String searchRecordType);

    /**
     * Get search record type descriptor for given record type.
     *
     * @param recordType record type descriptor
     * @return search record type descriptor
     */
    public SearchRecordTypeDesc getSearchRecordType(RecordTypeDesc recordType) {
        SearchRecordTypeDesc searchRecordType = getSearchRecordType(recordType.getSearchRecordType());
        return searchRecordType;
    }

    /**
     * Get class for given search field type.
     *
     * @param searchFieldType search field type name
     * @return class or {@code null} if specified type doesn't match any known type
     */
    public Class<?> getSearchFieldClass(String searchFieldType) {
        return searchFieldMap.get(searchFieldType);
    }

    /**
     * Get search field operator for given search field type and operator name.
     *
     * @see SearchFieldType
     * @see SearchFieldOperatorName
     *
     * @param searchFieldTypeName search field type name
     * @param operatorName operator name
     * @return search field operator
     */
    public Object getSearchFieldOperatorByName(String searchFieldTypeName, String operatorName) {
        SearchFieldType fieldType = SearchFieldType.getByFieldTypeName(searchFieldTypeName);
        return getSearchFieldOperator(fieldType, operatorName);
    }

    /**
     * Get search field operator for given search field type and operator name.
     *
     * @see SearchFieldOperatorType
     * @see SearchFieldOperatorName
     *
     * @param fieldType search field type
     * @param operatorName operator name
     * @return search field operator
     */
    public Object getSearchFieldOperator(SearchFieldType fieldType, String operatorName) {
        SearchFieldOperatorName operatorQName = new SearchFieldOperatorName(operatorName);
        SearchFieldOperatorType operatorType = SearchFieldType.getOperatorType(fieldType);
        if (operatorType != null) {
            SearchFieldOperatorTypeDesc<?> def = searchFieldOperatorTypeMap.get(operatorType);
            return def.getOperator(operatorName);
        }
        for (SearchFieldOperatorTypeDesc<?> def : searchFieldOperatorTypeMap.values()) {
            if (def.hasOperator(operatorQName)) {
                return def.getOperator(operatorName);
            }
        }
        throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR),
                i18n.unknownSearchFieldOperator(fieldType.getFieldTypeName(), operatorName));
    }

    /**
     * Get names of all available search operators.
     *
     * @return search operators' names
     */
    public List<String> getSearchOperatorNames(FieldDesc fieldDesc) {
        SearchFieldOperatorType operator = SearchFieldType
                .getOperatorType(SearchFieldType.getByFieldTypeName(fieldDesc.getValueType().getSimpleName()));
        Function<String, Boolean> func = operator != null ? s -> s.equals(operator.getOperatorTypeName())
                : s -> s.equals(SearchFieldOperatorType.DATE.getOperatorTypeName())
                        || s.equals(SearchFieldOperatorType.PREDEFINED_DATE.getOperatorTypeName());
        return searchFieldOperatorTypeMap.entrySet().stream().filter(entry -> func.apply(entry.getKey().getOperatorTypeName()))
                .flatMap(entry -> entry.getValue().getOperatorNames().stream().map(SearchFieldOperatorName::getQualifiedName))
                .collect(collectingAndThen(toList(), Collections::unmodifiableList));
    }

    /**
     * Get search field adapter for given search field type.
     *
     * @param fieldType search field type
     * @return search field adapter
     */
    public SearchFieldAdapter<?> getSearchFieldAdapter(SearchFieldType fieldType) {
        return searchFieldAdapterMap.get(fieldType);
    }

    /**
     * Get custom field type for given record type and custom field.
     *
     * @param recordType record type name
     * @param customFieldType custom field record type
     * @param customField custom field instance
     * @return custom field type
     */
    @SuppressWarnings("unchecked")
    public CustomFieldRefType getCustomFieldRefType(String recordType, BasicRecordType customFieldType, Object customField) {
        CustomFieldAdapter customFieldAdapter = customFieldAdapterMap.get(customFieldType);
        if (customFieldAdapter.appliesTo(recordType, customField)) {
            return customFieldAdapter.apply(customField);
        }
        return null;
    }

    /**
     * Determine whether a given property of NetSuite data object type is key field.
     *
     * @param propertyInfo property descriptor to be checked
     * @return {@code true} if property is key field, {@false otherwise}
     */
    protected boolean isKeyField(PropertyInfo propertyInfo) {
        return NsObjectTransducer.INTERNAL_ID.equals(propertyInfo.getName())
                || NsObjectTransducer.EXTERNAL_ID.equals(propertyInfo.getName())
                || NsObjectTransducer.SCRIPT_ID.equals(propertyInfo.getName());
    }

    /**
     * Create an instance of given data object type.
     *
     * @param typeName name of type
     * @param <T> type of data object
     * @return data object
     */
    @SuppressWarnings("unchecked")
    public <T> T createInstance(String typeName) {
        Class<T> clazz = (Class<T>) getTypeClass(typeName);
        if (clazz == null) {
            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR),
                    i18n.cannotCreateInstance(typeName));
        }
        return TypeUtils.createInstance(clazz);
    }

}