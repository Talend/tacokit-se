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
package org.talend.components.netsuite.runtime.client.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.netsuite.runtime.NetSuiteErrorCode;
import org.talend.components.netsuite.runtime.NsObjectTransducer;
import org.talend.components.netsuite.runtime.client.MetaDataSource;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.client.NsRef;
import org.talend.components.netsuite.runtime.client.NsSearchResult;
import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.CustomRecordTypeInfo;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.runtime.model.RefType;
import org.talend.components.netsuite.runtime.model.SearchRecordTypeDesc;
import org.talend.components.netsuite.runtime.model.beans.BeanInfo;
import org.talend.components.netsuite.runtime.model.beans.Beans;
import org.talend.components.netsuite.runtime.model.beans.PropertyInfo;
import org.talend.components.netsuite.runtime.model.search.SearchFieldAdapter;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorName;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorType;
import org.talend.components.netsuite.runtime.model.search.SearchFieldType;
import org.talend.components.netsuite.service.Messages;

/**
 * Responsible for building of NetSuite search record.
 *
 * <p>
 * Example:
 *
 * <pre>
 *     NetSuiteClientService clientService = ...;
 *
 *     SearchQuery s = clientService.newSearch();
 *     s.target("Account");
 *     s.condition(new SearchCondition("Type", "List.anyOf", Arrays.asList("bank")));
 *     s.condition(new SearchCondition("Balance", "Double.greaterThanOrEqualTo", Arrays.asList("10000.0", "")));
 *
 *     SearchResultSet rs = s.search();
 * </pre>
 *
 * @see NetSuiteClientService#search(Object)
 */
public class SearchQuery<SearchT, RecT> {

    private static final String LIST_ANY_OF = "List.anyOf";

    private static final String SEARCH_CUSTOM_FIELD_LIST = "SearchCustomFieldList";

    private static final String CONDITION = "condition";

    private static final String BASIC = "basic";

    private static final String SAVED_SEARCH_ID = "savedSearchId";

    private NetSuiteClientService<?> clientService;

    private Messages i18n;

    private MetaDataSource metaDataSource;

    /** Name of target record type. */
    private String recordTypeName;

    /** Meta information for target record type. */
    private RecordTypeInfo recordTypeInfo;

    /** Descriptor of search record. */
    private SearchRecordTypeDesc searchRecordTypeDesc;

    private SearchT search; // search class' instance

    private SearchT searchBasic; // search basic class' instance

    private SearchT searchAdvanced; // search advanced class' instance

    private String savedSearchId;

    /** List of custom search fields. */
    private List<Object> customFieldList = new ArrayList<>();

    public SearchQuery(NetSuiteClientService<?> clientService, MetaDataSource metaDataSource, Messages i18n,
            String recordTypeName, boolean customizationEnabled) {
        this.clientService = clientService;
        this.i18n = i18n;
        this.metaDataSource = metaDataSource != null ? metaDataSource : clientService.getMetaDataSource();

        this.recordTypeName = recordTypeName;

        recordTypeInfo = metaDataSource.getRecordType(recordTypeName, customizationEnabled);
        searchRecordTypeDesc = metaDataSource.getSearchRecordType(recordTypeName, customizationEnabled);

        // search not found or not supported
        if (searchRecordTypeDesc == null) {
            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.OPERATION_NOT_SUPPORTED),
                    i18n.searchRecordNotFound(recordTypeName));
        }
    }

    public SearchQuery<?, ?> savedSearchId(String savedSearchId) {
        this.savedSearchId = savedSearchId;
        return this;
    }

    /**
     * Performs lazy initialization of search query.
     */
    @SuppressWarnings("unchecked")
    private void initSearch() {
        if (searchBasic != null) {
            return;
        }
        try {
            // get a search class instance
            if (searchRecordTypeDesc.getSearchClass() != null) {
                search = (SearchT) searchRecordTypeDesc.getSearchClass().newInstance();
            }

            // get a advanced search class instance and set 'savedSearchId' into it
            searchAdvanced = null;
            if (StringUtils.isNotEmpty(savedSearchId)) {
                if (searchRecordTypeDesc.getSearchAdvancedClass() != null) {
                    searchAdvanced = (SearchT) searchRecordTypeDesc.getSearchAdvancedClass().newInstance();
                    Beans.setProperty(searchAdvanced, SAVED_SEARCH_ID, savedSearchId);
                } else {
                    throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.OPERATION_NOT_SUPPORTED),
                            i18n.advancedSearchNotAllowed(recordTypeName));
                }
            }

            // basic search class not found or supported
            if (searchRecordTypeDesc.getSearchBasicClass() == null) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.OPERATION_NOT_SUPPORTED),
                        i18n.searchNotAllowed(recordTypeName));
            }

            // get a basic search class instance
            searchBasic = (SearchT) searchRecordTypeDesc.getSearchBasicClass().newInstance();

        } catch (InstantiationException | IllegalAccessException e) {
            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR), e.getMessage(), e);
        }
    }

    /**
     * Add condition for search query.
     *
     * @param condition condition to be added
     * @return
     * @throws NetSuiteException if an error occurs during adding of condition
     */
    public SearchQuery<?, ?> condition(SearchCondition condition) {
        initSearch();
        BeanInfo searchMetaData = Beans.getBeanInfo(searchRecordTypeDesc.getSearchBasicClass());
        String fieldName = Beans.toInitialLower(condition.getFieldName());
        PropertyInfo propertyInfo = searchMetaData.getProperty(fieldName);

        if (propertyInfo != null) {
            Object searchField = processConditionForSearchRecord(searchBasic, condition);
            Beans.setProperty(searchBasic, fieldName, searchField);
        } else {
            SearchFieldOperatorName operatorQName = new SearchFieldOperatorName(condition.getOperatorName());
            String dataType = operatorQName.getDataType();
            SearchFieldType searchFieldType;
            try {
                searchFieldType = SearchFieldOperatorType.getSearchFieldType(dataType);
            } catch (UnsupportedOperationException e) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.OPERATION_NOT_SUPPORTED),
                        i18n.invalidDataType(dataType));
            }

            Object searchField = processCondition(searchFieldType, condition);
            customFieldList.add(searchField);
        }

        return this;
    }

    /**
     * Process search condition and update search record.
     *
     * @param searchRecord search record
     * @param condition condition
     * @return search field built for this condition
     * @throws NetSuiteException if an error occurs during processing of condition
     */
    private Object processConditionForSearchRecord(Object searchRecord, SearchCondition condition) {
        String fieldName = Beans.toInitialLower(condition.getFieldName());
        BeanInfo beanInfo = Beans.getBeanInfo(searchRecord.getClass());
        Class<?> searchFieldClass = beanInfo.getProperty(fieldName).getWriteType();
        SearchFieldType fieldType = SearchFieldType.getByFieldTypeName(searchFieldClass.getSimpleName());
        return processCondition(fieldType, condition);
    }

    /**
     * Process search condition and update search record.
     *
     * @param fieldType type of search field
     * @param condition condition
     * @return search field built for this condition
     */
    private Object processCondition(SearchFieldType fieldType, SearchCondition condition) {
        String searchFieldName = Beans.toInitialLower(condition.getFieldName());
        String searchOperator = condition.getOperatorName();
        List<String> searchValue = condition.getValues();
        SearchFieldAdapter<?> fieldAdapter = metaDataSource.getBasicMetaData().getSearchFieldAdapter(fieldType);
        return fieldAdapter.populate(searchFieldName, searchOperator, searchValue);
    }

    /**
     * Finalize building of search query and get built NetSuite's search record object.
     *
     * @return
     * @throws NetSuiteException if an error occurs during updating of search record
     */
    @SuppressWarnings("unchecked")
    private SearchT toNativeQuery() {
        initSearch();

        BasicRecordType basicRecordType = BasicRecordType.getByType(searchRecordTypeDesc.getType());
        if (BasicRecordType.TRANSACTION == basicRecordType) {
            SearchFieldAdapter<?> fieldAdapter = metaDataSource.getBasicMetaData().getSearchFieldAdapter(SearchFieldType.SELECT);
            Object searchTypeField = fieldAdapter.populate(LIST_ANY_OF, Arrays.asList(recordTypeInfo.getRecordType().getType()));
            Beans.setProperty(searchBasic, NsObjectTransducer.TYPE, searchTypeField);

        } else if (BasicRecordType.CUSTOM_RECORD == basicRecordType) {
            CustomRecordTypeInfo customRecordTypeInfo = (CustomRecordTypeInfo) recordTypeInfo;
            NsRef customizationRef = customRecordTypeInfo.getCustomizationRef();

            Object recType = metaDataSource.getBasicMetaData().createInstance(RefType.CUSTOMIZATION_REF.getTypeName());
            Beans.setProperty(recType, NsObjectTransducer.SCRIPT_ID, customizationRef.getScriptId());
            // Avoid using Internal Id for custom records
            Beans.setProperty(recType, NsObjectTransducer.TYPE, Beans.getEnumAccessor(
                    (Class<Enum<?>>) Beans.getBeanInfo(recType.getClass()).getProperty(NsObjectTransducer.TYPE).getWriteType())
                    .getEnumValue(customizationRef.getType()));

            Beans.setProperty(searchBasic, NsObjectTransducer.REC_TYPE, recType);
        }

        // Set custom fields
        if (!customFieldList.isEmpty()) {
            Object customFieldListWrapper = metaDataSource.getBasicMetaData().createInstance(SEARCH_CUSTOM_FIELD_LIST);
            List<Object> customFields = (List<Object>) Beans.getProperty(customFieldListWrapper, NsObjectTransducer.CUSTOM_FIELD);
            for (Object customField : customFieldList) {
                customFields.add(customField);
            }
            Beans.setProperty(searchBasic, NsObjectTransducer.CUSTOM_FIELD_LIST, customFieldListWrapper);
        }

        SearchT searchRecord;
        if (searchRecordTypeDesc.getSearchClass() != null) {
            Beans.setProperty(search, BASIC, searchBasic);
            searchRecord = search;
            if (searchAdvanced != null) {
                Beans.setProperty(searchAdvanced, CONDITION, search);
                searchRecord = searchAdvanced;
            }
        } else {
            searchRecord = searchBasic;
        }

        return searchRecord;
    }

    /**
     * Finalize building of search query and perform search.
     *
     * @return result set
     */
    public NsSearchResult<RecT> search() {
        Object searchRecord = toNativeQuery();
        NsSearchResult<RecT> result = clientService.search(searchRecord);
        if (!result.isSuccess()) {
            NetSuiteClientService.checkError(result.getStatus());
        }
        return result;
    }

}
