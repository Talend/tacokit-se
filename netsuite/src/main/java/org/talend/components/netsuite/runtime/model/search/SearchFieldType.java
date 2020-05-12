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
package org.talend.components.netsuite.runtime.model.search;

import java.util.HashMap;
import java.util.Map;

/**
 * Type of search field.
 */
public enum SearchFieldType {
    BOOLEAN("SearchBooleanField"),
    DATE("SearchDateField"),
    DOUBLE("SearchDoubleField"),
    LONG("SearchLongField"),
    MULTI_SELECT("SearchMultiSelectField"),
    SELECT("SearchEnumMultiSelectField"),
    STRING("SearchStringField"),
    TEXT_NUMBER("SearchTextNumberField"),
    CUSTOM_BOOLEAN("SearchBooleanCustomField"),
    CUSTOM_DATE("SearchDateCustomField"),
    CUSTOM_DOUBLE("SearchDoubleCustomField"),
    CUSTOM_LONG("SearchLongCustomField"),
    CUSTOM_MULTI_SELECT("SearchMultiSelectCustomField"),
    CUSTOM_SELECT("SearchEnumMultiSelectCustomField"),
    CUSTOM_STRING("SearchStringCustomField");

    /** Short name of NetSuite's native search field data object type. */
    private final String fieldTypeName;

    /** Table of search operators by search field types. */
    private static final Map<SearchFieldType, SearchFieldOperatorType> FIELD_OPERATOR_MAP;

    static {
        FIELD_OPERATOR_MAP = new HashMap<>();
        FIELD_OPERATOR_MAP.put(SearchFieldType.BOOLEAN, SearchFieldOperatorType.BOOLEAN);
        FIELD_OPERATOR_MAP.put(SearchFieldType.DOUBLE, SearchFieldOperatorType.DOUBLE);
        FIELD_OPERATOR_MAP.put(SearchFieldType.LONG, SearchFieldOperatorType.LONG);
        FIELD_OPERATOR_MAP.put(SearchFieldType.STRING, SearchFieldOperatorType.STRING);
        FIELD_OPERATOR_MAP.put(SearchFieldType.TEXT_NUMBER, SearchFieldOperatorType.TEXT_NUMBER);
        FIELD_OPERATOR_MAP.put(SearchFieldType.CUSTOM_BOOLEAN, SearchFieldOperatorType.BOOLEAN);
        FIELD_OPERATOR_MAP.put(SearchFieldType.CUSTOM_DOUBLE, SearchFieldOperatorType.DOUBLE);
        FIELD_OPERATOR_MAP.put(SearchFieldType.CUSTOM_LONG, SearchFieldOperatorType.LONG);
        FIELD_OPERATOR_MAP.put(SearchFieldType.CUSTOM_STRING, SearchFieldOperatorType.STRING);

        FIELD_OPERATOR_MAP.put(SearchFieldType.MULTI_SELECT, SearchFieldOperatorType.MULTI_SELECT);
        FIELD_OPERATOR_MAP.put(SearchFieldType.SELECT, SearchFieldOperatorType.ENUM_MULTI_SELECT);
        FIELD_OPERATOR_MAP.put(SearchFieldType.CUSTOM_MULTI_SELECT, SearchFieldOperatorType.MULTI_SELECT);
        FIELD_OPERATOR_MAP.put(SearchFieldType.CUSTOM_SELECT, SearchFieldOperatorType.ENUM_MULTI_SELECT);
    }

    SearchFieldType(String fieldTypeName) {
        this.fieldTypeName = fieldTypeName;
    }

    public String getFieldTypeName() {
        return fieldTypeName;
    }

    /**
     * Get search field type for given search field type name.
     *
     * @param fieldTypeName name of search field type
     * @return search field type
     */
    public static SearchFieldType getByFieldTypeName(String fieldTypeName) {
        for (SearchFieldType value : values()) {
            if (value.fieldTypeName.equals(fieldTypeName)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown field type name: " + fieldTypeName);
    }

    /**
     * Get search operator type for given search field type.
     *
     * @param searchFieldType search field type
     * @return search operator type or {@code null}
     */
    public static SearchFieldOperatorType getOperatorType(final SearchFieldType searchFieldType) {
        if (searchFieldType == null) {
            throw new IllegalArgumentException("Your Search Field Type cannot be null");
        }
        if (searchFieldType == CUSTOM_DATE || searchFieldType == SearchFieldType.DATE) {
            return null;
        }
        return FIELD_OPERATOR_MAP.get(searchFieldType);
    }
}
