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

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum SearchFieldOperatorType {
    BOOLEAN("Boolean", "SearchBooleanFieldOperator"),
    STRING("String", "SearchStringFieldOperator"),
    LONG("Long", "SearchLongFieldOperator"),
    DOUBLE("Double", "SearchDoubleFieldOperator"),
    DATE("Date", "SearchDateFieldOperator"),
    PREDEFINED_DATE("PredefinedDate", "SearchDate"),
    TEXT_NUMBER("TextNumber", "SearchTextNumberFieldOperator"),
    MULTI_SELECT("List", "SearchMultiSelectFieldOperator"),
    ENUM_MULTI_SELECT("List", "SearchEnumMultiSelectFieldOperator");

    private String dataType;

    private String operatorTypeName;

    public boolean dataTypeEquals(String dataType) {
        return this.dataType.equals(dataType);
    }

    public String getDataType() {
        return this.dataType;
    }

    public String getOperatorTypeName() {
        return operatorTypeName;
    }

    public static SearchFieldType getSearchFieldType(String dataType) {
        SearchFieldType searchFieldType;
        if (SearchFieldOperatorType.STRING.dataTypeEquals(dataType)) {
            searchFieldType = SearchFieldType.CUSTOM_STRING;
        } else if (SearchFieldOperatorType.BOOLEAN.dataTypeEquals(dataType)) {
            searchFieldType = SearchFieldType.CUSTOM_BOOLEAN;
        } else if (SearchFieldOperatorType.LONG.dataTypeEquals(dataType)) {
            searchFieldType = SearchFieldType.CUSTOM_LONG;
        } else if (SearchFieldOperatorType.DOUBLE.dataTypeEquals(dataType)) {
            searchFieldType = SearchFieldType.CUSTOM_DOUBLE;
        } else if (SearchFieldOperatorType.DATE.dataTypeEquals(dataType)
                || SearchFieldOperatorType.PREDEFINED_DATE.dataTypeEquals(dataType)) {
            searchFieldType = SearchFieldType.CUSTOM_DATE;
        } else if (SearchFieldOperatorType.MULTI_SELECT.dataTypeEquals(dataType)) {
            searchFieldType = SearchFieldType.CUSTOM_MULTI_SELECT;
        } else if (SearchFieldOperatorType.ENUM_MULTI_SELECT.dataTypeEquals(dataType)) {
            searchFieldType = SearchFieldType.CUSTOM_SELECT;
        } else {
            throw new UnsupportedOperationException("Invalid data type: " + dataType);
        }
        return searchFieldType;
    }
}
