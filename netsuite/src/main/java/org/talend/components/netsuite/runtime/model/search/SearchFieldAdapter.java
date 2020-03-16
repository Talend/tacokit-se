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
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.beans.BeanInfo;
import org.talend.components.netsuite.runtime.model.beans.Beans;

import java.util.List;

/**
 * Responsible for handling of search fields and populating of search field with data.
 */

@AllArgsConstructor
public abstract class SearchFieldAdapter<T> {

    private static final String INTERNAL_ID = "internalId";

    private static final String SCRIPT_ID = "scriptId";

    protected static final String SEARCH_VALUE = "searchValue";

    protected static final String SEARCH_VALUE_2 = "searchValue2";

    protected static final String OPERATOR = "operator";

    /** Used to get meta data of NetSuite data model. */
    protected BasicMetaData metaData;

    /** Type of search field which this adapter responsible for. */
    protected SearchFieldType fieldType;

    /** Class of search field data object type. */
    protected Class<T> fieldClass;

    public SearchFieldType getFieldType() {
        return fieldType;
    }

    /**
     * Populate search field with data.
     *
     * @param operatorName name of search operator to be applied
     * @param values search values to be applied
     * @return search field object
     */
    public T populate(String operatorName, List<String> values) {
        return populate(null, null, operatorName, values);
    }

    /**
     * Populate search field with data.
     *
     * @param internalId internal identifier to be applied to search field
     * @param operatorName name of search operator to be applied
     * @param values search values to be applied
     * @return search field object
     */
    public T populate(String internalId, String operatorName, List<String> values) {
        return populate(null, internalId, operatorName, values);
    }

    /**
     * Populate search field with data.
     *
     * @param fieldObject search field object to populate, can be {@code null}
     * @param internalId internal identifier to be applied to search field
     * @param operatorName name of search operator to be applied
     * @param values search values to be applied
     * @return search field object
     */
    public abstract T populate(T fieldObject, String internalId, String operatorName, List<String> values);

    /**
     * Create instance of NetSuite's search field.
     *
     * @param internalId internal identifier to be applied to a search field
     * @return search field object
     * @throws NetSuiteException if an error occurs during creation of a search field
     */
    protected T createField(String internalId) throws NetSuiteException {
        try {
            BeanInfo fieldTypeMetaData = Beans.getBeanInfo(fieldClass);
            T searchField = fieldClass.newInstance();
            if (fieldTypeMetaData.getProperty(SCRIPT_ID) != null && internalId != null) {
                Beans.setProperty(searchField, SCRIPT_ID, internalId);
                return searchField;
            }

            if (fieldTypeMetaData.getProperty(INTERNAL_ID) != null && internalId != null) {
                Beans.setProperty(searchField, INTERNAL_ID, internalId);
            }
            return searchField;
        } catch (IllegalAccessException | IllegalArgumentException | InstantiationException e) {
            throw new NetSuiteException(e.getMessage(), e);
        }
    }

}
