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

import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.beans.Beans;

import java.util.List;

/**
 * Search field adapter for {@code SearchMultiSelectField} and {@code SearchMultiSelectCustomField}.
 */
public class SearchMultiSelectFieldAdapter<T> extends SearchFieldAdapter<T> {

    public SearchMultiSelectFieldAdapter(BasicMetaData metaData, SearchFieldType fieldType, Class<T> fieldClass) {
        super(metaData, fieldType, fieldClass);
    }

    @Override
    public T populate(T fieldObject, String internalId, String operatorName, List<String> values) {
        T nsObject = fieldObject != null ? fieldObject : createField(internalId);

        List<Object> searchValue = (List<Object>) Beans.getSimpleProperty(nsObject, SEARCH_VALUE);

        for (int i = 0; i < values.size(); i++) {
            String temp;
            if ((temp = values.get(i)) != null && !temp.isEmpty()) {
                Object item;
                if (SearchFieldType.MULTI_SELECT == fieldType) {
                    item = metaData.createInstance("RecordRef");
                } else if (SearchFieldType.CUSTOM_MULTI_SELECT == fieldType) {
                    item = metaData.createInstance("ListOrRecordRef");
                    Beans.setSimpleProperty(item, "name", temp);
                } else {
                    continue;
                }
                Beans.setSimpleProperty(item, "internalId", temp);
                searchValue.add(item);
            }
        }

        Beans.setSimpleProperty(nsObject, OPERATOR,
                metaData.getSearchFieldOperatorByName(fieldType.getFieldTypeName(), operatorName));

        return nsObject;
    }
}
