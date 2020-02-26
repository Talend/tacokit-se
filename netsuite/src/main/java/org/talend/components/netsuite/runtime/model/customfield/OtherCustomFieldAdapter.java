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
package org.talend.components.netsuite.runtime.model.customfield;

import org.talend.components.netsuite.runtime.model.BasicRecordType;

import com.netsuite.webservices.v2019_2.setup.customization.OtherCustomField;

/**
 * Custom field adapter for {@link BasicRecordType#ENTITY_CUSTOM_FIELD} type.
 */
public class OtherCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    public OtherCustomFieldAdapter() {
        super(BasicRecordType.OTHER_CUSTOM_FIELD);
    }

    @Override
    public boolean appliesTo(String recordTypeName, T field) {
        if (field instanceof OtherCustomField) {
            String fieldTypeName = ((OtherCustomField) field).getRecType().getName();
            return recordTypeName.equalsIgnoreCase(fieldTypeName);
        } else
            return false;
    }

    @Override
    public String getPropertyName(String recordType) {
        throw new UnsupportedOperationException("getPropertyName() is not allowed to be called here");
    }

    @Override
    public CustomFieldRefType apply(T field) {
        return getFieldType(field);
    }

}
