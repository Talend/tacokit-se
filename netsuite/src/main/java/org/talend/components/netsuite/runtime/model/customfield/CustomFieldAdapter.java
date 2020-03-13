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

import lombok.AllArgsConstructor;
import lombok.Data;
import org.talend.components.netsuite.runtime.model.BasicRecordType;
import org.talend.components.netsuite.runtime.model.beans.Beans;

import java.util.Optional;

/**
 * Responsible for handling of custom field records and
 * adapting of NetSuite's custom field definition to internal typing.
 */
@Data
@AllArgsConstructor
public abstract class CustomFieldAdapter<T> {

    /** Type of custom field record. */
    protected BasicRecordType type;

    /**
     * Determine whether custom field is applicable to given record type.
     *
     * @param recordTypeName name of record type
     * @param field NetSuite's native custom field record object
     * @return {@code true} if custom field is applicable to specified record type, {@code false} otherwise
     */
    public boolean appliesTo(String recordTypeName, T field) {
        return Optional.ofNullable(getPropertyName(recordTypeName))
                .map(property -> (Boolean) Beans.getSimpleProperty(field, property)).orElse(false);
    }

    protected abstract String getPropertyName(String recordTypeName);

    /**
     * Apply NetSuite's native custom field record and get custom field type corresponding to it.
     *
     * @param field NetSuite's native custom field record object to be applied
     * @return custom field type
     */
    public abstract CustomFieldRefType apply(T field);

    /**
     * Determine custom field type for given custom field record object.
     *
     * @param field NetSuite's native custom field record object
     * @return custom field type
     */
    protected CustomFieldRefType getFieldType(T field) {
        Enum<?> fieldTypeEnumValue = (Enum<?>) Beans.getSimpleProperty(field, "fieldType");
        String fieldTypeName = Beans.getEnumAccessor((Class<Enum<?>>) fieldTypeEnumValue.getClass())
                .getStringValue(fieldTypeEnumValue);
        return CustomFieldRefType.getByCustomizationTypeOrDefault(fieldTypeName);
    }
}
