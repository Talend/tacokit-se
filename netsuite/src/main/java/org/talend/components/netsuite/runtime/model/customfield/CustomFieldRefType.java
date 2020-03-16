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

import java.util.HashMap;
import java.util.Map;

/**
 * Type of custom field data object type.
 */
public enum CustomFieldRefType {
    BOOLEAN("BooleanCustomFieldRef"),
    DOUBLE("DoubleCustomFieldRef"),
    LONG("LongCustomFieldRef"),
    STRING("StringCustomFieldRef"),
    DATE("DateCustomFieldRef"),
    SELECT("SelectCustomFieldRef"),
    MULTI_SELECT("MultiSelectCustomFieldRef");

    /** Short name of NetSuite's native custom field data object type. */
    private String typeName;

    CustomFieldRefType(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }

    /** Table of custom field ref types by custom data type names. */
    private static final Map<String, CustomFieldRefType> CUSTOM_FIELD_REF_TYPE_MAP = new HashMap<>();

    static {
        CUSTOM_FIELD_REF_TYPE_MAP.put("_checkBox", CustomFieldRefType.BOOLEAN);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_currency", CustomFieldRefType.DOUBLE);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_date", CustomFieldRefType.DATE);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_datetime", CustomFieldRefType.DATE);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_decimalNumber", CustomFieldRefType.DOUBLE);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_document", CustomFieldRefType.STRING);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_eMailAddress", CustomFieldRefType.STRING);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_freeFormText", CustomFieldRefType.STRING);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_help", CustomFieldRefType.STRING);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_hyperlink", CustomFieldRefType.STRING);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_image", CustomFieldRefType.STRING);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_inlineHTML", CustomFieldRefType.STRING);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_integerNumber", CustomFieldRefType.LONG);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_listRecord", CustomFieldRefType.SELECT);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_longText", CustomFieldRefType.STRING);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_multipleSelect", CustomFieldRefType.MULTI_SELECT);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_password", CustomFieldRefType.STRING);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_percent", CustomFieldRefType.DOUBLE);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_phoneNumber", CustomFieldRefType.STRING);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_richText", CustomFieldRefType.STRING);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_textArea", CustomFieldRefType.STRING);
        CUSTOM_FIELD_REF_TYPE_MAP.put("_timeOfDay", CustomFieldRefType.DATE);
    }

    /**
     * Get custom field data object type by NetSuite customization type.
     *
     * @param customizationType name of customization type
     * @return custom field data object type or {@code null}
     */
    public static CustomFieldRefType getByCustomizationType(String customizationType) {
        return CUSTOM_FIELD_REF_TYPE_MAP.get(customizationType);
    }

    /**
     * Get custom field data object type by NetSuite customization type and return default type
     * if specified type is unknown.
     *
     * @param customizationType name of customization type
     * @return custom field data object type or default type
     */
    public static CustomFieldRefType getByCustomizationTypeOrDefault(String customizationType) {
        CustomFieldRefType customFieldRefType = getByCustomizationType(customizationType);
        if (customFieldRefType == null) {
            customFieldRefType = CustomFieldRefType.STRING;
        }
        return customFieldRefType;
    }
}
