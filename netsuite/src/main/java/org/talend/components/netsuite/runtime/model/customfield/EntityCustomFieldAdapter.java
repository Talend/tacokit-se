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

import java.util.HashMap;
import java.util.Map;

/**
 * Custom field adapter for {@link BasicRecordType#ENTITY_CUSTOM_FIELD} type.
 */
public class EntityCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    private static final Map<String, String> ENTITY_TYPE_PROPERTY_MAP = new HashMap<>();

    static {
        ENTITY_TYPE_PROPERTY_MAP.put("contact", "appliesToContact");
        ENTITY_TYPE_PROPERTY_MAP.put("customer", "appliesToCustomer");
        ENTITY_TYPE_PROPERTY_MAP.put("employee", "appliesToEmployee");
        ENTITY_TYPE_PROPERTY_MAP.put("entityGroup", "appliesToGroup");
        ENTITY_TYPE_PROPERTY_MAP.put("otherNameCategory", "appliesToOtherName");
        ENTITY_TYPE_PROPERTY_MAP.put("partner", "appliesToPartner");
        ENTITY_TYPE_PROPERTY_MAP.put("pricingGroup", "appliesToPriceList");
        ENTITY_TYPE_PROPERTY_MAP.put("projectTask", "appliesToProject");
        ENTITY_TYPE_PROPERTY_MAP.put("message", "appliesToStatement");
        ENTITY_TYPE_PROPERTY_MAP.put("note", "appliesToStatement");
        ENTITY_TYPE_PROPERTY_MAP.put("vendor", "appliesToVendor");
        ENTITY_TYPE_PROPERTY_MAP.put("siteCategory", "appliesToWebSite");
    }

    public EntityCustomFieldAdapter() {
        super(BasicRecordType.ENTITY_CUSTOM_FIELD);
    }

    @Override
    public String getPropertyName(String recordType) {
        return ENTITY_TYPE_PROPERTY_MAP.get(recordType);
    }

    @Override
    public CustomFieldRefType apply(T field) {
        return getFieldType(field);
    }

}
