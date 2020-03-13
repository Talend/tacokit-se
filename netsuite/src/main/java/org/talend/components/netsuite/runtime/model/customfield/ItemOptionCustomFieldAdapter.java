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
 * Custom field adapter for {@link BasicRecordType#ITEM_OPTION_CUSTOM_FIELD} type.
 */
public class ItemOptionCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    private static final Map<String, String> ITEM_OPTIONS_TYPE_PROPERTY_MAP = new HashMap<>();
    static {
        ITEM_OPTIONS_TYPE_PROPERTY_MAP.put("purchaseOrder", "colPurchase");
        ITEM_OPTIONS_TYPE_PROPERTY_MAP.put("salesOrder", "colSale");
        ITEM_OPTIONS_TYPE_PROPERTY_MAP.put("opportunity", "colOpportunity");
        ITEM_OPTIONS_TYPE_PROPERTY_MAP.put("kitItem", "colKitItem");
    }

    public ItemOptionCustomFieldAdapter() {
        super(BasicRecordType.ITEM_OPTION_CUSTOM_FIELD);
    }

    @Override
    public String getPropertyName(String recordType) {
        return ITEM_OPTIONS_TYPE_PROPERTY_MAP.get(recordType);
    }

    @Override
    public CustomFieldRefType apply(T field) {
        return getFieldType(field);
    }

}
