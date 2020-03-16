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
 * Custom field adapter for {@link BasicRecordType#ITEM_CUSTOM_FIELD} type.
 */
public class ItemCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    private static final Map<String, String> ITEM_TYPE_PROPERTY_MAP = new HashMap<>();
    static {
        ITEM_TYPE_PROPERTY_MAP.put("entityGroup", "appliesToGroup");
        ITEM_TYPE_PROPERTY_MAP.put("inventoryItem", "appliesToInventory");
        ITEM_TYPE_PROPERTY_MAP.put("assemblyItem", "appliesToItemAssembly");
        ITEM_TYPE_PROPERTY_MAP.put("kitItem", "appliesToKit");
        ITEM_TYPE_PROPERTY_MAP.put("nonInventoryPurchaseItem", "appliesToNonInventory");
        ITEM_TYPE_PROPERTY_MAP.put("nonInventoryResaleItem", "appliesToNonInventory");
        ITEM_TYPE_PROPERTY_MAP.put("nonInventorySaleItem", "appliesToNonInventory");
        ITEM_TYPE_PROPERTY_MAP.put("otherChargePurchaseItem", "appliesToOtherCharge");
        ITEM_TYPE_PROPERTY_MAP.put("otherChargeResaleItem", "appliesToOtherCharge");
        ITEM_TYPE_PROPERTY_MAP.put("otherChargeSaleItem", "appliesToOtherCharge");
        ITEM_TYPE_PROPERTY_MAP.put("pricingGroup", "appliesToPriceList");
        ITEM_TYPE_PROPERTY_MAP.put("servicePurchaseItem", "appliesToService");
        ITEM_TYPE_PROPERTY_MAP.put("serviceResaleItem", "appliesToService");
        ITEM_TYPE_PROPERTY_MAP.put("serviceSaleItem", "appliesToService");
    }

    public ItemCustomFieldAdapter() {
        super(BasicRecordType.ITEM_CUSTOM_FIELD);
    }

    @Override
    public String getPropertyName(String recordType) {
        return ITEM_TYPE_PROPERTY_MAP.get(recordType);
    }

    @Override
    public CustomFieldRefType apply(T field) {
        return getFieldType(field);
    }

}
