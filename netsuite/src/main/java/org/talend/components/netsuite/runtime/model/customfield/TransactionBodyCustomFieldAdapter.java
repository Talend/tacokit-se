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
 * Custom field adapter for {@link BasicRecordType#TRANSACTION_BODY_CUSTOM_FIELD} type.
 */
public class TransactionBodyCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    private static final Map<String, String> TRANSACTION_BODY_TYPE_PROPERTY_MAP = new HashMap<>();
    static {
        TRANSACTION_BODY_TYPE_PROPERTY_MAP.put("assemblyBuild", "bodyAssemblyBuild");
        TRANSACTION_BODY_TYPE_PROPERTY_MAP.put("purchaseOrder", "bodyPurchase");
        TRANSACTION_BODY_TYPE_PROPERTY_MAP.put("journalEntry", "bodyJournal");
        TRANSACTION_BODY_TYPE_PROPERTY_MAP.put("expenseReport", "bodyExpenseReport");
        TRANSACTION_BODY_TYPE_PROPERTY_MAP.put("opportunity", "bodyOpportunity");
        TRANSACTION_BODY_TYPE_PROPERTY_MAP.put("itemReceipt", "bodyItemReceipt");
        TRANSACTION_BODY_TYPE_PROPERTY_MAP.put("itemFulfillment", "bodyItemFulfillment");
        TRANSACTION_BODY_TYPE_PROPERTY_MAP.put("inventoryAdjustment", "bodyInventoryAdjustment");
        TRANSACTION_BODY_TYPE_PROPERTY_MAP.put("customerPayment", "bodyCustomerPayment");
        TRANSACTION_BODY_TYPE_PROPERTY_MAP.put("vendorPayment", "bodyVendorPayment");
        TRANSACTION_BODY_TYPE_PROPERTY_MAP.put("vendorBill", "bodyPurchase");
        TRANSACTION_BODY_TYPE_PROPERTY_MAP.put("vendorCredit", "bodyPurchase");
        TRANSACTION_BODY_TYPE_PROPERTY_MAP.put("creditMemo", "bodySale");
        TRANSACTION_BODY_TYPE_PROPERTY_MAP.put("invoice", "bodySale");
    }

    public TransactionBodyCustomFieldAdapter() {
        super(BasicRecordType.TRANSACTION_BODY_CUSTOM_FIELD);
    }

    @Override
    public String getPropertyName(String recordType) {
        return TRANSACTION_BODY_TYPE_PROPERTY_MAP.get(recordType);
    }

    @Override
    public CustomFieldRefType apply(T field) {
        return getFieldType(field);
    }

}
