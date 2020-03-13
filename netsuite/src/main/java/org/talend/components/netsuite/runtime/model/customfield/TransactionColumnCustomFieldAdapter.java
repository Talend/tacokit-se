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
 * Custom field adapter for {@link BasicRecordType#TRANSACTION_COLUMN_CUSTOM_FIELD} type.
 */
public class TransactionColumnCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    private static final Map<String, String> TRANSACTION_COLUMN_TYPE_PROPERTY_MAP = new HashMap<>();
    static {
        TRANSACTION_COLUMN_TYPE_PROPERTY_MAP.put("expenseCategory", "colExpense");
        TRANSACTION_COLUMN_TYPE_PROPERTY_MAP.put("expenseReport", "colExpenseReport");
        TRANSACTION_COLUMN_TYPE_PROPERTY_MAP.put("assemblyBuild", "colBuild");
        TRANSACTION_COLUMN_TYPE_PROPERTY_MAP.put("itemReceipt", "colItemReceipt");
        TRANSACTION_COLUMN_TYPE_PROPERTY_MAP.put("itemFulfillment", "colItemFulfillment");
        TRANSACTION_COLUMN_TYPE_PROPERTY_MAP.put("purchaseOrder", "colPurchase");
        TRANSACTION_COLUMN_TYPE_PROPERTY_MAP.put("journalEntry", "colJournal");
        TRANSACTION_COLUMN_TYPE_PROPERTY_MAP.put("salesOrder", "colSale");
        TRANSACTION_COLUMN_TYPE_PROPERTY_MAP.put("opportunity", "colOpportunity");
        TRANSACTION_COLUMN_TYPE_PROPERTY_MAP.put("kitItem", "colKitItem");
        TRANSACTION_COLUMN_TYPE_PROPERTY_MAP.put("timeBill", "colTime");
    }

    public TransactionColumnCustomFieldAdapter() {
        super(BasicRecordType.TRANSACTION_COLUMN_CUSTOM_FIELD);
    }

    @Override
    public String getPropertyName(String recordType) {
        return TRANSACTION_COLUMN_TYPE_PROPERTY_MAP.get(recordType);
    }

    @Override
    public CustomFieldRefType apply(T field) {
        return getFieldType(field);
    }

}
