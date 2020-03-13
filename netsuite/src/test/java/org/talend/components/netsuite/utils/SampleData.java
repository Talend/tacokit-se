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
package org.talend.components.netsuite.utils;

import com.netsuite.webservices.v2019_2.lists.accounting.Account;
import com.netsuite.webservices.v2019_2.lists.accounting.types.AccountType;
import com.netsuite.webservices.v2019_2.platform.core.CustomFieldList;
import com.netsuite.webservices.v2019_2.platform.core.RecordRef;
import com.netsuite.webservices.v2019_2.platform.core.RecordRefList;
import com.netsuite.webservices.v2019_2.platform.core.StringCustomFieldRef;
import com.netsuite.webservices.v2019_2.platform.core.types.RecordType;
import com.netsuite.webservices.v2019_2.setup.customization.CustomRecord;
import com.netsuite.webservices.v2019_2.transactions.purchases.PurchaseOrder;
import com.netsuite.webservices.v2019_2.transactions.purchases.PurchaseOrderItem;
import com.netsuite.webservices.v2019_2.transactions.purchases.PurchaseOrderItemList;
import org.talend.sdk.component.api.record.Record;

import java.util.Arrays;
import java.util.Collections;

public class SampleData {

    public static Account prepareAccountRecord(Record record, String descriptionPrefix) {
        Account account = new Account();
        RecordRefList subsidiaries = new RecordRefList();
        RecordRef subsidiaryRef = new RecordRef();
        subsidiaryRef.setType(RecordType.SUBSIDIARY);
        subsidiaryRef.setInternalId("1");
        account.setAcctType(AccountType.OTHER_ASSET);
        subsidiaries.getRecordRef().add(subsidiaryRef);
        account.setSubsidiaryList(subsidiaries);
        if (record == null) {
            String id = Long.toString(System.currentTimeMillis());
            account.setAcctName("Test account" + id);
            account.setDescription(descriptionPrefix);
        } else {
            account.setAcctName(record.getString("AcctName"));
            account.setDescription(record.getString("Description") + " - Updated");
            account.setInternalId(record.getString("InternalId"));
            account.setExternalId(record.getString("ExternalId"));
        }
        return account;
    }

    public static CustomRecord prepareCustomRecord(Record record) {
        CustomRecord customRecord = new CustomRecord();
        CustomFieldList custFieldList = new CustomFieldList();
        StringCustomFieldRef custField1 = new StringCustomFieldRef();
        StringCustomFieldRef custField2 = new StringCustomFieldRef();
        custField1.setScriptId("custrecord79");
        custField2.setScriptId("custrecord80");
        String id = Long.toString(System.currentTimeMillis());
        if (record == null) {
            custField1.setValue("Test " + id);
            custField2.setValue("0.1.0" + id);
            customRecord.setName("Test name " + id);
        } else {
            custField1.setValue(record.getString("Custrecord79"));
            custField2.setValue("1.0.0" + id);
            customRecord.setName(record.getString("Name") + " Updated");
            customRecord.setExternalId(record.getString("InternalId"));
        }
        custFieldList.getCustomField().addAll(Arrays.asList(custField1, custField2));
        customRecord.setCustomFieldList(custFieldList);
        return customRecord;
    }

    public static PurchaseOrder preparePurchaseOrder() {
        // Bad practice to hard code internalIds, we had failed tests after truncating environment.
        // Need to consider better way of setting up values.
        String customFormId = "98";
        String vendorId = "5322";
        String employeeId = "5";
        String subsidiaryId = "1";
        String purchaseOrderItemId = "12";
        String id = Long.toString(System.currentTimeMillis());
        PurchaseOrder po = new PurchaseOrder();
        po.setSupervisorApproval(true);
        RecordRef ref = new RecordRef();
        ref.setInternalId(customFormId);
        po.setCustomForm(ref);
        ref = new RecordRef();
        ref.setInternalId(vendorId);
        ref.setType(RecordType.VENDOR);
        po.setEntity(ref);
        ref = new RecordRef();
        ref.setInternalId(employeeId);
        ref.setType(RecordType.EMPLOYEE);
        po.setEmployee(ref);
        ref = new RecordRef();
        ref.setInternalId(subsidiaryId);
        ref.setType(RecordType.SUBSIDIARY);
        po.setMessage("Message " + id);
        po.setSubsidiary(ref);
        po.setExchangeRate(1.00);
        CustomFieldList custFieldList = new CustomFieldList();
        StringCustomFieldRef custField1 = new StringCustomFieldRef();
        custField1.setScriptId("custbody111");
        custField1.setValue("SMTH " + subsidiaryId);
        StringCustomFieldRef custField2 = new StringCustomFieldRef();
        custField2.setScriptId("custbody_clarivates_custom");
        custField2.setValue("Integration test");
        custFieldList.getCustomField().addAll(Arrays.asList(custField1, custField2));
        po.setCustomFieldList(custFieldList);
        PurchaseOrderItemList poItemList = new PurchaseOrderItemList();
        PurchaseOrderItem item = new PurchaseOrderItem();
        ref = new RecordRef();
        ref.setInternalId(purchaseOrderItemId);
        ref.setType(RecordType.SERVICE_PURCHASE_ITEM);
        item.setItem(ref);
        poItemList.getItem().addAll(Collections.singletonList(item));
        po.setItemList(poItemList);
        return po;
    }
}
