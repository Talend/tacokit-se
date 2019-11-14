/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.netsuite.source;

import com.netsuite.webservices.v2018_2.lists.accounting.Account;
import com.netsuite.webservices.v2018_2.lists.accounting.types.AccountType;
import com.netsuite.webservices.v2018_2.platform.core.types.SearchStringFieldOperator;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.netsuite.NetSuiteBaseTest;
import org.talend.components.netsuite.dataset.NetSuiteDataSet;
import org.talend.components.netsuite.dataset.NetSuiteInputProperties;
import org.talend.components.netsuite.dataset.NetSuiteOutputProperties;
import org.talend.components.netsuite.dataset.SearchConditionConfiguration;
import org.talend.components.netsuite.test.TestCollector;
import org.talend.components.netsuite.utils.SampleData;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleFactory;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.input.Mapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@WithComponents("org.talend.components.netsuite")
public class NetSuiteSourceTest extends NetSuiteBaseTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @BeforeEach
    void setup() {
        TestCollector.reset();
    }

    @Test
    void testSearchBankAccounts() {
        log.info("Test 'search bank account' start ");
        NetSuiteInputProperties inputProperties = createInputProperties();
        NetSuiteDataSet dataSet = inputProperties.getDataSet();
        dataSet.setRecordType("Account");
        inputProperties.setSearchCondition(
                Collections.singletonList(new SearchConditionConfiguration("Type", "List.anyOf", "Bank", "")));

        List<Record> records = buildAndRunEmitterJob(inputProperties);

        Assertions.assertNotNull(records);
        Assertions.assertEquals(AccountType.BANK.value(), records.get(0).getString("AcctType"));
    }

    @Test
    void testSearchCustomRecords() {
        log.info("Test 'search custom records' start ");
        clientService.getMetaDataSource().setCustomizationEnabled(true);
        NetSuiteInputProperties inputProperties = createInputProperties();
        NetSuiteDataSet dataSet = inputProperties.getDataSet();
        dataSet.setRecordType("customrecord398");
        inputProperties.setSearchCondition(
                Collections.singletonList(new SearchConditionConfiguration("name", "String.doesNotContain", "TUP", "")));

        List<Record> records = buildAndRunEmitterJob(inputProperties);
        Assertions.assertTrue(records.size() > 1);
        records.stream().map(record -> record.get(String.class, "Name")).forEach(name -> {
            Assertions.assertNotNull(name);
            Assertions.assertFalse(name.contains("TUP"));
        });
    }

    @Test
    void testSearchSublistItems() {
        log.info("Test 'search sublist items' start ");
        Assertions.assertNotNull(searchSublistItems(false));
    }

    @Test
    void testSearchSublistItemsEmpty() {
        log.info("Test 'search sublist items empty' start ");
        Assertions.assertNull(searchSublistItems(true));
    }

    /**
     * @param bodyFieldsOnly In terms of documentation false means get all fields, true no item lists returned
     */
    private String searchSublistItems(final boolean bodyFieldsOnly) {
        dataStore.setEnableCustomization(true);
        service.getClientService(dataStore).setBodyFieldsOnly(bodyFieldsOnly);
        NetSuiteInputProperties inputProperties = createInputProperties();
        NetSuiteDataSet dataSet = inputProperties.getDataSet();
        dataSet.setRecordType("purchaseOrder");
        inputProperties.setSearchCondition(
                Collections.singletonList(new SearchConditionConfiguration("internalId", "List.anyOf", "9", "")));
        List<Record> records = buildAndRunEmitterJob(inputProperties);
        Assertions.assertEquals(1, records.size());

        return records.get(0).get(String.class, "ItemList");
    }

    @Test
    void testSearchCustomRecordWithCustomFields() {
        log.info("Test 'search custom record with custom fields' start ");
        clientService.getMetaDataSource().setCustomizationEnabled(true);
        NetSuiteInputProperties inputProperties = createInputProperties();
        NetSuiteDataSet dataSet = inputProperties.getDataSet();
        dataSet.setRecordType("customrecordsearch_date_type");
        inputProperties.setSearchCondition(Collections.singletonList(
                new SearchConditionConfiguration("custrecordtemp_value_for_search", "Long.between", "100", "200")));

        List<Record> records = buildAndRunEmitterJob(inputProperties);
        Assertions.assertEquals(1, records.size());
        Assertions.assertEquals("FirstRecord", records.get(0).get(String.class, "Name"));
    }

    @Test
    @DisplayName("Partition input data")
    void partitionInputDataTest() {
        String testIdPrefix = "PartitionInputDataTest_";
        int n = 10;
        NetSuiteInputProperties inputProperties = createInputProperties();
        NetSuiteDataSet dataSet = inputProperties.getDataSet();
        NetSuiteOutputProperties outputProperties = createOutputProperties();
        outputProperties.setDataSet(dataSet);
        dataSet.setRecordType("Account");

        // add records
        outputProperties.setAction(NetSuiteOutputProperties.DataAction.ADD);
        List<String> schemaFields = Arrays.asList("SubsidiaryList", "Description", "AcctName", "AcctType", "InternalId",
                "ExternalId");
        NsObjectInputTransducer inputTransducer = new NsObjectInputTransducer(clientService, messages, factory,
                service.getSchema(dataSet, schemaFields), "Account", "2018.2");
        List<Record> newRecords = new LinkedList<>();
        for (int i = 0; i < n; i++) {
            int num = i;
            newRecords.add(inputTransducer.read(() -> {
                Account res = SampleData.prepareAccountRecord(null);
                res.setAcctName(res.getAcctName() + num);
                res.setDescription(testIdPrefix + num);
                return res;
            }));
        }
        buildAndRunCollectorJob(outputProperties, newRecords);

        // select in parallel
        componentsHandler.resetState();
        inputProperties.setSearchCondition(Collections.singletonList(new SearchConditionConfiguration("description",
                "String." + SearchStringFieldOperator.STARTS_WITH.value(), testIdPrefix, "")));
        dataSet.setRecordType("Account");
        Mapper mapper = componentsHandler.asManager()
                .findMapper("NetSuite", "Input", 1, SimpleFactory.configurationByExample(inputProperties)).get();
        final List<Record> insertedRecords = componentsHandler.collect(Record.class, mapper, Integer.MAX_VALUE, 3)
                .collect(Collectors.toList());
        Assertions.assertNotNull(insertedRecords);
        try {
            Assertions.assertEquals(n, insertedRecords.size());
        } finally {
            // clean records
            outputProperties.setAction(NetSuiteOutputProperties.DataAction.DELETE);
            buildAndRunCollectorJob(outputProperties, insertedRecords);
        }
        Assertions.assertTrue(buildAndRunEmitterJob(inputProperties).isEmpty());
    }
}
