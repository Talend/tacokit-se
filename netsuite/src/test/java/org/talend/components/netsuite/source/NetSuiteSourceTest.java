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
package org.talend.components.netsuite.source;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.netsuite.NetSuiteBaseTest;
import org.talend.components.netsuite.dataset.NetSuiteDataSet;
import org.talend.components.netsuite.dataset.NetSuiteInputProperties;
import org.talend.components.netsuite.dataset.NetSuiteOutputProperties;
import org.talend.components.netsuite.dataset.SearchConditionConfiguration;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.model.TypeDesc;
import org.talend.components.netsuite.test.TestCollector;
import org.talend.components.netsuite.utils.SampleData;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleFactory;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.input.Mapper;

import com.netsuite.webservices.v2019_2.lists.accounting.Account;
import com.netsuite.webservices.v2019_2.lists.accounting.types.AccountType;
import com.netsuite.webservices.v2019_2.platform.core.types.SearchStringFieldOperator;

import lombok.extern.slf4j.Slf4j;

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
        NetSuiteInputProperties inputProperties = createInputProperties();
        NetSuiteDataSet dataSet = inputProperties.getDataSet();
        dataSet.setEnableCustomization(true);
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
    void testSearchBodyFieldsNotOnly() {
        log.info("Test 'search not only body fields' start ");
        Assertions.assertNotNull(searchSublistItems(false));
    }

    @Test
    void testSearchBodyFieldsOnly() {
        log.info("Test 'search only body fields' start ");
        Assertions.assertNull(searchSublistItems(true));
    }

    private String searchSublistItems(final boolean bodyFieldsOnly) {
        NetSuiteInputProperties inputProperties = createInputProperties();
        NetSuiteDataSet dataSet = inputProperties.getDataSet();
        dataSet.setRecordType("purchaseOrder");
        dataSet.setEnableCustomization(true);
        inputProperties.setBodyFieldsOnly(bodyFieldsOnly);
        inputProperties.setSearchCondition(
                Collections.singletonList(new SearchConditionConfiguration("internalId", "List.anyOf", "9", "")));
        List<Record> records = buildAndRunEmitterJob(inputProperties);
        Assertions.assertEquals(1, records.size());
        return records.get(0).get(String.class, "ItemList");
    }

    @Test
    void testSearchCustomRecordWithCustomFields() {
        log.info("Test 'search custom record with custom fields' start ");
        NetSuiteInputProperties inputProperties = createInputProperties();
        NetSuiteDataSet dataSet = inputProperties.getDataSet();
        dataSet.setRecordType("customrecordsearch_date_type");
        dataSet.setEnableCustomization(true);
        inputProperties.setSearchCondition(Collections.singletonList(
                new SearchConditionConfiguration("custrecordtemp_value_for_search", "Long.between", "100", "200")));
        List<Record> records = buildAndRunEmitterJob(inputProperties);
        Assertions.assertEquals(1, records.size());
        Assertions.assertEquals("FirstRecord", records.get(0).get(String.class, "Name"));
    }

    @Test
    @Disabled
    @DisplayName("Partition input data")
    void partitionInputDataTest() {
        log.info("Test 'partition input data' start ");
        String testIdPrefix = "PartitionInputDataTest_";
        int concurrency = 5;
        int numberOfRecords = 500;
        NetSuiteInputProperties inputProperties = createInputProperties();
        NetSuiteDataSet dataSet = inputProperties.getDataSet();
        NetSuiteOutputProperties outputProperties = createOutputProperties();
        outputProperties.setDataSet(dataSet);
        dataSet.setRecordType("Account");

        // add records
        log.info("Test 'partition input data' add records start ");
        outputProperties.setAction(NetSuiteOutputProperties.DataAction.ADD);
        List<String> schemaFields = Arrays.asList("SubsidiaryList", "Description", "AcctName", "AcctType", "InternalId",
                "ExternalId");
        NetSuiteClientService<?> clientService = netSuiteClientConnectionService.getClientService(dataSet.getDataStore(), i18n);
        TypeDesc typeDesc = clientService.getMetaDataSource().getTypeInfo("Account", dataSet.isEnableCustomization());
        NsObjectInputTransducer inputTransducer = new NsObjectInputTransducer(clientService.getBasicMetaData(), i18n, factory,
                netSuiteService.getSchema(dataSet, schemaFields, clientService), typeDesc, "2019.2");
        List<Record> newRecords = new LinkedList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            int num = i;
            newRecords.add(inputTransducer.read(() -> {
                Account res = SampleData.prepareAccountRecord(null, testIdPrefix + num);
                res.setAcctName(res.getAcctName() + num);
                return res;
            }));
        }
        buildAndRunCollectorJob(outputProperties, newRecords);

        // select in parallel
        log.info("Test 'partition input data' selecting start ");
        componentsHandler.resetState();
        inputProperties.setSearchCondition(Collections.singletonList(new SearchConditionConfiguration("description",
                "String." + SearchStringFieldOperator.STARTS_WITH.value(), testIdPrefix, "")));
        dataSet.setRecordType("Account");
        Mapper mapper = componentsHandler.asManager()
                .findMapper("NetSuite", "Input", 1, SimpleFactory.configurationByExample(inputProperties)).get();
        log.debug("Job execution timeout talend.component.junit.timeout=" + Integer.getInteger("talend.component.junit.timeout"));
        long startTime = System.nanoTime();
        final List<Record> insertedRecords = componentsHandler.collect(Record.class, mapper, Integer.MAX_VALUE, concurrency)
                .collect(Collectors.toList());
        log.info("Estimated time, ms: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
        Assertions.assertNotNull(insertedRecords);
        Set<String> idSet = insertedRecords.stream().map(r -> r.getString("InternalId")).collect(Collectors.toSet());
        try {
            Assertions.assertEquals(numberOfRecords, idSet.size());
        } finally {
            // clean records
            log.info("Test 'partition input data' clean records start ");
            outputProperties.setAction(NetSuiteOutputProperties.DataAction.DELETE);
            buildAndRunCollectorJob(outputProperties, insertedRecords);
        }
        log.info("Test 'partition input data' check cleaned start ");
        Assertions.assertTrue(buildAndRunEmitterJob(inputProperties).isEmpty());
    }
}
