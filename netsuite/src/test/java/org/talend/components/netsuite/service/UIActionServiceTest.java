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
package org.talend.components.netsuite.service;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.talend.components.netsuite.NetSuiteBaseTest;
import org.talend.components.netsuite.dataset.NetSuiteDataSet;
import org.talend.components.netsuite.dataset.NetSuiteOutputProperties;
import org.talend.components.netsuite.datastore.NetSuiteDataStore;
import org.talend.components.netsuite.datastore.NetSuiteDataStore.ApiVersion;
import org.talend.components.netsuite.datastore.NetSuiteDataStore.LoginType;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorName;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorType;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorTypeDesc;
import org.talend.components.netsuite.runtime.v2019_2.model.RecordTypeEnum;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues.Item;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import com.netsuite.webservices.v2019_2.platform.common.AccountSearchBasic;
import com.netsuite.webservices.v2019_2.platform.common.CustomRecordSearchBasic;
import com.netsuite.webservices.v2019_2.platform.common.TransactionSearchBasic;
import com.netsuite.webservices.v2019_2.platform.core.types.SearchDate;
import com.netsuite.webservices.v2019_2.platform.core.types.SearchDateFieldOperator;
import com.netsuite.webservices.v2019_2.platform.core.types.SearchMultiSelectFieldOperator;
import com.netsuite.webservices.v2019_2.transactions.purchases.PurchaseOrder;

import lombok.extern.slf4j.Slf4j;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@WithComponents("org.talend.components.netsuite")
class UIActionServiceTest extends NetSuiteBaseTest {

    @Service
    private UIActionService uiActionService;

    @Test
    void testHealthCheck() {
        log.info("Test 'health check' start ");
        assertEquals(HealthCheckStatus.Status.OK, uiActionService.validateConnection(dataStore).getStatus());
    }

    @Test
    void testHealthCheckFailed() {
        log.info("Test 'healthcheck failed' start ");
        NetSuiteDataStore dataStoreWrong = new NetSuiteDataStore();
        dataStoreWrong.setLoginType(LoginType.BASIC);
        dataStoreWrong.setRole("3");
        dataStoreWrong.setAccount(NETSUITE_ACCOUNT);
        dataStoreWrong.setApiVersion(ApiVersion.V2019_2);
        dataStoreWrong.setEmail("test_junit@talend.com");
        dataStoreWrong.setPassword("wrongPassword");
        dataStoreWrong.setApplicationId(UUID.randomUUID().toString());
        assertEquals(HealthCheckStatus.Status.KO, uiActionService.validateConnection(dataStoreWrong).getStatus());
    }

    @Test
    void testGuessSchema() {
        log.info("Test 'guess schema' start ");
        List<String> fields = Arrays.stream(PurchaseOrder.class.getDeclaredFields()).map(field -> field.getName().toLowerCase())
                .collect(toList());
        NetSuiteOutputProperties outputProperties = createOutputProperties();
        NetSuiteDataSet dataSet = outputProperties.getDataSet();
        dataSet.setRecordType("PurchaseOrder");
        // For custom fields need to re-create connection (might be a case for refactoring of Service)
        dataSet.setEnableCustomization(true);
        Schema schema = uiActionService.guessSchema(outputProperties.getDataSet());
        // In case of customization count of entries in schema must be more than actual class fields.
        assertTrue(fields.size() < schema.getEntries().size());
    }

    @Test
    void testLoadRecordTypes() {
        log.info("Test 'load record types' start ");
        List<String> expectedList = Arrays.stream(RecordTypeEnum.values()).map(RecordTypeEnum::getTypeName).sorted()
                .collect(toList());
        NetSuiteDataSet dataSet = createDefaultDataSet();
        SuggestionValues values = uiActionService.loadRecordTypes(dataSet.getDataStore(), dataSet.isEnableCustomization());
        List<String> actualList = values.getItems().stream().map(Item::getLabel).sorted().collect(toList());

        assertIterableEquals(expectedList, actualList);
    }

    @Test
    void testLoadCustomRecordTypes() {
        log.info("Test 'load custom record types' start ");
        NetSuiteDataSet dataSet = createDefaultDataSet();
        dataSet.setEnableCustomization(true);
        SuggestionValues values = uiActionService.loadRecordTypes(dataSet.getDataStore(), dataSet.isEnableCustomization());
        // For enabled customization we must have more record types.
        // Even for passing other tests, they must be defined
        assertTrue(RecordTypeEnum.values().length < values.getItems().size());
    }

    @Test
    void testLoadFields() {
        log.info("Test 'load fields' start ");
        List<String> expectedList = Arrays.stream(AccountSearchBasic.class.getDeclaredFields()).map(Field::getName)
                .filter(NetSuiteService.FILTER_EXTRA_SEARCH_FIELDS).sorted().collect(toList());
        NetSuiteDataSet dataSet = createDefaultDataSet();
        dataSet.setRecordType("Account");
        SuggestionValues values = uiActionService.loadFields(dataSet);
        List<String> actualList = values.getItems().stream().map(Item::getLabel).sorted().collect(toList());
        assertIterableEquals(expectedList, actualList);
    }

    @Test
    void testLoadCustomSearchFields() {
        log.info("Test 'load custom search fields' start ");
        List<String> expectedList = Arrays.stream(CustomRecordSearchBasic.class.getDeclaredFields()).map(Field::getName)
                .filter(NetSuiteService.FILTER_EXTRA_SEARCH_FIELDS).collect(toList());
        expectedList.add("custrecordtemp_value_for_search");
        Collections.sort(expectedList);

        NetSuiteDataSet dataSet = createDefaultDataSet();
        dataSet.setRecordType("customrecordsearch_date_type");
        dataSet.setEnableCustomization(true);

        SuggestionValues values = uiActionService.loadFields(dataSet);
        List<String> actualList = values.getItems().stream().map(Item::getLabel).sorted().collect(toList());
        assertIterableEquals(expectedList, actualList);
    }

    @Test
    void testLoadTransactionSearchFields() {
        log.info("Test 'load transaction search fields' start ");
        List<String> expectedList = Arrays.stream(TransactionSearchBasic.class.getDeclaredFields()).map(Field::getName)
                .filter(NetSuiteService.FILTER_EXTRA_SEARCH_FIELDS).sorted().collect(toList());

        NetSuiteDataSet dataSet = createDefaultDataSet();
        dataSet.setRecordType("PurchaseOrder");

        SuggestionValues values = uiActionService.loadFields(dataSet);
        List<String> actualList = values.getItems().stream().map(Item::getLabel).sorted().collect(toList());
        assertIterableEquals(expectedList, actualList);
    }

    @Test
    void testLoadSearchOperator() {
        log.info("Test 'load search operator' start ");
        NetSuiteDataSet dataSet = createDefaultDataSet();
        dataSet.setRecordType("Account");

        List<String> expectedList = SearchFieldOperatorTypeDesc
                .createForEnum(SearchFieldOperatorType.MULTI_SELECT, SearchMultiSelectFieldOperator.class).getOperatorNames()
                .stream().map(SearchFieldOperatorName::getQualifiedName).sorted().collect(toList());

        SuggestionValues values = uiActionService.loadOperators(dataSet, "internalId");

        List<String> actualList = values.getItems().stream().map(Item::getLabel).sorted().collect(toList());
        assertIterableEquals(expectedList, actualList);
    }

    @Test
    void testLoadSearchOperatorForDateType() {
        log.info("Test 'load search operator for Date type' start ");
        NetSuiteDataSet dataSet = createDefaultDataSet();
        dataSet.setRecordType("PurchaseOrder");

        List<String> expectedList = Stream.concat(
                SearchFieldOperatorTypeDesc.createForEnum(SearchFieldOperatorType.DATE, SearchDateFieldOperator.class)
                        .getOperatorNames().stream(),
                SearchFieldOperatorTypeDesc.createForEnum(SearchFieldOperatorType.PREDEFINED_DATE, SearchDate.class)
                        .getOperatorNames().stream())
                .map(SearchFieldOperatorName::getQualifiedName).sorted().collect(toList());

        SuggestionValues values = uiActionService.loadOperators(dataSet, "billedDate");

        List<String> actualList = values.getItems().stream().map(Item::getLabel).sorted().collect(toList());
        assertIterableEquals(expectedList, actualList);
    }
}
