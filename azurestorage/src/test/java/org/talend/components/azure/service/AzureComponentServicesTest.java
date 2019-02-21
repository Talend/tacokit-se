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
package org.talend.components.azure.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.talend.components.azure.common.AzureConnection;
import org.talend.components.azure.common.AzureTableConnection;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.SchemaImpl;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.EntityProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;

public class AzureComponentServicesTest {

    @Test
    public void testAllColumnsReturnsSameSize() {
        List<String> expectedColumnNames = new ArrayList<>();
        expectedColumnNames.add("column1");
        expectedColumnNames.add("column2");
        expectedColumnNames.add("column3");
        AzureComponentServices services = new AzureComponentServices();
        SuggestionValues columns = services.getColumnNames(expectedColumnNames);

        assertEquals(expectedColumnNames.size(), columns.getItems().size());
    }

    @Test
    public void testGetColumnNamesValuesAreNotCacheable() {
        SuggestionValues columnValues = new AzureComponentServices().getColumnNames(Collections.emptyList());

        assertFalse(columnValues.isCacheable());
    }

    @Test
    public void testColumnNamesRemainsTheSame() {
        String expectedColumn = "expectedName";
        List<String> expectedColumnList = Collections.singletonList(expectedColumn);

        SuggestionValues columns = new AzureComponentServices().getColumnNames(expectedColumnList);

        Iterator<SuggestionValues.Item> itemIterator = columns.getItems().iterator();
        assertEquals(1, columns.getItems().size());
        while (itemIterator.hasNext()) {
            SuggestionValues.Item item = itemIterator.next();
            assertEquals(expectedColumn, item.getId());
        }

    }

    @Test
    public void testHealthCheckOK() throws Exception {
        CloudTableClient mockedTableClient = Mockito.mock(CloudTableClient.class);
        AzureConnection connection = new AzureConnection();
        AzureConnectionService connectionService = Mockito.mock(AzureConnectionService.class);
        Mockito.when(connectionService.createCloudTableClient(any(), any())).thenReturn(mockedTableClient);
        AzureComponentServices componentServices = new AzureComponentServices();
        componentServices.i18nService = Mockito.mock(MessageService.class);
        componentServices.connectionService = connectionService;
        HealthCheckStatus status = componentServices.testConnection(connection);

        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

    @Test
    public void testHealthCheckFailed() throws Exception {
        CloudTableClient mockedTableClient = Mockito.mock(CloudTableClient.class);
        Mockito.when(
                mockedTableClient.listTablesSegmented(null, 1, null, null, AzureConnectionService.getTalendOperationContext()))
                .thenThrow(RuntimeException.class);
        AzureConnection connection = new AzureConnection();
        CloudStorageAccount mockedStorageAccount = Mockito.mock(CloudStorageAccount.class);
        Mockito.when(mockedStorageAccount.createCloudTableClient()).thenReturn(mockedTableClient);
        AzureConnectionService connectionService = Mockito.mock(AzureConnectionService.class);
        AzureComponentServices componentServices = new AzureComponentServices();
        componentServices.connectionService = connectionService;
        Mockito.when(connectionService.createStorageAccount(connection)).thenReturn(mockedStorageAccount);
        HealthCheckStatus status = componentServices.testConnection(connection);

        assertEquals(HealthCheckStatus.Status.KO, status.getStatus());
    }

    @Test
    public void testGetTableList() throws Exception {
        CloudTableClient mockedTableClient = Mockito.mock(CloudTableClient.class);
        String expectedTableName = "someTableName";
        Mockito.when(mockedTableClient.listTables(null, null, AzureConnectionService.getTalendOperationContext()))
                .thenReturn(Collections.singletonList(expectedTableName));
        AzureConnection connection = new AzureConnection();
        CloudStorageAccount mockedStorageAccount = Mockito.mock(CloudStorageAccount.class);
        Mockito.when(mockedStorageAccount.createCloudTableClient()).thenReturn(mockedTableClient);
        AzureConnectionService connectionService = Mockito.mock(AzureConnectionService.class);
        AzureComponentServices componentServices = new AzureComponentServices();
        componentServices.connectionService = connectionService;
        Mockito.when(connectionService.createStorageAccount(connection)).thenReturn(mockedStorageAccount);

        SuggestionValues tableList = componentServices.getTableNames(connection);

        assertNotNull(tableList);
        assertTrue(tableList.isCacheable());
        assertEquals(1, tableList.getItems().size());
        assertEquals(expectedTableName, tableList.getItems().iterator().next().getId());
    }

    @Test
    public void testGetTableListFailed() throws Exception {
        CloudTableClient mockedTableClient = Mockito.mock(CloudTableClient.class);
        Mockito.when(mockedTableClient.listTables(null, null, AzureConnectionService.getTalendOperationContext()))
                .thenThrow(new RuntimeException());
        AzureConnection connection = new AzureConnection();
        CloudStorageAccount mockedStorageAccount = Mockito.mock(CloudStorageAccount.class);
        Mockito.when(mockedStorageAccount.createCloudTableClient()).thenReturn(mockedTableClient);
        AzureConnectionService connectionService = Mockito.mock(AzureConnectionService.class);
        AzureComponentServices componentServices = new AzureComponentServices();
        componentServices.connectionService = connectionService;
        Mockito.when(connectionService.createStorageAccount(connection)).thenReturn(mockedStorageAccount);

        Assertions.assertThrows(RuntimeException.class, () -> componentServices.getTableNames(connection));
    }

    @Test
    public void testGuessSchema() throws Exception {
        int defaultColumnsSize = 3;
        AzureComponentServices componentServices = new AzureComponentServices();
        RecordBuilderFactory factory = Mockito.mock(RecordBuilderFactory.class);

        Schema.Entry.Builder mockedEntryBuilder = prepareEntryBuilderMock();
        Mockito.when(factory.newEntryBuilder()).thenReturn(mockedEntryBuilder);
        Schema.Builder mockedSchemaBuilder = prepareSchemaBuilderMock();
        Mockito.when(factory.newSchemaBuilder(Schema.Type.RECORD)).thenReturn(mockedSchemaBuilder);

        String testTableName = "someTableName";
        AzureTableConnection testDataSet = new AzureTableConnection();
        testDataSet.setTableName(testTableName);
        AzureConnectionService connectionService = Mockito.mock(AzureConnectionService.class);
        CloudStorageAccount mockedStorageAccount = Mockito.mock(CloudStorageAccount.class);
        Mockito.when(connectionService.createStorageAccount(any())).thenReturn(mockedStorageAccount);
        componentServices.factory = factory;
        componentServices.connectionService = connectionService;
        HashMap<String, EntityProperty> propertyHashMap = new HashMap<>();
        propertyHashMap.put("booleanColumn", new EntityProperty(true));
        propertyHashMap.put("intColumn", new EntityProperty(123));
        propertyHashMap.put("stringColumn", new EntityProperty("String value"));
        propertyHashMap.put("doubleColumn", new EntityProperty(12.3));
        DynamicTableEntity tableEntity = new DynamicTableEntity("someKey", "someKey2", propertyHashMap);
        Mockito.when(connectionService.executeQuery(any(), any(), any())).thenReturn(Collections.singletonList(tableEntity));

        componentServices.guessSchema(testDataSet);

        Mockito.verify(mockedSchemaBuilder).build();
        Mockito.verify(mockedSchemaBuilder, Mockito.times(defaultColumnsSize + propertyHashMap.size())).withEntry(any());
    }

    private Schema.Builder prepareSchemaBuilderMock() {
        Schema.Builder mockedSchemaBuilder = Mockito.mock(Schema.Builder.class);
        Mockito.when(mockedSchemaBuilder.withEntry(any())).thenReturn(mockedSchemaBuilder);
        Mockito.when(mockedSchemaBuilder.withType(any())).thenReturn(mockedSchemaBuilder);
        return mockedSchemaBuilder;
    }

    private Schema.Entry.Builder prepareEntryBuilderMock() {
        Schema.Entry.Builder mockedEntryBuilder = Mockito.mock(Schema.Entry.Builder.class);
        Mockito.when(mockedEntryBuilder.withName(any())).thenReturn(mockedEntryBuilder);
        Mockito.when(mockedEntryBuilder.withType(any())).thenReturn(mockedEntryBuilder);
        Mockito.when(mockedEntryBuilder.withNullable(anyBoolean())).thenReturn(mockedEntryBuilder);
        Mockito.when(mockedEntryBuilder.build()).thenReturn(new SchemaImpl.EntryImpl());
        return mockedEntryBuilder;
    }

    @Test
    public void testGuessSchemaFailing() throws Exception {
        String testTableName = "someTableName";
        AzureTableConnection testDataSet = new AzureTableConnection();
        testDataSet.setTableName(testTableName);
        AzureConnectionService connectionService = Mockito.mock(AzureConnectionService.class);
        AzureComponentServices componentServices = new AzureComponentServices();
        componentServices.connectionService = connectionService;
        CloudStorageAccount mockedStorageAccount = Mockito.mock(CloudStorageAccount.class);
        Mockito.when(connectionService.createStorageAccount(testDataSet.getConnection())).thenReturn(mockedStorageAccount);
        Mockito.when(connectionService.executeQuery(any(), any(), any())).thenThrow(RuntimeException.class);

        Assertions.assertThrows(RuntimeException.class, () -> componentServices.guessSchema(testDataSet));
    }
}