/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.bigquery.output;

import com.google.cloud.bigquery.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.talend.components.bigquery.dataset.TableDataSet;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.components.bigquery.service.BigQueryService;
import org.talend.components.bigquery.service.GoogleStorageService;
import org.talend.components.bigquery.service.I18nMessage;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BigQueryOutputTest {

    private BigQueryOutputConfig configuration;

    private BigQueryService service;

    private BigQuery bigQuery;

    private GoogleStorageService storageService;

    private RecordIORepository ioRepository;

    private I18nMessage i18n;

    @BeforeEach
    public void reinit() throws Exception {
        BigQueryConnection connection = new BigQueryConnection();
        connection.setProjectName("projectName");
        connection.setJsonCredentials("");

        configuration = new BigQueryOutputConfig();
        TableDataSet tableDataSet = new TableDataSet();
        tableDataSet.setTableName("tableName");
        tableDataSet.setGsBucket("gsBucket");
        tableDataSet.setBqDataset("bqDataset");
        tableDataSet.setConnection(connection);
        configuration.setDataSet(tableDataSet);

        service = Mockito.mock(BigQueryService.class);
        Mockito.when(service.guessSchema(configuration)).thenReturn(Schema.of(FieldList.of(getFields())));

        bigQuery = Mockito.mock(BigQuery.class);
        Mockito.when(service.createClient(connection)).thenReturn(bigQuery);

        storageService = Mockito.mock(GoogleStorageService.class);

        ioRepository = Mockito.mock(RecordIORepository.class);

        i18n = Mockito.mock(I18nMessage.class);
    }

    @Test
    public void runTest() throws Exception {

        InsertAllResponse response = Mockito.mock(InsertAllResponse.class);
        Mockito.when(bigQuery.insertAll(Mockito.any(InsertAllRequest.class))).thenReturn(response);
        Table table = Mockito.mock(Table.class);
        Mockito.when(bigQuery.getTable(Mockito.any(TableId.class))).thenReturn(table);
        TableDefinition definition = Mockito.mock(TableDefinition.class);
        Mockito.when(table.getDefinition()).thenReturn(definition);
        Mockito.when(definition.getSchema()).thenReturn(Schema.of(getFields()));

        BigQueryOutput beanUnderTest = new BigQueryOutput(configuration, service, storageService, ioRepository, i18n);
        beanUnderTest.init();

        beanUnderTest.beforeGroup();
        List<Record> records = getRecordsToStore();
        records.stream().forEach(beanUnderTest::onElement);
        beanUnderTest.afterGroup();

    }

    private List<Record> getRecordsToStore() {
        RecordBuilderFactory rbf = new RecordBuilderFactoryImpl(null);
        return IntStream.of(10).mapToObj(i -> {
            return rbf.newRecordBuilder().withString("f1", "A").withFloat("f2", 42.5f).withBoolean("f3", i % 2 == 0)
                    .withDateTime("f4", new Date()).withDateTime("f5", new Date()).withBytes("f6", new byte[] { 0x01, 0x02 })
                    .withInt("f7", i).build();
        }).collect(Collectors.toList());
    }

    public com.google.cloud.bigquery.Field[] getFields() {
        com.google.cloud.bigquery.Field[] fields = new com.google.cloud.bigquery.Field[] {
                com.google.cloud.bigquery.Field.of("f1", LegacySQLTypeName.STRING),
                com.google.cloud.bigquery.Field.of("f2", LegacySQLTypeName.FLOAT),
                com.google.cloud.bigquery.Field.of("f3", LegacySQLTypeName.BOOLEAN),
                com.google.cloud.bigquery.Field.of("f4", LegacySQLTypeName.DATE),
                com.google.cloud.bigquery.Field.of("f5", LegacySQLTypeName.DATETIME),
                com.google.cloud.bigquery.Field.of("f6", LegacySQLTypeName.BYTES),
                com.google.cloud.bigquery.Field.of("f7", LegacySQLTypeName.INTEGER) };
        return fields;
    }

}
