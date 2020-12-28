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
package org.talend.components.bigquery.input;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.talend.components.bigquery.dataset.TableDataSet;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.components.bigquery.service.BigQueryService;
import org.talend.components.bigquery.service.GoogleStorageService;
import org.talend.components.bigquery.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.storage.Storage;

public class BigQueryTableExtractInputTest {

    public static class SimpleGenericRecord implements GenericRecord {

        private final Map<String, Object> internalMap = new HashMap<>();

        private Schema schema;

        @Override
        public void put(String key, Object v) {
            internalMap.put(key, v);
        }

        @Override
        public Object get(String key) {
            return internalMap.get(key);
        }

        @Override
        public void put(int i, Object v) {
            internalMap.put(String.valueOf(i), v);
        }

        @Override
        public Object get(int i) {
            return internalMap.get(String.valueOf(i));
        }

        @Override
        public Schema getSchema() {
            if (schema == null) {
                schema = SchemaBuilder.record("X").fields()

                        .name("f1").type(Schema.create(Schema.Type.STRING)).noDefault()

                        .name("f2").type(Schema.create(Schema.Type.INT)).withDefault(0)

                        .name("f3").type(LogicalTypes.decimal(10, 3).addToSchema(Schema.create(Schema.Type.BYTES))).noDefault()

                        .name("f4")
                        .type(Schema.createArray(LogicalTypes.decimal(10, 3).addToSchema(Schema.create(Schema.Type.BYTES))))
                        .noDefault()

                        .endRecord();
            }

            return schema;
        }
    }

    private BigQueryTableExtractInputConfig configuration;

    private BigQueryService service;

    private GoogleStorageService storageService;

    private I18nMessage i18n;

    private RecordBuilderFactory builderFactory;

    private BigQueryConnection connection;

    private BigQuery bigQuery;

    private Storage storage;

    private DataFileStream<GenericRecord> dataStream;

    @BeforeEach
    public void reinit() throws Exception {
        connection = new BigQueryConnection();
        connection.setProjectName("projectName");
        connection.setJsonCredentials("");

        configuration = new BigQueryTableExtractInputConfig();
        TableDataSet tableDataSet = new TableDataSet();
        tableDataSet.setTableName("tableName");
        tableDataSet.setGsBucket("gsBucket");
        tableDataSet.setBqDataset("bqDataset");
        tableDataSet.setConnection(connection);
        configuration.setTableDataset(tableDataSet);

        i18n = Mockito.mock(I18nMessage.class);
        builderFactory = new RecordBuilderFactoryImpl(null);
        service = Mockito.mock(BigQueryService.class);
        storageService = Mockito.mock(GoogleStorageService.class);

        Mockito.doCallRealMethod().when(service).convertToTckField(Mockito.any(FieldValueList.class),
                Mockito.any(Record.Builder.class), Mockito.any(com.google.cloud.bigquery.Field.class),
                Mockito.any(com.google.cloud.bigquery.Schema.class));
        Mockito.doCallRealMethod().when(service).convertToTckSchema(Mockito.any(com.google.cloud.bigquery.Schema.class));
        Mockito.doCallRealMethod().when(service).convertToTckType(Mockito.any(LegacySQLTypeName.class),
                Mockito.any(com.google.cloud.bigquery.Field.Mode.class));

        Field rbField = BigQueryService.class.getDeclaredField("recordBuilderFactoryService");
        rbField.setAccessible(true);
        rbField.set(service, builderFactory);

        bigQuery = Mockito.mock(BigQuery.class);
        Mockito.when(service.createClient(connection)).thenReturn(bigQuery);
        BigQueryOptions bqOptions = Mockito.mock(BigQueryOptions.class);
        Mockito.when(bigQuery.getOptions()).thenReturn(bqOptions);
        Credentials credentials = Mockito.mock(Credentials.class);
        Mockito.when(bqOptions.getCredentials()).thenReturn(credentials);

        storage = Mockito.mock(Storage.class);
        Mockito.when(storageService.getStorage(credentials)).thenReturn(storage);
        dataStream = Mockito.mock(DataFileStream.class);
        Mockito.when(storageService.getDataFileStream(Mockito.eq(storage), Mockito.eq(tableDataSet.getGsBucket()),
                Mockito.anyString())).thenReturn(dataStream);
    }

    @Test
    public void justRun() throws Exception {

        Mockito.when(dataStream.hasNext()).thenReturn(true, false);
        Mockito.when(dataStream.next()).thenReturn(getGenericRecord(), null);

        String gsBlob = "aBlob";
        BigQueryTableExtractInput beanUnderTest = new BigQueryTableExtractInput(configuration, service, storageService, i18n,
                builderFactory, gsBlob, getTckSchema());
        beanUnderTest.init();

        Record record1 = beanUnderTest.next();
        Assertions.assertNotNull(record1);
        GenericRecord expected = getGenericRecord();
        Assertions.assertEquals(expected.get("f1"), record1.getString("f1"));
        Assertions.assertEquals(expected.get("f2"), record1.getInt("f2"));
        Assertions.assertEquals("100.101", record1.getString("f3"));
        Assertions.assertIterableEquals(Arrays.asList("100.101", "100"), record1.getArray(String.class, "f4"));
        Record record2 = beanUnderTest.next();
        Assertions.assertNull(record2);

        beanUnderTest.release();
        Mockito.verify(storageService, Mockito.times(1)).deleteBlob(storage, configuration.getTableDataset().getGsBucket(),
                gsBlob);
    }

    private org.talend.sdk.component.api.record.Schema getTckSchema() {
        org.talend.sdk.component.api.record.Schema.Builder schemaBuilder = builderFactory
                .newSchemaBuilder(org.talend.sdk.component.api.record.Schema.Type.RECORD);

        schemaBuilder.withEntry(builderFactory.newEntryBuilder().withName("f1")
                .withType(org.talend.sdk.component.api.record.Schema.Type.STRING).withNullable(true).build());
        schemaBuilder.withEntry(builderFactory.newEntryBuilder().withName("f2")
                .withType(org.talend.sdk.component.api.record.Schema.Type.INT).withNullable(true).build());
        schemaBuilder.withEntry(builderFactory.newEntryBuilder().withName("f3")
                .withType(org.talend.sdk.component.api.record.Schema.Type.STRING).withNullable(true).build());
        schemaBuilder.withEntry(
                builderFactory.newEntryBuilder().withName("f4").withType(org.talend.sdk.component.api.record.Schema.Type.ARRAY)
                        .withElementSchema(schemaBuilder.withEntry(builderFactory.newEntryBuilder().withName("element")
                                .withType(org.talend.sdk.component.api.record.Schema.Type.STRING).withNullable(true).build())
                                .build())
                        .withNullable(true).build());

        return schemaBuilder.build();
    }

    @Test
    public void ifBlobNullCallDelegate() throws Exception {
        BigQueryTableExtractInput beanUnderTest = new BigQueryTableExtractInput(configuration, service, storageService, i18n,
                builderFactory, null, null);
        beanUnderTest.init();

        // Inject delegate mock
        BigQueryTableInput mock = Mockito.mock(BigQueryTableInput.class);
        Field field = BigQueryTableExtractInput.class.getDeclaredField("delegateInput");
        field.setAccessible(true);
        field.set(beanUnderTest, mock);

        // Test
        Record record1 = beanUnderTest.next();
        Assertions.assertNull(record1);
        Mockito.verify(mock, Mockito.times(1)).next();

    }

    private GenericRecord getGenericRecord() {
        GenericRecord genericRecord = new SimpleGenericRecord();
        genericRecord.put("f1", "A");
        genericRecord.put("f2", 30);

        ByteBuffer buffer = ByteBuffer.wrap(new byte[] { 1, -121, 5 });
        ByteBuffer buffer2 = ByteBuffer.wrap(new byte[] { 1, -122, -96 });
        Collection<ByteBuffer> collectionBuffer = Arrays.asList(buffer, buffer2);
        genericRecord.put("f3", buffer);
        genericRecord.put("f4", collectionBuffer);

        return genericRecord;
    }

}
