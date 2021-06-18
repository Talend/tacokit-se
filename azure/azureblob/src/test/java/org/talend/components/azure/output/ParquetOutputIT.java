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
package org.talend.components.azure.output;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.microsoft.azure.storage.blob.CloudBlobContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.BaseIT;
import org.talend.components.azure.BlobTestUtils;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.datastore.AzureCloudConnection;
import org.talend.components.azure.source.BlobInputProperties;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@WithComponents("org.talend.components.azure")
class ParquetOutputIT extends BaseIT {

    private BlobOutputConfiguration blobOutputProperties;

    final String testStringValue = "test";

    final boolean testBooleanValue = true;

    final long testLongValue = 0L;

    final int testIntValue = 1;

    final double testDoubleValue = 2.0;

    final ZonedDateTime testDateValue = ZonedDateTime.now();

    final byte[] bytes = new byte[] { 1, 2, 3 };

    @Service
    private RecordBuilderFactory factory;

    @BeforeEach
    public void initDataset() {
        AzureCloudConnection dataStore = BlobTestUtils.createCloudConnection();

        AzureBlobDataset dataset = new AzureBlobDataset();
        dataset.setConnection(dataStore);
        dataset.setFileFormat(FileFormat.PARQUET);

        dataset.setContainerName(containerName);
        blobOutputProperties = new BlobOutputConfiguration();
        blobOutputProperties.setDataset(dataset);
        blobOutputProperties.getDataset().setDirectory("testDir");
        blobOutputProperties.setBlobNameTemplate("testFile");
    }

    @Test
    public void testOutput() throws Exception {
        final int recordSize = 6;

        Record testRecord = componentsHandler.findService(RecordBuilderFactory.class).newRecordBuilder()
                .withBoolean("booleanValue", testBooleanValue).withLong("longValue", testLongValue)
                .withInt("intValue", testIntValue).withDouble("doubleValue", testDoubleValue)
                .withDateTime("dateValue", testDateValue).withBytes("byteArray", bytes).build();

        List<Record> testRecords = new ArrayList<>();
        for (int i = 0; i < recordSize; i++) {
            testRecords.add(testRecord);
        }
        componentsHandler.setInputData(testRecords);

        String outputConfig = configurationByExample().forInstance(blobOutputProperties).configured().toQueryString();
        outputConfig += "&$configuration.$maxBatchSize=" + recordSize;
        Job.components().component("inputFlow", "test://emitter").component("outputComponent", "Azure://Output?" + outputConfig)
                .connections().from("inputFlow").to("outputComponent").build().run();

        CloudBlobContainer container = storageAccount.createCloudBlobClient().getContainerReference(containerName);

        Assertions.assertTrue(container.listBlobs(blobOutputProperties.getDataset().getDirectory(), false).iterator().hasNext(),
                "No files were created in test container");

        BlobInputProperties inputProperties = new BlobInputProperties();
        inputProperties.setDataset(blobOutputProperties.getDataset());

        String inputConfig = configurationByExample().forInstance(inputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size());
        Record firstRecord = records.get(0);
        Assertions.assertEquals(testRecord.getBoolean("booleanValue"), firstRecord.getBoolean("booleanValue"));
        Assertions.assertEquals(testRecord.getLong("longValue"), firstRecord.getLong("longValue"));
        Assertions.assertEquals(testRecord.getInt("intValue"), firstRecord.getInt("intValue"));
        Assertions.assertEquals(testRecord.getDouble("doubleValue"), firstRecord.getDouble("doubleValue"), 0.01);
        Assertions.assertEquals(testRecord.getDateTime("dateValue"), firstRecord.getDateTime("dateValue"));
        Assertions.assertArrayEquals(testRecord.getBytes("byteArray"), firstRecord.getBytes("byteArray"));
    }

    @Test
    public void testBatchSizeIsGreaterThanRowSize() throws Exception {
        final int recordSize = 5;

        Record testRecord = componentsHandler.findService(RecordBuilderFactory.class).newRecordBuilder()
                .withString("stringValue", testStringValue).withBoolean("booleanValue", testBooleanValue)
                .withLong("longValue", testLongValue).withInt("intValue", testIntValue).withDouble("doubleValue", testDoubleValue)
                .withDateTime("dateValue", testDateValue).withBytes("byteArray", bytes).build();

        List<Record> testRecords = new ArrayList<>();
        for (int i = 0; i < recordSize; i++) {
            testRecords.add(testRecord);
        }
        componentsHandler.setInputData(testRecords);

        String outputConfig = configurationByExample().forInstance(blobOutputProperties).configured().toQueryString();
        outputConfig += "&$configuration.$maxBatchSize=" + (recordSize * 100);
        Job.components().component("inputFlow", "test://emitter").component("outputComponent", "Azure://Output?" + outputConfig)
                .connections().from("inputFlow").to("outputComponent").build().run();

        CloudBlobContainer container = storageAccount.createCloudBlobClient().getContainerReference(containerName);

        Assertions.assertTrue(
                container.listBlobs(blobOutputProperties.getDataset().getDirectory() + "/", false).iterator().hasNext(),
                "No files were created in test container");
    }

    @Test
    void testOutputNull() {
        final int recordSize = 1;
        final int schemaSize = 9;

        Schema.Builder schemaBuilder = this.factory.newSchemaBuilder(Schema.Type.RECORD);
        Schema schema = schemaBuilder.withEntry(this.buildEntry("nullStringColumn", Schema.Type.STRING))
                .withEntry(this.buildEntry("nullStringColumn2", Schema.Type.STRING))
                .withEntry(this.buildEntry("nullIntColumn", Schema.Type.INT))
                .withEntry(this.buildEntry("nullLongColumn", Schema.Type.LONG))
                .withEntry(this.buildEntry("nullFloatColumn", Schema.Type.FLOAT))
                .withEntry(this.buildEntry("nullDoubleColumn", Schema.Type.DOUBLE))
                .withEntry(this.buildEntry("nullBooleanColumn", Schema.Type.BOOLEAN))
                .withEntry(this.buildEntry("nullByteArrayColumn", Schema.Type.BYTES))
                .withEntry(this.buildEntry("nullDateColumn", Schema.Type.DATETIME)).build();
        Record testRecord = componentsHandler.findService(RecordBuilderFactory.class).newRecordBuilder(schema)
                .withString("nullStringColumn", null).build();

        List<Record> testRecords = Collections.singletonList(testRecord);
        componentsHandler.setInputData(testRecords);

        String outputConfig = configurationByExample().forInstance(blobOutputProperties).configured().toQueryString();
        Job.components().component("inputFlow", "test://emitter").component("outputComponent", "Azure://Output?" + outputConfig)
                .connections().from("inputFlow").to("outputComponent").build().run();

        BlobInputProperties inputProperties = new BlobInputProperties();
        inputProperties.setDataset(blobOutputProperties.getDataset());

        String inputConfig = configurationByExample().forInstance(inputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size());
        Record firstRecord = records.get(0);

        Assertions.assertEquals(schemaSize, firstRecord.getSchema().getEntries().size());
        Assertions.assertNull(firstRecord.getString("nullStringColumn"));
        Assertions.assertNull(firstRecord.getString("nullStringColumn2"));
        Assertions.assertNull(firstRecord.get(Integer.class, "nullIntColumn"));
        Assertions.assertNull(firstRecord.get(Long.class, "nullLongColumn"));
        Assertions.assertNull(firstRecord.get(Float.class, "nullFloatColumn"));
        Assertions.assertNull(firstRecord.get(Double.class, "nullDoubleColumn"));
        Assertions.assertNull(firstRecord.get(Boolean.class, "nullBooleanColumn"));
        Assertions.assertNull(firstRecord.get(byte[].class, "nullByteArrayColumn"));
        Assertions.assertNull(firstRecord.getDateTime("nullDateColumn"));
    }

    private Schema.Entry buildEntry(final String name, final Schema.Type type) {
        return this.factory.newEntryBuilder().withType(type).withName(name).withNullable(true).build();
    }
}
