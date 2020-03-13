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
package org.talend.components.azure.output;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.BaseIT;
import org.talend.components.azure.BlobTestUtils;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.datastore.AzureCloudConnection;
import org.talend.components.azure.source.BlobInputProperties;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;
import org.talend.sdk.component.runtime.record.SchemaImpl;

import com.microsoft.azure.storage.blob.CloudBlobContainer;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

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

        Assert.assertTrue("No files were created in test container",
                container.listBlobs(blobOutputProperties.getDataset().getDirectory(), false).iterator().hasNext());

        BlobInputProperties inputProperties = new BlobInputProperties();
        inputProperties.setDataset(blobOutputProperties.getDataset());

        String inputConfig = configurationByExample().forInstance(inputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assert.assertEquals(recordSize, records.size());
        Record firstRecord = records.get(0);
        Assert.assertEquals(testRecord.getBoolean("booleanValue"), firstRecord.getBoolean("booleanValue"));
        Assert.assertEquals(testRecord.getLong("longValue"), firstRecord.getLong("longValue"));
        Assert.assertEquals(testRecord.getInt("intValue"), firstRecord.getInt("intValue"));
        Assert.assertEquals(testRecord.getDouble("doubleValue"), firstRecord.getDouble("doubleValue"), 0.01);
        Assert.assertEquals(testRecord.getDateTime("dateValue"), firstRecord.getDateTime("dateValue"));
        Assert.assertArrayEquals(testRecord.getBytes("byteArray"), firstRecord.getBytes("byteArray"));
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

        Assert.assertTrue("No files were created in test container",
                container.listBlobs(blobOutputProperties.getDataset().getDirectory() + "/", false).iterator().hasNext());
    }

    @Test
    void testOutputNull() {
        final int recordSize = 1;
        final int schemaSize = 9;
        Schema.Builder schemaBuilder = new SchemaImpl.BuilderImpl();
        Schema schema = schemaBuilder.withType(Schema.Type.RECORD)
                .withEntry(new SchemaImpl.EntryImpl("nullStringColumn", Schema.Type.STRING, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullStringColumn2", Schema.Type.STRING, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullIntColumn", Schema.Type.INT, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullLongColumn", Schema.Type.LONG, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullFloatColumn", Schema.Type.FLOAT, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullDoubleColumn", Schema.Type.DOUBLE, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullBooleanColumn", Schema.Type.BOOLEAN, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullByteArrayColumn", Schema.Type.BYTES, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullDateColumn", Schema.Type.DATETIME, true, null, null, null)).build();
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

        Assert.assertEquals(recordSize, records.size());
        Record firstRecord = records.get(0);

        Assert.assertEquals(schemaSize, firstRecord.getSchema().getEntries().size());
        Assert.assertNull(firstRecord.getString("nullStringColumn"));
        Assert.assertNull(firstRecord.getString("nullStringColumn2"));
        Assert.assertNull(firstRecord.get(Integer.class, "nullIntColumn"));
        Assert.assertNull(firstRecord.get(Long.class, "nullLongColumn"));
        Assert.assertNull(firstRecord.get(Float.class, "nullFloatColumn"));
        Assert.assertNull(firstRecord.get(Double.class, "nullDoubleColumn"));
        Assert.assertNull(firstRecord.get(Boolean.class, "nullBooleanColumn"));
        Assert.assertNull(firstRecord.get(byte[].class, "nullByteArrayColumn"));
        Assert.assertNull(firstRecord.getDateTime("nullDateColumn"));
    }
}
