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
package org.talend.components.azure.source;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.BaseIT;
import org.talend.components.azure.BlobTestUtils;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import com.microsoft.azure.storage.StorageException;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.azure")
class ParquetInputIT extends BaseIT {

    private static BlobInputProperties blobInputProperties;

    @BeforeEach
    void initDataset() {
        AzureBlobDataset dataset = new AzureBlobDataset();
        dataset.setConnection(dataStore);
        dataset.setFileFormat(FileFormat.PARQUET);

        dataset.setContainerName(containerName);
        dataset.setDirectory("excelHTML");
        blobInputProperties = new BlobInputProperties();
        blobInputProperties.setDataset(dataset);
    }

    @Test
    void testInput1File1Record() throws Exception {
        final int recordSize = 1;
        final int columnSize = 6;
        final boolean booleanValue = true;
        final long longValue = 0L;
        final int intValue = 1;
        final double doubleValue = 2.0;
        final long dateValue = 1556612530082L;
        final byte[] bytesValue = new byte[] { 1, 2, 3 };

        blobInputProperties.getDataset().setDirectory("parquet");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "parquet/testParquet1Record.parquet",
                "testParquet1Record.parquet");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assert.assertEquals("Records amount is different", recordSize, records.size());
        Record firstRecord = records.get(0);
        Assert.assertEquals(columnSize, firstRecord.getSchema().getEntries().size());
        Assert.assertEquals(booleanValue, firstRecord.getBoolean("booleanValue"));
        Assert.assertEquals(longValue, firstRecord.getLong("longValue"));
        Assert.assertEquals(intValue, firstRecord.getInt("intValue"));
        Assert.assertEquals(doubleValue, firstRecord.getDouble("doubleValue"), 0.01);
        Assert.assertEquals(ZonedDateTime.ofInstant(Instant.ofEpochMilli(dateValue), ZoneId.of("UTC")),
                firstRecord.getDateTime("dateValue"));
        Assert.assertArrayEquals(bytesValue, firstRecord.getBytes("byteArray"));
    }

    @Test
    void testInput1FileMultipleRecords() throws StorageException, IOException, URISyntaxException {
        final int recordSize = 6;
        blobInputProperties.getDataset().setDirectory("parquet");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "parquet/testParquet6Records.parquet",
                "testParquet6Records.parquet");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assert.assertEquals("Records amount is different", recordSize, records.size());
    }

    @Test
    void testInputMultipleFiles() throws Exception {
        final int recordSize = 1 + 6;
        blobInputProperties.getDataset().setDirectory("parquet");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "parquet/testParquet1Record.parquet",
                "testParquet1Record.parquet");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "parquet/testParquet6Records.parquet",
                "testParquet6Records.parquet");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assert.assertEquals("Records amount is different", recordSize, records.size());
    }

    @Test
    public void testInputReadFilesOnlyFromOneDir() throws Exception {
        final int recordSize = 1;
        // ignored dir
        blobInputProperties.getDataset().setDirectory("parquet_ignored");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "parquet/testParquet6Records.parquet",
                "testParquet6Records.parquet");
        blobInputProperties.getDataset().setDirectory("parquet");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "parquet/testParquet1Record.parquet",
                "testParquet1Record.parquet");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assert.assertEquals("Records amount is different", recordSize, records.size());
    }

    @Test
    void testInputFileWithNullValues() throws Exception {
        final int recordSize = 1;
        final int columnSize = 9;
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "parquet/testParquetNullRecord.parquet",
                "testParquetNullRecord.parquet");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assert.assertEquals("Records amount is different", recordSize, records.size());
        Record firstRecord = records.get(0);
        Assert.assertEquals(columnSize, firstRecord.getSchema().getEntries().size());
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
