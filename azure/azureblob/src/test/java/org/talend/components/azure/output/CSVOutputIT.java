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

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.BaseIT;
import org.talend.components.azure.BlobTestUtils;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.azure.common.csv.CSVFormatOptions;
import org.talend.components.azure.common.csv.RecordDelimiter;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.runtime.converters.CSVConverter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import com.microsoft.azure.storage.StorageException;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.azure")
class CSVOutputIT extends BaseIT {

    private BlobOutputConfiguration blobOutputProperties;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @BeforeEach
    void initDataset() {
        AzureBlobDataset dataset = new AzureBlobDataset();
        dataset.setConnection(dataStore);
        dataset.setFileFormat(FileFormat.CSV);

        CSVFormatOptions formatOptions = new CSVFormatOptions();
        formatOptions.setRecordDelimiter(RecordDelimiter.LF);
        dataset.setCsvOptions(formatOptions);
        dataset.setContainerName(containerName);
        blobOutputProperties = new BlobOutputConfiguration();
        blobOutputProperties.setDataset(dataset);
    }

    @Test
    void outputTestWithSixSameRecordsAndStandardConfig() throws StorageException, IOException, URISyntaxException {
        final int recordSize = 6;
        final boolean testBooleanValue = true;
        final long testLongValue = 0L;
        final int testIntValue = 1;
        final double testDoubleValue = 2.0;
        final ZonedDateTime testDateValue = ZonedDateTime.now();
        final byte[] bytes = new byte[] { 1, 2, 3 };

        blobOutputProperties.getDataset().setDirectory("testDir");
        blobOutputProperties.setBlobNameTemplate("testFile");
        blobOutputProperties.getDataset().getCsvOptions().setTextEnclosureCharacter("\"");

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
        Job.components().component("inputFlow", "test://emitter").component("outputComponent", "Azure://Output?" + outputConfig)
                .connections().from("inputFlow").to("outputComponent").build().run();
        BlobTestUtils.recordBuilderFactory = componentsHandler.findService(RecordBuilderFactory.class);
        List<Record> retrievedRecords = BlobTestUtils.readDataFromCSVDirectory(blobOutputProperties.getDataset().getDirectory(),
                storageAccount, blobOutputProperties.getDataset(),
                CSVConverter.of(recordBuilderFactory, blobOutputProperties.getDataset().getCsvOptions()).getCsvFormat());

        Assert.assertEquals(recordSize, retrievedRecords.size());
        Assert.assertEquals(testRecord.getSchema().getEntries().size(), retrievedRecords.get(0).getSchema().getEntries().size());
        Assert.assertEquals(String.valueOf(testRecord.getBoolean("booleanValue")), retrievedRecords.get(0).getString("field0"));
        Assert.assertEquals(String.valueOf(testRecord.getLong("longValue")), retrievedRecords.get(0).getString("field1"));
        Assert.assertEquals(String.valueOf(testRecord.getInt("intValue")), retrievedRecords.get(0).getString("field2"));
        Assert.assertEquals(String.valueOf(testRecord.getDouble("doubleValue")), retrievedRecords.get(0).getString("field3"));
        Assert.assertEquals(String.valueOf(testRecord.getDateTime("dateValue")), retrievedRecords.get(0).getString("field4"));
        Assert.assertEquals(Arrays.toString(testRecord.getBytes("byteArray")), retrievedRecords.get(0).getString("field5"));
    }
}