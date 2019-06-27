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

package org.talend.components.azure.source;

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
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.azure")
public class CSVInputIT extends BaseIT {

    private static BlobInputProperties blobInputProperties;

    @BeforeEach
    void initDataset() {
        AzureBlobDataset dataset = new AzureBlobDataset();
        dataset.setConnection(dataStore);
        dataset.setFileFormat(FileFormat.CSV);

        CSVFormatOptions formatOptions = new CSVFormatOptions();
        formatOptions.setRecordDelimiter(RecordDelimiter.LF);
        dataset.setCsvOptions(formatOptions);
        dataset.setContainerName(containerName);
        dataset.setObjectName("someDir");

        blobInputProperties = new BlobInputProperties();
        blobInputProperties.setDataset(dataset);
    }

    @Test
    void selectAllInputPipelineTest() throws Exception {
        final int recordSize = 10;
        List<String> columns = Arrays.asList(new String[] { "a", "b", "c" });
        BlobTestUtils.createAndPopulateFileInStorage(storageAccount, blobInputProperties.getDataset(), columns, recordSize);

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);
        Record firstRecord = records.get(0);

        Assert.assertEquals("Records amount is different", recordSize, records.size());
        Assert.assertEquals("Columns number is different", columns.size(), firstRecord.getSchema().getEntries().size());
    }

    @Test
    void selectFromNotExistingDirectory() {
        blobInputProperties.getDataset().setObjectName("notExistingDir");
        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assert.assertEquals("Records were taken from empty directory", 0, records.size());
    }

    @Test
    void testInputMultipleFiles() throws Exception {
        final int recordSize = 10 + 5;

        List<String> columns = Arrays.asList(new String[] { "a", "b", "c" });
        BlobTestUtils.createAndPopulateFileInStorage(storageAccount, blobInputProperties.getDataset(), columns, 10);
        BlobTestUtils.createAndPopulateFileInStorage(storageAccount, blobInputProperties.getDataset(), columns, 5);

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assert.assertEquals("Records amount is different", recordSize, records.size());
    }

    @Test
    void testHeaderIsGreaterThanFileContent() throws Exception {
        final int recordSize = 0;
        List<String> columns = Arrays.asList(new String[] { "a", "b", "c" });
        BlobTestUtils.createAndPopulateFileInStorage(storageAccount, blobInputProperties.getDataset(), columns, 1);
        blobInputProperties.getDataset().getCsvOptions().setUseHeader(true);
        blobInputProperties.getDataset().getCsvOptions().setHeader(5);

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assert.assertEquals("Records amount is different", recordSize, records.size());
    }

    @Test
    void testCorrectHeader() throws Exception {
        final int recordSize = 5 - 1;
        List<String> columns = Arrays.asList(new String[] { "a", "b", "c" });
        BlobTestUtils.createAndPopulateFileInStorage(storageAccount, blobInputProperties.getDataset(), columns, 5);
        blobInputProperties.getDataset().getCsvOptions().setUseHeader(true);
        blobInputProperties.getDataset().getCsvOptions().setHeader(1);

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assert.assertEquals("Records amount is different", recordSize, records.size());
    }

    @Test
    void testReadingFromFile() throws Exception {
        final int recordSize = 1;
        List<String> columns = Arrays.asList(new String[] { "a", "b", "c" });
        BlobTestUtils.createAndPopulateFileInStorage(storageAccount, blobInputProperties.getDataset(), columns, recordSize,
                "file1");
        blobInputProperties.getDataset().setObjectName("someDir/file1");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assert.assertEquals("Records amount is different", recordSize, records.size());
    }

    @Test
    void testReadFromFileWhenDirWithSameNameExist() throws Exception {
        final int recordSize = 10;
        final int recordSizeInAnotherFile = 20;
        List<String> columns = Arrays.asList(new String[] { "a", "b", "c" });
        List<String> columnsInAnotherFile = Arrays.asList(new String[] { "a", "b", "c", "d" });
        BlobTestUtils.createAndPopulateFileInStorage(storageAccount, blobInputProperties.getDataset(), columns, recordSize,
                "file1");
        BlobTestUtils.createAndPopulateFileInStorage(storageAccount, blobInputProperties.getDataset(), columnsInAnotherFile,
                recordSizeInAnotherFile, "file1/file1");
        blobInputProperties.getDataset().setObjectName("someDir/file1");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assert.assertEquals("Records amount is different", recordSize, records.size());
        Assert.assertEquals("File from directory was read instead of direct file", columns.size(),
                records.get(0).getSchema().getEntries().size());
    }
}
