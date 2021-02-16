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
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.BaseIT;
import org.talend.components.azure.BlobTestUtils;
import org.talend.components.common.formats.Encoding;
import org.talend.components.azure.common.FileFormat;
import org.talend.components.common.formats.excel.ExcelFormat;
import org.talend.components.common.formats.excel.ExcelFormatOptions;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import com.microsoft.azure.storage.StorageException;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.azure")
class Excel97IT extends BaseIT {

    private BlobInputProperties blobInputProperties;

    private double idValue = 1.0;

    private final String nameValue = "a";

    private final double longValue = 10000000000000.0;

    private final double doubleValue = 2.5;

    private final double dateValue = 43501.0;

    private final boolean booleanValue = true;

    @BeforeEach
    void initDataSet() {
        AzureBlobDataset dataset = new AzureBlobDataset();
        dataset.setConnection(dataStore);
        dataset.setFileFormat(FileFormat.EXCEL);
        ExcelFormatOptions excelFormatOptions = new ExcelFormatOptions();
        excelFormatOptions.setSheetName("Sheet1");
        excelFormatOptions.setExcelFormat(ExcelFormat.EXCEL97);
        excelFormatOptions.setEncoding(Encoding.UTF8);
        dataset.setExcelOptions(excelFormatOptions);

        dataset.setContainerName(containerName);
        dataset.setDirectory("excel97");
        blobInputProperties = new BlobInputProperties();
        blobInputProperties.setDataset(dataset);
    }

    @Test
    void test1File1RecordsWithoutHeader() throws StorageException, IOException, URISyntaxException {
        blobInputProperties.getDataset().getExcelOptions().setUseHeader(false);

        final int recordSize = 1;
        final int columnSize = 6;
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_1_record_no_header.xls",
                "excel_97_1_record_no_header.xls");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
        Record firstRecord = records.get(0);
        Assertions.assertEquals(columnSize, firstRecord.getSchema().getEntries().size(), "Record's schema is different");

        Assertions.assertEquals(idValue, firstRecord.getDouble("field0"), 0.01);
        Assertions.assertEquals(nameValue, firstRecord.getString("field1"));
        Assertions.assertEquals(longValue, firstRecord.getDouble("field2"), 0.01);
        Assertions.assertEquals(doubleValue, firstRecord.getDouble("field3"), 0.01);
        Assertions.assertEquals(dateValue, firstRecord.getDouble("field4"), 0.01);
        Assertions.assertEquals(booleanValue, firstRecord.getBoolean("field5"));
    }

    @Test
    void test1File5RecordsWithoutHeader() throws StorageException, IOException, URISyntaxException {
        final int recordSize = 5;
        final int columnSize = 6;

        blobInputProperties.getDataset().getExcelOptions().setUseHeader(false);
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_5_records_no_header.xls",
                "excel_97_5_records_no_header.xls");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
        Record firstRecord = records.get(0);
        Assertions.assertEquals(columnSize, firstRecord.getSchema().getEntries().size(), "Record's schema is different");
    }

    @Test
    void testMultipleFilesWithoutHeader() throws StorageException, IOException, URISyntaxException {
        final int recordSize = 1 + 5;

        blobInputProperties.getDataset().getExcelOptions().setUseHeader(false);

        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_1_record_no_header.xls",
                "excel_97_1_record_no_header.xls");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_5_records_no_header.xls",
                "excel_97_5_records_no_header.xls");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
    }

    @Test
    void testInput1FileWithHeader1Row() throws StorageException, IOException, URISyntaxException {
        blobInputProperties.getDataset().getExcelOptions().setUseHeader(true);
        blobInputProperties.getDataset().getExcelOptions().setHeader(1);

        final int recordSize = 1;
        final int columnSize = 6;
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_1_record_with_header.xls",
                "excel_97_1_record_with_header.xls");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
        Record firstRecord = records.get(0);
        Assertions.assertEquals(columnSize, firstRecord.getSchema().getEntries().size(), "Record's schema is different");

        Assertions.assertEquals(idValue, firstRecord.getDouble("id"), 0.01);
        Assertions.assertEquals(nameValue, firstRecord.getString("name"));
        Assertions.assertEquals(longValue, firstRecord.getDouble("longValue"), 0.01);
        Assertions.assertEquals(doubleValue, firstRecord.getDouble("doubleValue"), 0.01);
        Assertions.assertEquals(dateValue, firstRecord.getDouble("dateValue"), 0.01);
        Assertions.assertEquals(booleanValue, firstRecord.getBoolean("booleanValue"));
    }

    @Test
    void testInput1FileMultipleRows() throws StorageException, IOException, URISyntaxException {
        final int recordSize = 5;
        final int columnSize = 6;

        blobInputProperties.getDataset().getExcelOptions().setUseHeader(true);
        blobInputProperties.getDataset().getExcelOptions().setHeader(1);
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_5_records_with_header.xls",
                "excel_97_5_records_with_header.xls");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
        Record firstRecord = records.get(0);
        Assertions.assertEquals(columnSize, firstRecord.getSchema().getEntries().size(), "Record's schema is different");
    }

    @Test
    void testInputMultipleFilesWithHeaders() throws StorageException, IOException, URISyntaxException {
        final int recordSize = 1 + 5;

        blobInputProperties.getDataset().getExcelOptions().setUseHeader(true);
        blobInputProperties.getDataset().getExcelOptions().setHeader(1);

        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_1_record_with_header.xls",
                "excel_97_1_record_with_header.xls");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_5_records_with_header.xls",
                "excel_97_5_records_with_header.xls");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
    }

    @Test
    void test1File1RecordWithBigHeader() throws StorageException, IOException, URISyntaxException {
        final String nameValue = "a";
        final double longValue = 10000000000000.0;
        final double doubleValue = 2.5;
        final double dateValue = 43501.0;
        final boolean booleanValue = true;

        blobInputProperties.getDataset().getExcelOptions().setUseHeader(true);
        blobInputProperties.getDataset().getExcelOptions().setHeader(2);

        final int recordSize = 1;
        final int columnSize = 6;
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_1_record_with_big_header.xls",
                "excel_97_1_record_with_big_header.xls");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
        Record firstRecord = records.get(0);
        Assertions.assertEquals(columnSize, firstRecord.getSchema().getEntries().size(), "Record's schema is different");

        Assertions.assertEquals(idValue, firstRecord.getDouble("id"), 0.01);
        Assertions.assertEquals(nameValue, firstRecord.getString("name"));
        Assertions.assertEquals(longValue, firstRecord.getDouble("longValue"), 0.01);
        Assertions.assertEquals(doubleValue, firstRecord.getDouble("doubleValue"), 0.01);
        Assertions.assertEquals(dateValue, firstRecord.getDouble("dateValue"), 0.01);
        Assertions.assertEquals(booleanValue, firstRecord.getBoolean("booleanValue"));
    }

    @Test
    void test1File5RecordsWithBigHeader() throws StorageException, IOException, URISyntaxException {
        final int recordSize = 5;
        final int columnSize = 6;

        blobInputProperties.getDataset().getExcelOptions().setUseHeader(true);
        blobInputProperties.getDataset().getExcelOptions().setHeader(2);
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_5_records_with_big_header.xls",
                "excel_97_5_records_with_big_header.xls");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
        Record firstRecord = records.get(0);
        Assertions.assertEquals(columnSize, firstRecord.getSchema().getEntries().size(), "Record's schema is different");
    }

    @Test
    void testMultipleFilesWithBigHeaders() throws StorageException, IOException, URISyntaxException {
        final int recordSize = 1 + 5;

        blobInputProperties.getDataset().getExcelOptions().setUseHeader(true);
        blobInputProperties.getDataset().getExcelOptions().setHeader(2);

        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_1_record_with_big_header.xls",
                "excel_97_1_record_with_big_header.xls");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_5_records_with_big_header.xls",
                "excel_97_5_records_with_big_header.xls");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
    }

    @Test
    void test1FileWithFooter() throws StorageException, IOException, URISyntaxException {
        blobInputProperties.getDataset().getExcelOptions().setUseFooter(true);
        blobInputProperties.getDataset().getExcelOptions().setFooter(1);

        final int recordSize = 1;
        final int columnSize = 6;
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_1_record_footer.xls",
                "excel_97_1_record_footer.xls");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
        Record firstRecord = records.get(0);
        Assertions.assertEquals(columnSize, firstRecord.getSchema().getEntries().size(), "Record's schema is different");

        Assertions.assertEquals(idValue, firstRecord.getDouble("field0"), 0.01);
        Assertions.assertEquals(nameValue, firstRecord.getString("field1"));
        Assertions.assertEquals(longValue, firstRecord.getDouble("field2"), 0.01);
        Assertions.assertEquals(doubleValue, firstRecord.getDouble("field3"), 0.01);
        Assertions.assertEquals(dateValue, firstRecord.getDouble("field4"), 0.01);
        Assertions.assertEquals(booleanValue, firstRecord.getBoolean("field5"));
    }

    @Test
    void testReadFileWithEmptyCells() throws StorageException, IOException, URISyntaxException {
        final int recordSize = 2;
        final int columnSizeForFullRecord = 5;
        final int columnSizeForRecordsWithNulls = 2;
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_2_records_empty_cell.xls",
                "excel_97_2_records_empty_cell.xls");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
        Record fullRecord = records.get(0);
        Record recordWithEmptyCells = records.get(1);
        Assertions.assertEquals(columnSizeForFullRecord, fullRecord.getSchema().getEntries().size(),
                "Column number for row without empty cells is different");
        Assertions.assertEquals(columnSizeForRecordsWithNulls, recordWithEmptyCells.getSchema().getEntries().size(),
                "Column number for row with empty cells is different");
    }

    @Test
    void testSkipFileWithoutSpecifiedSheetName() throws StorageException, IOException, URISyntaxException {
        final int recordSize = 2; // 3 files, 1 with another sheet name (should be skipped)
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_1_record_no_header.xls",
                "excel_97_1_record_no_header.xls");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_1_record_another_sheet_name.xls",
                "excel_97_1_record_another_sheet_name.xls");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_1_record_no_header.xls",
                "excel_97_1_record_no_header2.xls");

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
    }

    @Test
    void testSeveralFilesWithHeaderAndFooters() throws Exception {
        final int recordSize = 3 * (5 - 1); // 3 files, 1 record as a footer in each
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_5_records_with_big_header.xls",
                "excel_97_5_records_with_big_header1.xls");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_5_records_with_big_header.xls",
                "excel_97_5_records_with_big_header2.xls");
        BlobTestUtils.uploadTestFile(storageAccount, blobInputProperties, "excel97/excel_97_5_records_with_big_header.xls",
                "excel_97_5_records_with_big_header3.xls");

        blobInputProperties.getDataset().getExcelOptions().setUseHeader(true);
        blobInputProperties.getDataset().getExcelOptions().setHeader(2);
        blobInputProperties.getDataset().getExcelOptions().setUseFooter(true);
        blobInputProperties.getDataset().getExcelOptions().setFooter(1);

        String inputConfig = configurationByExample().forInstance(blobInputProperties).configured().toQueryString();
        Job.components().component("azureInput", "Azure://Input?" + inputConfig).component("collector", "test://collector")
                .connections().from("azureInput").to("collector").build().run();
        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(recordSize, records.size(), "Records amount is different");
    }
}
