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
package org.talend.components.azure.runtime.output.excel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.UUID;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.talend.components.azure.common.excel.ExcelFormat;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.common.service.AzureComponentServices;
import org.talend.components.azure.output.BlobOutputConfiguration;
import org.talend.components.azure.runtime.converters.ExcelConverter;
import org.talend.components.azure.runtime.output.BlobFileWriter;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.sdk.component.api.record.Record;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;

public class ExcelBlobFileWriter extends BlobFileWriter {

    private BlobOutputConfiguration config;

    private ExcelConverter converter;

    private ByteArrayOutputStream bos;

    private int excel97MaxRows = 65_536;

    private int excel2007MaxRows = 1_048_576;

    private final String fileExtension;

    public ExcelBlobFileWriter(BlobOutputConfiguration config, AzureBlobComponentServices connectionServices) throws Exception {
        super(config, connectionServices);
        this.config = config;
        ExcelFormat format = config.getDataset().getExcelOptions().getExcelFormat();
        if (format == ExcelFormat.HTML) {
            throw new IllegalArgumentException("HTML excel output is not supported");
        } else if (format == ExcelFormat.EXCEL97) {
            fileExtension = ".xls";
        } else {
            fileExtension = ".xlsx";
        }

        if (config.getDataset().getExcelOptions().isUseHeader() || config.getDataset().getExcelOptions().isUseFooter()) {
            reduceMaxRowsSize();
        }
    }

    /**
     * Generates a temp file for batch (or part of batch)
     */
    @Override
    public void generateFile(String directoryName) throws URISyntaxException, StorageException {
        String itemName = directoryName + config.getBlobNameTemplate() + UUID.randomUUID() + fileExtension;

        CloudBlob excelFile = getContainer().getBlockBlobReference(itemName);
        while (excelFile.exists(null, null, AzureComponentServices.getTalendOperationContext())) {
            itemName = directoryName + config.getBlobNameTemplate() + UUID.randomUUID() + fileExtension;
            excelFile = getContainer().getBlockBlobReference(itemName);

        }
        setCurrentItem(excelFile);
    }

    @Override
    public void writeRecord(Record record) {
        super.writeRecord(record);
        try {
            if ((config.getDataset().getExcelOptions().getExcelFormat() == ExcelFormat.EXCEL97
                    && getBatch().size() == excel97MaxRows)
                    || (config.getDataset().getExcelOptions().getExcelFormat() == ExcelFormat.EXCEL2007
                            && getBatch().size() == excel2007MaxRows)) {
                flush();
                newBatch();
            }
        } catch (Exception e) {
            throw new BlobRuntimeException("Failed to split oversized batch", e);
        }
    }

    @Override
    public void newBatch() {
        super.newBatch();
        try {
            generateFile();
        } catch (Exception e) {
            throw new BlobRuntimeException(e);
        }
    }

    /**
     * @param sheet to append header
     * @param firstDataRecord for retrieving schema
     */
    private void appendHeader(Sheet sheet, Record firstDataRecord) {
        List<String> columnNames = converter.inferRecordColumns(firstDataRecord);

        for (int i = 0; i < config.getDataset().getExcelOptions().getHeader(); i++) {
            Row headerRow = sheet.createRow(i);
            for (int j = 0; j < columnNames.size(); j++) {
                Cell headerCell = headerRow.createCell(j);
                headerCell.setCellValue(columnNames.get(j));
            }
        }
    }

    private void appendFooter(Sheet sheet, int dataRowCounter) {
        for (int i = dataRowCounter; i < dataRowCounter + config.getDataset().getExcelOptions().getFooter(); i++) {
            Row footerRow = sheet.createRow(i);
            Cell footerCell = footerRow.createCell(0);
            footerCell.setCellValue("//footer line");

        }
    }

    @Override
    public void flush() throws IOException, StorageException {
        if (getBatch().isEmpty()) {
            return;
        }

        bos = new ByteArrayOutputStream();

        flushBatchToByteArray();

        getCurrentItem().upload(new ByteArrayInputStream(bos.toByteArray()), -1);
        bos.close();
        getBatch().clear();
    }

    private void flushBatchToByteArray() throws IOException {
        Workbook item = ExcelUtils.createWorkBook(config.getDataset().getExcelOptions().getExcelFormat());
        Sheet sheet = item.createSheet(config.getDataset().getExcelOptions().getSheetName());
        converter = ExcelConverter.ofOutput(sheet);
        int dataRowCounter = 0;
        if (config.getDataset().getExcelOptions().isUseHeader() && config.getDataset().getExcelOptions().getHeader() > 0) {
            appendHeader(sheet, getBatch().get(0));
            dataRowCounter += config.getDataset().getExcelOptions().getHeader();
        }
        converter.appendBatchToTheSheet(getBatch(), dataRowCounter);
        dataRowCounter += getBatch().size();

        if (config.getDataset().getExcelOptions().isUseFooter() && config.getDataset().getExcelOptions().getFooter() > 0) {
            appendFooter(sheet, dataRowCounter);
        }
        item.write(bos);
        item.close();
    }

    private void reduceMaxRowsSize() {
        if (config.getDataset().getExcelOptions().isUseHeader() && config.getDataset().getExcelOptions().getHeader() > 0) {
            this.excel97MaxRows -= config.getDataset().getExcelOptions().getHeader();
            this.excel2007MaxRows -= config.getDataset().getExcelOptions().getHeader();
        }

        if (config.getDataset().getExcelOptions().isUseFooter() && config.getDataset().getExcelOptions().getFooter() > 0) {
            this.excel97MaxRows -= config.getDataset().getExcelOptions().getFooter();
            this.excel2007MaxRows -= config.getDataset().getExcelOptions().getFooter();
        }
    }
}
