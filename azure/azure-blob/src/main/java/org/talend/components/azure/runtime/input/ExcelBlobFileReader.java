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
package org.talend.components.azure.runtime.input;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.talend.components.common.formats.excel.ExcelFormat;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.common.converters.ExcelConverter;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.monitorjbl.xlsx.StreamingReader;
import com.monitorjbl.xlsx.exceptions.MissingSheetException;
import com.monitorjbl.xlsx.impl.StreamingSheet;
import com.monitorjbl.xlsx.impl.StreamingWorkbook;
import avro.shaded.com.google.common.collect.Iterators;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExcelBlobFileReader extends BlobFileReader {

    public ExcelBlobFileReader(AzureBlobDataset config, RecordBuilderFactory recordBuilderFactory,
            AzureBlobComponentServices connectionServices, MessageService messageService)
            throws URISyntaxException, StorageException {
        super(config, recordBuilderFactory, connectionServices, messageService);
    }

    @Override
    protected ItemRecordIterator initItemRecordIterator(Iterable<ListBlobItem> blobItems) {
        if (getConfig().getExcelOptions().getExcelFormat() == ExcelFormat.EXCEL97) {
            return new BatchExcelRecordIterator(blobItems, getRecordBuilderFactory());
        } else {
            return new StreamingExcelRecordIterator(blobItems, getRecordBuilderFactory());
        }
    }

    private class BatchExcelRecordIterator extends BlobFileReader.ItemRecordIterator<Row> {

        private LinkedList<Row> rows;

        private ExcelConverter converter;

        private BatchExcelRecordIterator(Iterable<ListBlobItem> blobItemsList, RecordBuilderFactory recordBuilderFactory) {
            super(blobItemsList, recordBuilderFactory);
            this.rows = new LinkedList<>();
            takeFirstItem();
        }

        @Override
        protected Record convertToRecord(Row next) {
            return converter.toRecord(next);
        }

        @Override
        protected void readItem() {
            rows.clear(); // remove footer of previous item

            if (converter == null) {
                converter = ExcelConverter.of(super.getRecordBuilderFactory());
            }

            try (InputStream input = getCurrentItem().openInputStream()) {
                Workbook wb = new HSSFWorkbook(input);

                Sheet sheet = wb.getSheet(getConfig().getExcelOptions().getSheetName());

                if (sheet != null) {

                    if (getConfig().getExcelOptions().isUseHeader() && getConfig().getExcelOptions().getHeader() >= 1) {

                        Row headerRow = sheet.getRow(getConfig().getExcelOptions().getHeader() - 1);
                        if (converter.getColumnNames() == null) {
                            converter.inferSchemaNames(headerRow, true);
                        }
                    }
                    boolean isHeaderUsed = getConfig().getExcelOptions().isUseHeader();

                    for (int i = isHeaderUsed ? getConfig().getExcelOptions().getHeader() : 0; i < sheet
                            .getPhysicalNumberOfRows(); i++) {
                        Row row = sheet.getRow(i);
                        rows.add(row);
                    }
                } else {
                    log.warn("Excel file " + getCurrentItem().getName() + " was ignored since no sheet name exist: "
                            + getConfig().getExcelOptions().getSheetName());
                }

            } catch (StorageException | IOException e) {
                throw new BlobRuntimeException(e);
            }
        }

        @Override
        protected boolean hasNextRecordTaken() {
            return getConfig().getExcelOptions().isUseFooter() ? rows.size() > getConfig().getExcelOptions().getFooter()
                    : rows.size() > 0;
        }

        @Override
        protected Row takeNextRecord() {
            return rows.poll();
        }

        @Override
        protected void complete() {
            // NOOP
        }
    }

    private class StreamingExcelRecordIterator extends ItemRecordIterator<Row> {

        private ExcelConverter converter;

        private StreamingWorkbook currentWorkBook;

        private Iterator<Row> rowIterator;

        private LinkedList<Row> batch;

        public StreamingExcelRecordIterator(Iterable<ListBlobItem> blobItemsList, RecordBuilderFactory recordBuilderFactory) {
            super(blobItemsList, recordBuilderFactory);
            batch = new LinkedList<>();
            takeFirstItem();
        }

        @Override
        protected Record convertToRecord(Row next) {
            return converter.toRecord(next);
        }

        @Override
        protected void readItem() {
            batch.clear(); // clear footer of previous item

            if (converter == null) {
                converter = ExcelConverter.of(getRecordBuilderFactory());
            }

            try {
                currentWorkBook = (StreamingWorkbook) StreamingReader.builder().rowCacheSize(4096)
                        .open(getCurrentItem().openInputStream());
                StreamingSheet sheet = null;
                try {
                    sheet = (StreamingSheet) currentWorkBook.getSheet(getConfig().getExcelOptions().getSheetName());
                } catch (MissingSheetException e) {
                    log.warn("Excel file " + getCurrentItem().getName() + " was ignored since no sheet name exist: "
                            + getConfig().getExcelOptions().getSheetName());
                    rowIterator = Iterators.emptyIterator();
                    return;
                }

                rowIterator = sheet.rowIterator();

                if (getConfig().getExcelOptions().isUseHeader() && getConfig().getExcelOptions().getHeader() >= 1) {
                    for (int i = 0; i < getConfig().getExcelOptions().getHeader() - 1; i++) {
                        // skip extra header lines
                        if (rowIterator.hasNext()) {
                            rowIterator.next();
                        }
                    }
                    if (rowIterator.hasNext()) {
                        Row headerRow = rowIterator.next();
                        if (converter.getColumnNames() == null) {
                            converter.inferSchemaNames(headerRow, true);
                        }
                    }
                }

                if (getConfig().getExcelOptions().isUseFooter() && getConfig().getExcelOptions().getFooter() >= 1) {
                    for (int i = 0; i < getConfig().getExcelOptions().getFooter(); i++) {
                        // fill batch
                        getNextRecordFromStream();
                    }
                }
            } catch (StorageException e) {
                throw new BlobRuntimeException(e);
            }
        }

        @Override
        protected boolean hasNextRecordTaken() {
            if (getConfig().getExcelOptions().isUseFooter() && getConfig().getExcelOptions().getFooter() >= 1) {
                return getNextRecordFromStream() || batch.size() > getConfig().getExcelOptions().getFooter();
            } else {
                return rowIterator.hasNext();
            }
        }

        @Override
        protected Row takeNextRecord() {
            if (getConfig().getExcelOptions().isUseFooter() && getConfig().getExcelOptions().getFooter() >= 1) {
                return batch.poll();
            } else {
                return rowIterator.next();
            }
        }

        private boolean getNextRecordFromStream() {
            try {
                Row nextExcelRow = rowIterator.next();
                batch.add(nextExcelRow);
                return rowIterator.hasNext();
            } catch (NoSuchElementException e) {
                return false;
            }

        }

        @Override
        protected void complete() {
            try {
                currentWorkBook.close();
            } catch (IOException e) {
                log.warn("Can't close excel stream", e);
            }
        }
    }
}
