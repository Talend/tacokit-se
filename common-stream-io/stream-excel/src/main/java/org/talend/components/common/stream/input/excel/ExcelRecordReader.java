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
package org.talend.components.common.stream.input.excel;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.talend.components.common.collections.IteratorComposer;
import org.talend.components.common.stream.ExcelUtils;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.excel.ExcelConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExcelRecordReader implements RecordReader {

    /** excel configuration */
    private final ExcelConfiguration configuration;

    /** converter from excel row to talend record. */
    private final ExcelToRecord toRecord;

    /** current reading workbook */
    private Workbook currentWorkBook = null;

    public ExcelRecordReader(ExcelConfiguration configuration, RecordBuilderFactory factory) {
        this.configuration = configuration;
        this.toRecord = new ExcelToRecord(factory);
    }

    @Override
    public Iterator<Record> read(InputStream in) {
        try {
            this.close();
            this.currentWorkBook = ExcelUtils.readWorkBook(configuration.getExcelFormat(), in);
            final Sheet sheet = this.currentWorkBook.getSheet(this.configuration.getSheetName());
            Iterator<Row> rowIterator = sheet.iterator();
            this.parseHeaderRow(rowIterator);
            rowIterator = IteratorComposer.of(rowIterator).skipFooter(this.configuration.calcFooter()).build();

            final Spliterator<Row> rowSpliterator = Spliterators.spliteratorUnknownSize(rowIterator, 0);

            return StreamSupport.stream(rowSpliterator, false) // iteration on excel lines
                    .map(this.toRecord::toRecord) // Excel Row to Record.
                    .iterator();
        } catch (IOException exIO) {
            log.error("Error while reading excel input", exIO);
            throw new UncheckedIOException("Error while reading excel input", exIO);
        }
    }

    /**
     * Read header row to retrive schema.
     * 
     * @param rows : excel rows.
     * @return Record Schema.
     */
    private Schema parseHeaderRow(Iterator<Row> rows) {
        final int size = this.configuration.calcHeader();
        if (size >= 1) {
            for (int i = 1; i < size && rows.hasNext(); i++) {
                rows.next();
            }
            final Row headerRow = rows.next();
            return this.toRecord.inferSchema(headerRow, true);
        }
        return null;
    }

    @Override
    public void close() {
        if (this.currentWorkBook != null) {
            try {
                this.currentWorkBook.close();
                this.currentWorkBook = null;
            } catch (IOException exIO) {
                log.error("Error while closing excel input", exIO);
                throw new UncheckedIOException("Error while closing excel input", exIO);
            }
        }
    }
}
