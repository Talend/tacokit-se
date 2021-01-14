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
package org.talend.components.common.stream.input.excel;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.talend.components.common.collections.IteratorComposer;
import org.talend.components.common.stream.ExcelUtils;
import org.talend.components.common.stream.format.excel.ExcelConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExcelReader implements FormatReader {

    private final ExcelToRecord toRecord;

    public ExcelReader(RecordBuilderFactory recordBuilderFactory) {
        this.toRecord = new ExcelToRecord(recordBuilderFactory);
    }

    @Override
    public Iterator<Record> read(InputStream input, ExcelConfiguration configuration) {
        try {
            final Workbook currentWorkBook = ExcelUtils.readWorkBook(configuration.getExcelFormat(), input);
            final Sheet sheet = currentWorkBook.getSheet(configuration.getSheetName());
            final Iterator<Row> rowIterator = sheet.iterator();
            int headers = configuration.calcHeader();

            if (headers >= 1) {
                this.parseHeaderRow(rowIterator, headers);
            }

            return IteratorComposer.of(rowIterator).skipFooter(configuration.calcFooter())
                    .map((Row row) -> this.toRecord.toRecord(row)).closeable(currentWorkBook).build();
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
    private Schema parseHeaderRow(Iterator<Row> rows, int headers) {
        if (headers >= 1) {
            for (int i = 1; i < headers && rows.hasNext(); i++) {
                rows.next();
            }
            final Row headerRow = rows.next();
            return this.toRecord.inferSchema(headerRow, true);
        }
        return null;
    }
}
