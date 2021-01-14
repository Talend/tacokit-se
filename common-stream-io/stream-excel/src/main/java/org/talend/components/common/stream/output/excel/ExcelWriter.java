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
package org.talend.components.common.stream.output.excel;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.talend.components.common.stream.ExcelUtils;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.TargetFinder;
import org.talend.components.common.stream.format.excel.ExcelConfiguration;
import org.talend.sdk.component.api.record.Record;

public class ExcelWriter implements RecordWriter {

    private final TargetFinder target;

    private final RecordToExcel toExcel;

    private final Workbook excelWorkbook;

    private final Sheet excelSheet;

    private final ExcelConfiguration config;

    private boolean first = true;

    public ExcelWriter(ExcelConfiguration configuration, TargetFinder target) {

        this.target = target;
        this.toExcel = new RecordToExcel();

        this.excelWorkbook = ExcelUtils.createWorkBook(configuration.getExcelFormat());
        this.excelSheet = this.excelWorkbook.createSheet(configuration.getSheetName());
        this.config = configuration;
    }

    @Override
    public void add(Record record) {
        if (this.first) {
            this.appendHeader(record);
            this.first = false;
        }
        toExcel.from(this::buildRow, record);
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() throws IOException {
        this.appendFooter();
        try (final OutputStream outputStream = this.target.find()) {
            this.excelWorkbook.write(outputStream);
        }
    }

    private void appendHeader(Record firstDataRecord) {
        int header = this.config.calcHeader();
        if (header > 0) {
            // if more than one header.
            for (int i = 1; i < header; i++) {
                this.buildRow();
            }
            this.toExcel.buildHeader(this::buildRow, firstDataRecord.getSchema());
        }
    }

    private void appendFooter() {
        int footer = this.config.calcFooter();
        for (int i = 0; i < footer; i++) {
            final Row footerRow = this.buildRow();
            final Cell footerCell = footerRow.createCell(0);
            footerCell.setCellValue("//footer line");
        }
    }

    private Row buildRow() {
        int pos = this.excelSheet.getPhysicalNumberOfRows();
        return this.excelSheet.createRow(pos);
    }
}
