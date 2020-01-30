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
package org.talend.components.common.stream.output.excel;

import java.io.IOException;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.talend.components.common.stream.ExcelUtils;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.WritableTarget;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.ExcelConfiguration;
import org.talend.sdk.component.api.record.Record;

public class ExcelWriter implements RecordWriter {

    private final WritableTarget<Workbook> target;

    private final RecordToExcel toExcel;

    private final Workbook excelWorkbook;

    private final Sheet excelSheet;

    private final ExcelConfiguration config;

    private boolean first = true;

    public ExcelWriter(ExcelConfiguration configuration, WritableTarget<Workbook> target) {

        this.target = target;
        this.toExcel = new RecordToExcel();

        this.excelWorkbook = ExcelUtils.createWorkBook(configuration.getExcelFormat());
        this.excelSheet = this.excelWorkbook.createSheet(configuration.getSheetName());
        this.config = configuration;
    }

    @Override
    public void init(ContentFormat config) {
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
        this.target.write(this.excelWorkbook);
        this.target.close();
    }

    private void appendHeader(Record firstDataRecord) {
        if (this.config.headerSize() > 0) {
            // if more than one header.
            for (int i = 1; i < config.headerSize(); i++) {
                this.buildRow();
            }
            this.toExcel.buildHeader(this::buildRow, firstDataRecord.getSchema());
        }
    }

    private void appendFooter() {
        for (int i = 0; i < config.footerSize(); i++) {
            final Row footerRow = buildRow();
            final Cell footerCell = footerRow.createCell(0);
            footerCell.setCellValue("//footer line");
        }
    }

    private Row buildRow() {
        int pos = this.excelSheet.getPhysicalNumberOfRows();
        return this.excelSheet.createRow(pos);
    }
}
