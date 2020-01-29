package org.talend.components.common.stream.input.excel;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.stream.StreamSupport;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.talend.components.common.stream.ExcelUtils;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.ExcelConfiguration;
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

    public ExcelRecordReader(ExcelConfiguration configuration,
            RecordBuilderFactory factory) {
        this.configuration = configuration;
        this.toRecord = new ExcelToRecord(factory);
    }

    @Override
    public Iterator<Record> read(InputStream in) {
        try {
            this.close();
            this.currentWorkBook = ExcelUtils.readWorkBook(configuration.getExcelFormat(), in);
            final Sheet sheet = this.currentWorkBook.getSheet(this.configuration.getSheetName());
            passHeaderRow(sheet);

            return StreamSupport.stream(sheet.spliterator(), false)
                    .skip(this.configuration.headerSize())
                    .filter((Row row) -> row.getRowNum() <= sheet.getLastRowNum() - configuration.footerSize())
                    .map(this.toRecord::toRecord)
                    .iterator();
        }
        catch (IOException exIO) {
            log.error("Error while reading excel input", exIO);
            throw new UncheckedIOException("Error while reading excel input", exIO);
        }
    }

    private Schema passHeaderRow(Sheet sheet) {
        final int size = this.configuration.headerSize();
        if (size >= 1) {
            final Row headerRow = sheet.getRow(size - 1);
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
            }
            catch (IOException exIO) {
                log.error("Error while closing excel input", exIO);
                throw new UncheckedIOException("Error while closing excel input", exIO);
            }
        }
    }
}
