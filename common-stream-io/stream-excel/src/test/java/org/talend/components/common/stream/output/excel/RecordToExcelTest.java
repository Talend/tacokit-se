package org.talend.components.common.stream.output.excel;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import static org.junit.jupiter.api.Assertions.*;

class RecordToExcelTest {

    @Test
    void from() {
        final XSSFWorkbook wb = new XSSFWorkbook();
        final XSSFSheet sheet = wb.createSheet();
        final RecordToExcel toExcel = new RecordToExcel();

        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

        final Record record = factory.newRecordBuilder().withString("how", "fine").withInt("oth", 12).build();

        final Row row = toExcel.from(() -> sheet.createRow(0), record);
        Assertions.assertNotNull(row);

    }
}