package org.talend.components.common.stream.output.excel;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.ss.usermodel.Workbook;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.output.WritableTarget;
import org.talend.components.common.stream.format.Encoding;
import org.talend.components.common.stream.format.ExcelConfiguration;
import org.talend.components.common.stream.format.ExcelConfiguration.ExcelFormat;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import static org.junit.jupiter.api.Assertions.*;

class ExcelWriterTest {

    private RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

    @Test
    void add() throws IOException {
        final ExcelConfiguration cfg = new ExcelConfiguration();
        cfg.setUseFooter(true);
        cfg.setFooter(2);

        cfg.setUseHeader(true);
        cfg.setHeader(2);

        cfg.setCustomEncoding(Encoding.UFT8.name());

        cfg.setExcelFormat(ExcelFormat.EXCEL2007);
        cfg.setSheetName("talend_sheet");

        URL outrepo = Thread.currentThread().getContextClassLoader().getResource(".");
        File excelFile = new File(outrepo.getPath(), "excel.xlsx");
        if (excelFile.exists()) {
            excelFile.delete();
        }
        excelFile.createNewFile();
        final FileOutputStream out = new FileOutputStream(excelFile);
        final WritableTarget<Workbook> target = new ExcelTarget(out);

        try (ExcelWriter writer = new ExcelWriter(cfg, target)) {
            writer.add(this.buildRecords());
        }
        Assertions.assertTrue( excelFile.length() > 20,
                () -> "Length " + excelFile.length() + " is to small");
    }

    Iterable<Record> buildRecords() {
        List<Record> records = new ArrayList<>(3);
        Record rec1 = this.factory.newRecordBuilder()
                .withString("firstname", "peter")
                .withString("lastname", "falker")
                .withInt("age", 75)
                .build();
        records.add(rec1);

        Record rec2 = this.factory.newRecordBuilder()
                .withString("firstname", "steve")
                .withString("lastname", "jobs")
                .withInt("age", 70)
                .build();
        records.add(rec2);

        Record rec3 = this.factory.newRecordBuilder()
                .withString("firstname", "grigori")
                .withString("lastname", "perelman")
                .withInt("age", 55)
                .build();
        records.add(rec3);

        return records;
    }
}