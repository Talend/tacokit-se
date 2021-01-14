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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.TargetFinder;
import org.talend.components.common.stream.format.OptionalLine;
import org.talend.components.common.stream.format.excel.ExcelConfiguration;
import org.talend.components.common.stream.format.excel.ExcelConfiguration.ExcelFormat;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class ExcelWriterTest {

    private RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

    @Test
    void add() throws IOException {
        final ExcelConfiguration cfg = new ExcelConfiguration();
        cfg.setFooter(new OptionalLine());
        cfg.getFooter().setActive(true);
        cfg.getFooter().setSize(2);

        cfg.setHeader(new OptionalLine());
        cfg.getHeader().setActive(true);
        cfg.getHeader().setSize(2);

        // cfg.setEncoding(new Encoding());
        // cfg.getEncoding().setEncodingType(Type.UFT8);

        cfg.setExcelFormat(ExcelFormat.EXCEL2007);
        cfg.setSheetName("talend_sheet");

        URL outrepo = Thread.currentThread().getContextClassLoader().getResource(".");
        File excelFile = new File(outrepo.getPath(), "excel.xlsx");
        if (excelFile.exists()) {
            excelFile.delete();
        }
        excelFile.createNewFile();
        final TargetFinder target = () -> new FileOutputStream(excelFile);

        final ExcelWriterSupplier writerSupplier = new ExcelWriterSupplier();
        try (RecordWriter writer = writerSupplier.getWriter(target, cfg)) {
            writer.add(this.buildRecords());
        }
        Assertions.assertTrue(excelFile.length() > 20, () -> "Length " + excelFile.length() + " is to small");
    }

    Iterable<Record> buildRecords() {
        List<Record> records = new ArrayList<>(3);
        Record rec1 = this.factory.newRecordBuilder().withString("firstname", "peter").withString("lastname", "falker")
                .withInt("age", 75).build();
        records.add(rec1);

        Record rec2 = this.factory.newRecordBuilder().withString("firstname", "steve").withString("lastname", "jobs")
                .withInt("age", 70).build();
        records.add(rec2);

        Record rec3 = this.factory.newRecordBuilder().withString("firstname", "grigori").withString("lastname", "perelman")
                .withInt("age", 55).build();
        records.add(rec3);

        return records;
    }
}