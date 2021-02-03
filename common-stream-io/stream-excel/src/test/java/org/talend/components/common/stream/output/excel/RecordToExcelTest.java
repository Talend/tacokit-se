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

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import java.util.Arrays;

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

    @Test
    void withNull() {
        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

        final Schema.Entry a_null_string = factory.newEntryBuilder().withType(Schema.Type.STRING).withNullable(true)
                .withName("a_null_string").build();
        final Schema.Entry a_double = factory.newEntryBuilder().withType(Schema.Type.DOUBLE).withNullable(true)
                .withName("a_double").build();
        final Schema.Entry a_int = factory.newEntryBuilder().withType(Schema.Type.INT).withNullable(true).withName("a_int")
                .build();
        final Schema.Entry a_float = factory.newEntryBuilder().withType(Schema.Type.FLOAT).withNullable(true).withName("a_float")
                .build();
        final Schema.Entry a_long = factory.newEntryBuilder().withType(Schema.Type.LONG).withNullable(true).withName("a_long")
                .build();
        final Schema.Entry a_bool = factory.newEntryBuilder().withType(Schema.Type.BOOLEAN).withNullable(true).withName("a_bool")
                .build();
        final Schema.Entry some_bytes = factory.newEntryBuilder().withType(Schema.Type.BYTES).withNullable(true)
                .withName("some_bytes").build();
        final Schema.Entry a_datetime = factory.newEntryBuilder().withType(Schema.Type.DATETIME).withNullable(true)
                .withName("a_datetime").build();

        final Schema arraySchema = factory.newSchemaBuilder(Schema.Type.STRING).build();
        final Schema.Entry an_array = factory.newEntryBuilder().withType(Schema.Type.ARRAY).withElementSchema(arraySchema)
                .withNullable(true).withName("an_array").build();

        final Schema schema = factory.newSchemaBuilder(Schema.Type.RECORD).withEntry(a_double) // 0
                .withEntry(a_int) // 1
                .withEntry(a_float) // 2
                .withEntry(a_long) // 3
                .withEntry(a_bool) // 4
                .withEntry(a_null_string) // 5
                .withEntry(some_bytes) // 6
                .withEntry(an_array) // 7
                .withEntry(a_datetime) // 8
                .build();

        final Record record = factory.newRecordBuilder(schema).build();

        final XSSFWorkbook wb = new XSSFWorkbook();
        final XSSFSheet sheet = wb.createSheet();
        final RecordToExcel toExcel = new RecordToExcel();

        final Row row = toExcel.from(() -> sheet.createRow(0), record);
        Assertions.assertNotNull(row);

        // numbers & date generate empty cell if null as input
        Arrays.asList(0, 1, 2, 3, 8).stream().map(i -> row.getCell(i))
                .forEach(c -> Assertions.assertEquals("", c.getStringCellValue()));

        // string, bytes, array has "null" in cell if null as input
        Arrays.asList(5, 6, 7).stream().map(i -> row.getCell(i))
                .forEach(c -> Assertions.assertEquals("null", c.getStringCellValue()));

        // boolean cell is FALSE when not set when having null as input
        Assertions.assertEquals(false, row.getCell(4).getBooleanCellValue());
    }
}