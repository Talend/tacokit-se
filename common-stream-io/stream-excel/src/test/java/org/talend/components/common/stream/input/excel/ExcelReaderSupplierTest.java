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
package org.talend.components.common.stream.input.excel;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderRepository;
import org.talend.components.common.stream.format.Encoding;
import org.talend.components.common.stream.format.ExcelConfiguration;
import org.talend.components.common.stream.format.ExcelConfiguration.ExcelFormat;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class ExcelReaderSupplierTest {

    private double idValue = 1.0;

    private final String nameValue = "a";

    private final double longValue = 10000000000000.0;

    private final double doubleValue = 2.5;

    private final double dateValue = 43501.0;

    private final boolean booleanValue = true;

    private final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

    private final ExcelConfiguration config = new ExcelConfiguration();

    ExcelReaderSupplier supplier = new ExcelReaderSupplier();

    @BeforeEach
    void initDataSet() {
        config.setSheetName("Sheet1");
        config.setExcelFormat(ExcelFormat.EXCEL2007);
        config.setEncoding(Encoding.UFT8);
        config.setUseFooter(false);
        config.setUseHeader(false);
    }

    @Test
    void test1File1RecordsWithoutHeader() throws IOException {

        ExcelReaderSupplier supplier;
        config.setUseHeader(false);

        this.testOneValueFile("excel2007/excel_2007_1_record_no_header.xlsx");

        config.setExcelFormat(ExcelFormat.EXCEL97);
        this.testOneValueFile("excel97/excel_97_1_record_no_header.xls");
    }

    private void testOneValueFile(String path) throws IOException {
        try (final InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
                final RecordReader reader = RecordReaderRepository.getInstance().get(ExcelConfiguration.class).getReader(factory,
                        config)) {

            final Iterator<Record> records = reader.read(stream);
            for (int i = 0; i < 4; i++) {
                Assertions.assertTrue(records.hasNext(), "no record or not re-entrant"); // re-entrant.
            }
            final Record firstRecord = records.next();

            if (config.isUseHeader()) {

                Assert.assertEquals(idValue, firstRecord.getDouble("id"), 0.01);
                Assert.assertEquals(nameValue, firstRecord.getString("name"));
                Assert.assertEquals(longValue, firstRecord.getDouble("longValue"), 0.01);
                Assert.assertEquals(doubleValue, firstRecord.getDouble("doubleValue"), 0.01);
                Assert.assertEquals(dateValue, firstRecord.getDouble("dateValue"), 0.01);
                Assert.assertEquals(booleanValue, firstRecord.getBoolean("booleanValue"));
            } else {
                Assert.assertEquals(idValue, firstRecord.getDouble("field0"), 0.01);
                Assert.assertEquals(nameValue, firstRecord.getString("field1"));
                Assert.assertEquals(longValue, firstRecord.getDouble("field2"), 0.01);
                Assert.assertEquals(doubleValue, firstRecord.getDouble("field3"), 0.01);
                Assert.assertEquals(dateValue, firstRecord.getDouble("field4"), 0.01);
                Assert.assertEquals(booleanValue, firstRecord.getBoolean("field5"));
            }
            Assertions.assertFalse(records.hasNext(), "more than one record");
        }
    }

    private void testRecordsSize(String path, int nbeRecord) throws IOException {
        try (final InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
                final RecordReader reader = RecordReaderRepository.getInstance().get(ExcelConfiguration.class).getReader(factory,
                        config)) {

            final Iterator<Record> records = reader.read(stream);
            for (int i = 0; i < 4; i++) {
                Assertions.assertTrue(records.hasNext(), "no record or not re-entrant"); // re-entrant.
            }
            for (int i = 0; i < nbeRecord; i++) {
                this.ensureNext(records, i);
            }
            Assertions.assertFalse(records.hasNext(), "more than " + nbeRecord + " records");
        }
    }

    @Test
    void test1File5RecordsWithoutHeader() throws IOException {
        ExcelReaderSupplier supplier;
        config.setUseHeader(false);

        this.testRecordsSize("excel2007/excel_2007_5_records_no_header.xlsx", 5);

        config.setExcelFormat(ExcelFormat.EXCEL97);
        this.testRecordsSize("excel97/excel_97_5_records_no_header.xls", 5);
    }

    private Record ensureNext(Iterator<Record> records, int number) {
        Assertions.assertTrue(records.hasNext(), "no more record (" + (number + 1) + ")");
        final Record record = records.next();
        Assertions.assertNotNull(record);
        return record;
    }

    @Test
    void testInput1FileWithHeader1Row() throws IOException {
        this.config.setUseHeader(true);
        this.config.setHeader(1);
        this.testOneValueFile("excel2007/excel_2007_1_record_with_header.xlsx");

        config.setExcelFormat(ExcelFormat.EXCEL97);
        this.testOneValueFile("excel97/excel_97_1_record_with_header.xls");
    }

    @Test
    void testInput1FileMultipleRows() throws IOException {
        this.config.setUseHeader(true);
        this.config.setHeader(1);

        this.testRecordsSize("excel2007/excel_2007_5_records_with_header.xlsx", 5);

        config.setExcelFormat(ExcelFormat.EXCEL97);
        this.testRecordsSize("excel97/excel_97_5_records_with_header.xls", 5);
    }

    @Test
    void test1File1RecordWithBigHeader() throws IOException {
        this.config.setUseHeader(true);
        this.config.setHeader(2);

        this.testOneValueFile("excel2007/excel_2007_1_record_with_big_header.xlsx");

        config.setExcelFormat(ExcelFormat.EXCEL97);
        this.testOneValueFile("excel97/excel_97_1_record_with_big_header.xls");
    }

    @Test
    void test1File5RecordsWithBigHeader() throws IOException {
        this.config.setUseHeader(true);
        this.config.setHeader(2);

        this.testRecordsSize("excel2007/excel_2007_5_records_with_big_header.xlsx", 5);

        config.setExcelFormat(ExcelFormat.EXCEL97);
        this.testRecordsSize("excel97/excel_97_5_records_with_big_header.xls", 5);

    }

    @Test
    void test1FileWithFooter() throws IOException {

        this.config.setUseFooter(true);
        this.config.setFooter(1);

        this.testOneValueFile("excel2007/excel_2007_1_record_footer.xlsx");

        config.setExcelFormat(ExcelFormat.EXCEL97);
        this.testOneValueFile("excel97/excel_97_1_record_footer.xls");

    }

}