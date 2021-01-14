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
package org.talend.components.adlsgen2.common.format.csv;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.adlsgen2.common.format.FileEncoding;
import org.talend.components.adlsgen2.common.format.csv.CsvIterator.Builder;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.adlsgen2.runtime.formatter.CsvContentFormatter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;

import lombok.extern.slf4j.Slf4j;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
@WithComponents("org.talend.components.adlsgen2")
public class CsvConverterTest extends AdlsGen2TestBase {

    private CsvConfiguration csvConfiguration;

    private CsvConverter converter;

    @BeforeEach
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void csvWithPipeAsDelimiterCase() throws Exception {
        InputStream sample = getClass().getResource("/common/format/csv/pipe-separated.csv").openStream();
        csvConfiguration = new CsvConfiguration();
        csvConfiguration.setFieldDelimiter(CsvFieldDelimiter.OTHER);
        csvConfiguration.setCustomFieldDelimiter("|");
        CsvIterator it = Builder.of(recordBuilderFactory).withConfiguration(csvConfiguration).parse(sample);
        int counted = 0;
        while (it.hasNext()) {
            Record record = it.next();
            assertNotNull(record);
            counted++;
        }
        assertEquals(6, counted);
    }

    @Test
    public void csvWithTextEnclosureAndEscapeCase() throws Exception {
        InputStream sample = getClass().getResource("/common/format/csv/wicked-separated.csv").openStream();
        String result = "\"1\";\"1000.2\";\"ant\\\"ique\"\n" + "\"2\";\"2000.3\";\"stroll\"\n"
                + "\"3\";\"3000.3\";\"ant\\\\ique\"\n";
        csvConfiguration = new CsvConfiguration();
        csvConfiguration.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfiguration.setEscapeCharacter("\\");
        csvConfiguration.setTextEnclosureCharacter("\"");
        CsvIterator it = Builder.of(recordBuilderFactory).withConfiguration(csvConfiguration).parse(sample);
        outputConfiguration.getDataSet().setCsvConfiguration(csvConfiguration);
        CsvContentFormatter fmt = new CsvContentFormatter(outputConfiguration, recordBuilderFactory);
        int counted = 0;
        List<Record> records = new ArrayList<>();
        while (it.hasNext()) {
            Record record = it.next();
            records.add(record);
            assertNotNull(record);
            counted++;
        }
        assertEquals(3, counted);
        byte[] output = fmt.feedContent(records);
        assertEquals(result, new String(output));
    }

    @Test
    void csvEscaping() throws Exception {
        InputStream sample = getClass().getResource("/common/format/csv/escaping.csv").openStream();
        String result = "\"1\";\"transmit\"\r\n" + "\"2\";\"tran\\\"sfer\"\r\n" + "\"3\";\r\n" + ";\"password\"\r\n";
        csvConfiguration = new CsvConfiguration();
        csvConfiguration.setEscapeCharacter("\\");
        csvConfiguration.setTextEnclosureCharacter("\"");
        CsvIterator it = Builder.of(recordBuilderFactory).withConfiguration(csvConfiguration).parse(sample);
        outputConfiguration.getDataSet().setCsvConfiguration(csvConfiguration);
        CsvContentFormatter fmt = new CsvContentFormatter(outputConfiguration, recordBuilderFactory);
        int counted = 0;
        List<Record> records = new ArrayList<>();
        while (it.hasNext()) {
            Record record = it.next();
            records.add(record);
            assertNotNull(record);
            counted++;
        }
        assertEquals(4, counted);
        byte[] output = fmt.feedContent(records);
        assertEquals(result, new String(output));
    }

    @Test
    void withSjisEncoding() throws Exception {
        InputStream sample = getClass().getResource("/common/format/csv/SJIS-encoded.csv").openStream();
        csvConfiguration = new CsvConfiguration();
        csvConfiguration.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfiguration.setFileEncoding(FileEncoding.OTHER);
        csvConfiguration.setCustomFileEncoding("SJIS");
        CsvIterator it = Builder.of(recordBuilderFactory).withConfiguration(csvConfiguration).parse(sample);
        Record encoded = it.next();
        assertNotNull(encoded);
        assertEquals("2", encoded.getString("field0"));
        assertEquals("2000.3", encoded.getString("field1"));
        assertEquals("テスト", encoded.getString("field2"));
    }

    @Test
    void withGb2312Encoding() throws Exception {
        InputStream sample = getClass().getResource("/common/format/csv/GB2312-encoded.csv").openStream();
        csvConfiguration = new CsvConfiguration();
        csvConfiguration.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfiguration.setFieldDelimiter(CsvFieldDelimiter.OTHER);
        csvConfiguration.setCustomFieldDelimiter("，");
        csvConfiguration.setFileEncoding(FileEncoding.OTHER);
        csvConfiguration.setCustomFileEncoding("GB2312");
        CsvIterator it = Builder.of(recordBuilderFactory).withConfiguration(csvConfiguration).parse(sample);
        Record encoded = it.next();
        assertNotNull(encoded);
        assertEquals("护照", encoded.getString("field0"));
        assertEquals("名字", encoded.getString("field1"));
        assertEquals("生日", encoded.getString("field2"));
        assertEquals("电话号码", encoded.getString("field3"));
        assertEquals("最喜欢的电影", encoded.getString("field4"));
    }

    @Test
    void invalidEncoding() {
        csvConfiguration = new CsvConfiguration();
        csvConfiguration.setFileEncoding(FileEncoding.OTHER);
        csvConfiguration.setCustomFileEncoding("BZH");
        assertThrows(AdlsGen2RuntimeException.class, () -> csvConfiguration.effectiveFileEncoding());
    }
}
