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
package org.talend.components.common.stream.output.line;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Base64;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.talend.components.common.test.records.AssertionsBuilder;
import org.talend.components.common.test.records.DatasetGenerator;
import org.talend.components.common.test.records.DatasetGenerator.DataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class RecordSerializerLineHelperTest {

    private RecordBuilderFactory recordBuilderFactory;

    @BeforeEach
    public void before() {
        recordBuilderFactory = new RecordBuilderFactoryImpl("test");
    }

    @Test
    public void valuesFrom_simple() {
        final Record record = buildRecords_simple();
        final List<String> strings = RecordSerializerLineHelper.valuesFrom(record);

        assertEquals(5, strings.size());
        assertEquals("Smith", strings.get(0));
        assertEquals("35", strings.get(1));
        assertEquals("true", strings.get(2));
        assertEquals("2020-08-17T10:10:10.010Z[UTC]", strings.get(3));

        final String value4 = strings.get(4);
        final String decodedValue4 = new String(Base64.getDecoder().decode(value4));
        assertEquals("HelloBytes", decodedValue4);
    }

    private Record buildRecords_simple() {
        GregorianCalendar gc = new GregorianCalendar();
        gc.set(GregorianCalendar.YEAR, 2020);
        gc.set(GregorianCalendar.MONTH, 7);
        gc.set(GregorianCalendar.DAY_OF_MONTH, 17);
        gc.set(GregorianCalendar.HOUR_OF_DAY, 10);
        gc.set(GregorianCalendar.MINUTE, 10);
        gc.set(GregorianCalendar.SECOND, 10);
        gc.set(GregorianCalendar.MILLISECOND, 10);
        gc.setTimeZone(TimeZone.getTimeZone(ZoneId.of("UTC")));
        final Date date = gc.getTime();

        final Record rec1 = recordBuilderFactory.newRecordBuilder() //
                .withString(createEntry("name", Type.STRING, false), "Smith") //
                .withInt(createEntry("age", Type.INT, false), 35) //
                .withBoolean(createEntry("registered", Type.BOOLEAN, false), true) //
                .withDateTime(createEntry("last_save", Type.DATETIME, false), date) //
                .withBytes(createEntry("bytes", Type.BYTES, false), "HelloBytes".getBytes(StandardCharsets.UTF_8)).build();
        return rec1;
    }

    @Test
    public void valuesFrom_withNull() {
        final Record record = buildRecords_withNull();
        final List<String> strings = RecordSerializerLineHelper.valuesFrom(record);

        assertEquals("Smith", strings.get(0));
        assertEquals("35", strings.get(1));
        assertEquals("true", strings.get(2));
        assertNull(strings.get(3));
    }

    private Record buildRecords_withNull() {
        final Record rec1 = recordBuilderFactory.newRecordBuilder()
                .withString(createEntry("name", Schema.Type.STRING, false), "Smith")
                .withInt(createEntry("age", Schema.Type.INT, false), 35)
                .withBoolean(createEntry("registered", Schema.Type.BOOLEAN, false), true)
                .withDateTime(createEntry("last_save", Schema.Type.DATETIME, true), (Date) null).build();
        return rec1;
    }

    private Schema.Entry createEntry(final String name, final Schema.Type type, final boolean nullable) {
        return recordBuilderFactory.newEntryBuilder().withName(name).withRawName(name).withType(type).withNullable(nullable)
                .build();
    }

    @ParameterizedTest
    @MethodSource("testDataLine")
    void testRecordsLine(DataSet<List<String>> ds) {
        final List<String> values = RecordSerializerLineHelper.valuesFrom(ds.getRecord());
        ds.check(values);
    }

    private static Iterator<DataSet<List<String>>> testDataLine() {
        final AssertionsBuilder<List<String>> valueBuilder = new LineValueBuilder();
        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        final DatasetGenerator<List<String>> generator = new DatasetGenerator<>(factory, valueBuilder);
        return generator.generate(40);
    }

}