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
package org.talend.components.adlsgen2.common.format.avro;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Stream;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.junit5.WithComponents;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@WithComponents("org.talend.components.adlsgen2")
class AvroConverterTest extends AdlsGen2TestBase {

    private AvroConverter converter;

    private GenericRecord avro;

    @BeforeEach
    protected void setUp() throws Exception {
        super.setUp();
        converter = AvroConverter.of(recordBuilderFactory);
        avro = new GenericData.Record( //
                SchemaBuilder.builder().record("sample").fields() //
                        .name("string").type().stringType().noDefault() //
                        .name("int").type().intType().noDefault() //
                        .name("long").type().longType().noDefault() //
                        .name("double").type().doubleType().noDefault() //
                        .name("boolean").type().booleanType().noDefault() //
                        .endRecord());
        avro.put("string", "a string sample");
        avro.put("int", 710);
        avro.put("long", 710L);
        avro.put("double", 71.0);
        avro.put("boolean", true);
    }

    @Test
    void of() {
        assertNotNull(AvroConverter.of(recordBuilderFactory));
    }

    @Test
    void inferSchema() {
        Schema s = converter.inferSchema(avro);
        assertNotNull(s);
        assertEquals(5, s.getEntries().size());
        assertTrue(s.getType().equals(Type.RECORD));
        assertTrue(s.getEntries().stream().map(Entry::getName).collect(toList())
                .containsAll(Stream.of("string", "int", "long", "double", "boolean").collect(toList())));
    }

    @Test
    void toRecord() {
        Record record = converter.toRecord(avro);
        assertNotNull(record);
        assertEquals("a string sample", record.getString("string"));
        assertEquals(710, record.getInt("int"));
        assertEquals(710L, record.getLong("long"));
        assertEquals(71.0, record.getDouble("double"));
        assertEquals(true, record.getBoolean("boolean"));
    }

    @Test
    void fromSimpleRecord() {
        GenericRecord record = converter.fromRecord(versatileRecord);
        assertNotNull(record);
        assertEquals("Bonjour", record.get("string1"));
        assertEquals("Olà", record.get("string2"));
        assertEquals(71, record.get("int"));
        assertEquals(true, record.get("boolean"));
        assertEquals(1971L, record.get("long"));
        assertEquals(new Date(2019, 04, 22).getTime(), record.get("datetime"));
        assertEquals(20.5f, record.get("float"));
        assertEquals(20.5, record.get("double"));
    }

    @Test
    void fromComplexRecord() {
        GenericRecord record = converter.fromRecord(complexRecord);
        assertNotNull(record);
        assertEquals("ComplexR", record.get("name"));
        assertNotNull(record.get("record"));
        assertEquals(versatileRecord, record.get("record"));
        assertEquals(now.withZoneSameInstant(ZoneOffset.UTC),
                ZonedDateTime.ofInstant(Instant.ofEpochMilli((long) record.get("now")), ZoneOffset.UTC));
        assertEquals(Arrays.asList("ary1", "ary2", "ary3"), record.get("array"));
    }

    @Test
    void fromAndToComplexRecord() {
        GenericRecord from = converter.fromRecord(complexRecord);
        assertNotNull(from);
        Record to = converter.toRecord(from);
        assertNotNull(to);
        assertEquals("ComplexR", to.getString("name"));
        assertNotNull(to.getRecord("record"));
        assertEquals(versatileRecord, to.getRecord("record"));
        assertEquals(now.withZoneSameInstant(ZoneOffset.UTC), to.getDateTime("now").withZoneSameInstant(ZoneOffset.UTC));
        assertEquals(Arrays.asList("ary1", "ary2", "ary3"), to.getArray(String.class, "array"));
    }

    @Test
    void withNullFieldsInIcomingRecord() {
        avro = new GenericData.Record( //
                SchemaBuilder.builder().record("sample").fields() //
                        .name("string").type().stringType().noDefault() //
                        .name("int").type().intType().noDefault() //
                        .name("long").type().longType().noDefault() //
                        .name("double").type().doubleType().noDefault() //
                        .name("boolean").type().booleanType().noDefault() //
                        .endRecord());
        avro.put("string", null);
        avro.put("int", null);
        avro.put("long", null);
        avro.put("double", null);
        avro.put("boolean", null);
        Record record = converter.toRecord(avro);
        assertNotNull(record);
        assertEquals(5, record.getSchema().getEntries().size());
        assertNull(record.getString("string"));
        assertEquals(0, record.getInt("int"));
        assertEquals(0, record.getLong("long"));
        assertEquals(0, record.getDouble("double"));
        assertFalse(record.getBoolean("boolean"));
    }

    @Test
    void withTimeStampsInAndOut() {
        avro = new GenericData.Record( //
                SchemaBuilder.builder().record("sample").fields() //
                        .name("string").type().stringType().noDefault() //
                        .name("int").type().intType().noDefault() //
                        .name("long").type().longType().noDefault() //
                        .name("officialts").type().longType().noDefault() //
                        .endRecord());

    }
}
