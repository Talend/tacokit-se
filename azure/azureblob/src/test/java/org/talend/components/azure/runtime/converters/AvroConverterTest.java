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
package org.talend.components.azure.runtime.converters;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.record.SchemaImpl;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@WithComponents("org.talend.components.azure")
class AvroConverterTest {

    private AvroConverter converter;

    private GenericRecord avro;

    private RecordBuilderFactory recordBuilderFactory;

    @Injected
    protected BaseComponentsHandler componentsHandler;

    protected Record versatileRecord;

    protected Record complexRecord;

    private ZonedDateTime now = ZonedDateTime.now();

    @BeforeEach
    protected void setUp() throws Exception {
        recordBuilderFactory = componentsHandler.findService(RecordBuilderFactory.class);
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

        prepareTestRecords();
    }

    private void prepareTestRecords() {
        // some demo records
        versatileRecord = recordBuilderFactory.newRecordBuilder() //
                .withString("string1", "Bonjour") //
                .withString("string2", "Olà") //
                .withInt("int", 71) //
                .withBoolean("boolean", true) //
                .withLong("long", 1971L) //
                .withDateTime("datetime", LocalDateTime.of(2019, 04, 22, 0, 0).atZone(ZoneOffset.UTC)) //
                .withFloat("float", 20.5f) //
                .withDouble("double", 20.5) //
                .build();

        Entry er = recordBuilderFactory.newEntryBuilder().withName("record").withType(Type.RECORD)
                .withElementSchema(versatileRecord.getSchema()).build();
        Entry ea = recordBuilderFactory.newEntryBuilder().withName("array").withType(Type.ARRAY)
                .withElementSchema(recordBuilderFactory.newSchemaBuilder(Type.ARRAY).withType(Type.STRING).build()).build();

        complexRecord = recordBuilderFactory.newRecordBuilder() //
                .withString("name", "ComplexR") //
                .withRecord(er, versatileRecord) //
                .withDateTime("now", now) //
                .withArray(ea, Arrays.asList("ary1", "ary2", "ary3")).build();
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
        assertEquals(LocalDateTime.of(2019, 04, 22, 0, 0).atZone(ZoneOffset.UTC).toInstant().toEpochMilli(),
                record.get("datetime"));
        assertEquals(20.5f, record.get("float"));
        assertEquals(20.5, record.get("double"));
    }

    @Test
    void fromComplexRecord() {
        GenericRecord record = converter.fromRecord(complexRecord);
        assertNotNull(record);
        System.err.println(record);
        assertEquals("ComplexR", record.get("name"));
        assertNotNull(record.get("record"));
        GenericRecord subrecord = (GenericRecord) record.get("record");
        assertEquals("Bonjour", subrecord.get("string1"));
        assertEquals("Olà", subrecord.get("string2"));
        assertEquals(71, subrecord.get("int"));
        assertEquals(true, subrecord.get("boolean"));
        assertEquals(1971L, subrecord.get("long"));
        assertEquals(LocalDateTime.of(2019, 04, 22, 0, 0).atZone(ZoneOffset.UTC).toInstant().toEpochMilli(),
                subrecord.get("datetime"));
        assertEquals(20.5f, subrecord.get("float"));
        assertEquals(20.5, subrecord.get("double"));

        assertEquals(now.withZoneSameInstant(ZoneOffset.UTC),
                ZonedDateTime.ofInstant(Instant.ofEpochMilli((long) record.get("now")), ZoneOffset.UTC));
        assertEquals(Arrays.asList("ary1", "ary2", "ary3"), record.get("array"));
    }

    @Test
    void fromAndToRecord() {
        GenericRecord from = converter.fromRecord(versatileRecord);
        assertNotNull(from);
        Record to = converter.toRecord(from);
        assertNotNull(to);
        assertEquals("Bonjour", to.getString("string1"));
        assertEquals("Olà", to.getString("string2"));
        assertEquals(71, to.getInt("int"));
        assertEquals(true, to.getBoolean("boolean"));
        assertEquals(1971L, to.getLong("long"));
        assertEquals(LocalDateTime.of(2019, 04, 22, 0, 0).atZone(ZoneOffset.UTC).toInstant(),
                to.getDateTime("datetime").toInstant());
        assertEquals(20.5f, to.getFloat("float"));
        assertEquals(20.5, to.getDouble("double"));
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
        assertFalse(record.getOptionalInt("int").isPresent());
        assertFalse(record.getOptionalBoolean("boolean").isPresent());
        assertFalse(record.getOptionalLong("long").isPresent());
        assertFalse(record.getOptionalDouble("double").isPresent());
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

    @Test
    void withAllowNullColumnSchema() {
        Schema schema = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(new SchemaImpl.EntryImpl("nullStringColumn", Schema.Type.STRING, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullStringColumn2", Schema.Type.STRING, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullIntColumn", Schema.Type.INT, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullLongColumn", Schema.Type.LONG, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullFloatColumn", Schema.Type.FLOAT, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullDoubleColumn", Schema.Type.DOUBLE, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullBooleanColumn", Schema.Type.BOOLEAN, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullByteArrayColumn", Schema.Type.BYTES, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullDateColumn", Schema.Type.DATETIME, true, null, null, null)).build();
        Record testRecord = recordBuilderFactory.newRecordBuilder(schema).withString("nullStringColumn", "myString").build();

        assertNotNull(testRecord.getString("nullStringColumn"));
        assertFalse(testRecord.getOptionalInt("nullIntColumn").isPresent());
    }
}