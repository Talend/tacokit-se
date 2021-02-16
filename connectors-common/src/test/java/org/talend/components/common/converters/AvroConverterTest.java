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
package org.talend.components.common.converters;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.avro.LogicalTypes;
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

@WithComponents("org.talend.components.common")
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
        converter = AvroConverter.of(recordBuilderFactory, null);
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
        assertNotNull(AvroConverter.of(recordBuilderFactory, null));
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

        assertEquals(now.toInstant().toEpochMilli(), (long) record.get("now"));

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

    @Test
    void testArrays() {
        List<String> strings = Arrays.asList("string1", "string2");
        List<Integer> integers = Arrays.asList(12345, 56789);
        List<Long> longs = Arrays.asList(12345L, 67890L);
        List<Float> floats = Arrays.asList(54321.9f, 0x1.2p3f);
        List<Double> doubles = Arrays.asList(1.2345699E05, 5.678999E04);
        List<Boolean> booleans = Arrays.asList(true, false);
        List<byte[]> bytes = Arrays.asList("string1".getBytes(), "string2".getBytes());
        List<Record> records = Arrays.asList(versatileRecord, versatileRecord);
        //
        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        Record input = recordBuilderFactory.newRecordBuilder() //
                .withArray(entryBuilder.withName("strings").withType(Schema.Type.ARRAY)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Type.STRING).build()).build(), strings)
                .withArray(entryBuilder.withName("integers").withType(Schema.Type.ARRAY)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Type.INT).build()).build(), integers)
                .withArray(entryBuilder.withName("longs").withType(Schema.Type.ARRAY)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Type.LONG).build()).build(), longs)
                .withArray(entryBuilder.withName("floats").withType(Schema.Type.ARRAY)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Type.FLOAT).build()).build(), floats)
                .withArray(entryBuilder.withName("doubles").withType(Schema.Type.ARRAY)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Type.DOUBLE).build()).build(), doubles)
                .withArray(entryBuilder.withName("booleans").withType(Schema.Type.ARRAY)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Type.BOOLEAN).build()).build(), booleans)
                .withArray(entryBuilder.withName("bytes").withType(Schema.Type.ARRAY)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Type.BYTES).build()).build(), bytes)
                .withArray(entryBuilder.withName("records").withType(Schema.Type.ARRAY)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Type.RECORD).build()).build(), records)
                .build();
        //
        GenericRecord converted = converter.fromRecord(input);
        assertNotNull(converted);
        assertEquals(strings, converted.get("strings"));
        assertEquals(integers, converted.get("integers"));
        assertEquals(longs, converted.get("longs"));
        assertEquals(floats, converted.get("floats"));
        assertEquals(doubles, converted.get("doubles"));
        assertEquals(booleans, converted.get("booleans"));
        assertEquals(bytes, converted.get("bytes"));
        List<GenericRecord> avroRecs = (List<GenericRecord>) converted.get("records");
        assertNotNull(avroRecs);
        assertEquals(2, avroRecs.size());
        for (GenericRecord r : avroRecs) {
            assertNotNull(r);
            assertEquals("Bonjour", r.get("string1"));
            assertEquals("Olà", r.get("string2"));
            assertEquals(71, r.get("int"));
            assertEquals(true, r.get("boolean"));
            assertEquals(1971L, r.get("long"));
            assertEquals(LocalDateTime.of(2019, 04, 22, 0, 0).atZone(ZoneOffset.UTC).toInstant().toEpochMilli(),
                    r.get("datetime"));
            assertEquals(20.5f, r.get("float"));
            assertEquals(20.5, r.get("double"));
        }
        Record from = converter.toRecord(converted);
        assertNotNull(from);
        assertEquals(strings, from.getArray(String.class, "strings"));
        assertEquals(integers, from.getArray(Integer.class, "integers"));
        assertEquals(longs, from.getArray(Long.class, "longs"));
        assertEquals(floats, from.getArray(Float.class, "floats"));
        assertEquals(doubles, from.getArray(Double.class, "doubles"));
        assertEquals(booleans, from.getArray(Boolean.class, "booleans"));
        assertEquals(bytes, from.getArray(Byte.class, "bytes"));
        List<Record> tckRecs = (List<Record>) from.getArray(Record.class, "records");
        assertNotNull(tckRecs);
        assertEquals(2, tckRecs.size());
        for (Record r : tckRecs) {
            assertNotNull(r);
            assertEquals("Bonjour", r.getString("string1"));
            assertEquals("Olà", r.getString("string2"));
            assertEquals(71, r.getInt("int"));
            assertEquals(true, r.getBoolean("boolean"));
            assertEquals(1971L, r.getLong("long"));
            assertEquals(LocalDateTime.of(2019, 04, 22, 0, 0).atZone(ZoneOffset.UTC).toInstant(),
                    r.getDateTime("datetime").toInstant());
            assertEquals(20.5f, r.getFloat("float"));
            assertEquals(20.5, r.getDouble("double"));
        }
    }

    @Test
    void checkLogicalTypes() {
        assertEquals(LogicalTypes.date().getName(), AvroConverter.AVRO_LOGICAL_TYPE_DATE);
        assertEquals(LogicalTypes.timeMillis().getName(), AvroConverter.AVRO_LOGICAL_TYPE_TIME_MILLIS);
        assertEquals(LogicalTypes.timestampMillis().getName(), AvroConverter.AVRO_LOGICAL_TYPE_TIMESTAMP_MILLIS);
    }
}