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

import java.io.InputStream;
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
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.record.SchemaImpl;

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

    private AvroConfiguration avroConfiguration;

    @BeforeEach
    protected void setUp() throws Exception {
        super.setUp();
        converter = AvroConverter.of(recordBuilderFactory, avroConfiguration);
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
        assertNotNull(AvroConverter.of(recordBuilderFactory, avroConfiguration));
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
        assertNull(record.get(Integer.class, "int"));
        assertNull(record.get(Long.class, "long"));
        assertNull(record.get(Double.class, "double"));
        assertNull(record.get(Boolean.class, "boolean"));
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

    // @Test
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
        System.out.println("schema: " + testRecord.getSchema());
        System.out.println("record: " + testRecord);
        assertNotNull(testRecord.getString("nullStringColumn"));
        assertNull(testRecord.getInt("nullIntColumn"));
    }

    @Test
    void withNullData() throws Exception {
        InputStream business = getClass().getResource("/common/format/avro/null-data.avro").openStream();
        AvroIterator iter = AvroIterator.Builder.of(recordBuilderFactory).withConfiguration(new AvroConfiguration())
                .parse(business);
        Record first = null;
        first = iter.next();
        assertNotNull(first);
        assertFalse(iter.hasNext());
        assertEquals(2, first.getSchema().getEntries().size());
        assertEquals("a", first.getString("stringColumn"));
        assertNull(first.get(Integer.class, "intColumn"));
    }

    @Test
    void withBusinessAvroFile() throws Exception {
        InputStream business = getClass().getResource("/common/format/avro/business.avro").openStream();
        AvroIterator iter = AvroIterator.Builder.of(recordBuilderFactory).withConfiguration(new AvroConfiguration())
                .parse(business);
        Record first, last = null;
        first = iter.next();
        while (iter.hasNext()) {
            last = iter.next();
        }
        assertNotNull(first);
        assertEquals(7, first.getSchema().getEntries().size());
        assertEquals(0, first.getInt("business_id"));
        assertEquals("Betty's Cafe", first.getString("name"));
        assertEquals("Club", first.getString("category"));
        assertEquals(4.0, first.getFloat("rating"));
        assertEquals(2647, first.getInt("num_of_reviews"));
        assertNotNull(first.getRecord("attributes"));
        assertNotNull(first.getRecord("attributes").getRecord("good_for"));
        assertEquals(false, first.getRecord("attributes").getRecord("good_for").getBoolean("dessert"));
        assertEquals(true, first.getRecord("attributes").getRecord("good_for").getBoolean("kids"));
        assertEquals(false, first.getRecord("attributes").getRecord("good_for").getBoolean("drinks"));
        assertEquals(false, first.getRecord("attributes").getRecord("good_for").getBoolean("breakfast"));
        assertEquals(false, first.getRecord("attributes").getRecord("good_for").getBoolean("lunch"));
        assertEquals(true, first.getRecord("attributes").getRecord("good_for").getBoolean("dinner"));
        assertNotNull(first.getRecord("attributes").getRecord("parking"));
        assertEquals(false, first.getRecord("attributes").getRecord("parking").getBoolean("lot"));
        assertEquals(false, first.getRecord("attributes").getRecord("parking").getBoolean("valet"));
        assertEquals(false, first.getRecord("attributes").getRecord("parking").getBoolean("lot"));
        assertEquals(true, first.getRecord("attributes").getBoolean("take_reservations"));
        assertEquals("quiet", first.getRecord("attributes").getString("noise_level"));
        assertNotNull(first.getRecord("location"));
        assertEquals("STANDARD", first.getRecord("location").getString("zipType"));
        assertEquals("72132", first.getRecord("location").getString("zip"));
        assertEquals(false, first.getRecord("location").getBoolean("decomissionned"));
        assertEquals("1400", first.getRecord("location").getString("taxReturnsFiled"));
        assertEquals("NA-US-AR-REDFIELD", first.getRecord("location").getString("location"));
        assertEquals("2653", first.getRecord("location").getString("estimatedPopulation"));
        assertEquals("PRIMARY", first.getRecord("location").getString("locationType"));
        assertEquals("56190766", first.getRecord("location").getString("totalWages"));
        assertEquals("AR", first.getRecord("location").getString("state"));
        assertEquals(-92.18f, first.getRecord("location").getFloat("longitude"));
        assertEquals(34.44f, first.getRecord("location").getFloat("latitude"));
        assertEquals("REDFIELD", first.getRecord("location").getString("city"));
        // last record
        assertNotNull(last);
        assertEquals(7, last.getSchema().getEntries().size());
        assertEquals(999, last.getInt("business_id"));
        assertEquals("Irene's Restaurant", last.getString("name"));
        assertEquals("Cafe", last.getString("category"));
        assertEquals(2.0, last.getFloat("rating"));
        assertEquals(15992, last.getInt("num_of_reviews"));
        assertNotNull(last.getRecord("attributes"));
        assertNotNull(last.getRecord("attributes").getRecord("good_for"));
        assertEquals(true, last.getRecord("attributes").getRecord("good_for").getBoolean("dessert"));
        assertEquals(false, last.getRecord("attributes").getRecord("good_for").getBoolean("kids"));
        assertEquals(true, last.getRecord("attributes").getRecord("good_for").getBoolean("drinks"));
        assertEquals(true, last.getRecord("attributes").getRecord("good_for").getBoolean("breakfast"));
        assertEquals(true, last.getRecord("attributes").getRecord("good_for").getBoolean("lunch"));
        assertEquals(false, last.getRecord("attributes").getRecord("good_for").getBoolean("dinner"));
        assertNotNull(last.getRecord("attributes").getRecord("parking"));
        assertEquals(true, last.getRecord("attributes").getRecord("parking").getBoolean("lot"));
        assertEquals(false, last.getRecord("attributes").getRecord("parking").getBoolean("valet"));
        assertEquals(true, last.getRecord("attributes").getRecord("parking").getBoolean("lot"));
        assertEquals(false, last.getRecord("attributes").getBoolean("take_reservations"));
        assertEquals("noisy", last.getRecord("attributes").getString("noise_level"));
        assertNotNull(last.getRecord("location"));
        assertEquals("STANDARD", last.getRecord("location").getString("zipType"));
        assertEquals("23069", last.getRecord("location").getString("zip"));
        assertEquals(false, last.getRecord("location").getBoolean("decomissionned"));
        assertEquals("1452", last.getRecord("location").getString("taxReturnsFiled"));
        assertEquals("NA-US-VA-HANOVER", last.getRecord("location").getString("location"));
        assertEquals("2561", last.getRecord("location").getString("estimatedPopulation"));
        assertEquals("PRIMARY", last.getRecord("location").getString("locationType"));
        assertEquals("57841342", last.getRecord("location").getString("totalWages"));
        assertEquals("VA", last.getRecord("location").getString("state"));
        assertEquals(-77.37f, last.getRecord("location").getFloat("longitude"));
        assertEquals(37.76f, last.getRecord("location").getFloat("latitude"));
        assertEquals("HANOVER", last.getRecord("location").getString("city"));
    }
}
