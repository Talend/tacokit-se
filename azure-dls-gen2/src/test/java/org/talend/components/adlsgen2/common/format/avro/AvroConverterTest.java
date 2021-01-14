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
package org.talend.components.adlsgen2.common.format.avro;

import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.apache.avro.LogicalTypes;
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

import lombok.extern.slf4j.Slf4j;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
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

        assertEquals(now.withZoneSameInstant(ZoneOffset.UTC).toInstant().toEpochMilli(), ZonedDateTime
                .ofInstant(Instant.ofEpochMilli((long) record.get("now")), ZoneOffset.UTC).toInstant().toEpochMilli());
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
        // get back a GenericRecord
        GenericRecord gen = converter.fromRecord(last);
        assertNotNull(gen);
        assertEquals(7, gen.getSchema().getFields().size());
        assertEquals(999, gen.get("business_id"));
        assertEquals("Irene's Restaurant", gen.get("name"));
        assertEquals("Cafe", gen.get("category"));
        assertEquals(2.0f, gen.get("rating"));
        assertEquals(15992, gen.get("num_of_reviews"));
        assertNotNull(gen.get("attributes"));
        assertNotNull(((GenericRecord) gen.get("attributes")).get("good_for"));
        GenericRecord goodfor = (GenericRecord) ((GenericRecord) gen.get("attributes")).get("good_for");
        assertEquals(true, goodfor.get("dessert"));
        assertEquals(false, goodfor.get("kids"));
        assertEquals(true, goodfor.get("drinks"));
        assertEquals(true, goodfor.get("breakfast"));
        assertEquals(true, goodfor.get("lunch"));
        assertEquals(false, goodfor.get("dinner"));
        assertNotNull(((GenericRecord) gen.get("attributes")).get("parking"));
        GenericRecord parking = (GenericRecord) ((GenericRecord) gen.get("attributes")).get("parking");
        assertEquals(true, parking.get("lot"));
        assertEquals(false, parking.get("valet"));
        assertEquals(true, parking.get("lot"));
        assertEquals(false, ((GenericRecord) gen.get("attributes")).get("take_reservations"));
        assertEquals("noisy", ((GenericRecord) gen.get("attributes")).get("noise_level"));
        assertNotNull(gen.get("location"));
        assertEquals("STANDARD", ((GenericRecord) gen.get("location")).get("zipType"));
        assertEquals("23069", ((GenericRecord) gen.get("location")).get("zip"));
        assertEquals(false, ((GenericRecord) gen.get("location")).get("decomissionned"));
        assertEquals("1452", ((GenericRecord) gen.get("location")).get("taxReturnsFiled"));
        assertEquals("NA-US-VA-HANOVER", ((GenericRecord) gen.get("location")).get("location"));
        assertEquals("2561", ((GenericRecord) gen.get("location")).get("estimatedPopulation"));
        assertEquals("PRIMARY", ((GenericRecord) gen.get("location")).get("locationType"));
        assertEquals("57841342", ((GenericRecord) gen.get("location")).get("totalWages"));
        assertEquals("VA", ((GenericRecord) gen.get("location")).get("state"));
        assertEquals(-77.37f, ((GenericRecord) gen.get("location")).get("longitude"));
        assertEquals(37.76f, ((GenericRecord) gen.get("location")).get("latitude"));
        assertEquals("HANOVER", ((GenericRecord) gen.get("location")).get("city"));
    }

    @Test
    void withBigBusinessAvroFile() throws Exception {
        InputStream businessIs = getClass().getResource("/common/format/avro/big_business.avro").openStream();
        AvroIterator iter = AvroIterator.Builder.of(recordBuilderFactory).withConfiguration(new AvroConfiguration())
                .parse(businessIs);
        Record first, last = null;
        first = iter.next();
        while (iter.hasNext()) {
            last = iter.next();
        }
        assertNotNull(first);
        assertEquals(3, first.getSchema().getEntries().size());
        assertEquals(0, first.getInt("business_id"));
        assertEquals("Betty's Cafe", first.getRecord("business").getString("name"));
        assertEquals("Club", first.getRecord("business").getString("category"));
        assertEquals(4.0, first.getRecord("business").getFloat("rating"));
        assertEquals(2647, first.getRecord("business").getInt("num_of_reviews"));
        assertNotNull(first.getRecord("business").getRecord("attributes"));
        assertNotNull(first.getRecord("business").getRecord("attributes").getRecord("good_for"));
        assertEquals(false, first.getRecord("business").getRecord("attributes").getRecord("good_for").getBoolean("dessert"));
        assertEquals(true, first.getRecord("business").getRecord("attributes").getRecord("good_for").getBoolean("kids"));
        assertEquals(false, first.getRecord("business").getRecord("attributes").getRecord("good_for").getBoolean("drinks"));
        assertEquals(false, first.getRecord("business").getRecord("attributes").getRecord("good_for").getBoolean("breakfast"));
        assertEquals(false, first.getRecord("business").getRecord("attributes").getRecord("good_for").getBoolean("lunch"));
        assertEquals(true, first.getRecord("business").getRecord("attributes").getRecord("good_for").getBoolean("dinner"));
        assertNotNull(first.getRecord("business").getRecord("attributes").getRecord("parking"));
        assertEquals(false, first.getRecord("business").getRecord("attributes").getRecord("parking").getBoolean("lot"));
        assertEquals(false, first.getRecord("business").getRecord("attributes").getRecord("parking").getBoolean("valet"));
        assertEquals(false, first.getRecord("business").getRecord("attributes").getRecord("parking").getBoolean("lot"));
        assertEquals(true, first.getRecord("business").getRecord("attributes").getBoolean("take_reservations"));
        assertEquals("quiet", first.getRecord("business").getRecord("attributes").getString("noise_level"));
        assertNotNull(first.getRecord("business").getRecord("location"));
        assertEquals("STANDARD", first.getRecord("business").getRecord("location").getString("zipType"));
        assertEquals("72132", first.getRecord("business").getRecord("location").getString("zip"));
        assertEquals(false, first.getRecord("business").getRecord("location").getBoolean("decomissionned"));
        assertEquals("1400", first.getRecord("business").getRecord("location").getString("taxReturnsFiled"));
        assertEquals("NA-US-AR-REDFIELD", first.getRecord("business").getRecord("location").getString("location"));
        assertEquals("2653", first.getRecord("business").getRecord("location").getString("estimatedPopulation"));
        assertEquals("PRIMARY", first.getRecord("business").getRecord("location").getString("locationType"));
        assertEquals("56190766", first.getRecord("business").getRecord("location").getString("totalWages"));
        assertEquals("AR", first.getRecord("business").getRecord("location").getString("state"));
        assertEquals(-92.18f, first.getRecord("business").getRecord("location").getFloat("longitude"));
        assertEquals(34.44f, first.getRecord("business").getRecord("location").getFloat("latitude"));
        assertEquals("REDFIELD", first.getRecord("business").getRecord("location").getString("city"));
        Collection<Record> reviews = first.getArray(Record.class, "reviews");
        assertNotNull(reviews);
        Record lastReview = null;
        int reviewCount = 0;
        for (Record review : reviews) {
            lastReview = review;
            reviewCount++;
        }
        assertEquals(166, reviewCount);
        assertNotNull(lastReview);
        assertEquals(0, lastReview.getInt("business_id"));
        assertEquals(3656, lastReview.getRecord("user").getInt("user_id"));
        assertEquals("Doris", lastReview.getRecord("user").getString("name"));
        assertEquals("FEMALE", lastReview.getRecord("user").getString("gender"));
        assertEquals(17, lastReview.getRecord("user").getInt("age"));
        assertEquals(1202, lastReview.getRecord("user").getInt("review_count"));
        assertEquals(2.0, lastReview.getRecord("user").getFloat("avg_rating"));
        assertEquals(410, lastReview.getRecord("user").getRecord("user_votes").getInt("helpful"));
        assertEquals(605.0, lastReview.getRecord("user").getRecord("user_votes").getInt("cool"));
        assertEquals(496.0, lastReview.getRecord("user").getRecord("user_votes").getInt("unhelpful"));
        assertEquals(255, lastReview.getRecord("user").getInt("friends_count"));
        assertEquals(4.0, lastReview.getFloat("rating"));
        assertEquals("27/09/2010", lastReview.getString("date"));
        assertEquals(96, lastReview.getString("review_text").length());
        assertEquals(13, lastReview.getRecord("votes").getInt("helpful"));
        assertEquals(5, lastReview.getRecord("votes").getInt("cool"));
        assertEquals(8, lastReview.getRecord("votes").getInt("unhelpful"));
        //
        // get back a GenericRecord
        //
        GenericRecord gen = converter.fromRecord(first);
        assertNotNull(gen);
        assertEquals(3, gen.getSchema().getFields().size());
        GenericRecord business = (GenericRecord) gen.get("business");
        assertEquals(0, gen.get("business_id"));
        assertEquals("Betty's Cafe", business.get("name"));
        assertEquals("Club", business.get("category"));
        assertEquals(4.0f, business.get("rating"));
        assertEquals(2647, business.get("num_of_reviews"));
        GenericRecord attributes = (GenericRecord) business.get("attributes");
        assertNotNull(attributes);
        GenericRecord goodFor = (GenericRecord) attributes.get("good_for");
        assertNotNull(goodFor);
        assertEquals(false, goodFor.get("dessert"));
        assertEquals(true, goodFor.get("kids"));
        assertEquals(false, goodFor.get("drinks"));
        assertEquals(false, goodFor.get("breakfast"));
        assertEquals(false, goodFor.get("lunch"));
        assertEquals(true, goodFor.get("dinner"));
        GenericRecord parking = (GenericRecord) attributes.get("parking");
        assertNotNull(parking);
        assertEquals(false, parking.get("lot"));
        assertEquals(false, parking.get("valet"));
        assertEquals(false, parking.get("lot"));
        assertEquals(true, attributes.get("take_reservations"));
        assertEquals("quiet", attributes.get("noise_level"));
        GenericRecord location = (GenericRecord) business.get("location");
        assertNotNull(location);
        assertEquals("STANDARD", location.get("zipType"));
        assertEquals("72132", location.get("zip"));
        assertEquals(false, location.get("decomissionned"));
        assertEquals("1400", location.get("taxReturnsFiled"));
        assertEquals("NA-US-AR-REDFIELD", location.get("location"));
        assertEquals("2653", location.get("estimatedPopulation"));
        assertEquals("PRIMARY", location.get("locationType"));
        assertEquals("56190766", location.get("totalWages"));
        assertEquals("AR", location.get("state"));
        assertEquals(-92.18f, location.get("longitude"));
        assertEquals(34.44f, location.get("latitude"));
        assertEquals("REDFIELD", location.get("city"));
        // reviews
        List<GenericRecord> grReviews = (List<GenericRecord>) gen.get("reviews");
        assertNotNull(grReviews);
        GenericRecord grLastReview = null;
        reviewCount = 0;
        for (GenericRecord review : grReviews) {
            reviewCount++;
            grLastReview = review;
        }
        assertEquals(166, reviewCount);
        assertNotNull(grLastReview);
        assertEquals(0, grLastReview.get("business_id"));
        GenericRecord grUser = (GenericRecord) grLastReview.get("user");
        assertEquals(3656, grUser.get("user_id"));
        assertEquals("Doris", grUser.get("name"));
        assertEquals("FEMALE", grUser.get("gender"));
        assertEquals(17, grUser.get("age"));
        assertEquals(1202, grUser.get("review_count"));
        assertEquals(2.0f, grUser.get("avg_rating"));
        GenericRecord grUserVotes = (GenericRecord) grUser.get("user_votes");
        assertEquals(410, grUserVotes.get("helpful"));
        assertEquals(605, grUserVotes.get("cool"));
        assertEquals(496, grUserVotes.get("unhelpful"));
        assertEquals(255, grUser.get("friends_count"));
        assertEquals(4.0f, grLastReview.get("rating"));
        assertEquals("27/09/2010", grLastReview.get("date"));
        assertEquals(96, grLastReview.get("review_text").toString().length());
        GenericRecord grVotes = (GenericRecord) grLastReview.get("votes");
        assertEquals(13, grVotes.get("helpful"));
        assertEquals(5, grVotes.get("cool"));
        assertEquals(8, grVotes.get("unhelpful"));
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
