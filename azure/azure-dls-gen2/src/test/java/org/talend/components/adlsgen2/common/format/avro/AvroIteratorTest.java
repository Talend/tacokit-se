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
import java.util.Collection;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.common.converters.AvroConverter;
import org.talend.components.common.formats.AvroFormatOptions;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@WithComponents("org.talend.components.adlsgen2")
class AvroIteratorTest {

    private AvroConverter converter;

    private RecordBuilderFactory recordBuilderFactory;

    @Injected
    protected BaseComponentsHandler componentsHandler;

    @BeforeEach
    void setUp() {
        recordBuilderFactory = componentsHandler.findService(RecordBuilderFactory.class);
        converter = AvroConverter.of(recordBuilderFactory, null);
    }

    @Test
    void withBigBusinessAvroFile() throws Exception {
        InputStream businessIs = getClass().getResource("/common/format/avro/big_business.avro").openStream();
        AvroIterator iter = AvroIterator.Builder.of(recordBuilderFactory).withConfiguration(new AvroFormatOptions())
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
    void withNullData() throws Exception {
        InputStream business = getClass().getResource("/common/format/avro/null-data.avro").openStream();
        AvroIterator iter = AvroIterator.Builder.of(recordBuilderFactory).withConfiguration(new AvroFormatOptions())
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
        AvroIterator iter = AvroIterator.Builder.of(recordBuilderFactory).withConfiguration(new AvroFormatOptions())
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

}