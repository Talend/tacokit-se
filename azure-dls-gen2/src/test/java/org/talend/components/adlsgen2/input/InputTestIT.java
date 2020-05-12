/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.adlsgen2.input;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.adlsgen2.common.format.FileEncoding;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.common.format.avro.AvroConfiguration;
import org.talend.components.adlsgen2.common.format.csv.CsvConfiguration;
import org.talend.components.adlsgen2.common.format.csv.CsvFieldDelimiter;
import org.talend.components.adlsgen2.common.format.csv.CsvRecordSeparator;
import org.talend.components.adlsgen2.common.format.json.JsonConfiguration;
import org.talend.components.adlsgen2.common.format.parquet.ParquetConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@WithComponents("org.talend.components.adlsgen2")
public class InputTestIT extends AdlsGen2TestBase {

    @Test
    void readCsvWithHeader() {
        CsvConfiguration csvConfig = new CsvConfiguration();
        csvConfig.setFieldDelimiter(CsvFieldDelimiter.SEMICOLON);
        csvConfig.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfig.setCsvSchema("IdCustomer;FirstName;lastname;address;enrolled;zip;state");
        csvConfig.setHeader(true);
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath(basePathIn + "csv-w-header");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("mycomponent", "Azure://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(1000, records.size());
    }

    @Test
    void readCsvWithoutHeader() {
        CsvConfiguration csvConfig = new CsvConfiguration();
        csvConfig.setFieldDelimiter(CsvFieldDelimiter.SEMICOLON);
        csvConfig.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfig.setCsvSchema("IdCustomer;FirstName;lastname;address;enrolled;zip;state");
        csvConfig.setHeader(false);
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath(basePathIn + "csv-wo-header");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("mycomponent", "Azure://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(1000, records.size());
    }

    @Test
    void readAvro() {
        dataSet.setFormat(FileFormat.AVRO);
        AvroConfiguration avroConfig = new AvroConfiguration();
        dataSet.setAvroConfiguration(avroConfig);
        dataSet.setBlobPath(basePathIn + "avro");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("mycomponent", "Azure://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(1000, records.size());
    }

    @Test
    void readAvroBusiness() {
        dataSet.setFormat(FileFormat.AVRO);
        AvroConfiguration avroConfig = new AvroConfiguration();
        dataSet.setAvroConfiguration(avroConfig);
        dataSet.setBlobPath(basePathIn + "business-avro");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("in", "Azure://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("in") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(1000, records.size());
        Record first = records.get(0);
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
    }

    @Test
    void readParquet() {
        dataSet.setFormat(FileFormat.PARQUET);
        ParquetConfiguration parquetConfig = new ParquetConfiguration();
        dataSet.setAvroConfiguration(parquetConfig);
        dataSet.setBlobPath(basePathIn + "parquet");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("mycomponent", "Azure://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(51, records.size());
    }

    @Test
    void readAvroBigBusiness() {
        dataSet.setFormat(FileFormat.AVRO);
        AvroConfiguration avroConfig = new AvroConfiguration();
        dataSet.setAvroConfiguration(avroConfig);
        dataSet.setBlobPath(basePathIn + "big_business.avro");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("in", "Azure://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("in") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(1000, records.size());
        Record first = records.get(0);
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

    }

    @Test
    void readJson() {
        JsonConfiguration jsonConfig = new JsonConfiguration();
        dataSet.setFormat(FileFormat.JSON);
        dataSet.setJsonConfiguration(jsonConfig);
        dataSet.setBlobPath(basePathIn + "json");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("mycomponent", "Azure://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(5000, records.size());
    }

    @Test
    void blobPathIsFileInsteadOfFolder() {
        AvroConfiguration avroConfig = new AvroConfiguration();
        dataSet.setFormat(FileFormat.AVRO);
        dataSet.setAvroConfiguration(avroConfig);
        dataSet.setBlobPath(basePathIn + "business-avro/business.avro");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("mycomponent", "Azure://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(1000, records.size());
    }

    @Test
    void csvEncodedInSJis() {
        CsvConfiguration csvConfiguration = new CsvConfiguration();
        csvConfiguration.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfiguration.setFileEncoding(FileEncoding.OTHER);
        csvConfiguration.setCustomFileEncoding("SJIS");
        dataSet.setFormat(FileFormat.CSV);
        dataSet.setCsvConfiguration(csvConfiguration);
        dataSet.setBlobPath(basePathIn + "encoding/SJIS-encoded.csv");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("in", "Azure://AdlsGen2Input?" + config) //
                .component("out", "test://collector") //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        Record encoded = records.get(0);
        assertNotNull(encoded);
        assertEquals("2", encoded.getString("field0"));
        assertEquals("2000.3", encoded.getString("field1"));
        assertEquals("テスト", encoded.getString("field2"));
    }

    @Test
    void readAvroBusinessGeneratedBySink() {
        dataSet.setFormat(FileFormat.AVRO);
        AvroConfiguration avroConfig = new AvroConfiguration();
        dataSet.setAvroConfiguration(avroConfig);
        dataSet.setBlobPath(basePathOut + "business-avro");
        inputConfiguration.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("in", "Azure://AdlsGen2Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("in") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(1000, records.size());
        Record first = records.get(0);
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
    }

}
