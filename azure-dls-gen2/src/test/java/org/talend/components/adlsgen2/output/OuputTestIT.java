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
package org.talend.components.adlsgen2.output;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.adlsgen2.common.format.FileEncoding;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.common.format.avro.AvroConfiguration;
import org.talend.components.adlsgen2.common.format.csv.CsvConfiguration;
import org.talend.components.adlsgen2.common.format.csv.CsvFieldDelimiter;
import org.talend.components.adlsgen2.common.format.csv.CsvRecordSeparator;
import org.talend.components.adlsgen2.dataset.AdlsGen2DataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;
import org.talend.sdk.component.runtime.record.SchemaImpl;

import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@WithComponents("org.talend.components.adlsgen2")
public class OuputTestIT extends AdlsGen2TestBase {

    CsvConfiguration csvConfig;

    AdlsGen2DataSet outDs;

    @BeforeEach
    void setConfiguration() {
        outDs = new AdlsGen2DataSet();
        outDs.setConnection(connection);
        outDs.setFilesystem(storageFs);
        //
        csvConfig = new CsvConfiguration();
        csvConfig.setFieldDelimiter(CsvFieldDelimiter.SEMICOLON);
        csvConfig.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfig.setCsvSchema("IdCustomer;FirstName;lastname;address;enrolled;zip;state");
        csvConfig.setHeader(true);
    }

    @Test
    public void fromCsvToAvro() {
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath(basePathIn + "csv-w-header");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //

        outDs.setFormat(FileFormat.AVRO);
        outDs.setBlobPath(basePathOut + "avro");
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate("data-");
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        outConfig += "&$configuration.$maxBatchSize=150";
        //
        Job.components() //
                .component("in", "Azure://AdlsGen2Input?" + inConfig) //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
    }

    @Test
    public void fromCsvToJson() {
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath(basePathIn + "csv-w-header");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //

        outDs.setFormat(FileFormat.JSON);
        outDs.setBlobPath(basePathOut + "json");
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate("data-");
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        outConfig += "&$configuration.$maxBatchSize=150";
        //
        Job.components() //
                .component("in", "Azure://AdlsGen2Input?" + inConfig) //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
    }

    @Test
    public void fromCsvToParquet() {
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath(basePathIn + "csv-w-header");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //

        outDs.setFormat(FileFormat.PARQUET);
        outDs.setBlobPath(basePathOut + "parquet");
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate("data-");
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        outConfig += "&$configuration.$maxBatchSize=250";
        //
        Job.components() //
                .component("in", "Azure://AdlsGen2Input?" + inConfig) //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
    }

    @Test
    public void fromCsvToCsv() {
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath(basePathIn + "csv-w-header");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //
        outDs.setFormat(FileFormat.CSV);
        outDs.setBlobPath(basePathOut + "csv");
        outDs.setCsvConfiguration(csvConfig);
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate("IT-csv-2-csv-whdr-data2222-");
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        outConfig += "&$configuration.$maxBatchSize=250";
        //
        Job.components() //
                .component("in", "Azure://AdlsGen2Input?" + inConfig) //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
    }

    @Test
    public void fromCsvToCsvQuoted() {
        csvConfig.setCsvSchema("");
        csvConfig.setHeader(false);
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath(basePathIn + "csv-w-header");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //
        outDs.setFormat(FileFormat.CSV);
        outDs.setBlobPath(basePathOut + "csv");
        csvConfig.setHeader(true);
        csvConfig.setCsvSchema("");
        csvConfig.setTextEnclosureCharacter("\"");
        csvConfig.setEscapeCharacter("\\");
        csvConfig.setFieldDelimiter(CsvFieldDelimiter.COMMA);
        outDs.setCsvConfiguration(csvConfig);
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate("IT-csv-2-csv-whdr-QUOTED__-");
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        outConfig += "&$configuration.$maxBatchSize=250";
        //
        Job.components() //
                .component("in", "Azure://AdlsGen2Input?" + inConfig) //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
    }

    @Test
    void testOutputNull() {
        dataSet.setFormat(FileFormat.AVRO);
        AvroConfiguration avroConfig = new AvroConfiguration();
        dataSet.setAvroConfiguration(avroConfig);
        dataSet.setBlobPath(basePathOut + "avro-output-nulls");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //
        outDs.setFormat(FileFormat.AVRO);
        outDs.setBlobPath(basePathOut + "avro-output-nulls");
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate("avro-null-data-");
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        final int schemaSize = 9;
        Schema.Builder schemaBuilder = new SchemaImpl.BuilderImpl();
        Schema schema = schemaBuilder.withType(Schema.Type.RECORD)
                .withEntry(new SchemaImpl.EntryImpl("nullStringColumn", Schema.Type.STRING, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullStringColumn2", Schema.Type.STRING, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullIntColumn", Schema.Type.INT, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullLongColumn", Schema.Type.LONG, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullFloatColumn", Schema.Type.FLOAT, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullDoubleColumn", Schema.Type.DOUBLE, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullBooleanColumn", Schema.Type.BOOLEAN, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullByteArrayColumn", Schema.Type.BYTES, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("nullDateColumn", Schema.Type.DATETIME, true, null, null, null)).build();
        Record testRecord = components.findService(RecordBuilderFactory.class).newRecordBuilder(schema)
                .withString("nullStringColumn", null).build();
        List<Record> testRecords = Collections.singletonList(testRecord);
        components.setInputData(testRecords);
        Job.components() //
                .component("in", "test://emitter") //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build().run();
        Job.components() //
                .component("in", "Azure://AdlsGen2Input?" + inConfig) //
                .component("out", "test://collector") //
                .connections() //
                .from("in") //
                .to("out") //
                .build().run();
        List<Record> records = components.getCollectedData(Record.class);
        Record firstRecord = records.get(0);
        Assert.assertEquals(schemaSize, firstRecord.getSchema().getEntries().size());
        Assert.assertNull(firstRecord.getString("nullStringColumn"));
        Assert.assertNull(firstRecord.getString("nullStringColumn2"));
        Assert.assertNull(firstRecord.get(Integer.class, "nullIntColumn"));
        Assert.assertNull(firstRecord.get(Long.class, "nullLongColumn"));
        Assert.assertNull(firstRecord.get(Float.class, "nullFloatColumn"));
        Assert.assertNull(firstRecord.get(Double.class, "nullDoubleColumn"));
        Assert.assertNull(firstRecord.get(Boolean.class, "nullBooleanColumn"));
        Assert.assertNull(firstRecord.get(byte[].class, "nullByteArrayColumn"));
        Assert.assertNull(firstRecord.getDateTime("nullDateColumn"));
    }

    @Test
    void testSchemaIsNotMissingForNullsInFirstRecord() {
        dataSet.setFormat(FileFormat.AVRO);
        AvroConfiguration avroConfig = new AvroConfiguration();
        dataSet.setAvroConfiguration(avroConfig);
        dataSet.setBlobPath(basePathOut + "avro-nulls");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //
        outDs.setFormat(FileFormat.AVRO);
        outDs.setBlobPath(basePathOut + "avro-nulls");
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate("avro-null-data-");
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        final int fieldSize = 2;
        Schema.Builder schemaBuilder = new SchemaImpl.BuilderImpl();
        Schema schema = schemaBuilder.withType(Schema.Type.RECORD)
                .withEntry(new SchemaImpl.EntryImpl("stringColumn", Schema.Type.STRING, true, null, null, null))
                .withEntry(new SchemaImpl.EntryImpl("intColumn", Schema.Type.INT, true, null, null, null)).build();
        List<Record> testRecords = new ArrayList<>();
        testRecords.add(components.findService(RecordBuilderFactory.class).newRecordBuilder(schema)
                .withString("stringColumn", "a").build()); // stringColumn:a, intColumn:null
        testRecords
                .add(components.findService(RecordBuilderFactory.class).newRecordBuilder(schema).withString("stringColumn", "b") //
                        .withInt("intColumn", Integer.MAX_VALUE) //
                        .build()); // stringColumn:a,
        // intColumn:not null
        components.setInputData(testRecords);
        Job.components() //
                .component("in", "test://emitter") //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build().run();
        Job.components() //
                .component("in", "Azure://AdlsGen2Input?" + inConfig) //
                .component("out", "test://collector") //
                .connections() //
                .from("in") //
                .to("out") //
                .build().run();
        List<Record> records = components.getCollectedData(Record.class);
        Assert.assertEquals(fieldSize, records.get(0).getSchema().getEntries().size());
        Assert.assertEquals(fieldSize, records.get(1).getSchema().getEntries().size());
    }

    @ParameterizedTest
    @ValueSource(strings = { "SJIS", "GB2312", "ISO-8859-1" })
    void outputToCsvEncoded(String encoding) {
        CsvConfiguration csvConfiguration = new CsvConfiguration();
        csvConfiguration.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfiguration.setFileEncoding(FileEncoding.OTHER);
        csvConfiguration.setCustomFileEncoding(encoding);
        csvConfiguration.setCsvSchema("id;value");
        //
        String sample = "bb";
        String sampleA = "テスト";
        String sampleB = "电话号码";
        String sampleC = "cèt été, il va faïre bôt !";
        switch (encoding) {
        case "SJIS":
            sample = sampleA;
            break;
        case "GB2312":
            sample = sampleB;
            break;
        case "ISO-8859-1":
            sample = sampleC;
            break;
        default:
            fail("Should not be here for encoding:" + encoding);
        }
        //
        Schema schema = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD) //
                .withEntry(new SchemaImpl.EntryImpl("id", Schema.Type.INT, true, null, null, null)) //
                .withEntry(new SchemaImpl.EntryImpl("value", Type.STRING, true, null, null, null)) //
                .build();
        List<Record> testRecords = new ArrayList<>();
        testRecords.add(recordBuilderFactory.newRecordBuilder(schema) //
                .withInt("id", 1) //
                .withString("value", sample) //
                .build());
        //
        String blobPath = String.format("%scsv-encoded-%s", basePathOut, encoding);
        //
        // now outputs
        //
        outDs.setFormat(FileFormat.CSV);
        outDs.setCsvConfiguration(csvConfiguration);
        outDs.setBlobPath(blobPath);
        outputConfiguration.setDataSet(outDs);
        outputConfiguration.setBlobNameTemplate(String.format("csv-%s-encoded-data-", encoding));
        String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        components.setInputData(testRecords);
        Job.components() //
                .component("in", "test://emitter") //
                .component("out", "Azure://AdlsGen2Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build().run();
        // now read back
        dataSet.setFormat(FileFormat.CSV);
        dataSet.setCsvConfiguration(csvConfiguration);
        dataSet.setBlobPath(blobPath);
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
        assertNotNull(records);
        assertFalse(records.isEmpty());
        for (Record encoded : records) {
            assertNotNull(encoded);
            log.warn("[outputToCsvEncoded] {}", encoded);
            assertEquals("1", encoded.getString("id"));
            assertEquals(sample, encoded.getString("value"));
        }
    }

}
