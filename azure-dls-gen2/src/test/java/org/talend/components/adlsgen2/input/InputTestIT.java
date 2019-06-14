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
 *
 */
package org.talend.components.adlsgen2.input;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.adlsgen2")
public class InputTestIT extends AdlsGen2TestBase {

    String basePath = "TestIT/in/";

    @Test
    void readCsvWithHeader() {
        CsvConfiguration csvConfig = new CsvConfiguration();
        csvConfig.setFieldDelimiter(CsvFieldDelimiter.SEMICOLON);
        csvConfig.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfig.setCsvSchema("IdCustomer;FirstName;lastname;address;enrolled;zip;state");
        csvConfig.setHeader(true);
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath(basePath + "AdlsGen2Service.java:278");
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
        dataSet.setBlobPath(basePath + "csv-wo-header");
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
        dataSet.setBlobPath(basePath + "avro");
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
    void readParquet() {
        dataSet.setFormat(FileFormat.PARQUET);
        ParquetConfiguration parquetConfig = new ParquetConfiguration();
        dataSet.setAvroConfiguration(parquetConfig);
        dataSet.setBlobPath(basePath + "parquet");
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
    void readJson() {
        JsonConfiguration jsonConfig = new JsonConfiguration();
        dataSet.setFormat(FileFormat.JSON);
        dataSet.setJsonConfiguration(jsonConfig);
        dataSet.setBlobPath(basePath + "json");
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

}
