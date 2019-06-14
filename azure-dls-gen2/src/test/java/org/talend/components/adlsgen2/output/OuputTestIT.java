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
package org.talend.components.adlsgen2.output;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.common.format.csv.CsvConfiguration;
import org.talend.components.adlsgen2.common.format.csv.CsvFieldDelimiter;
import org.talend.components.adlsgen2.common.format.csv.CsvRecordSeparator;
import org.talend.components.adlsgen2.dataset.AdlsGen2DataSet;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.adlsgen2")
public class OuputTestIT extends AdlsGen2TestBase {

    String basePathIn = "TestIT/in/";

    String basePathOut = "TestIT/out/";

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

}
