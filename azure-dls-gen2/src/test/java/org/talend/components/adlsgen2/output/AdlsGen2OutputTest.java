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
package org.talend.components.adlsgen2.output;

import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.common.format.csv.CsvConfiguration;
import org.talend.components.adlsgen2.common.format.csv.CsvFieldDelimiter;
import org.talend.components.adlsgen2.common.format.csv.CsvRecordSeparator;
import org.talend.components.adlsgen2.dataset.AdlsGen2DataSet;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection.AuthMethod;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.junit.http.internal.impl.AzureStorageCredentialsRemovalResponseLocator;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import lombok.extern.slf4j.Slf4j;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@HttpApi(useSsl = true, responseLocator = AzureStorageCredentialsRemovalResponseLocator.class)
@WithComponents("org.talend.components.adlsgen2")
class AdlsGen2OutputTest extends AdlsGen2TestBase {

    @Service
    private LocalConfiguration configuration;

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    public void produceCsv(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        outputConfiguration.setOverwrite(true);
        outputConfiguration.getDataSet().setBlobPath("customers_test_produce.csv");
        components.setInputData(asList(createData(), createData(), createData()));
        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        Job.components() //
                .component("emitter", "test://emitter") //
                .component("out", "Azure-DLS-Gen2://Output?" + config) //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    public void fromCsvToJson(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        CsvConfiguration csvConfig = new CsvConfiguration();
        csvConfig.setFieldDelimiter(CsvFieldDelimiter.SEMICOLON);
        csvConfig.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfig.setCsvSchema("");
        csvConfig.setHeader(true);
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath("demo_gen2/in/customers.csv");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //
        AdlsGen2DataSet outDs = new AdlsGen2DataSet();
        outDs.setConnection(connection);
        outDs.setFilesystem(storageFs);
        outDs.setFormat(FileFormat.JSON);
        outDs.setBlobPath("demo_gen2/out/customers.json");
        outputConfiguration.setOverwrite(true);
        outputConfiguration.setDataSet(outDs);
        components.setInputData(asList(createData(), createData(), createData()));
        //

        final String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        Job.components() //
                .component("emitter", "Azure-DLS-Gen2://Input?" + inConfig) //
                .component("out", "Azure-DLS-Gen2://Output?" + outConfig) //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    public void fromCsvToAvro(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        CsvConfiguration csvConfig = new CsvConfiguration();
        csvConfig.setFieldDelimiter(CsvFieldDelimiter.SEMICOLON);
        csvConfig.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfig.setCsvSchema("");
        csvConfig.setHeader(true);
        dataSet.setCsvConfiguration(csvConfig);
        dataSet.setBlobPath("demo_gen2/in/customers.csv");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //
        AdlsGen2DataSet outDs = new AdlsGen2DataSet();
        outDs.setConnection(connection);
        outDs.setFilesystem(storageFs);
        outDs.setFormat(FileFormat.AVRO);
        outDs.setBlobPath("demo_gen2/out/customers-from-csv.avro");
        outputConfiguration.setOverwrite(true);
        outputConfiguration.setDataSet(outDs);
        //
        final String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        Job.components() //
                .component("in", "Azure-DLS-Gen2://Input?" + inConfig) //
                .component("out", "Azure-DLS-Gen2://Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    public void fromAvroToCsv(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        dataSet.setFormat(FileFormat.AVRO);
        dataSet.setBlobPath("demo_gen2/in/customers.avro");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //
        AdlsGen2DataSet outDs = new AdlsGen2DataSet();
        outDs.setConnection(connection);
        outDs.setFilesystem(storageFs);
        outDs.setFormat(FileFormat.CSV);
        CsvConfiguration csvConfig = new CsvConfiguration();
        csvConfig.setFieldDelimiter(CsvFieldDelimiter.SEMICOLON);
        csvConfig.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfig.setCsvSchema("");
        csvConfig.setHeader(true);
        outDs.setCsvConfiguration(csvConfig);
        outDs.setBlobPath("demo_gen2/out/customers-from-avro-wo-header.csv");
        outputConfiguration.setOverwrite(true);
        outputConfiguration.setDataSet(outDs);
        final String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        //
        Job.components() //
                .component("in", "Azure-DLS-Gen2://Input?" + inConfig) //
                .component("out", "Azure-DLS-Gen2://Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        for (Record r : records) {
            assertNotNull(r);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    public void fromAvroToCsvWithHeader(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        dataSet.setFormat(FileFormat.AVRO);
        dataSet.setBlobPath("demo_gen2/in/customers.avro");
        inputConfiguration.setDataSet(dataSet);
        final String inConfig = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        //
        AdlsGen2DataSet outDs = new AdlsGen2DataSet();
        outDs.setConnection(connection);
        outDs.setFilesystem(storageFs);
        outDs.setFormat(FileFormat.CSV);
        CsvConfiguration csvConfig = new CsvConfiguration();
        csvConfig.setFieldDelimiter(CsvFieldDelimiter.SEMICOLON);
        csvConfig.setRecordSeparator(CsvRecordSeparator.LF);
        csvConfig.setCsvSchema("Zid;ZFirstname;ZLastname;ZAddress;ZRegistrationDate;ZRevenue;ZStates");
        csvConfig.setHeader(true);
        outDs.setCsvConfiguration(csvConfig);
        outDs.setBlobPath("demo_gen2/out/customers-from-avro-w-header.csv");
        outputConfiguration.setOverwrite(true);
        outputConfiguration.setDataSet(outDs);
        final String outConfig = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        //
        Job.components() //
                .component("in", "Azure-DLS-Gen2://Input?" + inConfig) //
                .component("out", "Azure-DLS-Gen2://Output?" + outConfig) //
                .connections() //
                .from("in") //
                .to("out") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        for (Record r : records) {
            log.warn("[fromAvroToCsv] {}", r);
        }
    }

}
