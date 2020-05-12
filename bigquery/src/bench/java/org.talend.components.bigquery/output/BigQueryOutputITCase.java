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
package org.talend.components.bigquery.output;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.bigquery.BigQueryTestUtil;
import org.talend.components.bigquery.dataset.TableDataSet;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration;
import org.talend.sdk.component.junit.environment.Environments;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents(value = "org.talend.components.bigquery")
public class BigQueryOutputITCase {

    @Service
    public RecordBuilderFactory rbf;

    @Injected
    private BaseComponentsHandler handler;

    @Rule
    public final SimpleComponentRule COMPONENTS = new SimpleComponentRule("org.talend.sdk.component.mycomponent");

    @BeforeEach
    void buildConfig() throws IOException {
        // Inject needed services
        handler.injectServices(this);

    }

    @Test
    public void fillData() {
        BigQueryConnection connection = BigQueryTestUtil.getConnection();

        TableDataSet dataset = new TableDataSet();
        dataset.setConnection(connection);
        dataset.setBqDataset("dataset_rlecomte");
        dataset.setTableName("TableWithData");

        BigQueryOutputConfig config = new BigQueryOutputConfig();
        config.setDataSet(dataset);

        String configURI = configurationByExample().forInstance(config).configured().toQueryString();

        final int nbrecords = 1_000;

        Iterable<Record> inputData = new RandomDataGenerator(nbrecords, rbf);

        COMPONENTS.setInputData(inputData);

        long start = System.currentTimeMillis();

        Job.components().component("source", "test://emitter").component("output", "BigQuery://BigQueryOutput?" + configURI)
                .connections().from("source").to("output").build().run();

        long end = System.currentTimeMillis();

        System.out.println(nbrecords + " in " + (end - start) + " ms");

    }

    @Test
    public void run() {

        String jsonCredentials = "";
        try (FileInputStream in = new FileInputStream("C:\\Users\\rlecomte\\Documents\\Engineering-4e7ac6cf93f4.json");
                BufferedInputStream bIn = new BufferedInputStream(in)) {
            byte[] buffer = new byte[1024];
            int read = 0;
            while ((read = bIn.read(buffer)) > 0) {
                jsonCredentials += new String(buffer, 0, read, StandardCharsets.UTF_8);
            }
            jsonCredentials = jsonCredentials.replace("\n", " ").trim();
        } catch (IOException ioe) {
            Assertions.fail(ioe);
        }

        BigQueryConnection connection = new BigQueryConnection();
        connection.setProjectName("engineering-152721");
        connection.setJsonCredentials(jsonCredentials);

        TableDataSet dataset = new TableDataSet();
        dataset.setConnection(connection);
        dataset.setBqDataset("dataset_rlecomte");
        dataset.setTableName("OutputTest");

        BigQueryOutputConfig config = new BigQueryOutputConfig();
        config.setDataSet(dataset);
        config.setTableOperation(BigQueryOutputConfig.TableOperation.CREATE_IF_NOT_EXISTS);
        String schema = "[\r\n{\"name\":\"k\", \"type\":\"STRING\"},\r\n{\"name\":\"v\", \"type\":\"STRING\"}\r\n]";
        System.out.println(schema);
        config.setTableSchemaFields(schema);

        String configURI = configurationByExample().forInstance(config).configured().toQueryString();
        System.out.println(configURI);

        List<Record> inputData = new ArrayList<>();
        inputData.add(rbf.newRecordBuilder().withString("k", "entry1").withString("v", "value1").build());
        inputData.add(rbf.newRecordBuilder().withString("k", "entry2").withString("v", "value2").build());

        COMPONENTS.setInputData(inputData);

        Job.components().component("source", "test://emitter").component("output", "BigQuery://BigQueryOutput?" + configURI)
                .connections().from("source").to("output").build().run();

    }
}
