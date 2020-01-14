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
package org.talend.components.bigquery.input;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.bigquery.dataset.QueryDataSet;
import org.talend.components.bigquery.dataset.TableDataSet;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.IntStream;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Environment(SparkRunnerEnvironment.class)
@WithComponents(value = "org.talend.components.bigquery")
public class BigQueryTableInputITCase {

    @Rule
    public final SimpleComponentRule COMPONENTS = new SimpleComponentRule("org.talend.components.bigquery");

    // @EnvironmentalTest
    public void justLoop() {
        OptionalDouble avg = IntStream.range(0, 1).mapToLong(i -> {
            try {
                return run();
            } catch (Exception e) {
                e.printStackTrace();
                ;
                return 0l;
            }
        }).average();

        avg.ifPresent(System.out::println);
    }

    // @Test
    public long run() {

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
        dataset.setTableName("TableWithData");

        BigQueryTableInputConfig config = new BigQueryTableInputConfig();
        config.setTableDataset(dataset);

        String configURI = configurationByExample().forInstance(config).configured().toQueryString();
        // System.out.println(configURI);

        long start = System.currentTimeMillis();

        Job.components().component("source", "BigQuery://BigQueryTableInput?" + configURI).component("output", "test://collector")
                .connections().from("source").to("output").build().run();

        List<Record> records = COMPONENTS.getCollectedData(Record.class);

        long end = System.currentTimeMillis();

        // System.out.println(records);

        // Assertions.assertNotNull(records);
        // System.out.println(records.size() + " in " + (end - start) + "ms");
        // Assertions.assertNotEquals(0, records.size());

        return end - start;

    }
}
