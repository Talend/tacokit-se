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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.bigquery.BigQueryTestUtil;
import org.talend.components.bigquery.dataset.TableDataSet;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.Environments;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.io.IOException;
import java.util.List;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Environments({ @Environment(SparkRunnerEnvironment.class) })
@WithComponents("org.talend.components.bigquery")
public class BigQueryTableExtractInputITCase {

    @Injected
    private BaseComponentsHandler handler;

    @Rule
    public final SimpleComponentRule COMPONENTS = new SimpleComponentRule("org.talend.sdk.component.mycomponent");

    @BeforeEach
    void buildConfig() throws IOException {
        // Inject needed services
        handler.injectServices(this);

    }

    @EnvironmentalTest
    public void run() {

        COMPONENTS.resetState();

        BigQueryConnection connection = BigQueryTestUtil.getConnection();

        TableDataSet dataset = new TableDataSet();
        dataset.setConnection(connection);
        dataset.setBqDataset("dataset_rlecomte");
        dataset.setTableName("TableWithData");
        dataset.setGsBucket("tdi_rlecomte");


        BigQueryTableExtractInputConfig config = new BigQueryTableExtractInputConfig();
        config.setTableDataset(dataset);

        String configURI = configurationByExample().forInstance(config).configured().toQueryString();


        Job.components()
                .component("input", "BigQuery://BigQueryTableExtractInput?" + configURI)
                .component("output", "test://collector")
                .connections()
                .from("input")
                .to("output")
                .build()
                .run();

        List<Record> records = COMPONENTS.getCollectedData(Record.class);

        Assertions.assertNotNull(records);
        Assertions.assertNotEquals(0, records.size());

    }
}
