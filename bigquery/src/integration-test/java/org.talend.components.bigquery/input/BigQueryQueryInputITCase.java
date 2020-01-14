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

import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.bigquery.BigQueryTestUtil;
import org.talend.components.bigquery.dataset.QueryDataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleCollector;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.io.IOException;
import java.util.List;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents(value = "org.talend.components.bigquery")
@Slf4j
public class BigQueryQueryInputITCase {

    @Injected
    private BaseComponentsHandler handler;

    @Rule
    public final SimpleComponentRule COMPONENTS = new SimpleComponentRule("org.talend.sdk.component.junit");

    @BeforeEach
    void buildConfig() throws IOException {
        // Inject needed services
        handler.injectServices(this);
    }

    @Test
    public void justTest() {
        QueryDataSet dataset = new QueryDataSet();
        dataset.setConnection(BigQueryTestUtil.getConnection());
        dataset.setUseLegacySql(true);
        dataset.setQuery("SELECT COUNT(ID) FROM dataset_rlecomte.TableWithData WHERE IS_TRUE = true");

        BigQueryQueryInputConfig config = new BigQueryQueryInputConfig();
        config.setQueryDataset(dataset);

        String configURI = configurationByExample().forInstance(config).configured().toQueryString();

        try {
            Job.components()
                    .component("input", "BigQuery://BigQueryQueryInput?" + configURI)
                    .component("output", "test://collector")
                    .connections()
                    .from("input")
                    .to("output")
                    .build()
                    .run();
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail(e);
        }

        List<Record> records = COMPONENTS.getCollectedData(Record.class);

        Assertions.assertNotNull(records);
        Assertions.assertNotEquals(0, records.size());
    }
}
