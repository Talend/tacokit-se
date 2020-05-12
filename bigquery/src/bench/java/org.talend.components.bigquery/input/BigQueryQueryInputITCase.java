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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.talend.components.bigquery.BigQueryTestUtil;
import org.talend.components.bigquery.dataset.QueryDataSet;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.beam.TalendIO;
import org.talend.sdk.component.runtime.input.Mapper;

import java.io.IOException;

@Environment(SparkRunnerEnvironment.class)
@WithComponents(value = "org.talend.components.bigquery")
@Slf4j
public class BigQueryQueryInputITCase {

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
    public void justTest() {
        QueryDataSet dataset = new QueryDataSet();
        dataset.setConnection(BigQueryTestUtil.getConnection());
        dataset.setUseLegacySql(true);
        dataset.setQuery("SELECT COUNT(ID) FROM dataset_rlecomte.TableWithData WHERE IS_TRUE = true");

        BigQueryQueryInputConfig config = new BigQueryQueryInputConfig();
        config.setQueryDataset(dataset);

        BigQueryTableExtractInputITCase.Counter counter = new BigQueryTableExtractInputITCase.Counter();

        BigQueryTableExtractInputITCase.Counter.reset();

        long start = System.currentTimeMillis();
        Mapper mapper = handler.createMapper(BigQueryQueryInput.class, config);
        Pipeline.create(
                PipelineOptionsFactory.fromArgs("--runner=org.apache.beam.runners.spark.SparkRunner", "--filesToStage=").create())
                .apply(TalendIO.read(mapper)).apply(ParDo.of(counter)).getPipeline().run().waitUntilFinish();
        long end = System.currentTimeMillis();

        Assertions.assertNotEquals(0, counter.getCounter());
        log.info(counter.getCounter() + " in " + (end - start) + "ms");
    }
}
