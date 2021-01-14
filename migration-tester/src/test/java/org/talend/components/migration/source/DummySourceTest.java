/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.migration.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.talend.components.migration.conf.DSE;
import org.talend.components.migration.conf.DSO;
import org.talend.components.migration.conf.SourceConfig;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.manager.chain.Job;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Environment(ContextualEnvironment.class)
@EnvironmentConfiguration(environment = "Contextual", systemProperties = {})
// EnvironmentConfiguration is necessary for each
// @Environment
@Environment(SparkRunnerEnvironment.class)
@EnvironmentConfiguration(environment = "Spark", systemProperties = {
        @EnvironmentConfiguration.Property(key = "talend.beam.job.runner", value = "org.apache.beam.runners.spark.SparkRunner"),
        @EnvironmentConfiguration.Property(key = "talend.beam.job.filesToStage", value = ""),
        @EnvironmentConfiguration.Property(key = "spark.ui.enabled", value = "false") })

@WithComponents(value = "org.talend.components.migration")
class DummySourceTest {

    @Injected
    private BaseComponentsHandler handler;

    private SourceConfig config;

    @BeforeEach
    void buildConfig() throws IOException {
        // Inject needed services
        handler.injectServices(this);

        config = new SourceConfig();
        DSO dso = new DSO();
        dso.setDso_legacy("Datastore legacy");
        DSE dse = new DSE();
        dse.setDse_legacy("Dataset legacy");

        dse.setDso(dso);
        config.setDse(dse);

        config.setSource_legacy("Legacy data");
    }

    @EnvironmentalTest
    void testInput() {

        config.getDse().getDso().setDso_shouldNotBeEmpty("set !");

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components() //
                .component("emitter", "Tester://dummySource?" + configStr) //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        final List<SourceConfig> records = handler.getCollectedData(SourceConfig.class);
        assertEquals(1, records.size());
    }

    @EnvironmentalTest
    void testInputKo() {

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();

        try {
            Job.components() //
                    .component("emitter", "Tester://dummySource?" + configStr) //
                    .component("out", "test://collector") //
                    .connections() //
                    .from("emitter") //
                    .to("out") //
                    .build() //
                    .run();

            final List<SourceConfig> records = handler.getCollectedData(SourceConfig.class);
            fail("Pipeline should not run since dso_shouldNotBeEmpty is not set.");
        } catch (Exception e) {
            assertEquals("- Property 'configuration.dse.dso.dso_shouldNotBeEmpty' is required.", e.getMessage());
        }

    }

}