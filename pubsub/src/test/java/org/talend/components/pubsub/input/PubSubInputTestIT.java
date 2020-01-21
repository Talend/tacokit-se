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
package org.talend.components.pubsub.input;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.impl.StaticLoggerBinder;
import org.talend.components.pubsub.PubSubTestUtil;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.components.pubsub.datastore.PubSubDataStore;
import org.talend.components.pubsub.service.PubSubService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleFactory;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

@Slf4j
@Environment(ContextualEnvironment.class)
@EnvironmentConfiguration(environment = "Contextual", systemProperties = {})
@WithComponents(value = "org.talend.components.pubsub")
@Tag("IT")
public class PubSubInputTestIT {

    @Service
    protected PubSubService service;

    @Injected
    private BaseComponentsHandler componentsHandler;

    private PubSubInputConfiguration configuration;

    @BeforeEach
    void buildConfig() {

        final StaticLoggerBinder binder = StaticLoggerBinder.getSingleton();

        componentsHandler.injectServices(this);

        PubSubDataStore dataStore = PubSubTestUtil.getDataStore();

        PubSubDataSet dataset = new PubSubDataSet();
        dataset.setDataStore(dataStore);
        dataset.setTopic("rlecomteTopic");
        dataset.setSubscription("ITSub");

        configuration = new PubSubInputConfiguration();
        configuration.setDataSet(dataset);
        configuration.setConsumeMsg(true);

    }

    @EnvironmentalTest
    public void readMessageCSV() {
        configuration.getDataSet().setValueFormat(PubSubDataSet.ValueFormat.CSV);
        configuration.getDataSet().setFieldDelimiter(PubSubDataSet.CSVDelimiter.SEMICOLON);

        final String configStr = SimpleFactory.configurationByExample().forInstance(configuration).configured().toQueryString();

        Job.components().component("source", "PubSub://PubSubInput?" + configStr).component("target", "test://collector")
                .connections().from("source").to("target").build().property("streaming.maxRecords", 5)
                .property("streaming.maxDurationMs", 60_000).run();

        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertNotNull(records);
        Assertions.assertNotEquals(0, records.size(), "No record in result");
        log.info(records.toString());
    }

    @EnvironmentalTest
    public void readMessageAvro() throws InterruptedException {
        configuration.getDataSet().setValueFormat(PubSubDataSet.ValueFormat.AVRO);
        configuration.getDataSet().setAvroSchema(PubSubTestUtil.getAvroSchemaString());

        final String configStr = SimpleFactory.configurationByExample().forInstance(configuration).configured().toQueryString();

        final AtomicBoolean flag = new AtomicBoolean(false);

        new Thread() {

            @Override
            public void run() {
                Job.components().component("source", "PubSub://PubSubInput?" + configStr).component("target", "test://collector")
                        .connections().from("source").to("target").build().property("streaming.maxRecords", 5)
                        .property("streaming.maxDurationMs", 60_000).run();

                flag.set(true);
            }
        }.start();

        IntStream.range(0, 5).forEach(i -> PubSubTestUtil.sendAvroRecord(service, configuration.getDataSet().getTopic()));

        while (!flag.get()) {
            Thread.sleep(500);
        }

        List<Record> records = componentsHandler.getCollectedData(Record.class);
        Assertions.assertNotNull(records);
        Assertions.assertNotEquals(0, records.size(), "No record in result");
        log.info(records.toString());

    }

}
