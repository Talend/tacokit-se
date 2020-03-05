/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.rest.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.auth.Authentication;
import org.talend.components.rest.configuration.auth.Authorization;
import org.talend.components.rest.configuration.auth.Basic;
import org.talend.components.rest.virtual.ComplexRestConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

/*
 * When -Dtalend.junit.http.capture=true is given
 * use -Dorg.talend.components.common.service.http.digest.authorization_header=_Authorization_
 * to change the name of the header, if not, tck proxy will exclude this header.
 */
@Slf4j

@Environment(ContextualEnvironment.class)
@EnvironmentConfiguration(environment = "Contextual", systemProperties = {}) // EnvironmentConfiguration is necessary for each

/*
 * @Environment(DirectRunnerEnvironment.class) // Direct runner not necessary since already SparkRunner
 *
 * @EnvironmentConfiguration(environment = "Direct", systemProperties = {
 *
 * @EnvironmentConfiguration.Property(key = "talend.beam.job.runner", value = "org.apache.beam.runners.direct.DirectRunner")
 * })
 */

@Environment(SparkRunnerEnvironment.class)
@EnvironmentConfiguration(environment = "Spark", systemProperties = {
        @EnvironmentConfiguration.Property(key = "talend.beam.job.runner", value = "org.apache.beam.runners.spark.SparkRunner"),
        @EnvironmentConfiguration.Property(key = "talend.beam.job.filesToStage", value = ""),
        @EnvironmentConfiguration.Property(key = "spark.ui.enabled", value = "false") })

@WithComponents(value = "org.talend.components.rest")
@HttpApi(useSsl = true)
public class ClientTestWithMockProxyTest {

    @Injected
    private BaseComponentsHandler handler;

    @Service
    RestService service;

    private ComplexRestConfiguration config;

    @BeforeEach
    void before() {
        config = RequestConfigBuilderTest.getEmptyRequestConfig();
    }

    // @EnvironmentalTest
    public void testDigestAuthWithQopPostMan() {

        String user = "postman";
        String pwd = "password";

        Basic basic = new Basic();
        basic.setUsername(user);
        basic.setPassword(pwd);

        Authentication auth = new Authentication();
        auth.setType(Authorization.AuthorizationType.Digest);
        auth.setBasic(basic);
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setAuthentication(auth);

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("https://postman-echo.com");
        config.getDataset().getRestConfiguration().getDataset().setResource("digest-auth");
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setAuthentication(auth);
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);

        CompletePayload resp = service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration()));
        assertEquals(200, resp.getStatus());
    }

    @EnvironmentalTest
    void testFactsCompletePayload() {
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("facts");
        config.getDataset().getRestConfiguration().getDataset().setCompletePayload(true);
        config.getDataset().getJSonExtractorConfiguration().setPointer("/one_element");

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components() //
                .component("emitter", "REST://Input?" + configStr) //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        final List<Record> records = handler.getCollectedData(Record.class);

        assertEquals(1, records.size());
        Record record = records.get(0);
        valideCompletePayloadSchema(record, Schema.Type.RECORD);
    }

    @EnvironmentalTest
    void testFactsRootCompletePayload() {
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("facts");
        config.getDataset().getRestConfiguration().getDataset().setCompletePayload(true);
        config.getDataset().getJSonExtractorConfiguration().setPointer("");

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components() //
                .component("emitter", "REST://Input?" + configStr) //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        final List<Record> records = handler.getCollectedData(Record.class);

        assertEquals(1, records.size());
        Record record = records.get(0);
        valideCompletePayloadSchema(record, Schema.Type.RECORD);
    }

    @EnvironmentalTest
    void testFactsRoot() {
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("facts");
        config.getDataset().getRestConfiguration().getDataset().setCompletePayload(false);
        config.getDataset().getJSonExtractorConfiguration().setPointer("");

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components() //
                .component("emitter", "REST://Input?" + configStr) //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        final List<Record> records = handler.getCollectedData(Record.class);
        assertEquals(1, records.size());
        assertEquals(1, records.get(0).getSchema().getEntries().size());
        assertEquals(Schema.Type.ARRAY, records.get(0).getSchema().getEntries().get(0).getType());
        assertEquals("all", records.get(0).getSchema().getEntries().get(0).getName());
        assertEquals("000001", ((ArrayList<Record>) records.get(0).getArray(Record.class, "all")).get(0).getString("_id"));
    }

    @EnvironmentalTest
    void testFacts() {
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("facts");
        config.getDataset().getRestConfiguration().getDataset().setCompletePayload(false);
        config.getDataset().getJSonExtractorConfiguration().setPointer("/all");

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components() //
                .component("emitter", "REST://Input?" + configStr) //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        final List<Record> records = handler.getCollectedData(Record.class);
        assertEquals(4, records.size());

        Record record = records.get(0);
        assertEquals("000001", record.getString("_id"));
        assertEquals("First fact.", record.getString("text"));
        assertEquals("fact", record.getString("type"));
        assertEquals(6.0d, record.getDouble("upvotes"));

        record = records.get(3);
        assertEquals("64654654", record.getString("_id"));
        assertEquals("Last fact.", record.getString("text"));
        assertEquals("fact", record.getString("type"));
        assertEquals(5.0d, record.getDouble("upvotes"));
    }

    @EnvironmentalTest
    void testFactsWithTRACE() {
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.TRACE);
        config.getDataset().getRestConfiguration().getDataset().setResource("facts");
        config.getDataset().getRestConfiguration().getDataset().setCompletePayload(false);
        config.getDataset().getJSonExtractorConfiguration().setPointer("/all");

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components() //
                .component("emitter", "REST://Input?" + configStr) //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        final List<Record> records = handler.getCollectedData(Record.class);
        assertEquals(4, records.size());

        Record record = records.get(0);
        assertEquals("000001", record.getString("_id"));
        assertEquals("First fact.", record.getString("text"));
        assertEquals("fact", record.getString("type"));
        assertEquals(6.0d, record.getDouble("upvotes"));

        record = records.get(3);
        assertEquals("64654654", record.getString("_id"));
        assertEquals("Last fact.", record.getString("text"));
        assertEquals("fact", record.getString("type"));
        assertEquals(5.0d, record.getDouble("upvotes"));
    }

    @EnvironmentalTest
    void testPlainText() {
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("facts");
        config.getDataset().getRestConfiguration().getDataset().setCompletePayload(false);
        config.getDataset().getJSonExtractorConfiguration().setPointer("/all");

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components() //
                .component("emitter", "REST://Input?" + configStr) //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        final List<Record> records = handler.getCollectedData(Record.class);
        assertEquals(1, records.size());

        Record record = records.get(0);
        assertEquals("This is the plain text body.", record.getString("body"));

        assertEquals(1, record.getSchema().getEntries().size());
        assertEquals("body", record.getSchema().getEntries().get(0).getName());
        assertEquals(Schema.Type.STRING, record.getSchema().getEntries().get(0).getType());

    }

    @EnvironmentalTest
    void testPlainTextCompletePayload() {
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("facts");
        config.getDataset().getRestConfiguration().getDataset().setCompletePayload(true);
        config.getDataset().getJSonExtractorConfiguration().setPointer("");

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components() //
                .component("emitter", "REST://Input?" + configStr) //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        final List<Record> records = handler.getCollectedData(Record.class);
        assertEquals(1, records.size());

        Record record = records.get(0);
        assertEquals("This is the plain text body.", record.getString("body"));

        valideCompletePayloadSchema(record, Schema.Type.STRING);
    }

    @EnvironmentalTest
    void testXML() {
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("facts");
        config.getDataset().getRestConfiguration().getDataset().setCompletePayload(false);
        config.getDataset().getJSonExtractorConfiguration().setPointer("/all");

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components() //
                .component("emitter", "REST://Input?" + configStr) //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        final List<Record> records = handler.getCollectedData(Record.class);
        assertEquals(1, records.size());

        Record record = records.get(0);
        assertEquals("<xml><books><book id=\"1\"><title>Romeo and Juliet</title><isbn>21321321</isbn></book></books></xml>",
                record.getString("body"));
    }

    private void valideCompletePayloadSchema(final Record record, final Schema.Type bodyType) {
        assertEquals(3, record.getSchema().getEntries().size());

        Schema.Entry body = record.getSchema().getEntries().get(2);
        assertEquals("body", body.getName());
        assertEquals(bodyType, body.getType());

        Schema.Entry headers = record.getSchema().getEntries().get(1);
        assertEquals("headers", headers.getName());
        assertEquals(Schema.Type.RECORD, headers.getType());

        Schema.Entry status = record.getSchema().getEntries().get(0);
        assertEquals("status", status.getName());
        assertEquals(Schema.Type.DOUBLE, status.getType());
    }

    @EnvironmentalTest
    void testJSONArray() {
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("facts");
        config.getDataset().getRestConfiguration().getDataset().setCompletePayload(false);

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components() //
                .component("emitter", "REST://Input?" + configStr) //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        final List<Record> records = handler.getCollectedData(Record.class);
        assertEquals(7, records.size());

        Record record = records.get(0);
        assertEquals("red", record.getString("color"));
        assertEquals("#f00", record.getString("value"));

        record = records.get(6);
        assertEquals("black", record.getString("color"));
        assertEquals("#000", record.getString("value"));
    }

}
