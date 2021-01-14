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
package org.talend.components.rest.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.junit.jupiter.api.BeforeEach;
import org.talend.components.rest.configuration.Format;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.configuration.auth.Authentication;
import org.talend.components.rest.configuration.auth.Authorization;
import org.talend.components.rest.configuration.auth.Basic;
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

import lombok.extern.slf4j.Slf4j;

/*
 * When -Dtalend.junit.http.capture=true is given
 * use -Dorg.talend.components.common.service.http.digest.authorization_header=_Authorization_
 * to change the name of the header, if not, tck proxy will exclude this header.
 */
@Slf4j

@Environment(ContextualEnvironment.class)
@EnvironmentConfiguration(environment = "Contextual", systemProperties = {})

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

    @Service
    RecordBuilderService recordBuilderService;

    private RequestConfig config;

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
        config.getDataset().getDatastore().setAuthentication(auth);

        config.getDataset().getDatastore().setBase("https://postman-echo.com");
        config.getDataset().setResource("digest-auth");
        config.getDataset().getDatastore().setAuthentication(auth);
        config.getDataset().setMethodType(HttpMethod.GET);

        Iterator<Record> resp = recordBuilderService.buildFixedRecord(service.execute(config), config);
        assertEquals(200, resp.next().getInt("status"));
    }

    @EnvironmentalTest
    void testFactsCompletePayload() {
        config.getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("facts");
        config.getDataset().setCompletePayload(true);
        config.getDataset().setFormat(Format.JSON);

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

        assertEquals("000001", record.getRecord("body").getRecord("one_element").getString("_id"));
    }

    @EnvironmentalTest
    void testFactsRootCompletePayload() {
        config.getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("facts");
        config.getDataset().setCompletePayload(true);
        config.getDataset().setFormat(Format.JSON);

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
        config.getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("facts");
        config.getDataset().setCompletePayload(false);
        config.getDataset().setFormat(Format.JSON);

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
        config.getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("facts");
        config.getDataset().setCompletePayload(false);
        config.getDataset().setFormat(Format.JSON);

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

        final List<Record> all = (List<Record>) records.get(0).getArray(Record.class, "all");
        assertEquals(4, all.size());

        Record record = all.get(0);
        assertEquals("000001", record.getString("_id"));
        assertEquals("First fact.", record.getString("text"));
        assertEquals("fact", record.getString("type"));
        assertEquals(6.0d, record.getDouble("upvotes"));

        record = all.get(3);
        assertEquals("64654654", record.getString("_id"));
        assertEquals("Last fact.", record.getString("text"));
        assertEquals("fact", record.getString("type"));
        assertEquals(5.0d, record.getDouble("upvotes"));
    }

    @EnvironmentalTest
    void testFactsWithTRACE() {
        config.getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().setMethodType(HttpMethod.TRACE);
        config.getDataset().setResource("facts");
        config.getDataset().setCompletePayload(false);
        config.getDataset().setFormat(Format.JSON);

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

        final List<Record> all = (List<Record>) records.get(0).getArray(Record.class, "all");
        assertEquals(4, all.size());

        Record record = all.get(0);
        assertEquals("000001", record.getString("_id"));
        assertEquals("First fact.", record.getString("text"));
        assertEquals("fact", record.getString("type"));
        assertEquals(6.0d, record.getDouble("upvotes"));

        record = all.get(3);
        assertEquals("64654654", record.getString("_id"));
        assertEquals("Last fact.", record.getString("text"));
        assertEquals("fact", record.getString("type"));
        assertEquals(5.0d, record.getDouble("upvotes"));
    }

    // @EnvironmentalTest
    void jsonWithError() {
        final Locale aDefault = Locale.getDefault();
        Locale.setDefault(Locale.ENGLISH);
        config.getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("jsonWithError");
        config.getDataset().setCompletePayload(false);
        config.getDataset().setFormat(Format.JSON);

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
        try {
            Job.components() //
                    .component("emitter", "REST://Input?" + configStr) //
                    .component("out", "test://collector") //
                    .connections() //
                    .from("emitter") //
                    .to("out") // The body's answer can't be read as JSON.
                    .build() //
                    .run();
            fail("JSON file has error and shouldn't be parsed.");
        } catch (IllegalArgumentException e) {
            assertEquals("The body's answer can't be read as JSON.",
                    (e instanceof IllegalArgumentException) ? e.getMessage() : e.getCause().getCause().getMessage());
        }
        Locale.setDefault(aDefault);
    }

    @EnvironmentalTest
    void testPlainText() {
        config.getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("facts");
        config.getDataset().setCompletePayload(false);

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
        config.getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("facts");
        config.getDataset().setCompletePayload(true);

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
        config.getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("facts");
        config.getDataset().setCompletePayload(false);

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
        assertEquals(Schema.Type.ARRAY, headers.getType());
        assertEquals(Schema.Type.RECORD, headers.getElementSchema().getType());
        assertEquals("key", headers.getElementSchema().getEntries().get(0).getName());
        assertEquals("value", headers.getElementSchema().getEntries().get(1).getName());

        Schema.Entry status = record.getSchema().getEntries().get(0);
        assertEquals("status", status.getName());
        assertEquals(Schema.Type.INT, status.getType());
    }

    @EnvironmentalTest
    void testJSONArray() {
        config.getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("facts");
        config.getDataset().setCompletePayload(false);
        config.getDataset().setFormat(Format.JSON);

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

    // A nested array of records
    @EnvironmentalTest
    void testJSONNestedArray() {
        config.getDataset().getDatastore().setBase("https://fakefacts.com/");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("facts");
        config.getDataset().setCompletePayload(false);
        config.getDataset().setFormat(Format.JSON);

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
        assertEquals("Durance", record.getString("name"));
        assertEquals("4, allée d'Orléans 4400 NANTES", record.getString("adresse"));
        final Collection<Record> books = record.getArray(Record.class, "books");
        assertEquals(3, books.size());

        final Record civilization = books.stream().findFirst().get();
        assertEquals("12345", civilization.getString("isbn"));
        assertEquals("Civilizations", civilization.getString("title"));
        assertEquals("Laurent Binet", civilization.getString("author"));
    }

}
