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
package org.talend.components.rest.source;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.service.RequestConfigBuilderTest;
import org.talend.components.rest.service.client.ContentType;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration.Property;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@Environment(ContextualEnvironment.class)
@EnvironmentConfiguration(environment = "Contextual", systemProperties = {}) // EnvironmentConfiguration is necessary for each
                                                                             // @Environment
@Environment(SparkRunnerEnvironment.class)
@EnvironmentConfiguration(environment = "Spark", systemProperties = {
        @Property(key = "talend.beam.job.runner", value = "org.apache.beam.runners.spark.SparkRunner"),
        @Property(key = "talend.beam.job.filesToStage", value = ""), @Property(key = "spark.ui.enabled", value = "false") })

@WithComponents(value = "org.talend.components.rest")
class RestEmitterTest {

    @Injected
    private BaseComponentsHandler handler;

    private RequestConfig config;

    private HttpServer server;

    private int port;

    @BeforeEach
    void buildConfig() throws IOException {
        // Inject needed services
        handler.injectServices(this);

        config = RequestConfigBuilderTest.getEmptyRequestConfig();

        config.getDataset().getDatastore().setConnectionTimeout(5000);
        config.getDataset().getDatastore().setReadTimeout(5000);

        // start server
        server = HttpServer.create(new InetSocketAddress(0), 0);
        port = server.getAddress().getPort();

        config.getDataset().getDatastore().setBase("http://localhost:" + port);
    }

    @AfterEach
    void after() {
        // stop server
        server.stop(0);
    }

    private void setServerContextAndStart(HttpHandler handler) {
        server.createContext("/", handler);
        server.start();
    }

    @EnvironmentalTest
    void testInput() {

        config.getDataset().setResource("get");
        config.getDataset().setMethodType(HttpMethod.GET);

        config.getDataset().setHasQueryParams(true);
        config.getDataset()
                .setQueryParams(Arrays.asList(new Param("param1", "param1_value"), new Param("param2", "param1_value2")));

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();

        final StringBuilder parameters = new StringBuilder();
        this.setServerContextAndStart(httpExchange -> {
            String params = httpExchange.getRequestURI().getQuery();

            parameters.append(params);

            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

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
        assertEquals("param1=param1_value&param2=param1_value2", parameters.toString());
    }

    @EnvironmentalTest
    void testOptionsPathFlagsActivated() throws IOException {
        testOptionsPathFlags(true);
    }

    @EnvironmentalTest
    void testOptionsPathFlagsDeactivated() throws IOException {
        testOptionsPathFlags(false);
    }

    private void testOptionsPathFlags(final boolean hasOptions) {
        config.getDataset().setMethodType(HttpMethod.POST);
        config.getDataset().setResource("post/{module}/{id}");

        // Can't be tested the same way has other option since
        // when path param are not substituted the url contains "{...}"
        // and a 400 error is returned
        config.getDataset().setHasPathParams(hasOptions);
        List<Param> pathParams = Arrays.asList(new Param[] { new Param("module", "myModule"), new Param("id", "myId") });
        config.getDataset().setPathParams(pathParams);

        this.setServerContextAndStart(httpExchange -> {

            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

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

        if (hasOptions) {
            assertEquals(200, records.get(0).getInt("status"));
        } else {
            assertEquals(400, records.get(0).getInt("status"));
        }
    }

    @EnvironmentalTest
    void testQueryContentType() {
        List<ContentTypeParams> params = new ArrayList<>();
        params.add(new ContentTypeParams(RequestBody.Type.JSON, "{\"key\" : \"value\"}", RequestBody.Type.JSON.getContentType()));

        for (ContentTypeParams p : params) {
            _testQueryContentType(p.getType(), p.getContent(), p.getContentType());
        }
    }

    private void _testQueryContentType(final RequestBody.Type type, final String body, final String contentType) {
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("get");
        config.getDataset().setHasBody(true);

        final RequestBody requestBody = new RequestBody();
        requestBody.setType(RequestBody.Type.JSON);
        requestBody.setJsonValue("{\"key\" : \"value\"}");
        config.getDataset().setBody(requestBody);

        final StringBuilder receivedContentType = new StringBuilder();

        this.setServerContextAndStart(httpExchange -> {
            receivedContentType.append(httpExchange.getRequestHeaders().getFirst(ContentType.HEADER_KEY));

            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

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
        assertEquals(type.getContentType(), receivedContentType.toString());
    }

    @Data
    private static class ContentTypeParams {

        final RequestBody.Type type;

        final String content;

        final String contentType;
    }

}
