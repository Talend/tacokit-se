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
package org.talend.components.rest.source;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.service.RecordBuilderService;
import org.talend.components.rest.service.RequestConfigBuilderTest;
import org.talend.components.rest.service.RestService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Environment(ContextualEnvironment.class)
@EnvironmentConfiguration(environment = "Contextual", systemProperties = {})
// EnvironmentConfiguration is necessary for each
// @Environment

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
public class PollingTest {

    @Injected
    private BaseComponentsHandler handler;

    @Service
    private RestService restService;

    @Service
    private RecordBuilderService recordBuilderService;

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
    void testResume() {
        config.getDataset().setResource("get");
        config.getDataset().setMethodType(HttpMethod.GET);

        final AtomicInteger index = new AtomicInteger(0);

        this.setServerContextAndStart(httpExchange -> {
            String params = httpExchange.getRequestURI().getQuery();

            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();

            os.write(String.valueOf(index.incrementAndGet()).getBytes());
            os.close();
        });

        RestEmitter input = new RestEmitter(config, restService, recordBuilderService);
        final Record first = input.next();
        final String body1 = first.getString("body");
        Assertions.assertEquals("1", body1);
        input.resume("Not used parameter");
        final Record second = input.next();
        final String body2 = second.getString("body");
        Assertions.assertEquals("2", body2);
    }

}
