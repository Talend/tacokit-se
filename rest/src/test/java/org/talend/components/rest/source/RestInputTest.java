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
package org.talend.components.rest.source;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.service.Client;
import org.talend.components.rest.service.ClientTestWithHttpbin;
import org.talend.components.rest.service.RequestConfigBuilder;
import org.talend.components.rest.service.RestService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReaderFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@WithComponents(value = "org.talend.components.rest")
class RestInputTest {

    @Service
    RestService service;

    @Service
    JsonReaderFactory jsonReaderFactory;

    @Service
    JsonBuilderFactory jsonBuilderFactory;

    @Injected
    private BaseComponentsHandler handler;

    private RequestConfig config;

    @Rule
    public final SimpleComponentRule components = new SimpleComponentRule("org.talend.sdk.component.mycomponent");

    private HttpServer server;

    private int port;

    @BeforeEach
    void buildConfig() throws IOException {
        // Inject needed services
        handler.injectServices(this);

        Client client = service.getClient();

        config = RequestConfigBuilder.getEmptyRequestConfig();

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

    @Test
    void testInput() throws MalformedURLException {

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
                .component("emitter", "Rest://Input?" + configStr) //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        final List<Record> records = components.getCollectedData(Record.class);

        assertEquals(1, records.size());
        assertEquals("param1=param1_value&param2=param1_value2", parameters.toString());
    }

    /*
     * @Test
     * void testOutput() {
     * final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
     * 
     * components.setInputData(createData(10));
     * 
     * Job.components() //
     * .component("emitter", "test://emitter") //
     * .component("out", "Rest://Output?" + configStr) //
     * .connections() //
     * .from("emitter") //
     * .to("out") //
     * .build() //
     * .run();
     * 
     * }
     * 
     * private Collection<Record> createData(int n) {
     * RecordBuilderFactory factory = components.findService(RecordBuilderFactory.class);
     * 
     * Collection<Record> records = new ArrayList<>();
     * for (int i = 0; i < n; i++) {
     * records.add(factory.newRecordBuilder().withString("last_name", "last_name_value" + i)
     * .withString("first_name", "first_name_value" + i)
     * .withInt("age", 10 + 1)
     * .withBoolean("male", (i % 2 == 0))
     * .withDateTime("birthdate", Date.from(LocalDate.of(1980+i, Month.JANUARY, i+1).atStartOfDay().toInstant(ZoneOffset.UTC)))
     * .withDouble("size", 1.785)
     * .build());
     * }
     * 
     * return records;
     * 
     * }
     */

}