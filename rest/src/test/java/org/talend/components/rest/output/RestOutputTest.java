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
package org.talend.components.rest.output;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.service.RequestConfigBuilder;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@WithComponents(value = "org.talend.components.rest")
class RestOutputTest {

    private final static int NB_RECORDS = 10;

    @Rule
    public final SimpleComponentRule components = new SimpleComponentRule("org.talend.sdk.component.mycomponent");

    private HttpServer server;

    private int port;

    @Injected
    private BaseComponentsHandler handler;

    private RequestConfig config;

    @BeforeEach
    void buildConfig() throws IOException {
        // Inject needed services
        handler.injectServices(this);

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
    void testOutput() throws IOException {
        config.getDataset().setMethodType(HttpMethod.POST);
        config.getDataset().setResource("post/{module}/{id}");

        config.getDataset().setHasPathParams(true);
        List<Param> pathParams = Arrays.asList(new Param[] { new Param("module", "{/module}"), new Param("id", "{/id}") });
        config.getDataset().setPathParams(pathParams);

        config.getDataset().setHasHeaders(true);
        List<Param> headers = Arrays.asList(new Param[] { new Param("head_1", "header/{/id}"),
                new Param("head_2", "page:{/pagination/page} on {/pagination/total}") });
        config.getDataset().setHeaders(headers);

        config.getDataset().setHasQueryParams(true);
        List<Param> params = Arrays
                .asList(new Param[] { new Param("param_1", "param{/id}&/encoded < >"), new Param("param_2", "{/user_name}") });
        config.getDataset().setQueryParams(params);

        config.getDataset().setHasBody(true);
        Path resourceDirectory = Paths.get("src/test/resources/org/talend/components/rest/body/BodyWithParams.json");
        final String content = Files.lines(resourceDirectory).collect(Collectors.joining("\n"));
        config.getDataset().getBody().setType(RequestBody.Type.JSON);
        config.getDataset().getBody().setJsonValue(content);

        final List<Record> data = createData(NB_RECORDS);
        final AtomicInteger index = new AtomicInteger(0);

        this.setServerContextAndStart(httpExchange -> {
            int i = index.getAndIncrement();

            // Check URL path
            StringBuilder uri = new StringBuilder("/post/");
            uri.append(data.get(i).getString("module")).append("/").append(data.get(i).getInt("id"));
            Assertions.assertEquals(uri.toString(), httpExchange.getRequestURI().getPath());

            // Check query headers
            StringBuilder header_1 = new StringBuilder("header/");
            header_1.append(data.get(i).getInt("id"));
            Assertions.assertEquals(header_1.toString(), Optional.ofNullable(httpExchange.getRequestHeaders().get("head_1"))
                    .orElse(Collections.emptyList()).stream().findFirst().orElse(""));

            StringBuilder header_2 = new StringBuilder("page:");
            header_2.append(data.get(i).getRecord("pagination").getInt("page")).append(" on ")
                    .append(data.get(i).getRecord("pagination").getInt("total"));
            Assertions.assertEquals(header_2.toString(), Optional.ofNullable(httpExchange.getRequestHeaders().get("head_2"))
                    .orElse(Collections.emptyList()).stream().findFirst().orElse(""));

            // Check query parameters
            String requestUri = httpExchange.getRequestURI().toASCIIString();
            String[] queryParamsAsArray = requestUri.substring(requestUri.indexOf('?') + 1).split("&");
            Map<String, String> queryParams = Arrays.stream(queryParamsAsArray)
                    .collect(Collectors.toMap(s -> s.split("=")[0], s -> s.split("=")[1]));

            Assertions.assertEquals("param" + i + "&/encoded < >", URLDecoder.decode(queryParams.get("param_1"), "UTF-8"));
            Assertions.assertEquals(data.get(i).getString("user_name"), URLDecoder.decode(queryParams.get("param_2"), "UTF-8"));

            // Check Body
            BufferedReader br = new BufferedReader(new InputStreamReader(httpExchange.getRequestBody(), "UTF-8"));
            String requestBody = br.lines().collect(Collectors.joining("\n"));
            String expected = content.replaceAll("\\$\\{/book/title\\}", "Title_" + i)
                    .replaceAll("\\$\\{/book/market/price\\}", String.valueOf(1.35 * i))
                    .replaceAll("\\$\\{/book/identification/id\\}", String.valueOf(i))
                    .replaceAll("\\$\\{/book/identification/isbn\\}", "ISBN_" + i);
            Assertions.assertEquals(expected, requestBody);

            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
        components.setInputData(data);
        Job.components() //
                .component("emitter", "test://emitter") //
                .component("out", "Rest://Output?" + configStr) //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

    }

    @Test
    void testOptionsFlags() throws IOException {
        config.getDataset().setMethodType(HttpMethod.POST);
        config.getDataset().setResource("post/path1/path2");

        /*
         * config.getDataset().setHasPathParams(false);
         * List<Param> pathParams = Arrays.asList(new Param[]{new Param("module", "{/module}"), new Param("id", "{/id}")});
         * config.getDataset().setPathParams(pathParams);
         */

        config.getDataset().setHasHeaders(false);
        List<Param> headers = Arrays.asList(new Param[] { new Param("head_1", "header/{/id}"),
                new Param("head_2", "page:{/pagination/page} on {/pagination/total}") });
        config.getDataset().setHeaders(headers);

        config.getDataset().setHasQueryParams(false);
        List<Param> params = Arrays
                .asList(new Param[] { new Param("param_1", "param{/id}&/encoded < >"), new Param("param_2", "{/user_name}") });
        config.getDataset().setQueryParams(params);

        config.getDataset().setHasBody(false);
        Path resourceDirectory = Paths.get("src/test/resources/org/talend/components/rest/body/BodyWithParams.json");
        String content = Files.lines(resourceDirectory).collect(Collectors.joining("\n"));
        config.getDataset().getBody().setType(RequestBody.Type.JSON);
        config.getDataset().getBody().setJsonValue(content);

        final List<Record> data = createData(NB_RECORDS);

        this.setServerContextAndStart(httpExchange -> {

            // Check Path param : see RestInputTest.testOptionsPathFlags
            // Can't be tested here since he test is based on the returned http code

            // Check query headers
            Assertions.assertNull(httpExchange.getRequestHeaders().get("head_1"));
            Assertions.assertNull(httpExchange.getRequestHeaders().get("head_2"));

            // Check query parameters
            Assertions.assertTrue(httpExchange.getRequestURI().toASCIIString().indexOf('?') < 0);

            // Check body
            BufferedReader br = new BufferedReader(new InputStreamReader(httpExchange.getRequestBody(), "UTF-8"));
            byte[] queryBody = br.lines().collect(Collectors.joining("\n")).getBytes();
            Assertions.assertEquals(0, queryBody.length);

            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
        components.setInputData(data);
        Job.components() //
                .component("emitter", "test://emitter") //
                .component("out", "Rest://Output?" + configStr) //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();
    }

    private List<Record> createData(int n) {
        RecordBuilderFactory factory = components.findService(RecordBuilderFactory.class);

        List<Record> records = new ArrayList<>();
        for (int i = 0; i < n; i++) {

            records.add(factory.newRecordBuilder().withInt("id", i)
                    .withRecord("pagination",
                            factory.newRecordBuilder().withInt("page", 10 + i).withInt("total", 100 + i).build())
                    .withString("module", "module_" + i).withString("user_name", "<user> user_" + i + " /<user>")
                    .withRecord("book", factory.newRecordBuilder().withString("title", "Title_" + i)
                            .withRecord("market", factory.newRecordBuilder().withDouble("price", 1.35 * i).build())
                            .withRecord("identification",
                                    factory.newRecordBuilder().withInt("id", i).withString("isbn", "ISBN_" + i).build())
                            .build())
                    .build());

        }

        return records;

    }

}