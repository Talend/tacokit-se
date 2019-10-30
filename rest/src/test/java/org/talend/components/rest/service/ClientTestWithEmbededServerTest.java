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
package org.talend.components.rest.service;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@WithComponents(value = "org.talend.components.rest")
public class ClientTestWithEmbededServerTest {

    @Service
    RestService service;

    @Injected
    private BaseComponentsHandler handler;

    private RequestConfig config;

    private boolean followRedirects_backup;

    private HttpServer server;

    private int port;

    @BeforeEach
    void before() throws IOException {
        followRedirects_backup = HttpURLConnection.getFollowRedirects();
        HttpURLConnection.setFollowRedirects(false);

        // Inject needed services
        handler.injectServices(this);

        config = RequestConfigBuilderTest.getEmptyRequestConfig();
        config.getDataset().getDatastore().setBase("http://localhost");

        // start server
        server = HttpServer.create(new InetSocketAddress(0), 0);
        port = server.getAddress().getPort();
    }

    private void setServerContextAndStart(HttpHandler handler) {
        server.createContext("/", handler);
        server.start();
    }

    @AfterEach
    void after() {
        // stop server
        server.stop(0);

        // set jvm redirection as before
        HttpURLConnection.setFollowRedirects(followRedirects_backup);
    }

    @ParameterizedTest
    @CsvSource(value = { "POST,TEXT,src/test/resources/org/talend/components/rest/body/empty.txt",
            "POST,TEXT,src/test/resources/org/talend/components/rest/body/Multilines.txt",
            "POST,JSON,src/test/resources/org/talend/components/rest/body/Example.json",
            "POST,XML,src/test/resources/org/talend/components/rest/body/Example.xml" })
    void testBody(final String method, final String type, final String filename) throws IOException {
        Path resourceDirectory = Paths.get(filename);
        String content = Files.lines(resourceDirectory).collect(Collectors.joining("\n"));

        config.getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().setResource("post");
        config.getDataset().setMethodType(HttpMethod.valueOf(method));
        config.getDataset().setHasBody(true);

        RequestBody.Type bodyType = RequestBody.Type.valueOf(type);
        config.getDataset().getBody().setType(bodyType);
        config.getDataset().getBody().setTextContent(content);

        final AtomicReference<String> requestContentType = new AtomicReference<>();
        this.setServerContextAndStart(httpExchange -> {
            BufferedReader br = new BufferedReader(new InputStreamReader(httpExchange.getRequestBody(), "UTF-8"));
            byte[] answerBody = br.lines().collect(Collectors.joining("\n")).getBytes();
            final int code = 200;

            List<String> requestContentTypeHeaderList = httpExchange.getRequestHeaders().entrySet().stream()
                    .filter(e -> "content-type".equals(e.getKey().toLowerCase())).findFirst().map(Map.Entry::getValue)
                    .orElse(Collections.emptyList());
            requestContentType.set(requestContentTypeHeaderList.stream().findFirst().orElse(""));

            httpExchange.getResponseHeaders().add("Method", httpExchange.getRequestMethod());

            httpExchange.sendResponseHeaders(code, answerBody.length);
            OutputStream os = httpExchange.getResponseBody();
            os.write(answerBody);
            os.close();
        });

        Record resp = service.execute(config);

        Collection<Record> headers = resp.getArray(Record.class, "headers");
        String requestMethod = headers.stream().filter(e -> "Method".equals(e.getString("key"))).findFirst()
                .map(h -> h.getString("value")).orElse("");

        String requestBody = resp.getString("body");

        assertEquals(200, resp.getInt("status"));
        assertEquals(method, requestMethod);
        assertEquals(content, requestBody);
        assertEquals(bodyType.getContentType(), requestContentType.get());
    }

    @ParameterizedTest
    @CsvSource(value = { "POST,TEXT,src/test/resources/org/talend/components/rest/body/empty.txt",
            "POST,TEXT,src/test/resources/org/talend/components/rest/body/Multilines.txt",
            "POST,JSON,src/test/resources/org/talend/components/rest/body/Example.json",
            "POST,XML,src/test/resources/org/talend/components/rest/body/Example.xml" })
    void testBodyForceContentType(final String method, final String type, final String filename) throws IOException {
        Path resourceDirectory = Paths.get(filename);
        String content = Files.lines(resourceDirectory).collect(Collectors.joining("\n"));

        final String forcedContentType = "text/forced; charset=utf8";

        config.getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().setResource("post");
        config.getDataset().setMethodType(HttpMethod.valueOf(method));
        config.getDataset().setHasBody(true);

        config.getDataset().setHasHeaders(true);
        config.getDataset().setHeaders(Collections.singletonList(new Param("Content-Type", forcedContentType)));

        RequestBody.Type bodyType = RequestBody.Type.valueOf(type);
        config.getDataset().getBody().setType(bodyType);
        config.getDataset().getBody().setTextContent(content);

        final AtomicReference<String> requestContentType = new AtomicReference<>();
        this.setServerContextAndStart(httpExchange -> {
            BufferedReader br = new BufferedReader(new InputStreamReader(httpExchange.getRequestBody(), "UTF-8"));
            byte[] answerBody = br.lines().collect(Collectors.joining("\n")).getBytes();
            final int code = 200;

            List<String> requestContentTypeHeaderList = httpExchange.getRequestHeaders().entrySet().stream()
                    .filter(e -> "content-type".equals(e.getKey().toLowerCase())).findFirst().map(Map.Entry::getValue)
                    .orElse(Collections.emptyList());
            requestContentType.set(requestContentTypeHeaderList.stream().findFirst().orElse(""));

            httpExchange.getResponseHeaders().add("Method", httpExchange.getRequestMethod());

            httpExchange.sendResponseHeaders(code, answerBody.length);
            OutputStream os = httpExchange.getResponseBody();
            os.write(answerBody);
            os.close();
        });

        Record resp = service.execute(config);

        Collection<Record> headers = resp.getArray(Record.class, "headers");
        String requestMethod = headers.stream().filter(e -> "Method".equals(e.getString("key"))).findFirst()
                .map(h -> h.getString("value")).orElse("");

        String requestBody = resp.getString("body");

        assertEquals(200, resp.getInt("status"));
        assertEquals(method, requestMethod);
        assertEquals(content, requestBody);
        assertEquals(forcedContentType, requestContentType.get());
    }

    @ParameterizedTest
    @CsvSource(value = { "GET,false,3,302,GET", "POST,false,4,302,POST", "PUT,false,5,302, PUT", "GET,true,6,302,GET",
            "POST,true,7,302,GET", "PUT,true,8,302,GET", "GET,false,3,303,GET", "POST,false,3,303,GET",
            "DELETE,false,3,303,GET" })
    void testForceGetOnRedirect(final String method, final boolean forceGet, final int nbRedirect, final int redirectCode,
            final String expectedMethod) throws IOException {
        final List<ClientTestWithEmbededServerTest.Request> calls = new ArrayList<>();
        final AtomicInteger counter = new AtomicInteger(0);

        this.setServerContextAndStart(httpExchange -> {
            calls.add(new ClientTestWithEmbededServerTest.Request(httpExchange.getRequestMethod(),
                    httpExchange.getRequestURI().toASCIIString()));
            httpExchange.getResponseHeaders().add("Location",
                    "http://localhost:" + port + "/redirection/" + counter.getAndIncrement());

            int code = redirectCode;
            byte[] body = new byte[0];

            if (counter.get() >= nbRedirect) {
                body = "Done.".getBytes();
                code = 200;
            }
            httpExchange.sendResponseHeaders(code, body.length);
            OutputStream os = httpExchange.getResponseBody();
            os.write(body);
            os.close();
        });

        config.getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().setResource("redirection");
        config.getDataset().setMethodType(HttpMethod.valueOf(method));
        config.getDataset().setMaxRedirect(-1);
        config.getDataset().setForce_302_redirect(forceGet);

        Record resp = service.execute(config);

        assertEquals(200, resp.getInt("status"));
        assertEquals(method, calls.get(0).getMethod());

        assertEquals(nbRedirect, calls.size());
        assertEquals(expectedMethod, calls.get(calls.size() - 1).getMethod());

    }

    @ParameterizedTest
    @CsvSource(value = { "shift_jis,src/test/resources/org/talend/components/rest/body/encoded.shift_jis.txt" })
    void testEncoding(final String encoding, final String filename) throws IOException {
        byte[] bytes = Files.readAllBytes(Paths.get(filename));
        final String contentType = "text/plain; " + ContentType.CHARSET_KEY + encoding;
        final String requestBody = new String(bytes, Charset.forName(encoding));

        AtomicReference<String> receivedBody = new AtomicReference<>();
        this.setServerContextAndStart(httpExchange -> {

            String charsetName = ContentType.getCharsetName(httpExchange.getRequestHeaders());

            InputStream is = httpExchange.getRequestBody();
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            int nRead;
            byte[] data = new byte[1024];
            while ((nRead = is.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, nRead);
            }
            byte[] rawRequestBody = buffer.toByteArray();
            receivedBody.set(new String(rawRequestBody, Charset.forName(charsetName)));

            httpExchange.getResponseHeaders().add(ContentType.HEADER_KEY, contentType);
            httpExchange.sendResponseHeaders(200, rawRequestBody.length);
            OutputStream os = httpExchange.getResponseBody();
            os.write(rawRequestBody);
            os.close();
        });

        config.getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().setMethodType(HttpMethod.POST);

        config.getDataset().setHasBody(true);
        config.getDataset().getBody().setType(RequestBody.Type.TEXT);

        config.getDataset().getBody().setTextContent(requestBody);

        config.getDataset().setHasHeaders(true);
        config.getDataset().setHeaders(Collections.singletonList(new Param(ContentType.HEADER_KEY, contentType)));

        Record resp = service.execute(config);
        String body = resp.getString("body");

        assertEquals(200, resp.getInt("status"));
        assertEquals(requestBody, receivedBody.get());
        assertEquals(requestBody, body);
    }

    @ParameterizedTest
    @CsvSource(value = { "TEXT,src/test/resources/org/talend/components/rest/body/Multilines.txt,text/plain",
            "JSON,src/test/resources/org/talend/components/rest/body/Example.json,application/json",
            "XML,src/test/resources/org/talend/components/rest/body/Example.xml,text/xml" })
    void testForceContentType(final String type, final String filename, final String expected) throws IOException {
        Path resourceDirectory = Paths.get(filename);
        String content = Files.lines(resourceDirectory).collect(Collectors.joining("\n"));

        AtomicReference<String> contentType = new AtomicReference<>();
        this.setServerContextAndStart(httpExchange -> {

            contentType.set(Optional.ofNullable(httpExchange.getRequestHeaders().get(ContentType.HEADER_KEY))
                    .orElse(Collections.emptyList()).stream().findFirst().orElse(""));

            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

        config.getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().setMethodType(HttpMethod.POST);
        config.getDataset().setHasBody(true);
        config.getDataset().getBody().setType(RequestBody.Type.valueOf(type));
        config.getDataset().getBody().setTextContent(content);

        Record resp = service.execute(config);
        assertEquals(200, resp.getInt("status"));
        assertEquals(expected, contentType.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testForceContentType(final boolean parametersActivated) {

        final AtomicBoolean hasContentType = new AtomicBoolean();
        this.setServerContextAndStart(httpExchange -> {

            hasContentType.set(httpExchange.getRequestHeaders().containsKey(ContentType.HEADER_KEY));

            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

        config.getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().setMethodType(HttpMethod.POST);
        config.getDataset().getBody().setType(RequestBody.Type.TEXT);
        config.getDataset().getBody().setTextContent("Not empty");
        config.getDataset().setHasBody(parametersActivated);
        config.getDataset().setHeaders(Collections.singletonList(new Param(ContentType.HEADER_KEY, "text/plain")));
        config.getDataset().setHasHeaders(parametersActivated);

        Record resp = service.execute(config);
        assertEquals(200, resp.getInt("status"));
        assertEquals(parametersActivated, hasContentType.get());
    }

    /**
     * If body and headers are activated, but no Content-Type set, we force the body content-Type.
     */
    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void testForceContentTypeWhenHasBodyAndHeaders(final boolean alreadyHasContentTypeHeader) {

        final AtomicReference<String> receivedContentType = new AtomicReference<>();
        this.setServerContextAndStart(httpExchange -> {

            receivedContentType.set(Optional.ofNullable(httpExchange.getRequestHeaders().get(ContentType.HEADER_KEY))
                    .orElse(Collections.emptyList()).stream().findFirst().orElse(""));

            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

        config.getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().setMethodType(HttpMethod.POST);
        config.getDataset().getBody().setType(RequestBody.Type.TEXT);
        config.getDataset().getBody().setTextContent("Not empty");
        config.getDataset().setHasBody(true);

        List<Param> headers = new ArrayList<>();
        headers.add(new Param("header1", "value1"));
        if (alreadyHasContentTypeHeader) {
            headers.add(new Param(ContentType.HEADER_KEY, "text/forced_type"));
        }
        config.getDataset().setHeaders(headers);
        config.getDataset().setHasHeaders(true);

        Record resp = service.execute(config);
        assertEquals(200, resp.getInt("status"));

        assertEquals(alreadyHasContentTypeHeader ? "text/forced_type" : RequestBody.Type.TEXT.getContentType(),
                receivedContentType.get());
    }

    @Test
    void testHealthCheck() {

        final AtomicInteger code = new AtomicInteger(200);
        this.setServerContextAndStart(httpExchange -> {
            httpExchange.sendResponseHeaders(code.get(), 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

        config.getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().setMethodType(HttpMethod.POST);

        HealthCheckStatus healthCheckStatus = service.healthCheck(config.getDataset().getDatastore());
        assertEquals(HealthCheckStatus.Status.OK, healthCheckStatus.getStatus());

        config.getDataset().getDatastore().setBase("http://localhost:" + port);
        code.set(403); // Force server to return 403
        healthCheckStatus = service.healthCheck(config.getDataset().getDatastore());
        assertEquals(HealthCheckStatus.Status.KO, healthCheckStatus.getStatus());

        config.getDataset().getDatastore().setBase("http://<>{}   localhost:");
        code.set(200); // Force server to return 403
        healthCheckStatus = service.healthCheck(config.getDataset().getDatastore());
        assertEquals(HealthCheckStatus.Status.KO, healthCheckStatus.getStatus());
    }

    @ParameterizedTest
    @CsvSource(value = { "text/plain; charset=shift_jis,shift_jis", "text/html; charset=ascii; other=nothing,ascii",
            "text/html, null", "charset=ascii, ascii" })
    void testGetCharsetName(final String header, final String expected) {
        final Map<String, List<String>> headers = new HashMap<>(singletonMap(ContentType.HEADER_KEY, singletonList(header)));
        for (int i = 0; i < 3; i++) {
            headers.put("key" + i, Arrays.asList("valA" + i, "valB" + i));
        }

        assertEquals(expected, "" + ContentType.getCharsetName(headers));
    }

    @Data
    @AllArgsConstructor
    private final static class Request {

        private final String method;

        private final String url;

    }
}
