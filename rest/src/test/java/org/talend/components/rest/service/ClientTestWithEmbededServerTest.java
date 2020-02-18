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
import org.talend.components.rest.configuration.Dataset;
import org.talend.components.rest.configuration.Datastore;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.service.client.ContentType;
import org.talend.components.rest.virtual.ComplexRestConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
@WithComponents(value = "org.talend.components.rest")
public class ClientTestWithEmbededServerTest {

    public final static int CONNECTION_TIMEOUT = Integer.valueOf(System.getProperty(
            "org.talend.components.rest.service.ClientTestWithEmbededServerTest.connection_timeout", String.valueOf(30000)));

    public final static int READ_TIMEOUT = Integer.valueOf(System.getProperty(
            "org.talend.components.rest.service.ClientTestWithEmbededServerTest.read_timeout", String.valueOf(120000)));

    @Service
    RestService service;

    @Service
    private JsonReaderFactory jsonReaderFactory;

    private ComplexRestConfiguration config;

    private boolean followRedirects_backup;

    private HttpServer server;

    private int port;

    @BeforeEach
    void before() throws IOException {
        followRedirects_backup = HttpURLConnection.getFollowRedirects();
        HttpURLConnection.setFollowRedirects(false);

        config = RequestConfigBuilderTest.getEmptyRequestConfig();
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost");

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setReadTimeout(CONNECTION_TIMEOUT);
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setReadTimeout(READ_TIMEOUT);

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

    /**
     * TRACE must be used with HTTPS :
     * https://github.com/JetBrains/jdk8u_jdk/blob/ab6ffad025a3ebe8c83816834b0c68a235ab38a3/src/share/classes/sun/net/www/protocol/http/HttpURLConnection.java#L1329
     */
    @Test
    void testTrace() {
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setResource("post");
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.TRACE);
        config.getDataset().getRestConfiguration().getDataset().setHasBody(false);

        this.setServerContextAndStart(httpExchange -> {
            byte[] answerBody = new byte[0];
            final int code = 200;

            httpExchange.sendResponseHeaders(code, answerBody.length);
            OutputStream os = httpExchange.getResponseBody();
            os.write(answerBody);
            os.close();
        });

        // If body is empty no exception
        CompletePayload completePayload = service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration()));
        assertEquals(200, completePayload.getStatus());

        // TRACE must be used with https
        config.getDataset().getRestConfiguration().getDataset().setHasBody(true);
        config.getDataset().getRestConfiguration().getDataset().getBody().setType(RequestBody.Type.TEXT);
        // Some data in body is needed to raise the exception :
        // https://github.com/Talend/component-runtime/blob/7a4f24e0c876d6b0cd9d2686b6740cb960bc39ce/component-runtime-manager/src/main/java/org/talend/sdk/component/runtime/manager/service/http/ExecutionContext.java#L75
        config.getDataset().getRestConfiguration().getDataset().getBody()
                .setTextContent("Must be not empty to have TRACE exception");

        assertThrows(java.lang.IllegalStateException.class, () -> service.execute(config.getDataset().getRestConfiguration()));
    }

    @ParameterizedTest
    @CsvSource(value = { "POST,TEXT,src/test/resources/org/talend/components/rest/body/empty.txt",
            "POST,TEXT,src/test/resources/org/talend/components/rest/body/Multilines.txt",
            "POST,JSON,src/test/resources/org/talend/components/rest/body/Example.json",
            "POST,XML,src/test/resources/org/talend/components/rest/body/Example.xml" })
    void testBody(final String method, final String type, final String filename) throws IOException {
        Path resourceDirectory = Paths.get(filename);
        Object content = Files.lines(resourceDirectory).collect(Collectors.joining("\n"));

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setResource("post");
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.valueOf(method));
        config.getDataset().getRestConfiguration().getDataset().setHasBody(true);

        RequestBody.Type bodyType = RequestBody.Type.valueOf(type);
        config.getDataset().getRestConfiguration().getDataset().getBody().setType(bodyType);
        config.getDataset().getRestConfiguration().getDataset().getBody().setTextContent(content.toString());

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

        CompletePayload resp = service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration()));

        Set<Map.Entry<String, String>> headers = resp.getHeaders().entrySet();
        String requestMethod = headers.stream().filter(e -> "Method".equals(e.getKey())).findFirst().map(h -> h.getValue())
                .orElse("");

        Object requestBody = resp.getBody();

        assertEquals(200, resp.getStatus());
        assertEquals(method, requestMethod);

        if ("JSON".equals(type)) {
            final JsonReader reader = jsonReaderFactory.createReader(new StringReader(content.toString()));
            content = reader.readObject();
            reader.close();
        }
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
        Object content = Files.lines(resourceDirectory).collect(Collectors.joining("\n"));

        final String forcedContentType = "text/forced; charset=utf8";

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setResource("post");
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.valueOf(method));
        config.getDataset().getRestConfiguration().getDataset().setHasBody(true);

        config.getDataset().getRestConfiguration().getDataset().setHasHeaders(true);
        config.getDataset().getRestConfiguration().getDataset()
                .setHeaders(Collections.singletonList(new Param("Content-Type", forcedContentType)));

        RequestBody.Type bodyType = RequestBody.Type.valueOf(type);
        config.getDataset().getRestConfiguration().getDataset().getBody().setType(bodyType);
        config.getDataset().getRestConfiguration().getDataset().getBody().setTextContent(content.toString());

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

        CompletePayload resp = service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration()));

        Set<Map.Entry<String, String>> headers = resp.getHeaders().entrySet();
        String requestMethod = headers.stream().filter(e -> "Method".equals(e.getKey())).findFirst().map(h -> h.getValue())
                .orElse("");

        Object requestBody = resp.getBody();

        assertEquals(200, resp.getStatus());
        assertEquals(method, requestMethod);

        if ("JSON".equals(type)) {
            final JsonReader reader = jsonReaderFactory.createReader(new StringReader(content.toString()));
            content = reader.readObject();
            reader.close();
        }
        assertEquals(content, requestBody);

        assertEquals(forcedContentType, requestContentType.get());
    }

    @ParameterizedTest
    @CsvSource(value = { "GET,false,3,302,GET", "POST,false,4,302,POST", "PUT,false,5,302, PUT", "GET,true,6,302,GET",
            "POST,true,7,302,GET", "PUT,true,8,302,GET", "GET,false,3,303,GET", "POST,false,3,303,GET",
            "DELETE,false,3,303,GET" })
    void testForceGetOnRedirect(final String method, final boolean forceGet, final int nbRedirect, final int redirectCode,
            final String expectedMethod) {
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

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setResource("redirection");
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.valueOf(method));
        config.getDataset().getRestConfiguration().getDataset().setMaxRedirect(-1);
        config.getDataset().getRestConfiguration().getDataset().setForce_302_redirect(forceGet);

        CompletePayload resp = service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration()));

        assertEquals(200, resp.getStatus());
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

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.POST);

        config.getDataset().getRestConfiguration().getDataset().setHasBody(true);
        config.getDataset().getRestConfiguration().getDataset().getBody().setType(RequestBody.Type.TEXT);

        config.getDataset().getRestConfiguration().getDataset().getBody().setTextContent(requestBody);

        config.getDataset().getRestConfiguration().getDataset().setHasHeaders(true);
        config.getDataset().getRestConfiguration().getDataset()
                .setHeaders(Collections.singletonList(new Param(ContentType.HEADER_KEY, contentType)));

        CompletePayload resp = service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration()));
        Object body = resp.getBody();

        assertEquals(200, resp.getStatus());
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

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.POST);
        config.getDataset().getRestConfiguration().getDataset().setHasBody(true);
        config.getDataset().getRestConfiguration().getDataset().getBody().setType(RequestBody.Type.valueOf(type));
        config.getDataset().getRestConfiguration().getDataset().getBody().setTextContent(content);

        CompletePayload resp = service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration()));
        assertEquals(200, resp.getStatus());
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

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.POST);
        config.getDataset().getRestConfiguration().getDataset().getBody().setType(RequestBody.Type.TEXT);
        config.getDataset().getRestConfiguration().getDataset().getBody().setTextContent("Not empty");
        config.getDataset().getRestConfiguration().getDataset().setHasBody(parametersActivated);
        config.getDataset().getRestConfiguration().getDataset()
                .setHeaders(Collections.singletonList(new Param(ContentType.HEADER_KEY, "text/plain")));
        config.getDataset().getRestConfiguration().getDataset().setHasHeaders(parametersActivated);

        CompletePayload resp = service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration()));
        assertEquals(200, resp.getStatus());
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

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.POST);
        config.getDataset().getRestConfiguration().getDataset().getBody().setType(RequestBody.Type.TEXT);
        config.getDataset().getRestConfiguration().getDataset().getBody().setTextContent("Not empty");
        config.getDataset().getRestConfiguration().getDataset().setHasBody(true);

        List<Param> headers = new ArrayList<>();
        headers.add(new Param("header1", "value1"));
        if (alreadyHasContentTypeHeader) {
            headers.add(new Param(ContentType.HEADER_KEY, "text/forced_type"));
        }
        config.getDataset().getRestConfiguration().getDataset().setHeaders(headers);
        config.getDataset().getRestConfiguration().getDataset().setHasHeaders(true);

        CompletePayload resp = service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration()));
        assertEquals(200, resp.getStatus());

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

        final RequestConfig minimalConfig = new RequestConfig();
        final Dataset dse = new Dataset();
        final Datastore dso = new Datastore();
        dse.setDatastore(dso);
        minimalConfig.setDataset(dse);

        minimalConfig.getDataset().getDatastore().setBase("http://localhost:" + port);

        HealthCheckStatus healthCheckStatus = service.healthCheck(minimalConfig.getDataset().getDatastore());
        assertEquals(HealthCheckStatus.Status.OK, healthCheckStatus.getStatus());

        minimalConfig.getDataset().getDatastore().setBase("http://localhost:" + port);
        code.set(403); // Force server to return 403
        healthCheckStatus = service.healthCheck(minimalConfig.getDataset().getDatastore());
        assertEquals(HealthCheckStatus.Status.KO, healthCheckStatus.getStatus());

        minimalConfig.getDataset().getDatastore().setBase("http://<>{}   localhost:");
        code.set(200); // Force server to return 403
        healthCheckStatus = service.healthCheck(minimalConfig.getDataset().getDatastore());
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

    @Test
    void testReadTimeout() {
        this.setServerContextAndStart(httpExchange -> {
            String ok = "ok";
            httpExchange.sendResponseHeaders(200, ok.length());

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                log.error("Sleep to test read timeout failed, ", e);
            }

            OutputStream os = httpExchange.getResponseBody();
            os.write(ok.getBytes());
            os.close();
        });

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setReadTimeout(300);
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);

        try {
            service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration()));
            fail("Timeout exception should be raised");
        } catch (IllegalStateException e) {
            Throwable cause = e.getCause();
            assertTrue(SocketTimeoutException.class.isInstance(cause));
        }

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setReadTimeout(1000);
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        CompletePayload resp = service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration()));
        assertEquals(200, resp.getStatus());
    }

    @Test
    void testNullParams() {
        this.setServerContextAndStart(httpExchange -> {
            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("get");

        config.getDataset().getRestConfiguration().getDataset().setHasHeaders(true);
        config.getDataset().getRestConfiguration().getDataset().setHeaders(null);

        config.getDataset().getRestConfiguration().getDataset().setHasQueryParams(true);
        config.getDataset().getRestConfiguration().getDataset().setQueryParams(null);

        config.getDataset().getRestConfiguration().getDataset().setHasPathParams(true);
        config.getDataset().getRestConfiguration().getDataset().setPathParams(null);

        config.getDataset().getRestConfiguration().getDataset().setHasBody(true);
        RequestBody body = new RequestBody();
        body.setType(RequestBody.Type.FORM_DATA);
        body.setParams(null);
        config.getDataset().getRestConfiguration().getDataset().setBody(body);

        CompletePayload resp = service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration()));
        assertEquals(200, resp.getStatus());

    }

    @Test
    void testDuplicateKeysPathParams() {
        this.setServerContextAndStart(httpExchange -> {
            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("get");

        config.getDataset().getRestConfiguration().getDataset().setHasPathParams(true);
        config.getDataset().getRestConfiguration().getDataset()
                .setPathParams(Arrays.asList(new Param("aaaa", null), new Param("aaaa", "val"), new Param(null, "val"),
                        new Param("key2", "val"), new Param("", ""), new Param("key3", "")));

        assertThrows(IllegalStateException.class,
                () -> service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration())));
    }

    @Test
    void testDuplicateKeysQueryParams() {
        this.setServerContextAndStart(httpExchange -> {
            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("get");

        config.getDataset().getRestConfiguration().getDataset().setHasQueryParams(true);
        config.getDataset().getRestConfiguration().getDataset()
                .setQueryParams(Arrays.asList(new Param("aaa", null), new Param("key1", "val"), new Param("aaa", "val"),
                        new Param("key2", "val"), new Param("", ""), new Param("key3", "")));

        assertThrows(IllegalStateException.class,
                () -> service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration())));
    }

    @Test
    void testDuplicateKeysHeaders() {
        this.setServerContextAndStart(httpExchange -> {
            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("get");

        config.getDataset().getRestConfiguration().getDataset().setHasHeaders(true);
        config.getDataset().getRestConfiguration().getDataset()
                .setHeaders(Arrays.asList(new Param("xxx1", null), new Param("aaa", "val"), new Param(null, "val"),
                        new Param("key2", "val"), new Param("aaa", ""), new Param("key3", "")));

        assertThrows(IllegalStateException.class,
                () -> service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration())));
    }

    @Test
    void testDuplicateKeysBodyParameters() {
        this.setServerContextAndStart(httpExchange -> {
            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("get");

        config.getDataset().getRestConfiguration().getDataset().setHasBody(true);
        RequestBody body = new RequestBody();
        body.setType(RequestBody.Type.FORM_DATA);
        body.setParams(Arrays.asList(new Param("xxx1", null), new Param("aaa", "val"), new Param(null, "val"),
                new Param("key2", "val"), new Param("aaa", ""), new Param("key3", "")));
        config.getDataset().getRestConfiguration().getDataset().setBody(body);

        assertThrows(IllegalStateException.class,
                () -> service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration())));
    }

    @Test
    void testBodyFormaData() throws IOException {

        final AtomicReference<String> receivedBody = new AtomicReference<>();
        this.setServerContextAndStart(httpExchange -> {

            BufferedReader br = new BufferedReader(new InputStreamReader(httpExchange.getRequestBody(), "UTF-8"));
            receivedBody.set(br.lines().collect(Collectors.joining("\n")));

            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("get");

        config.getDataset().getRestConfiguration().getDataset().setHasBody(true);
        RequestBody body = new RequestBody();
        body.setType(RequestBody.Type.FORM_DATA);
        body.setParams(Arrays.asList(new Param("key1", null), new Param("key2", "val2"), new Param(null, "val"),
                new Param("key3", "val3"), new Param("", ""), new Param("key4", "val4")));
        config.getDataset().getRestConfiguration().getDataset().setBody(body);

        CompletePayload resp = service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration()));

        Path expectedFormaDataFile = Paths.get("src/test/resources/org/talend/components/rest/body/formData.txt");
        String expectedFormaData = Files.lines(expectedFormaDataFile).collect(Collectors.joining("\n"));

        assertEquals(200, resp.getStatus());
        assertEquals(expectedFormaData, receivedBody.get());
    }

    @Test
    void testBodyXxxFormUrlEncoded() throws IOException {

        final AtomicReference<String> receivedBody = new AtomicReference<>();
        this.setServerContextAndStart(httpExchange -> {

            BufferedReader br = new BufferedReader(new InputStreamReader(httpExchange.getRequestBody(), "UTF-8"));
            receivedBody.set(br.lines().collect(Collectors.joining("\n")));

            httpExchange.sendResponseHeaders(200, 0);
            OutputStream os = httpExchange.getResponseBody();
            os.write(new byte[0]);
            os.close();
        });

        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("http://localhost:" + port);
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().getRestConfiguration().getDataset().setResource("get");

        config.getDataset().getRestConfiguration().getDataset().setHasBody(true);
        RequestBody body = new RequestBody();
        body.setType(RequestBody.Type.X_WWW_FORM_URLENCODED);
        body.setParams(Arrays.asList(new Param("key1", null), new Param("key2", "val2"), new Param(null, "val"),
                new Param("key3", "val3"), new Param("", ""), new Param("key4", "val4")));
        config.getDataset().getRestConfiguration().getDataset().setBody(body);

        CompletePayload resp = service.buildFixedRecord(service.execute(config.getDataset().getRestConfiguration()));

        Path expectedFormaDataFile = Paths.get("src/test/resources/org/talend/components/rest/body/xxxFormUrlEncoded.txt");
        String expectedFormaData = Files.lines(expectedFormaDataFile).collect(Collectors.joining("\n"));

        assertEquals(200, resp.getStatus());
        assertEquals(expectedFormaData, receivedBody.get());
    }

    @Data
    @AllArgsConstructor
    private final static class Request {

        private final String method;

        private final String url;

    }
}
