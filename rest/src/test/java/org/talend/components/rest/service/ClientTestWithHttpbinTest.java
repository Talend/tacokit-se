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

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.auth.Authentication;
import org.talend.components.rest.configuration.auth.Authorization;
import org.talend.components.rest.configuration.auth.Basic;
import org.talend.components.rest.virtual.ComplexRestConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
@Testcontainers
@Tag("ITs") // Identify those tests as integration tests to exclude them since some difficulties to run them on ci currently
@WithComponents(value = "org.talend.components.rest")
public class ClientTestWithHttpbinTest {

    private static GenericContainer<?> httpbin;

    public static Supplier<String> HTTPBIN_BASE;

    private final static int CONNECT_TIMEOUT = 30000;

    private final static int READ_TIMEOUT = 30000;

    @Service
    RestService service;

    @Service
    JsonReaderFactory jsonReaderFactory;

    @Injected
    private BaseComponentsHandler handler;

    private ComplexRestConfiguration config;

    private boolean followRedirects_backup;

    @BeforeAll
    static void startHttpBinContainer() {
        httpbin = new GenericContainer<>("kennethreitz/httpbin").withExposedPorts(80).waitingFor(Wait.forHttp("/"));
        httpbin.start();
        HTTPBIN_BASE = () -> System.getProperty("org.talend.components.rest.httpbin_base",
                "http://localhost:" + httpbin.getMappedPort(80));
    }

    @AfterAll
    static void stopHttpBinContainer() {
        httpbin.stop();
    }

    @BeforeEach
    void before() {
        followRedirects_backup = HttpURLConnection.getFollowRedirects();
        HttpURLConnection.setFollowRedirects(false);

        // Inject needed services
        handler.injectServices(this);

        config = RequestConfigBuilderTest.getEmptyRequestConfig();

        config.getRestConfiguration().getDataset().getDatastore().setBase(HTTPBIN_BASE.get());
        config.getRestConfiguration().getDataset().getDatastore().setConnectionTimeout(CONNECT_TIMEOUT);
        config.getRestConfiguration().getDataset().getDatastore().setReadTimeout(READ_TIMEOUT);
    }

    @AfterEach
    void after() {
        HttpURLConnection.setFollowRedirects(followRedirects_backup);
    }

    @Test
    void httpbinGet() throws MalformedURLException {
        config.getRestConfiguration().getDataset().setResource("get");
        config.getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);

        Record resp = service.buildFixedRecord(service.execute(config.getRestConfiguration()));

        assertEquals(200, resp.getInt("status"));

        String body = resp.getString("body");
        JsonObject bodyJson = jsonReaderFactory.createReader(new ByteArrayInputStream((body == null ? "" : body).getBytes()))
                .readObject();

        assertEquals(service.buildUrl(config.getRestConfiguration(), Collections.emptyMap()), bodyJson.getString("url"));

        JsonObject headersJson = bodyJson.getJsonObject("headers");
        URL base = new URL(HTTPBIN_BASE.get());
        assertEquals(base.getHost() + ":" + base.getPort(), headersJson.getString("Host"));
    }

    /**
     * If there are some parameters set, if false is given to setHasXxxx those parameters should not be passed.
     *
     * @throws Exception
     */
    @Test
    void testParamsDisabled() throws MalformedURLException {
        HttpMethod[] verbs = { HttpMethod.DELETE, HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT };
        config.getRestConfiguration().getDataset().setResource("get");
        config.getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);

        List<Param> queryParams = new ArrayList<>();
        queryParams.add(new Param("params1", "value1"));
        config.getRestConfiguration().getDataset().setHasQueryParams(false);
        config.getRestConfiguration().getDataset().setQueryParams(queryParams);

        List<Param> headerParams = new ArrayList<>();
        headerParams.add(new Param("Header1", "simple value"));
        config.getRestConfiguration().getDataset().setHasHeaders(false);
        config.getRestConfiguration().getDataset().setHeaders(headerParams);

        Record resp = service.buildFixedRecord(service.execute(config.getRestConfiguration()));

        assertEquals(200, resp.getInt("status"));

        JsonReader payloadReader = jsonReaderFactory.createReader(new StringReader(resp.getString("body")));
        JsonObject payload = payloadReader.readObject();

        assertEquals(0, payload.getJsonObject("args").size());
        URL base = new URL(HTTPBIN_BASE.get());
        assertEquals(base.getHost() + ":" + base.getPort(), payload.getJsonObject("headers").getString("Host"));

    }

    @Test
    void testQueryAndHeaderParams() {
        HttpMethod[] verbs = { HttpMethod.DELETE, HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT };
        for (HttpMethod m : verbs) {
            config.getRestConfiguration().getDataset().setResource(m.name().toLowerCase());
            config.getRestConfiguration().getDataset().setMethodType(m);

            List<Param> queryParams = new ArrayList<>();
            queryParams.add(new Param("params1", "value1"));
            queryParams.add(new Param("params2", "<name>Dupont & Dupond</name>"));
            config.getRestConfiguration().getDataset().setHasQueryParams(true);
            config.getRestConfiguration().getDataset().setQueryParams(queryParams);

            List<Param> headerParams = new ArrayList<>();
            headerParams.add(new Param("Header1", "simple value"));
            headerParams.add(new Param("Header2", "<name>header Dupont & Dupond</name>"));
            config.getRestConfiguration().getDataset().setHasHeaders(true);
            config.getRestConfiguration().getDataset().setHeaders(headerParams);

            Record resp = service.buildFixedRecord(service.execute(config.getRestConfiguration()));

            assertEquals(200, resp.getInt("status"));

            JsonReader payloadReader = jsonReaderFactory.createReader(new StringReader(resp.getString("body")));
            JsonObject payload = payloadReader.readObject();

            assertEquals("value1", payload.getJsonObject("args").getString("params1"));
            assertEquals("<name>Dupont & Dupond</name>", payload.getJsonObject("args").getString("params2"));
            assertEquals("simple value", payload.getJsonObject("headers").getString("Header1"));
            assertEquals("<name>header Dupont & Dupond</name>", payload.getJsonObject("headers").getString("Header2"));
        }
    }

    @Test
    void testBasicAuth() {
        String user = "my_user";
        String pwd = "my_password";

        Basic basic = new Basic();
        basic.setUsername(user);
        basic.setPassword(pwd);

        Authentication auth = new Authentication();
        auth.setType(Authorization.AuthorizationType.Basic);
        auth.setBasic(basic);

        config.getRestConfiguration().getDataset().getDatastore().setAuthentication(auth);
        config.getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);

        config.getRestConfiguration().getDataset().setResource("/basic-auth/" + user + "/wrong_" + pwd);
        Record respForbidden = service.buildFixedRecord(service.execute(config.getRestConfiguration()));
        assertEquals(401, respForbidden.getInt("status"));

        config.getRestConfiguration().getDataset().setResource("/basic-auth/" + user + "/" + pwd);
        Record respOk = service.buildFixedRecord(service.execute(config.getRestConfiguration()));
        assertEquals(200, respOk.getInt("status"));
    }

    @Test
    void testBearerAuth() {
        Authentication auth = new Authentication();
        auth.setType(Authorization.AuthorizationType.Bearer);

        config.getRestConfiguration().getDataset().getDatastore().setBase(HTTPBIN_BASE.get());
        config.getRestConfiguration().getDataset().getDatastore().setAuthentication(auth);
        config.getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getRestConfiguration().getDataset().setResource("/bearer");

        auth.setBearerToken("");
        Record respKo = service.buildFixedRecord(service.execute(config.getRestConfiguration()));
        assertEquals(401, respKo.getInt("status"));

        auth.setBearerToken("token-123456789");
        Record respOk = service.buildFixedRecord(service.execute(config.getRestConfiguration()));
        assertEquals(200, respOk.getInt("status"));
    }

    @ParameterizedTest
    @CsvSource(value = { "GET", "POST", "PUT" })
    void testRedirect(final String method) {

        String redirect_url = HTTPBIN_BASE.get() + "/" + method.toLowerCase() + "?redirect=ok";
        config.getRestConfiguration().getDataset().setResource("redirect-to?url=" + redirect_url);
        config.getRestConfiguration().getDataset().setMethodType(HttpMethod.valueOf(method));
        config.getRestConfiguration().getDataset().setMaxRedirect(1);

        Record resp = service.buildFixedRecord(service.execute(config.getRestConfiguration()));
        assertEquals(200, resp.getInt("status"));

        JsonReader payloadReader = jsonReaderFactory.createReader(new StringReader(resp.getString("body")));
        JsonObject payload = payloadReader.readObject();

        assertEquals("ok", payload.getJsonObject("args").getString("redirect"));
    }

    @ParameterizedTest
    @CsvSource(value = { "GET,", "GET,http://www.google.com" })
    void testRedirectOnlySameHost(final String method, final String redirect_url) throws MalformedURLException {
        String mainHost = new URL(HTTPBIN_BASE.get()).getHost();

        config.getRestConfiguration().getDataset()
                .setResource("redirect-to?url=" + ("".equals(redirect_url) ? mainHost : redirect_url));
        config.getRestConfiguration().getDataset().setMethodType(HttpMethod.valueOf(method));
        config.getRestConfiguration().getDataset().setMaxRedirect(1);
        config.getRestConfiguration().getDataset().setOnly_same_host(true);

        if ("".equals(redirect_url)) {
            Record resp = service.buildFixedRecord(service.execute(config.getRestConfiguration()));
            assertEquals(200, resp.getInt("status"));

            JsonReader payloadReader = jsonReaderFactory.createReader(new StringReader(resp.getString("body")));
            JsonObject payload = payloadReader.readObject();

            assertEquals("ok", payload.getJsonObject("args").getString("redirect"));
        } else {
            assertThrows(IllegalArgumentException.class,
                    () -> service.buildFixedRecord(service.execute(config.getRestConfiguration())));
        }
    }

    @ParameterizedTest
    @CsvSource(value = { "6,-1", "3,3", "3,5" })
    void testRedirectNOk(final int nbRedirect, final int maxRedict) {
        config.getRestConfiguration().getDataset().setResource("redirect/" + nbRedirect);
        config.getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getRestConfiguration().getDataset().setMaxRedirect(maxRedict);

        Record resp = service.buildFixedRecord(service.execute(config.getRestConfiguration()));
        assertEquals(200, resp.getInt("status"));
    }

    @ParameterizedTest
    @CsvSource(value = { "3,0", "3,1", "3,2", "5,4" })
    void testRedirectNko(final int nbRedirect, final int maxRedict) {
        config.getRestConfiguration().getDataset().setResource("redirect/" + nbRedirect);
        config.getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getRestConfiguration().getDataset().setMaxRedirect(maxRedict);

        if (maxRedict == 0) {
            // When maxRedirect == 0 then redirect is disabled
            // we only return the response
            Record resp = service.buildFixedRecord(service.execute(config.getRestConfiguration()));
            assertEquals(302, resp.getInt("status"));
        } else {
            Exception e = assertThrows(IllegalArgumentException.class,
                    () -> service.buildFixedRecord(service.execute(config.getRestConfiguration())));
        }
    }

    @ParameterizedTest
    @CsvSource(value = { "auth-int,MD5", "auth,MD5", "auth-int,MD5-sess", "auth,MD5-sess", "auth-int,SHA-256", "auth,SHA-256",
            "auth-int,SHA-512", "auth,SHA-512" })
    void testDisgestAuth(final String qop, final String algo) {
        String user = "my_user";
        String pwd = "my_password";

        Basic basic = new Basic();
        basic.setUsername(user);
        basic.setPassword(pwd);

        Authentication auth = new Authentication();
        auth.setType(Authorization.AuthorizationType.Digest);
        auth.setBasic(basic);

        testDigestAuthWithQop(200, user, pwd, auth, qop);
        testDigestAuthWithQop(401, user, pwd + "x", auth, qop);

        testDigestAuthWithQopAlgo(200, user, pwd, auth, qop, algo);
        testDigestAuthWithQopAlgo(401, user, pwd + "x", auth, qop, algo);
    }

    private void testDigestAuthWithQop(final int expected, final String user, final String pwd, final Authentication auth,
            final String qop) {
        config.getRestConfiguration().getDataset().getDatastore().setAuthentication(auth);
        config.getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getRestConfiguration().getDataset().setResource("digest-auth/" + qop + "/" + user + "/" + pwd);

        Record resp = service.buildFixedRecord(service.execute(config.getRestConfiguration()));
        assertEquals(expected, resp.getInt("status"));
    }

    private void testDigestAuthWithQopAlgo(final int expected, final String user, final String pwd, final Authentication auth,
            final String qop, final String algo) {
        config.getRestConfiguration().getDataset().getDatastore().setAuthentication(auth);
        config.getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getRestConfiguration().getDataset().setResource("digest-auth/" + qop + "/" + user + "/" + pwd + "/" + algo);

        Record resp = service.buildFixedRecord(service.execute(config.getRestConfiguration()));
        assertEquals(expected, resp.getInt("status"));
    }

    @ParameterizedTest
    @CsvSource(value = { "json", "xml", "html" })
    void testformats(final String type) {
        // Currently return body as String
        config.getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);
        config.getRestConfiguration().getDataset().setResource(type);

        Record resp = service.buildFixedRecord(service.execute(config.getRestConfiguration()));

        assertEquals(200, resp.getInt("status"));
    }

    @Test
    void testBodyFormData() {
        config.getRestConfiguration().getDataset().setHasBody(true);

        RequestBody body = new RequestBody();
        body.setType(RequestBody.Type.FORM_DATA);
        body.setParams(Arrays.asList(new Param("form_data_1", "<000 001"), new Param("form_data_2", "<000 002")));
        config.getRestConfiguration().getDataset().setBody(body);
        config.getRestConfiguration().getDataset().setMethodType(HttpMethod.POST);
        config.getRestConfiguration().getDataset().setResource("post");

        Record resp = service.buildFixedRecord(service.execute(config.getRestConfiguration()));
        JsonReader payloadReader = jsonReaderFactory.createReader(new StringReader(resp.getString("body")));
        JsonObject payload = payloadReader.readObject();
        JsonObject form = payload.getJsonObject("form");

        assertEquals(form.getString("form_data_1"), "<000 001");
        assertEquals(form.getString("form_data_2"), "<000 002");
    }

    @Test
    void testBodyXwwwformURLEncoded() {
        config.getRestConfiguration().getDataset().setHasBody(true);

        RequestBody body = new RequestBody();
        body.setType(RequestBody.Type.X_WWW_FORM_URLENCODED);
        body.setParams(Arrays.asList(new Param("form_data_1", "<000 001"), new Param("form_data_2", "<000 002")));
        config.getRestConfiguration().getDataset().setBody(body);
        config.getRestConfiguration().getDataset().setMethodType(HttpMethod.POST);
        config.getRestConfiguration().getDataset().setResource("post");

        Record resp = service.buildFixedRecord(service.execute(config.getRestConfiguration()));
        JsonReader payloadReader = jsonReaderFactory.createReader(new StringReader(resp.getString("body")));
        JsonObject payload = payloadReader.readObject();
        JsonObject form = payload.getJsonObject("form");

        assertEquals(form.getString("form_data_1"), "<000 001");
        assertEquals(form.getString("form_data_2"), "<000 002");
    }

}
