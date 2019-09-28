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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.configuration.auth.Authentication;
import org.talend.components.rest.configuration.auth.Authorization;
import org.talend.components.rest.configuration.auth.Basic;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonValue;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
// @HttpApi(useSsl = true)
@WithComponents(value = "org.talend.components.rest")
class ClientTest {

    private final static String HTTP_BIN_BASE_LATEST = "https://httpbin.org";

    private final static String HTTP_BIN_BASE = "http://tal-rd22.talend.lan:8084";

    private final static String DONT_CHECK = "%DONT_CHECK%";

    /*
     * static {
     * System.setProperty("talend.junit.http.capture", "true");
     * }
     */

    @Service
    RestService service;

    @Service
    JsonReaderFactory jsonReaderFactory;

    @Injected
    private BaseComponentsHandler handler;

    private RequestConfig config;

    @BeforeEach
    void buildConfig() {
        // Inject needed services
        handler.injectServices(this);

        Client client = service.getClient();

        config = RequestConfigBuilder.getEmptyRequestConfig();

        config.getDataset().getDatastore().setBase(HTTP_BIN_BASE_LATEST);
        config.getDataset().setConnectionTimeout(5000);
        config.getDataset().setReadTimeout(5000);
    }

    @Test
    void httpbinGet() throws Exception {
        config.getDataset().setResource("get");
        config.getDataset().setMethodType(HttpMethod.GET);

        Record resp = service.execute(config);

        assertEquals(200, resp.getInt("status"));

        List<Record> headers = Collections.unmodifiableList(new ArrayList<>(resp.getArray(Record.class, "headers")));

        Map<String, String> headerToCheck = new HashMap<>();
        headerToCheck.put("Server", "gunicorn/19.7.1");
        headerToCheck.put("Access-Control-Allow-Origin", "*");
        headerToCheck.put("Access-Control-Allow-Credentials", "true");
        headerToCheck.put("Connection", "close");
        headerToCheck.put("Content-Length", "298");
        headerToCheck.put("X-Powered-By", "Flask");
        headerToCheck.put("Content-Type", "application/json");
        headerToCheck.put("Date", DONT_CHECK);
        headerToCheck.put("X-Processed-Time", DONT_CHECK);

        headers.forEach(e -> {
            String expected = headerToCheck.get(e.getString("key"));
            if (!DONT_CHECK.equals(expected)) {
                assertEquals(expected, e.getString("value"));
            }
            headerToCheck.remove(e.getString("key"));
        });
        assertTrue(headerToCheck.isEmpty());

        String bodyS = resp.getString("body");
        JsonObject bodyJson = jsonReaderFactory.createReader(new ByteArrayInputStream((bodyS == null ? "" : bodyS).getBytes()))
                .readObject();

        assertEquals(JsonValue.ValueType.OBJECT, bodyJson.getJsonObject("args").getValueType());

        Map<String, String> headersValid = new HashMap<>();
        headersValid.put("Accept", "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2");
        headersValid.put("Connection", "keep-alive");
        headersValid.put("Host", HTTP_BIN_BASE.substring(7));
        headersValid.put("User-Agent", DONT_CHECK);

        JsonObject headersJson = bodyJson.getJsonObject("headers");
        headersJson.keySet().stream().forEach(k -> {
            if (!DONT_CHECK.equals(headersValid.get(k))) {
                assertEquals(headersValid.get(k), headersJson.getString(k));
            }
            headersValid.remove(k);
        });
        headersValid.keySet().stream().forEach(k -> System.out.println("==> " + k));
        assertTrue(headersValid.isEmpty());

        assertEquals(HTTP_BIN_BASE + "/get", bodyJson.getString("url"));
    }

    /**
     * If there are some parameters set, if false is given to setHasXxxx those parameters should not be passed.
     *
     * @throws Exception
     */
    @Test
    void testParamsDisabled() throws Exception {
        HttpMethod[] verbs = { HttpMethod.DELETE, HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT };
        config.getDataset().setResource("get");
        config.getDataset().setMethodType(HttpMethod.GET);

        List<Param> queryParams = new ArrayList<>();
        queryParams.add(new Param("params1", "value1"));
        config.getDataset().setHasQueryParams(false);
        config.getDataset().setQueryParams(queryParams);

        List<Param> headerParams = new ArrayList<>();
        headerParams.add(new Param("Header1", "simple value"));
        config.getDataset().setHasHeaders(false);
        config.getDataset().setHeaders(headerParams);

        Record resp = service.execute(config);

        assertEquals(200, resp.getInt("status"));

        JsonReader payloadReader = jsonReaderFactory.createReader(new StringReader(resp.getString("body")));
        JsonObject payload = payloadReader.readObject();

        assertEquals(0, payload.getJsonObject("args").size());
        assertEquals(4, payload.getJsonObject("headers").size());

    }

    @Test
    void testQueryAndHeaderParams() throws Exception {
        HttpMethod[] verbs = { HttpMethod.DELETE, HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT };
        for (HttpMethod m : verbs) {
            config.getDataset().setResource(m.name().toLowerCase());
            config.getDataset().setMethodType(m);

            List<Param> queryParams = new ArrayList<>();
            queryParams.add(new Param("params1", "value1"));
            queryParams.add(new Param("params2", "<name>Dupont & Dupond</name>"));
            config.getDataset().setHasQueryParams(true);
            config.getDataset().setQueryParams(queryParams);

            List<Param> headerParams = new ArrayList<>();
            headerParams.add(new Param("Header1", "simple value"));
            headerParams.add(new Param("Header2", "<name>header Dupont & Dupond</name>"));
            config.getDataset().setHasHeaders(true);
            config.getDataset().setHeaders(headerParams);

            Record resp = service.execute(config);

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
    void testBasicAuth() throws Exception {
        String user = "my_user";
        String pwd = "my_password";

        Basic basic = new Basic();
        basic.setUsername(user);
        basic.setPassword(pwd);

        Authentication auth = new Authentication();
        auth.setType(Authorization.AuthorizationType.Basic);
        auth.setBasic(basic);

        config.getDataset().setAuthentication(auth);
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("/basic-auth/{user}/{pwd}");
        config.setStopIfNotOk(false);

        // httpbin expects login/pwd given in header Basic as path param
        config.getDataset().setResource("/basic-auth/" + user + "/wrong_" + pwd);
        Record respForbidden = service.execute(config);
        assertEquals(401, respForbidden.getInt("status"));

        config.getDataset().setResource("/basic-auth/" + user + "/" + pwd);
        Record respOk = service.execute(config);
        assertEquals(200, respOk.getInt("status"));
    }

    @Test
    void testBearerAuth() throws Exception {
        String token = "123456789";

        Authentication auth = new Authentication();
        auth.setType(Authorization.AuthorizationType.Bearer);

        config.getDataset().getDatastore().setBase(HTTP_BIN_BASE_LATEST);
        config.getDataset().setAuthentication(auth);
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("/bearer");
        config.setStopIfNotOk(false);

        auth.setBearerToken("");
        Record respKo = service.execute(config);
        assertEquals(401, respKo.getInt("status"));

        auth.setBearerToken("token-123456789");
        Record respOk = service.execute(config);
        assertEquals(200, respOk.getInt("status"));
    }

    @Test
    void testRedirect() throws Exception {
        String redirect_url = HTTP_BIN_BASE + "/get?redirect=ok";
        config.getDataset().setResource("redirect-to?url=" + redirect_url);

        config.getDataset().setMethodType(HttpMethod.GET);

        Record resp = service.execute(config);
        assertEquals(200, resp.getInt("status"));

        JsonReader payloadReader = jsonReaderFactory.createReader(new StringReader(resp.getString("body")));
        JsonObject payload = payloadReader.readObject();

        assertEquals("ok", payload.getJsonObject("args").getString("redirect"));
    }

    @Test
    void testDigestAuth() {
        String user = "my_user";
        String pwd = "my_password";
        String qop = "auth-int";

        Basic basic = new Basic();
        basic.setUsername(user);
        basic.setPassword(pwd);

        Authentication auth = new Authentication();
        auth.setType(Authorization.AuthorizationType.Digest);
        auth.setBasic(basic);

        config.getDataset().setAuthentication(auth);
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setResource("digest-auth/"+qop+"/"+user+"/"+pwd);

        Record resp = service.execute(config);
        assertEquals(200, resp.getInt("status"));
    }

    /*
     * @Test
     * void testReadStreaming() throws Exception {
     * config.getDataset().setMethodType(HttpMethod.GET);
     * config.getDataset().setResource("/stream/10");
     * 
     * Record resp = service.execute(config);
     * assertEquals(200, resp.getInt("status"));
     * }
     * 
     * @Test
     * void testReadBytes() throws Exception {
     * config.getDataset().setMethodType(HttpMethod.GET);
     * config.getDataset().setResource("/bytes/100");
     * 
     * Record resp = service.execute(config);
     * assertEquals(200, resp.getInt("status"));
     * }
     * 
     * @Test
     * void testReadHml() throws Exception {
     * config.getDataset().setMethodType(HttpMethod.GET);
     * config.getDataset().setResource("links/10/0");
     * 
     * Record resp = service.execute(config);
     * assertEquals(200, resp.getInt("status"));
     * }
     */

}