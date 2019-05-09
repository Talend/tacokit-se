package org.talend.components.rest.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
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

    private final static String HTTP_BIN_BASE = "http://tal-rd22.talend.lan:8084";

    private final static String DONT_CHECK = "%DONT_CHECK%";

    /*
     * static {
     * System.setProperty("talend.junit.http.capture", "true");
     * }
     */

    /*
     * @Service
     * Client client;
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
        config = RequestConfigBuilder.getEmptyRequestConfig();
    }

    @Test
    void httpbinGet() throws Exception {
        // Inject needed services
        handler.injectServices(this);

        Client client = service.getClient();

        config.getDataset().getDatastore().setBase(HTTP_BIN_BASE);
        config.getDataset().setResource("get");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setConnectionTimeout(5000);
        config.getDataset().setReadTimeout(5000);

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
     * @throws Exception
     */
    @Test
    void testParamsDisabled() throws Exception {
        // Inject needed services
        handler.injectServices(this);

        Client client = service.getClient();

        config.getDataset().getDatastore().setBase(HTTP_BIN_BASE);
        config.getDataset().setConnectionTimeout(5000);
        config.getDataset().setReadTimeout(5000);

        HttpMethod[] verbs = {HttpMethod.DELETE, HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT};
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
        // Inject needed services
        handler.injectServices(this);

        Client client = service.getClient();

        config.getDataset().getDatastore().setBase(HTTP_BIN_BASE);
        config.getDataset().setConnectionTimeout(5000);
        config.getDataset().setReadTimeout(5000);

        HttpMethod[] verbs = {HttpMethod.DELETE, HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT};
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
    void testPathParams() throws Exception {
        // Inject needed services
        handler.injectServices(this);

        Client client = service.getClient();

        config.getDataset().getDatastore().setBase(HTTP_BIN_BASE);
        config.getDataset().setConnectionTimeout(5000);
        config.getDataset().setReadTimeout(5000);
        config.getDataset().setResource("get/{resource}/{id}/{field}");
        config.getDataset().setMethodType(HttpMethod.GET);

        config.getDataset().setHasQueryParams(false);
        config.getDataset().setHasHeaders(false);

        List<Param> pathParams = new ArrayList<>();
        pathParams.add(new Param("resource", "leads"));
        pathParams.add(new Param("id", "124"));
        pathParams.add(new Param("field", "name"));
        config.getDataset().setHasPathParams(true);
        config.getDataset().setPathParams(pathParams);

        Record resp = service.execute(config);

        assertEquals(200, resp.getInt("status"));

        JsonReader payloadReader = jsonReaderFactory.createReader(new StringReader(resp.getString("body")));
        JsonObject payload = payloadReader.readObject();

        assertEquals("", payload.getString("url"));

    }

}