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
@HttpApi(useSsl = true)
@WithComponents(value = "org.talend.components.rest")
class ClientTest {

    static {
        System.setProperty("talend.junit.http.capture", "true");
    }

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

        config.getDataset().getDatastore().setBase("https://httpbin.org");
        config.getDataset().setResource("get");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setConnectionTimeout(5000);
        config.getDataset().setReadTimeout(5000);

        Record resp = service.execute(config);

        assertEquals(200, resp.getInt("status"));

        List<Record> headers = Collections.unmodifiableList(new ArrayList<>(resp.getArray(Record.class, "headers")));

        Map<String, String> headerToCheck = new HashMap<>();
        headerToCheck.put("X-Frame-Options", "DENY");
        headerToCheck.put("Referrer-Policy", "no-referrer-when-downgrade");
        headerToCheck.put("Server", "nginx");
        headerToCheck.put("Access-Control-Allow-Origin", "*");
        headerToCheck.put("X-Content-Type-Options", "nosniff");
        headerToCheck.put("Access-Control-Allow-Credentials", "true");
        headerToCheck.put("Connection", "keep-alive");
        headerToCheck.put("X-XSS-Protection", "1; mode=block");
        headerToCheck.put("Content-Length", "252");
        headerToCheck.put("Content-Type", "application/json");
        headerToCheck.put("X-Talend-Proxy-JUnit", "true"); // added by the unit test http proxy

        headers.forEach(e -> {
            assertEquals(headerToCheck.get(e.getString("key")), e.getString("value"));
            headerToCheck.remove(e.getString("key"));
        });
        assertEquals(headerToCheck.size(), 0);

        String bodyS = resp.getString("body");
        JsonObject bodyJson = jsonReaderFactory.createReader(new ByteArrayInputStream((bodyS == null ? "" : bodyS).getBytes()))
                .readObject();

        assertEquals(JsonValue.ValueType.OBJECT, bodyJson.getJsonObject("args").getValueType());

        Map<String, String> headersValid = new HashMap<>();
        headersValid.put("Accept", "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2");
        headersValid.put("Host", "httpbin.org");
        headersValid.put("User-Agent", "Java/1.8.0_211");
        JsonObject headersJson = bodyJson.getJsonObject("headers");
        headersJson.keySet().stream().forEach(k -> assertEquals(headersValid.get(k), headersJson.getString(k)));

        assertEquals("84.14.92.154, 84.14.92.154", bodyJson.getString("origin"));
        assertEquals("https://httpbin.org/get", bodyJson.getString("url"));
    }

    @Test
    void httpbinGetWithQueryParams() throws Exception {
        // Inject needed services
        handler.injectServices(this);

        Client client = service.getClient();

        config.getDataset().getDatastore().setBase("https://httpbin.org");
        config.getDataset().setResource("get");
        config.getDataset().setMethodType(HttpMethod.GET);
        config.getDataset().setConnectionTimeout(5000);
        config.getDataset().setReadTimeout(5000);

        List<Param> queryParams = new ArrayList<>();
        queryParams.add(new Param("params1", "value1"));
        queryParams.add(new Param("params2", "<name>Dupont & Dupond</name>"));
        config.getDataset().setQueryParams(queryParams);

        Record resp = service.execute(config);

        assertEquals(200, resp.getInt("status"));

        JsonReader payloadReader = jsonReaderFactory.createReader(new StringReader(resp.getString("body")));
        JsonObject payload = payloadReader.readObject();

        assertEquals("value1", payload.getJsonObject("args").getString("params1"));
        assertEquals("<name>Dupont & Dupond</name>", payload.getJsonObject("args").getString("params2"));

    }

}