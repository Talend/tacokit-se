package org.talend.components.rest.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.talend.components.rest.configuration.Dataset;
import org.talend.components.rest.configuration.Datastore;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.configuration.auth.Authentication;
import org.talend.components.rest.configuration.auth.Authorization;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonString;
import javax.json.JsonValue;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@HttpApi(useSsl = true)
@WithComponents(value = "org.talend.components.rest")
class ClientTest extends BaseTest {

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

    @Test
    void getJSon() throws Exception {
        // Inject needed services
        handler.injectServices(this);

        Client client = service.getClient();

        RequestConfig config = new RequestConfig();
        Datastore dso = new Datastore();
        dso.setBase("https://httpbin.org");

        Authentication auth = new Authentication();
        auth.setType(Authorization.AuthorizationType.NoAuth);

        Dataset dse = new Dataset();
        dse.setResource("get");
        dse.setDatastore(dso);
        dse.setAuthentication(auth);
        dse.setMethodType(HttpMethod.GET);
        dse.setConnectionTimeout(1000);
        dse.setReadTimeout(1000);

        RequestBody body = new RequestBody();
        body.setType(RequestBody.Type.RAW);

        config.setDataset(dse);
        config.getDataset().setBody(body);
        config.getDataset().setHasHeaders(false);
        config.getDataset().setHasQueryParam(false);
        config.getDataset().setHasQueryParam(false);

        Record resp = service.execute(config);

        assertEquals(200, resp.getInt("status"));

        List<Record> headers = Collections.unmodifiableList(new ArrayList<>(resp.getArray(Record.class, "headers")));

        Map<String, String> headerToCheck = new HashMap<>();
        headerToCheck.put("Server", "nginx");
        headerToCheck.put("Access-Control-Allow-Origin", "*");
        headerToCheck.put("Access-Control-Allow-Credentials", "true");
        headerToCheck.put("Connection", "keep-alive");
        headerToCheck.put("Content-Length", "252");
        headerToCheck.put("Content-Type", "application/json");
        headerToCheck.put("X-Talend-Proxy-JUnit", "true"); // added by the unit test http proxy
        headers.forEach(e -> assertEquals(headerToCheck.get(e.getString("key")), e.getString("value")));

        String bodyS = resp.getString("body");
        JsonObject bodyJson = jsonReaderFactory.createReader(new ByteArrayInputStream((bodyS == null ? "" : bodyS).getBytes()))
                .readObject();

        assertEquals(JsonValue.ValueType.OBJECT, bodyJson.getJsonObject("args").getValueType());

        Map<String, String> headersValid = new HashMap<>();
        headersValid.put("Accept", "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2");
        headersValid.put("Host", "httpbin.org");
        headersValid.put("User-Agent", "Java/1.8.0_131");
        JsonObject headersJson = bodyJson.getJsonObject("headers");
        headersJson.keySet().stream().forEach(k -> assertEquals(headersValid.get(k), headersJson.getString(k)));

        //JsonObject originJson = bodyJson.getJsonObject("origin");
        assertEquals("93.24.102.140, 93.24.102.140", bodyJson.getString("origin"));
        assertEquals("https://httpbin.org/get", bodyJson.getString("url"));
    }

}