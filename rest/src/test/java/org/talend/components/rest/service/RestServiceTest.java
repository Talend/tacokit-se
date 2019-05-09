package org.talend.components.rest.service;


import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@WithComponents(value = "org.talend.components.rest")
public class RestServiceTest {

    /*
     * @Service
     * Client client;
     */
    @Service
    RestService service;

    @Injected
    private BaseComponentsHandler handler;

    private RequestConfig config;


    @BeforeEach
    void buildConfig() {
        config = RequestConfigBuilder.getEmptyRequestConfig();
    }


    @Test
    void setPahParams() throws Exception {
        // Inject needed services
        handler.injectServices(this);

        config.getDataset().getDatastore().setBase("");
        config.getDataset().setConnectionTimeout(5000);
        config.getDataset().setReadTimeout(5000);
        config.getDataset().setResource("get/{resource}/{id}/{field}/id/{id}/resource/{resource}/end");
        config.getDataset().setMethodType(HttpMethod.GET);

        config.getDataset().setHasQueryParams(false);
        config.getDataset().setHasHeaders(false);

        List<Param> pathParams = new ArrayList<>();
        pathParams.add(new Param("resource", "leads"));
        pathParams.add(new Param("id", "124"));
        pathParams.add(new Param("field", "name"));
        config.getDataset().setHasPathParams(true);
        config.getDataset().setPathParams(pathParams);

        String finalResource = service.setPathParams(config.getDataset().getResource(), config.getDataset().getHasPathParams(), config.pathParams());

        assertEquals("get/leads/124/name/id/124/resource/leads/end", finalResource);

    }

}
