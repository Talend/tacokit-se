package org.talend.components.rest.service;

import com.sun.org.apache.regexp.internal.RECompiler;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    @Service
    protected RecordBuilderFactory recordBuilderFactory;

    @Injected
    private BaseComponentsHandler handler;

    private RequestConfig config;

    @BeforeEach
    void buildConfig() {
        // Inject needed services
        handler.injectServices(this);
        config = RequestConfigBuilder.getEmptyRequestConfig();
    }

    @Test
    void setPahParams() throws Exception {
        config.getDataset().getDatastore().setBase("");
        config.getDataset().setConnectionTimeout(5000);
        config.getDataset().setReadTimeout(5000);
        config.getDataset().setResource("get/{resource}/{id}/{field}/id/{id}/resource/{resource}/end");
        config.getDataset().setMethodType(HttpMethod.GET);

        config.getDataset().setHasQueryParams(false);
        config.getDataset().setHasHeaders(false);

        List<String[]> paramList = new ArrayList<>();
        paramList.add(new String[] { "leads", "124", "name" });
        paramList.add(new String[] { "{leads}", "{124}", "{name}" });

        for (String[] params : paramList) {
            List<Param> pathParams = new ArrayList<>();
            pathParams.add(new Param("resource", params[0]));
            pathParams.add(new Param("id", params[1]));
            pathParams.add(new Param("field", params[2]));
            config.getDataset().setHasPathParams(true);
            config.getDataset().setPathParams(pathParams);

            String finalResource = service.setPathParams(config.getDataset().getResource(),
                    config.getDataset().getHasPathParams(), config.pathParams());

            assertEquals("get/" + params[0] + "/" + params[1] + "/" + params[2] + "/id/" + params[1] + "/resource/" + params[0]
                    + "/end", finalResource);
        }
    }

    @Test
    void setParamsFromRecords() throws Exception {
        int id = 150;
        String name = "paco";
        ZonedDateTime now = ZonedDateTime.now();

        Record record = recordBuilderFactory.newRecordBuilder() //
                .withInt("id", id).withString("name", name) //
                .withDateTime("date", now) //
                .withString("unused", "unused") //
                .build();

        List<Param> queryParams = new ArrayList<>();
        queryParams.add(new Param("id", "${id}"));
        queryParams.add(new Param("name", "<name>${name}</name>"));
        queryParams.add(new Param("complexe", "<name>${name}</name><id>${id}</id><unexists>${unexists}</unexists>"));
        config.getDataset().setHasQueryParams(true);
        config.getDataset().setQueryParams(queryParams);

        Map<String, String> updatedQueryParams = service.updateParamsFromRecord(config.queryParams(), record);

        assertEquals("" + id, updatedQueryParams.get("id"));

    }

}
