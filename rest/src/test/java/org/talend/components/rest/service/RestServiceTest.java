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

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.talend.components.common.text.Substitutor;
import org.talend.components.rest.configuration.HttpMethod;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestBody;
import org.talend.components.rest.virtual.ComplexRestConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.service.RecordPointerFactoryImpl;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@WithComponents(value = "org.talend.components.rest")
public class RestServiceTest {

    @Service
    RestService service;

    @Service
    protected RecordBuilderFactory recordBuilderFactory;

    @Injected
    private BaseComponentsHandler handler;

    private ComplexRestConfiguration config;

    @BeforeEach
    void buildConfig() {
        // Inject needed services
        handler.injectServices(this);
        config = RequestConfigBuilderTest.getEmptyRequestConfig();
    }

    @Test
    void setPathParams() {
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase("");
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setConnectionTimeout(5000);
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setReadTimeout(5000);
        config.getDataset().getRestConfiguration().getDataset()
                .setResource("get/{resource}/{id}/{field}/id/{id}/resource/{resource}/end");
        config.getDataset().getRestConfiguration().getDataset().setMethodType(HttpMethod.GET);

        config.getDataset().getRestConfiguration().getDataset().setHasQueryParams(false);
        config.getDataset().getRestConfiguration().getDataset().setHasHeaders(false);

        List<String[]> paramList = new ArrayList<>();
        paramList.add(new String[] { "leads", "124", "name" });
        paramList.add(new String[] { "{leads}", "{124}", "{name}" });

        for (String[] params : paramList) {
            List<Param> pathParams = new ArrayList<>();
            pathParams.add(new Param("resource", params[0]));
            pathParams.add(new Param("id", params[1]));
            pathParams.add(new Param("field", params[2]));
            config.getDataset().getRestConfiguration().getDataset().setHasPathParams(true);
            config.getDataset().getRestConfiguration().getDataset().setPathParams(pathParams);

            String finalResource = service.setPathParams(config.getDataset().getRestConfiguration().getDataset().getResource(),
                    config.getDataset().getRestConfiguration().getDataset().isHasPathParams(),
                    config.getDataset().getRestConfiguration().pathParams());

            assertEquals("get/" + params[0] + "/" + params[1] + "/" + params[2] + "/id/" + params[1] + "/resource/" + params[0]
                    + "/end", finalResource);
        }
    }

    @Test
    void setParamsFromRecords() {
        int id = 150;
        String name = "paco";
        ZonedDateTime now = ZonedDateTime.now();

        Record record = recordBuilderFactory.newRecordBuilder() //
                .withInt("id", id) //
                .withString("name", name) //
                .withDateTime("date", now) //
                .withString("unused", "unused") //
                .build();

        List<Param> queryParams = new ArrayList<>();
        queryParams.add(new Param("id", "${/id}"));
        queryParams.add(new Param("name", "<name>${/name}</name>"));
        queryParams.add(new Param("complexe1", "<name>${/name}</name><id>${/id}</id>"));
        queryParams.add(new Param("complexe2", "<name>${/name}</name><id>${/id}</id><unexists>${/unexists:-default}</unexists>"));
        queryParams.add(new Param("complexe3",
                "<name>${/name}</name><id>${/id}</id><unexists>${/unexists:-default}</unexists><escaped>\\${escaped:-none}<escaped>"));

        config.getDataset().getRestConfiguration().getDataset().setHasQueryParams(true);
        config.getDataset().getRestConfiguration().getDataset().setQueryParams(queryParams);

        Substitutor substitutor = new Substitutor(new Substitutor.KeyFinder("${", "}"),
                new RecordDictionary(record, new RecordPointerFactoryImpl(null)));

        Map<String, String> updatedQueryParams = service
                .updateParamsFromRecord(config.getDataset().getRestConfiguration().queryParams(), substitutor);

        assertEquals("" + id, updatedQueryParams.get("id"));
        assertEquals("<name>" + name + "</name>", updatedQueryParams.get("name"));
        assertEquals("<name>" + name + "</name><id>" + id + "</id>", updatedQueryParams.get("complexe1"));
        assertEquals("<name>" + name + "</name><id>" + id + "</id><unexists>default</unexists>",
                updatedQueryParams.get("complexe2"));
        assertEquals("<name>" + name + "</name><id>" + id + "</id><unexists>default</unexists><escaped>${escaped:-none}<escaped>",
                updatedQueryParams.get("complexe3"));
    }

    @ParameterizedTest
    @CsvSource(value = { "http://www.domain.com,,http://www.domain.com", "http://www.domain.com/,,http://www.domain.com/",
            "http://www.domain.com,get,http://www.domain.com/get", "http://www.domain.com/,get,http://www.domain.com/get",
            "http://www.domain.com,/get,http://www.domain.com/get",
            "   http://www.domain.com/ ,  /get ,http://www.domain.com//get", })
    void buildUrl(final String base, final String resource, final String expected) {
        config.getDataset().getRestConfiguration().getDataset().getDatastore().setBase(base);
        config.getDataset().getRestConfiguration().getDataset().setResource(resource == null ? "   " : resource);
        config.getDataset().getRestConfiguration().getDataset().setHasPathParams(false);
        assertEquals(expected, service.buildUrl(config.getDataset().getRestConfiguration(), Collections.emptyMap()));
    }

    @Test
    void paramsFilterEmpty() {
        config.getDataset().getRestConfiguration().getDataset().setHasPathParams(true);
        config.getDataset().getRestConfiguration().getDataset()
                .setPathParams(Arrays.asList(new Param("xxx1", null), new Param("key1", "val"), new Param(null, "val"),
                        new Param("key2", "val"), new Param("", ""), new Param("key3", "")));
        config.getDataset().getRestConfiguration().getDataset().setHasQueryParams(true);
        config.getDataset().getRestConfiguration().getDataset()
                .setQueryParams(Arrays.asList(new Param("xxx1", null), new Param("key1", "val"), new Param(null, "val"),
                        new Param("key2", "val"), new Param("", ""), new Param("key3", "")));
        config.getDataset().getRestConfiguration().getDataset().setHasHeaders(true);
        config.getDataset().getRestConfiguration().getDataset()
                .setHeaders(Arrays.asList(new Param("xxx1", null), new Param("key1", "val"), new Param(null, "val"),
                        new Param("key2", "val"), new Param("", ""), new Param("key3", "")));
        config.getDataset().getRestConfiguration().getDataset().setHasBody(true);
        config.getDataset().getRestConfiguration().getDataset().getBody().setType(RequestBody.Type.FORM_DATA);
        config.getDataset().getRestConfiguration().getDataset().getBody()
                .setParams(Arrays.asList(new Param("xxx1", null), new Param("key1", "val"), new Param(null, "val"),
                        new Param("key2", "val"), new Param("", ""), new Param("key3", "")));

        config.getDataset().getRestConfiguration().pathParams().forEach((k, v) -> {
            assertTrue(k != null);
            assertFalse(k.isEmpty());
            assertFalse(v == null);
        });
        assertEquals(4, config.getDataset().getRestConfiguration().pathParams().size());

        config.getDataset().getRestConfiguration().queryParams().forEach((k, v) -> {
            assertTrue(k != null);
            assertFalse(k.isEmpty());
            assertFalse(v == null);
        });
        assertEquals(4, config.getDataset().getRestConfiguration().queryParams().size());

        config.getDataset().getRestConfiguration().headers().forEach((k, v) -> {
            assertTrue(k != null);
            assertFalse(k.isEmpty());
            assertFalse(v == null);
        });
        assertEquals(4, config.getDataset().getRestConfiguration().headers().size());

        config.getDataset().getRestConfiguration().getDataset().getBody().getParams().forEach(p -> {
            assertTrue(p != null);
            assertFalse(p.getKey().isEmpty());
            assertFalse(p.getValue() == null);
        });
        assertEquals(4, config.getDataset().getRestConfiguration().getDataset().getBody().getParams().size());
    }

    @Test
    void paramsTransformNullToEmpty() {
        config.getDataset().getRestConfiguration().getDataset().setHasPathParams(true);
        config.getDataset().getRestConfiguration().getDataset()
                .setPathParams(Arrays.asList(new Param("xxx1", null), new Param("key1", "val"), new Param(null, "val"),
                        new Param("key2", "val"), new Param("", ""), new Param("key3", "")));
        config.getDataset().getRestConfiguration().getDataset().setHasQueryParams(true);
        config.getDataset().getRestConfiguration().getDataset()
                .setQueryParams(Arrays.asList(new Param("xxx1", null), new Param("key1", "val"), new Param(null, "val"),
                        new Param("key2", "val"), new Param("", ""), new Param("key3", "")));
        config.getDataset().getRestConfiguration().getDataset().setHasHeaders(true);
        config.getDataset().getRestConfiguration().getDataset()
                .setHeaders(Arrays.asList(new Param("xxx1", null), new Param("key1", "val"), new Param(null, "val"),
                        new Param("key2", "val"), new Param("", ""), new Param("key3", "")));
        config.getDataset().getRestConfiguration().getDataset().setHasBody(true);
        config.getDataset().getRestConfiguration().getDataset().getBody().setType(RequestBody.Type.FORM_DATA);
        config.getDataset().getRestConfiguration().getDataset().getBody()
                .setParams(Arrays.asList(new Param("xxx1", null), new Param("key1", "val"), new Param(null, "val"),
                        new Param("key2", "val"), new Param("", ""), new Param("key3", "")));

        assertEquals("", config.getDataset().getRestConfiguration().pathParams().get("xxx1"));
        assertEquals("", config.getDataset().getRestConfiguration().queryParams().get("xxx1"));
        assertEquals("", config.getDataset().getRestConfiguration().headers().get("xxx1"));

        config.getDataset().getRestConfiguration().getDataset().getBody().getParams().stream().forEach(p -> {
            assertFalse(p.getValue() == null);
            if ("xxx1".equals(p.getKey())) {
                assertTrue(p.getValue().isEmpty());
            }
        });

    }

    @Test
    void testHasNoDuplicates() {
        List<Param> withDuplciates = Arrays.asList(new Param("xxx1", null), new Param("key1", "val"), new Param("xxx1", "val"),
                new Param("key2", "val"), new Param("", ""), new Param("key3", ""));
        List<Param> withoutDuplciates = Arrays.asList(new Param("xxx1", null), new Param("key1", "val"), new Param("xxx2", "val"),
                new Param("key2", "val"), new Param("", ""), new Param("key3", ""));

        assertTrue(service.hasNoDuplicates(null));
        assertTrue(service.hasNoDuplicates(withoutDuplciates));
        assertFalse(service.hasNoDuplicates(withDuplciates));
    }

}
