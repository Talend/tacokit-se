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
package org.talend.components.rest.source;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.components.rest.service.Client;
import org.talend.components.rest.service.RequestConfigBuilder;
import org.talend.components.rest.service.RestService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonReaderFactory;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@WithComponents(value = "org.talend.components.rest")
class RestSinkTest {

    private final static String HTTP_BIN_BASE = "http://tal-rd22.talend.lan:8084";

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

        config.getDataset().getDatastore().setBase(HTTP_BIN_BASE);
        config.getDataset().setConnectionTimeout(5000);
        config.getDataset().setReadTimeout(5000);
    }

    @Test
    void testOutput() {

        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components() //
                .component("emitter", "test://emitter") //
                .component("out", "Rest://Output?" + configStr) //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();
    }

}