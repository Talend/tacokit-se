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
package org.talend.components.workday.input;

import org.apache.xbean.propertyeditor.PropertyEditorRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.talend.components.workday.WorkdayException;
import org.talend.components.workday.dataset.WQLDataSet;
import org.talend.components.workday.service.*;
import org.talend.sdk.component.api.service.http.HttpClientFactory;
import org.talend.sdk.component.runtime.manager.reflect.ParameterModelService;
import org.talend.sdk.component.runtime.manager.reflect.ReflectionService;
import org.talend.sdk.component.runtime.manager.service.http.HttpClientFactoryImpl;

import javax.json.JsonObject;
import javax.json.bind.JsonbBuilder;
import java.lang.reflect.Field;
import java.util.HashMap;

class WQLProducerTest {

    private static WQLDataSet dataset;

    private static WQLConfiguration cfg;

    private static WorkdayReaderService service;

    @BeforeAll
    private static void init() throws NoSuchFieldException, IllegalAccessException {
        final PropertyEditorRegistry propertyEditorRegistry = new PropertyEditorRegistry();
        HttpClientFactory factory = new HttpClientFactoryImpl("test",
                new ReflectionService(new ParameterModelService(propertyEditorRegistry), propertyEditorRegistry),
                JsonbBuilder.create(), new HashMap<>());
        final WorkdayReader reader = factory.create(WorkdayReader.class, "https://api.workday.com");
        service = new WorkdayReaderService();
        service.setReader(reader);

        final AccessTokenProvider provider = factory.create(AccessTokenProvider.class, "https://auth.api.workday.com");
        AccessTokenService providerService = new AccessTokenService();
        final Field serviceField = AccessTokenService.class.getDeclaredField("service");
        serviceField.setAccessible(true);
        serviceField.set(providerService, provider);

        cfg = new WQLConfiguration();
        dataset = new WQLDataSet();
        WQLProducerTest.dataset.setDatastore(ConfigHelper.buildDataStore());
        WQLProducerTest.dataset.getDatastore().setEndpoint("https://api.workday.com");
        cfg.setDataSet(dataset);
        service.setAccessToken(providerService);
    }

    @Test
    void nextOK() {
        String query = "SELECT accountCurrency, bankAccountSecuritySegment, priorDayAccountBalance " + "FROM financialAccounts";
        dataset.setQuery(query);

        WQLProducer producer = new WQLProducer(cfg, service);
        JsonObject o = producer.next();
        Assertions.assertNotNull(o);
    }

    @Test
    void nextError() {
        String query = "SELECT accountCurrency, bankAccountSecuritySegment, priorDayAccountBalance "
                + "FROM UnkownfinancialAccounts";
        dataset.setQuery(query);
        WQLProducer producer = new WQLProducer(cfg, service);

        Assertions.assertThrows(WorkdayException.class, producer::next);
    }
}