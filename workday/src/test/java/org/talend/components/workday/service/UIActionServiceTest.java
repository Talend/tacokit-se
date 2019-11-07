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
package org.talend.components.workday.service;

import org.apache.xbean.propertyeditor.PropertyEditorRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.http.HttpClientFactory;
import org.talend.sdk.component.runtime.manager.reflect.ParameterModelService;
import org.talend.sdk.component.runtime.manager.reflect.ReflectionService;
import org.talend.sdk.component.runtime.manager.service.http.HttpClientFactoryImpl;

import javax.json.bind.JsonbBuilder;
import java.lang.reflect.Field;
import java.util.HashMap;

class UIActionServiceTest {

    @Test
    void validateConnection() throws NoSuchFieldException, IllegalAccessException {
        UIActionService service = new UIActionService();

        final PropertyEditorRegistry propertyEditorRegistry = new PropertyEditorRegistry();
        HttpClientFactory factory = new HttpClientFactoryImpl("test",
                new ReflectionService(new ParameterModelService(propertyEditorRegistry), propertyEditorRegistry),
                JsonbBuilder.create(), new HashMap<>());

        AccessTokenProvider provider = factory.create(AccessTokenProvider.class, "https://auth.api.workday.com");
        WorkdayDataStore wds = ConfigHelper.buildDataStore();

        I18n intern = new I18n() {

            @Override
            public String healthCheckOk() {
                return "OK";
            }

            @Override
            public String healthCheckFailed(String msg, String cause) {
                return "KO";
            }
        };

        final Field serviceField = UIActionService.class.getDeclaredField("service");
        serviceField.setAccessible(true);
        serviceField.set(service, provider);

        final Field internationalField = UIActionService.class.getDeclaredField("i18n");
        internationalField.setAccessible(true);
        internationalField.set(service, intern);

        final HealthCheckStatus healthCheckStatus = service.validateConnection(wds);
        Assertions.assertNotNull(healthCheckStatus);
        Assertions.assertEquals(HealthCheckStatus.Status.OK, healthCheckStatus.getStatus());

        wds.setClientSecret("FAUX");
        final HealthCheckStatus healthCheckStatusKO = service.validateConnection(wds);
        Assertions.assertNotNull(healthCheckStatusKO);
        Assertions.assertEquals(HealthCheckStatus.Status.KO, healthCheckStatusKO.getStatus());
    }

    @Test
    void validateEndpoint() {
        UIActionService service = new UIActionService();

        final ValidationResult result = service.validateEndpoint("http://correct/url");
        Assertions.assertNotNull(result);
        Assertions.assertEquals(result.getStatus(), ValidationResult.Status.OK);

        final ValidationResult resultKO = service.validateEndpoint("http!//correct/url");
        Assertions.assertNotNull(resultKO);
        Assertions.assertEquals(resultKO.getStatus(), ValidationResult.Status.KO);
    }
}