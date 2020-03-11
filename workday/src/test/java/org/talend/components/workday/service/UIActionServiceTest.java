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
package org.talend.components.workday.service;

import java.lang.reflect.Field;
import java.util.HashMap;

import javax.json.bind.JsonbBuilder;

import org.apache.xbean.propertyeditor.PropertyEditorRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.workday.WorkdayBaseTest;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.http.HttpClientFactory;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.runtime.manager.reflect.ParameterModelService;
import org.talend.sdk.component.runtime.manager.reflect.ReflectionService;
import org.talend.sdk.component.runtime.manager.service.http.HttpClientFactoryImpl;

@HttpApi(useSsl = true)
class UIActionServiceTest extends WorkdayBaseTest {

    @Test
    void validateConnection() throws NoSuchFieldException, IllegalAccessException {

        final UIActionService service = buildService();
        final HealthCheckStatus healthCheckStatus = service.validateConnection(this.buildDataStore());
        Assertions.assertNotNull(healthCheckStatus);
        Assertions.assertEquals(HealthCheckStatus.Status.OK, healthCheckStatus.getStatus());

    }

    @Test
    void validateConnectionKO() throws NoSuchFieldException, IllegalAccessException {
        final WorkdayDataStore wds = this.buildDataStore();
        wds.setClientSecret("FAUX");
        final UIActionService service = buildService();
        final HealthCheckStatus healthCheckStatusKO = service.validateConnection(wds);
        Assertions.assertNotNull(healthCheckStatusKO);
        Assertions.assertEquals(HealthCheckStatus.Status.KO, healthCheckStatusKO.getStatus());
    }

    private UIActionService buildService() throws NoSuchFieldException, IllegalAccessException {
        UIActionService service = new UIActionService();

        final PropertyEditorRegistry propertyEditorRegistry = new PropertyEditorRegistry();
        HttpClientFactory factory = new HttpClientFactoryImpl("test",
                new ReflectionService(new ParameterModelService(propertyEditorRegistry), propertyEditorRegistry),
                JsonbBuilder.create(), new HashMap<>());

        AccessTokenProvider provider = factory.create(AccessTokenProvider.class, WorkdayBaseTest.defaultAuthenticationURL);

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

        return service;
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