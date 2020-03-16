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

import java.util.HashMap;

import javax.json.bind.JsonbBuilder;

import org.apache.xbean.propertyeditor.PropertyEditorRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.workday.WorkdayBaseTest;
import org.talend.components.workday.WorkdayException;
import org.talend.components.workday.datastore.Token;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.sdk.component.api.service.http.HttpClientFactory;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.runtime.manager.reflect.ParameterModelService;
import org.talend.sdk.component.runtime.manager.reflect.ReflectionService;
import org.talend.sdk.component.runtime.manager.service.http.HttpClientFactoryImpl;

@HttpApi(useSsl = true)
class AccessTokenProviderTest extends WorkdayBaseTest {

    @Test
    void getAccessToken() {
        final PropertyEditorRegistry propertyEditorRegistry = new PropertyEditorRegistry();
        HttpClientFactory factory = new HttpClientFactoryImpl("test",
                new ReflectionService(new ParameterModelService(propertyEditorRegistry), propertyEditorRegistry),
                JsonbBuilder.create(), new HashMap<>());

        AccessTokenProvider provider = factory.create(AccessTokenProvider.class, WorkdayBaseTest.defaultAuthenticationURL);
        WorkdayDataStore wds = this.buildDataStore();

        Token tk = provider.getAccessToken(wds);
        Assertions.assertNotNull(tk);
        Assertions.assertEquals("Bearer", tk.getTokenType());
    }

    @Test
    void getAccessTokenError() {
        final PropertyEditorRegistry propertyEditorRegistry = new PropertyEditorRegistry();
        HttpClientFactory factory = new HttpClientFactoryImpl("test",
                new ReflectionService(new ParameterModelService(propertyEditorRegistry), propertyEditorRegistry),
                JsonbBuilder.create(), new HashMap<>());

        AccessTokenProvider provider = factory.create(AccessTokenProvider.class, WorkdayBaseTest.defaultAuthenticationURL);

        WorkdayDataStore wds = this.buildDataStore();
        wds.setClientSecret("fautSecret");

        Assertions.assertThrows(WorkdayException.class, () -> provider.getAccessToken(wds));
    }
}