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
package org.talend.component.common.service.http;

import org.apache.xbean.propertyeditor.PropertyEditorRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.service.http.HttpClientFactory;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.runtime.manager.reflect.ParameterModelService;
import org.talend.sdk.component.runtime.manager.reflect.ReflectionService;
import org.talend.sdk.component.runtime.manager.service.http.HttpClientFactoryImpl;

import javax.json.JsonObject;
import javax.json.bind.JsonbBuilder;
import java.util.Collections;
import java.util.HashMap;

class DigestClientTest {

    private HttpClientFactory clientFactory;

    {
        PropertyEditorRegistry propertyEditorRegistry = new PropertyEditorRegistry();
        clientFactory = new HttpClientFactoryImpl("test",
                new ReflectionService(new ParameterModelService(propertyEditorRegistry), propertyEditorRegistry),
                JsonbBuilder.create(), Collections.emptyMap());
    }

    private DigestClient client;

    @BeforeEach
    void client() {
        client = clientFactory.create(DigestClient.class, null);
    }

    @Test
    void digestAuth() {
        Response<JsonObject> realm = client.getRealm("GET", "http://httpbin.org/digest-auth/qop-value/user-value/pwd-value",
                new HashMap<>());
    }
}
