/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.adlsgen2.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import javax.json.Json;
import javax.json.JsonObject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.adlsgen2.ClientGen2Fake;
import org.talend.components.adlsgen2.FakeResponse;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection.AuthMethod;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues.Item;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.ComponentManager;

@WithComponents("org.talend.components.adlsgen2")
class UIActionServiceTest extends AdlsGen2TestBase {

    @Injected
    private BaseComponentsHandler componentsHandler;

    private UIActionService uiActionService;

    @BeforeEach
    protected void setUp() throws Exception {
        super.setUp();

        final ComponentManager manager = componentsHandler.asManager();
        final JsonObject filesystems = Json.createObjectBuilder()
                .add("filesystems", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder().add("etag", "0x8D89D1980D8BD4B").add("name", "ct1").build()).build())
                .build();
        ClientGen2Fake fake = new ClientGen2Fake(new FakeResponse<>(200, filesystems, null, null));
        ClientGen2Fake.inject(manager, fake);

        this.uiActionService = this.componentsHandler.findService(UIActionService.class);
    }

    @Test
    void filesystemListSuggestions() {
        connection.setAuthMethod(AuthMethod.SAS);
        SuggestionValues fs = this.uiActionService.filesystemList(connection);
        assertNotNull(fs);
        for (Item i : fs.getItems()) {
            assertNotNull(i);
        }
    }

    @Test
    void testHealthCheck() {
        connection.setAuthMethod(AuthMethod.SAS);
        final HealthCheckStatus status = this.uiActionService.validateConnection(connection);
        assertEquals(HealthCheckStatus.Status.OK, status.getStatus());
    }

}
