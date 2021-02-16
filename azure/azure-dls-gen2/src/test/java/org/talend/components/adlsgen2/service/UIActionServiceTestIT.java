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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection.AuthMethod;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues.Item;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@WithComponents("org.talend.components.adlsgen2")
class UIActionServiceTestIT extends AdlsGen2TestBase {

    @Service
    UIActionService ui;

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    void filesystemListSuggestions(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        SuggestionValues fs = ui.filesystemList(connection);
        assertNotNull(fs);
        for (Item i : fs.getItems()) {
            assertNotNull(i);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    void testHealthCheck(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        assertEquals(HealthCheckStatus.Status.OK, ui.validateConnection(connection).getStatus());
    }

}
