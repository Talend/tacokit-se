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
package org.talend.components.marketo.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.talend.components.marketo.MarketoBaseTest;
import org.talend.components.marketo.MarketoBaseTestIT;
import org.talend.components.marketo.datastore.MarketoDataStore;
import org.talend.sdk.component.api.DecryptedServer;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.WithMavenServers;
import org.talend.sdk.component.maven.Server;

import lombok.extern.slf4j.Slf4j;

@TestInstance(Lifecycle.PER_CLASS)
@Slf4j
@WithMavenServers
@WithComponents("org.talend.components.marketo")
public class UIActionServiceTestIT extends MarketoBaseTestIT {

    @Service
    protected UIActionService service;

    @Test
    void getListNamesExceedingBatchLimit() {
        SuggestionValues lists = service.getListNames(dataStore);
        Assertions.assertNotNull(lists);
        log.info("[getListNamesExceedingBatchLimit] list count: {}", lists.getItems().size());
        Assertions.assertNotEquals(0, lists.getItems().size());
        Assertions.assertTrue(lists.getItems().size() > 300);
    }

}
