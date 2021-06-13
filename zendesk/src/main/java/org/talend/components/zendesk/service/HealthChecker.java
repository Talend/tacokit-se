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
package org.talend.components.zendesk.service;

import java.io.Serializable;

import org.talend.components.zendesk.common.ZendeskDataStore;
import org.talend.components.zendesk.service.http.ZendeskHttpClientService;
import org.talend.sdk.component.api.service.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class HealthChecker implements Serializable {

    @Service
    private ZendeskHttpClientService zendeskHttpClientService;

    public boolean checkHealth(ZendeskDataStore dataStore) {
        Long userId = zendeskHttpClientService.getCurrentUser(dataStore).getId();
        return userId != null;
    }
}