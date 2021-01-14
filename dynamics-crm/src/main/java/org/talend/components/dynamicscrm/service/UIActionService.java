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
package org.talend.components.dynamicscrm.service;

import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.KO;
import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.OK;

import java.util.List;
import java.util.stream.Collectors;

import javax.naming.AuthenticationException;

import org.talend.components.dynamicscrm.datastore.DynamicsCrmConnection;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class UIActionService {

    public static final String ACTION_HEALTHCHECK_DYNAMICS365 = "ACTION_HEALTHCHECK_DYNAMICS365";

    public static final String ACTION_ENTITY_SETS_DYNAMICS365 = "ACTION_ENTITY_SETS_DYNAMICS365";

    @Service
    private I18n i18n;

    @Service
    private DynamicsCrmService service;

    @HealthCheck(ACTION_HEALTHCHECK_DYNAMICS365)
    public HealthCheckStatus validateConnection(@Option final DynamicsCrmConnection connection) {
        try {
            service.createClient(connection, null);
        } catch (AuthenticationException e) {
            return new HealthCheckStatus(KO, i18n.authenticationFailed(e.getMessage()));
        } catch (Exception e) {
            return new HealthCheckStatus(KO, i18n.connectionFailed(e.getMessage()));
        }
        return new HealthCheckStatus(OK, i18n.healthCheckOk());
    }

    @Suggestions(ACTION_ENTITY_SETS_DYNAMICS365)
    public SuggestionValues entitySetsList(@Option final DynamicsCrmConnection connection) throws AuthenticationException {
        List<SuggestionValues.Item> items = service.getEntitySetNames(connection).stream()
                .map(s -> new SuggestionValues.Item(s, s)).collect(Collectors.toList());
        return new SuggestionValues(true, items);
    }
}
