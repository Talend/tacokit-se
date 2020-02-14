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
package org.talend.components.google.storage.service;

import org.talend.components.google.storage.datastore.GSDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class GSService {

    public static final String ACTION_HEALTH_CHECK = "GOOGLE_STORAGE_HEALTH_CHECK";

    @Service
    private I18nMessage i18n;

    @Service
    private CredentialService credentialService;

    @HealthCheck(ACTION_HEALTH_CHECK)
    public HealthCheckStatus healthCheck(@Option GSDataStore connection) {

        if (connection.getJsonCredentials() == null || "".equals(connection.getJsonCredentials().trim())) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.credentialsRequired());
        }
        try {
            final GoogleCredentials credentials = credentialService.getCredentials(connection);
            final Storage storage = credentialService.newStorage(credentials);
            storage.list();
            return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.successConnection());
        } catch (final Exception e) {
            log.error("[HealthCheckStatus] {}", e.getMessage());
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.errorConnection(e.getMessage()));
        }

    }
}
