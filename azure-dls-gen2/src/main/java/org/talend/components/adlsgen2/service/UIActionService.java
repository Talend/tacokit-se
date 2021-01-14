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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection.AuthMethod;
import org.talend.components.adlsgen2.datastore.Constants;
import org.talend.components.adlsgen2.runtime.AdlsDatastoreRuntimeInfo;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues.Item;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import lombok.extern.slf4j.Slf4j;
import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.KO;
import static org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus.Status.OK;

@Slf4j
@Service
public class UIActionService {

    public static final String ACTION_HEALTHCHECK = "ACTION_HEALTHCHECK";

    public static final String ACTION_FILESYSTEMS = "ACTION_FILESYSTEMS";

    @Service
    private AdlsGen2Service service;

    @Service
    private AdlsActiveDirectoryService activeDirectoryService;

    @Service
    private I18n i18n;

    @HealthCheck(ACTION_HEALTHCHECK)
    public HealthCheckStatus validateConnection(@Option final AdlsGen2Connection connection) {
        AdlsDatastoreRuntimeInfo connectionRuntimeInfo = new AdlsDatastoreRuntimeInfo(connection, activeDirectoryService);
        try {
            service.filesystemList(connectionRuntimeInfo);
        } catch (Exception e) {
            String msg;
            if (connection.getAuthMethod() == AuthMethod.SAS) {
                msg = i18n.healthCheckSAS();
            } else if (connection.getAuthMethod() == AuthMethod.SharedKey) {
                msg = i18n.healthCheckSharedKey();
            } else {
                msg = i18n.healthCheckActiveDirectory();
            }
            return new HealthCheckStatus(KO, i18n.healthCheckFailed(msg, e.getMessage()));
        }
        return new HealthCheckStatus(OK, i18n.healthCheckOk());
    }

    @Suggestions(ACTION_FILESYSTEMS)
    public SuggestionValues filesystemList(@Option final AdlsGen2Connection connection) {
        AdlsDatastoreRuntimeInfo connectionRuntimeInfo = new AdlsDatastoreRuntimeInfo(connection, activeDirectoryService);
        List<Item> items = new ArrayList<>();
        for (String s : service.filesystemList(connectionRuntimeInfo)) {
            items.add(new SuggestionValues.Item(s, s));
        }
        return new SuggestionValues(false, items);
    }
}
