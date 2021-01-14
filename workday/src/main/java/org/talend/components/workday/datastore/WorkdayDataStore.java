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
package org.talend.components.workday.datastore;

import lombok.Data;
import org.talend.components.workday.service.UIActionService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Base64;

@Data
@DataStore(WorkdayDataStore.NAME)
@GridLayout({ //
        @GridLayout.Row({ "clientId", "clientSecret" }), //
        @GridLayout.Row({ "tenantAlias" }) //
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row("authEndpoint"), @GridLayout.Row("endpoint") })
@Checkable(UIActionService.HEALTH_CHECK)
@Documentation(WorkdayDataStore.NAME)
public class WorkdayDataStore implements Serializable {

    private static final long serialVersionUID = -8628647674176772061L;

    public static final String NAME = "WorkdayDataStore";

    @Option
    @Validable(UIActionService.VALIDATION_URL_PROPERTY)
    @Documentation("Workday token Auth Endpoint (host only, ie: https://auth.api.workday.com/v1/token)")
    private String authEndpoint = "https://auth.api.workday.com";

    @Option
    @Documentation("Workday Client Id")
    private String clientId;

    @Option
    @Credential
    @Documentation("Workday Client Secret")
    private String clientSecret;

    @Option
    @Documentation("Workday endpoint for REST services")
    private String endpoint = "https://api.workday.com";

    @Option
    @Documentation("Workday tenant alias")
    private String tenantAlias;

    private transient Token token = null;

    public String getAuthorizationHeader() {
        final String idSecret = this.clientId + ':' + this.clientSecret;
        final String idForHeader = Base64.getEncoder().encodeToString(idSecret.getBytes(Charset.defaultCharset()));
        return "Basic " + idForHeader;
    }

}
