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
package org.talend.components.adlsgen2.datastore;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

import static org.talend.components.adlsgen2.service.UIActionService.ACTION_HEALTHCHECK;

@Data
@DataStore("AdlsGen2Connection")
@Checkable(ACTION_HEALTHCHECK)
@GridLayout({ //
        @GridLayout.Row("authMethod"), //
        @GridLayout.Row("accountName"), //
        @GridLayout.Row("sharedKey"), //
        @GridLayout.Row("tenantId"), //
        @GridLayout.Row({ "clientId", "clientSecret" }), //
        @GridLayout.Row("sas"), //
})
@Documentation("The datastore to connect Azure Data Lake Storage Gen2")
public class AdlsGen2Connection implements Serializable {

    @Option
    @Required
    @Documentation("Storage Account Name")
    private String accountName;

    @Option
    @Required
    @Documentation("Authentication method")
    private AuthMethod authMethod;

    @Option
    // @Required
    @ActiveIf(target = "authMethod", value = "SharedKey")
    @Documentation("Storage Shared Key")
    private String sharedKey;

    @Option
    // @Required
    @ActiveIf(target = "authMethod", value = "AccessToken")
    @Documentation("Tenant Id")
    private String tenantId;

    @Option
    // @Required
    @ActiveIf(target = "authMethod", value = "AccessToken")
    @Documentation("Client Id")
    private String clientId;

    @Option
    // @Required
    @ActiveIf(target = "authMethod", value = "AccessToken")
    @Documentation("Client Secret")
    private String clientSecret;

    @Option
    // @Required
    @ActiveIf(target = "authMethod", value = "SAS")
    @Documentation("Shared Access Signature")
    private String sas;

    public String apiUrl() {
        return String.format(Constants.DFS_URL, getAccountName());
    }

    public String apiUrlWithSas() {
        String url = String.format(Constants.DFS_URL, getAccountName());
        if (authMethod.equals(AuthMethod.SAS)) {
            url = url + getSas();
        }
        return url;
    }

    public String oauthUrl() {
        return String.format(Constants.TOKEN_URL, getTenantId());
    }

    public enum AuthMethod {
        SharedKey,
        SAS,
    }

}
