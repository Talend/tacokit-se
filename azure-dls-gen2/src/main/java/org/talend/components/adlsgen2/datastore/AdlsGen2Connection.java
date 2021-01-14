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
package org.talend.components.adlsgen2.datastore;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import static org.talend.components.adlsgen2.service.UIActionService.ACTION_HEALTHCHECK;
import static org.talend.sdk.component.api.configuration.ui.layout.GridLayout.FormType.ADVANCED;

@Data
@DataStore("AdlsGen2Connection")
@Checkable(ACTION_HEALTHCHECK)
@GridLayout({ //
        @GridLayout.Row("authMethod"), //
        @GridLayout.Row("accountName"), //
        @GridLayout.Row("endpointSuffix"), //
        @GridLayout.Row("sharedKey"), //
        @GridLayout.Row("sas"), //
        @GridLayout.Row("tenantId"), //
        @GridLayout.Row("clientId"), //
        @GridLayout.Row("clientSecret"), //
})
@GridLayout(names = ADVANCED, value = { @GridLayout.Row("timeout") })
@Documentation("The datastore to connect Azure Data Lake Storage Gen2")
public class AdlsGen2Connection implements Serializable {

    @Option
    @Required
    @Documentation("Storage Account Name")
    private String accountName;

    @Option
    @Required
    @Documentation("Endpoint suffix")
    private String endpointSuffix = Constants.DFS_DEFAULT_ENDPOINT_SUFFIX;

    @Option
    @Required
    @Documentation("Authentication method")
    private AuthMethod authMethod;

    @Option
    @Credential
    @ActiveIf(target = "authMethod", value = "SharedKey")
    @Documentation("Storage Shared Key")
    private String sharedKey;

    @Option
    @ActiveIf(target = "authMethod", value = "SAS")
    @Documentation("Shared Access Signature")
    private String sas;

    @Option
    @ActiveIf(target = "authMethod", value = "ActiveDirectory")
    @Documentation("ID of Azure active directory (tenant)")
    private String tenantId;

    @Option
    @ActiveIf(target = "authMethod", value = "ActiveDirectory")
    @Documentation("ID of Azure active directory authentication application")
    private String clientId;

    @Option
    @ActiveIf(target = "authMethod", value = "ActiveDirectory")
    @Credential
    @Documentation("Secret key for provided auth application")
    private String clientSecret;

    @Option
    @Min(5)
    @Documentation("Timeout")
    private Integer timeout = 600;

    public String apiUrl() {
        return String.format(Constants.DFS_URL_PATTERN, getAccountName(), getEndpointSuffix());
    }

    public enum AuthMethod {
        SharedKey,
        SAS,
        ActiveDirectory
    }

}
