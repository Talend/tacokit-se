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
        @GridLayout.Row("sharedKey"), //
        @GridLayout.Row("sas"), //
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
    @Min(5)
    @Documentation("Timeout")
    private Integer timeout = 600;

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

    public enum AuthMethod {
        SharedKey,
        SAS,
    }

}
