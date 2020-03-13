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
package org.talend.components.netsuite.datastore;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import org.talend.components.netsuite.service.UIActionService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@DataStore("NetSuiteConnection")
@Checkable(UIActionService.HEALTH_CHECK)
@GridLayouts({ @GridLayout({ @GridLayout.Row({ "apiVersion" }), @GridLayout.Row({ "account" }), @GridLayout.Row({ "loginType" }),
        @GridLayout.Row({ "email" }), @GridLayout.Row({ "password" }), @GridLayout.Row({ "role" }),
        @GridLayout.Row({ "applicationId" }), @GridLayout.Row({ "consumerKey", "consumerSecret" }),
        @GridLayout.Row({ "tokenId", "tokenSecret" }) }) })
@Documentation("Provides all needed properties for establishing connection")
public class NetSuiteDataStore implements Serializable {

    @Option
    @DefaultValue("V2019_2")
    @Documentation("NetSuite API version")
    private ApiVersion apiVersion;

    @Option
    @Required
    @Documentation("NetSuite company account")
    private String account;

    @Option
    @DefaultValue("BASIC")
    @Documentation("Login Type. By default - BASIC, connects using email and password; TBA - token-based authentication, connects using tokens")
    private LoginType loginType;

    @Option
    @ActiveIf(target = "loginType", value = "BASIC")
    @Documentation("User email address")
    private String email;

    @Option
    @ActiveIf(target = "loginType", value = "BASIC")
    @Credential
    @Documentation("User password")
    private String password;

    @Option
    @ActiveIf(target = "loginType", value = "BASIC")
    @Pattern("^[1-9][0-9]*$")
    @Documentation("Role assigned")
    private String role;

    @Option
    @ActiveIf(target = "loginType", value = "BASIC")
    @Documentation("Application ID specified for WebService login")
    private String applicationId;

    @Option
    @ActiveIf(target = "loginType", value = "TBA")
    @Documentation("Consumer Key that is used for Token-Based authentication")
    private String consumerKey;

    @Option
    @ActiveIf(target = "loginType", value = "TBA")
    @Credential
    @Documentation("Consumer Secret that is used for Token-Based authentication")
    private String consumerSecret;

    @Option
    @ActiveIf(target = "loginType", value = "TBA")
    @Documentation("Token Id that is used for Token-Based authentication")
    private String tokenId;

    @Option
    @ActiveIf(target = "loginType", value = "TBA")
    @Credential
    @Documentation("Token Secret that is used for Token-Based authentication")
    private String tokenSecret;

    /**
     * Supported NetSuite API versions.
     */
    @AllArgsConstructor
    public enum ApiVersion {
        V2019_2("2019.2", "https://webservices.netsuite.com/services/NetSuitePort_2019_2");

        @Getter
        private String version;

        @Getter
        private String endpoint;
    }

    /**
     * Supported Login Types.
     */
    public enum LoginType {
        BASIC,
        TBA
    }
}
