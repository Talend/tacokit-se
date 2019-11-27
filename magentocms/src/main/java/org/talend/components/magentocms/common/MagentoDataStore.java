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
package org.talend.components.magentocms.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@DataStore(ConfigurationHelper.DATA_STORE_ID)
@Checkable(ConfigurationHelper.DATA_STORE_HEALTH_CHECK)
@GridLayout({ @GridLayout.Row({ "magentoWebServerUrl" }), @GridLayout.Row({ "authenticationType" }),
        @GridLayout.Row({ "authenticationOauth1Configuration" }), @GridLayout.Row({ "authenticationTokenConfiguration" }),
        @GridLayout.Row({ "authenticationLoginPasswordConfiguration" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "magentoRestVersion" }) })
@Documentation("Data store settings. Magento's server connection and authentication preferences")
public class MagentoDataStore implements Serializable {

    @Option
    @Required
    @Documentation("URL of web server (including port after ':'), e.g. 'http://mymagentoserver.com:1234'")
    @Validable(ConfigurationHelper.VALIDATE_WEB_SERVER_URL_ID)
    private String magentoWebServerUrl = "";

    @Option
    @Documentation("The version of Magento REST ,e.g. 'V1'")
    private RestVersion magentoRestVersion = RestVersion.V1;

    @Option
    @Documentation("authentication type (OAuth 1.0, Token, Login etc.)")
    private AuthenticationType authenticationType = AuthenticationType.LOGIN_PASSWORD;

    @Option
    @Documentation("authentication OAuth 1.0 settings")
    @ActiveIf(target = "authenticationType", value = { "OAUTH_1" })
    private AuthenticationOauth1Configuration authenticationOauth1Configuration;

    @Option
    @Documentation("authentication Token configuration")
    @ActiveIf(target = "authenticationType", value = { "AUTHENTICATION_TOKEN" })
    private AuthenticationTokenConfiguration authenticationTokenConfiguration;

    @Option
    @Documentation("authentication Login settings")
    @ActiveIf(target = "authenticationType", value = { "LOGIN_PASSWORD" })
    private AuthenticationLoginPasswordConfiguration authenticationLoginPasswordConfiguration;

    public AuthenticationConfiguration getAuthSettings() throws UnknownAuthenticationTypeException {
        if (authenticationType == AuthenticationType.OAUTH_1) {
            return authenticationOauth1Configuration;
        } else if (authenticationType == AuthenticationType.AUTHENTICATION_TOKEN) {
            return authenticationTokenConfiguration;
        } else if (authenticationType == AuthenticationType.LOGIN_PASSWORD) {
            return authenticationLoginPasswordConfiguration;
        }
        throw new UnknownAuthenticationTypeException();
    }

    public String getMagentoBaseUrl() {
        return "index.php/rest/" + magentoRestVersion;
    }

}