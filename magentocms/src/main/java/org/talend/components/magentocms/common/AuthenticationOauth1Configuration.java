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
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@GridLayout({ @GridLayout.Row({ "authenticationOauth1ConsumerKey" }), @GridLayout.Row({ "authenticationOauth1ConsumerSecret" }),
        @GridLayout.Row({ "authenticationOauth1AccessToken" }), @GridLayout.Row({ "authenticationOauth1AccessTokenSecret" }) })
@Documentation("'OAuth 1.0' authentication settings")
public class AuthenticationOauth1Configuration implements Serializable, AuthenticationConfiguration {

    @Option
    @Documentation("authentication OAuth 1.0 consumer key")
    @Validable(ConfigurationHelper.VALIDATE_AUTH_OAUTH_PARAMETER_ID)
    private String authenticationOauth1ConsumerKey = "";

    @Option
    @Credential
    @Documentation("authentication OAuth 1.0 consumer secret")
    @Validable(ConfigurationHelper.VALIDATE_AUTH_OAUTH_PARAMETER_ID)
    private String authenticationOauth1ConsumerSecret = "";

    @Option
    @Documentation("authentication OAuth 1.0 access token")
    @Validable(ConfigurationHelper.VALIDATE_AUTH_OAUTH_PARAMETER_ID)
    private String authenticationOauth1AccessToken = "";

    @Option
    @Credential
    @Documentation("authentication OAuth 1.0 access token secret")
    @Validable(ConfigurationHelper.VALIDATE_AUTH_OAUTH_PARAMETER_ID)
    private String authenticationOauth1AccessTokenSecret = "";

}
