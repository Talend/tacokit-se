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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
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
@EqualsAndHashCode
@GridLayout({ @GridLayout.Row({ "authenticationLogin" }), @GridLayout.Row({ "authenticationPassword" }) })
@Documentation("'Login' authentication settings")
public class AuthenticationLoginPasswordConfiguration implements Serializable, AuthenticationConfiguration {

    @Option
    @Documentation("authentication login for 'Login' authentication")
    @Validable(ConfigurationHelper.VALIDATE_AUTH_LOGIN_PASSWORD_LOGIN_ID)
    private String authenticationLogin = "";

    @Option
    @Credential
    @Documentation("authentication password for 'Login' authentication")
    private String authenticationPassword = "";

}
