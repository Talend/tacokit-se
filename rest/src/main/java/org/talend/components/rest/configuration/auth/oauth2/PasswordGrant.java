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
package org.talend.components.rest.configuration.auth.oauth2;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "accessTokeUrl" }), @GridLayout.Row({ "username" }),
        @GridLayout.Row({ "password" }), @GridLayout.Row({ "clientId" }), @GridLayout.Row({ "clientSecret" }),
        @GridLayout.Row({ "scopes" }), })
public class PasswordGrant implements Serializable {

    @Option
    @Pattern("^(http|https)://")
    @Documentation("")
    private final String accessTokeUrl;

    @Option
    @Documentation("")
    private final String username;

    @Option
    @Credential
    @Documentation("")
    private final String password;

    @Option
    @Documentation("")
    private final String clientId;

    @Option
    @Credential
    @Documentation("")
    private final String clientSecret;

    @Option
    @Documentation("")
    private String scopes;

}
